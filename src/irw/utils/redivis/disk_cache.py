"""On-disk cache for downloaded IRW tables, shared with the R package.

Every table download counts against the Redivis rolling 30-day export cap, and
until this module nothing outlived the interpreter: a restarted session exported
again what the last one already had. The cache keeps each downloaded table as a
Parquet file that the R package (`Rpkg/R/disk-cache.R`) reads and writes too, so
a table fetched in either language is not exported again until it changes.

The format is a contract between the two packages -- change it in both, or bump
FORMAT to start a new tree. Spec: ben-domingue/irw#2253.

    <root>/v1/<kind>/<table>/<hash>.parquet

- ``kind`` is ``tables`` or ``itemtext``.
- ``table`` is the name Redivis reports for the table, not what the caller typed.
- ``hash`` is the table's Redivis content hash (``properties["hash"]``). It is
  keyed on the table, not on the dataset's version tag: a release re-tags a
  whole shard, and keying on the tag would re-export every unchanged table in it.
  The hash stays put across releases for an unchanged table and moves when the
  table is rebuilt, even at the same row count (checked 2026-09-19: the three
  pisa2015 tables across v52.1 -> v53.0).

The file holds the raw download, before any of fetch()'s own transforms, and
carries its provenance in the Parquet schema metadata (``irw_*`` keys).

The cache is checked after the shard search has found the table and before the
export, so validity is the hash the search just read, and a hit costs only the
metadata requests every fetch already makes.
"""

from __future__ import annotations

import datetime
import logging
import os
import re
import shutil
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Union

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

logger = logging.getLogger(__name__)

FORMAT = "v1"
KINDS = ("tables", "itemtext")

_META_PREFIX = "irw_"
_OFF_VALUES = {"0", "false", "no", "off"}

_state: Dict[str, Any] = {"enabled": None, "announced": False}


# --- configuration ----------------------------------------------------------


def cache_root() -> Path:
    """The cache folder: ``IRW_CACHE_DIR``, else the platform's user cache dir.

    Computed by hand rather than with platformdirs or R's `tools::R_user_dir`,
    because the two packages must land on the same folder and those two do not
    (R's adds an ``R/`` level).
    """
    override = os.environ.get("IRW_CACHE_DIR")
    if override:
        return Path(override).expanduser()
    if sys.platform == "win32":
        base = os.environ.get("LOCALAPPDATA") or str(Path.home() / "AppData" / "Local")
        return Path(base) / "irw" / "Cache"
    if sys.platform == "darwin":
        return Path.home() / "Library" / "Caches" / "irw"
    base = os.environ.get("XDG_CACHE_HOME") or str(Path.home() / ".cache")
    return Path(base) / "irw"


def enabled() -> bool:
    """Whether fetches read and write the cache. On unless switched off.

    `set_cache()` wins for the session; otherwise ``IRW_CACHE=0`` (or
    false/no/off) switches it off.
    """
    if _state["enabled"] is not None:
        return bool(_state["enabled"])
    return os.environ.get("IRW_CACHE", "").strip().lower() not in _OFF_VALUES


def set_enabled(value: Optional[bool]) -> None:
    """Set the session override; None returns to the ``IRW_CACHE`` setting."""
    _state["enabled"] = None if value is None else bool(value)


# --- paths ------------------------------------------------------------------


_UNSAFE = re.compile(r"[^A-Za-z0-9._-]")


def _safe(part: str) -> str:
    # IRW names are already filename-safe; this is a guard, not a mapping the
    # R side has to reproduce for any real table.
    return _UNSAFE.sub("_", part)


def entry_path(kind: str, table: str, content_hash: str) -> Path:
    if kind not in KINDS:
        raise ValueError(f"kind must be one of {KINDS}; got {kind!r}")
    return cache_root() / FORMAT / kind / _safe(table) / f"{_safe(content_hash)}.parquet"


def table_identity(tbl: Any) -> Optional[Dict[str, str]]:
    """Name, hash and address of a loaded Redivis table handle, or None.

    None -- no hash, or a handle that was never `get()`-ed -- means the table
    cannot be cached, and the fetch goes to Redivis as it always has.
    """
    props = getattr(tbl, "properties", None) or {}
    content_hash = props.get("hash")
    name = props.get("name") or getattr(tbl, "name", None)
    if not content_hash or not name:
        return None
    dataset = getattr(tbl, "dataset", None)
    return {
        "table": str(name),
        "hash": str(content_hash),
        "reference": str(props.get("qualifiedReference") or getattr(tbl, "qualified_reference", "") or ""),
        "pinned": "true" if getattr(dataset, "_irw_pin", None) is not None else "false",
    }


class Entry:
    """Where one table lives in the cache, for a fetch that has found it."""

    def __init__(self, path: Path, identity: Dict[str, str], kind: str):
        self.path = path
        self.identity = identity
        self.kind = kind

    def read(self) -> Optional[pd.DataFrame]:
        return read(self.path)

    def write(self, table: pa.Table) -> None:
        write(self.path, table, self.identity, self.kind)


def lookup(tbl: Any, kind: str) -> Optional[Entry]:
    """The cache entry for a loaded table handle, or None if it cannot be cached."""
    if not enabled():
        return None
    identity = table_identity(tbl)
    if identity is None:
        return None
    return Entry(entry_path(kind, identity["table"], identity["hash"]), identity, kind)


# --- read / write -----------------------------------------------------------


def read(path: Path) -> Optional[pd.DataFrame]:
    """The cached frame at `path`, or None on a miss.

    Columns come back as ArrowDtype, which is what redivis'
    `to_pandas_dataframe` returns, so a hit and a miss hand fetch() the same
    frame. An unreadable file is deleted and reported as a miss: a truncated
    write should cost one re-export, not a failed fetch.
    """
    if not path.is_file():
        return None
    try:
        table = pq.read_table(path)
    except Exception as e:
        logger.warning("Discarding unreadable IRW cache file %s (%s); fetching again.", path, e)
        _unlink(path)
        return None
    return to_frame(table)


def to_frame(table: pa.Table) -> pd.DataFrame:
    """Arrow -> pandas exactly as redivis' `to_pandas_dataframe` does it.

    redivis 0.20.x converts with ``types_mapper=pd.ArrowDtype`` (its default
    ``dtype_backend="pyarrow"``). Doing the same on a hit and on a miss is what
    makes the two indistinguishable to fetch().
    """
    return table.to_pandas(types_mapper=pd.ArrowDtype)


def write(path: Path, table: pa.Table, identity: Dict[str, str], kind: str) -> None:
    """Store the Arrow `table` at `path`, then drop older copies of it.

    Stored as Redivis sent it, so the column types in the file are Redivis'
    and not a pandas round trip -- that is what R reads.

    Never raises: a cache that cannot be written leaves the fetch exactly as it
    was without one. The file is written beside its final name and renamed
    into place, so a reader never sees half a file and two processes writing
    the same table leave one whole copy.
    """
    tmp = path.with_name(f"{path.name}.tmp-{os.getpid()}")
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        meta = dict(table.schema.metadata or {})
        meta.update({
            f"{_META_PREFIX}format".encode(): FORMAT.encode(),
            f"{_META_PREFIX}kind".encode(): kind.encode(),
            f"{_META_PREFIX}table".encode(): identity["table"].encode(),
            f"{_META_PREFIX}hash".encode(): identity["hash"].encode(),
            f"{_META_PREFIX}reference".encode(): identity["reference"].encode(),
            f"{_META_PREFIX}fetched_at".encode(): datetime.datetime.now(
                datetime.timezone.utc).isoformat(timespec="seconds").encode(),
            f"{_META_PREFIX}writer".encode(): b"python",
        })
        pq.write_table(table.replace_schema_metadata(meta), tmp)
        os.replace(tmp, path)
    except Exception as e:
        _unlink(tmp)
        logger.warning("Could not write IRW cache file %s: %s", path, e)
        return

    _announce_once()
    # A pinned fetch keeps what is there: the live copy of the same table is
    # still the one an unpinned fetch will want.
    if identity.get("pinned") != "true":
        _sweep(path)


def _sweep(keep: Path) -> None:
    """Remove other copies of the table `keep` holds.

    Matched on the table name stored inside each file, not on the folder name,
    so two tables differing only in case cannot sweep each other on a
    case-insensitive filesystem.
    """
    table = _file_meta(keep).get("table")
    for other in keep.parent.glob("*.parquet"):
        if other == keep:
            continue
        if table is None or _file_meta(other).get("table") in (table, None):
            _unlink(other)


def _announce_once() -> None:
    if _state["announced"]:
        return
    _state["announced"] = True
    print(
        f"irw: saved a copy of this table in {cache_root()}, so it is not exported "
        "again while it is unchanged. See irw.cache_info(); switch off with "
        "irw.set_cache(False) or IRW_CACHE=0."
    )


def _unlink(path: Path) -> None:
    try:
        path.unlink()
    except FileNotFoundError:
        pass
    except Exception as e:
        logger.debug("Could not remove %s: %s", path, e)


# --- inspection -------------------------------------------------------------


def _file_meta(path: Path) -> Dict[str, str]:
    try:
        raw = pq.read_schema(path).metadata or {}
    except Exception:
        return {}
    return {
        k.decode()[len(_META_PREFIX):]: v.decode()
        for k, v in raw.items()
        if k.decode().startswith(_META_PREFIX)
    }


def _entries() -> List[Path]:
    base = cache_root() / FORMAT
    if not base.is_dir():
        return []
    return sorted(p for kind in KINDS for p in (base / kind).glob("*/*.parquet"))


_INFO_COLUMNS = ["kind", "table", "hash", "version", "reference", "size_mb", "fetched_at", "path"]


def info() -> pd.DataFrame:
    """One row per cached file."""
    rows = []
    for p in _entries():
        meta = _file_meta(p)
        ref = meta.get("reference", "")
        version = re.search(r":(v\d+_\d+)\.", ref)
        rows.append({
            "kind": p.parent.parent.name,
            "table": meta.get("table", p.parent.name),
            "hash": meta.get("hash", p.stem),
            "version": version.group(1).replace("_", ".") if version else None,
            "reference": ref or None,
            "size_mb": round(p.stat().st_size / 1e6, 3),
            "fetched_at": meta.get("fetched_at"),
            "path": str(p),
        })
    return pd.DataFrame(rows, columns=_INFO_COLUMNS)


def clear(table: Union[str, Iterable[str], None] = None) -> int:
    """Delete everything, or the named tables' files. Returns bytes freed.

    Names match case-insensitively, and ``name`` also clears ``name__items``,
    so one call clears a table and its item text.
    """
    if table is None:
        wanted = None
    else:
        names = [table] if isinstance(table, str) else list(table)
        wanted = {n.lower() for n in names} | {f"{n.lower()}__items" for n in names}

    freed = 0
    for p in _entries():
        name = _file_meta(p).get("table", p.parent.name)
        if wanted is not None and name.lower() not in wanted:
            continue
        freed += p.stat().st_size
        _unlink(p)
    if wanted is None:
        # Leftovers of writes that died before their rename.
        base = cache_root() / FORMAT
        for p in (base.glob("*/*/*.tmp-*") if base.is_dir() else []):
            freed += p.stat().st_size
            _unlink(p)
    _prune_empty()
    return freed


def _prune_empty() -> None:
    base = cache_root() / FORMAT
    for kind in KINDS:
        root = base / kind
        if not root.is_dir():
            continue
        for d in root.iterdir():
            if d.is_dir() and not any(d.iterdir()):
                shutil.rmtree(d, ignore_errors=True)
