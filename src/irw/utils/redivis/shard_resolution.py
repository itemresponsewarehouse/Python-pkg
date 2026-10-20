"""Resolve which core shard holds a table before the shards are searched.

The shard search is newest-first and stops at the first hit, so fetching a
table that lives in `item_response_warehouse` costs one not-found lookup per
newer shard before the right one is reached -- seven shards today, and one
more every time the 1000-table cap is hit. The published `irw_meta` metadata
table already records, per table, the shard it lives in (its `dataset`
column), so that ordering can be known rather than discovered.

Two things keep this from costing more than it saves:

- **Probe first, build on a miss.** The newest shard is still tried before the
  map is read, so the common case -- a recently added table -- is unchanged,
  and a table Redivis has but the metadata does not yet know about (a fetch
  between an upload and the next metadata release) still resolves by search.
  The map is only read once a probe has actually missed.
- **Reorder, never truncate.** Every shard stays in the list. So a table the
  map places wrongly is still found, the not-found path is reached exactly
  when it was before, and the only observable difference is the order.

The deliberate wrong case: a table present in two shards whose metadata is
stale returns the older copy rather than the newer one. That is the same
answer the uploader's `ELSEWHERE` check exists to prevent ever arising, and
shadowed copies are a defect either way.

Ported from `Rpkg/R/shard-resolution.R` (ben-domingue/irw#3127, piece A).
Switched off with `IRW_SHARD_RESOLUTION=0`.
"""

from __future__ import annotations

import logging
import os
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

_OFF_VALUES = {"0", "false", "no", "off"}

# Sentinel distinguishes "not built yet" from "built, and the map is unusable".
_UNSET = object()
_state: Dict[str, Any] = {"map": _UNSET, "map_version": None}


def reset_state() -> None:
    """Forget the cached shard map (tests)."""
    _state["map"] = _UNSET
    _state["map_version"] = None


def resolution_enabled() -> bool:
    """Whether shard resolution is switched on."""
    value = os.environ.get("IRW_SHARD_RESOLUTION", "").strip().lower()
    if not value:
        return True
    return value not in _OFF_VALUES


def _bare_ref(ref: str) -> str:
    """A shard reference reduced to the part the metadata records.

    Three spellings have to meet here. `MAIN_REFS` carries reference ids
    (`item_response_warehouse_3:5xaj`), a pinned handle's `_id` carries a
    version tag (`...@v46.0`), and the metadata's `dataset` column carries
    neither (`item_response_warehouse_3`). Reducing both sides to the bare
    name is also what makes this survive a reference id rotating, which they
    do.
    """
    return ref.lower().split("@", 1)[0].split(":", 1)[0].strip()


def _shard_key(ds: Any) -> str:
    """The shard's bare Redivis reference.

    Deliberately not `ds.name`: that is the *display* name once the handle has
    been `get()` ("Item Response Warehouse 7"), which matches nothing in the
    metadata. The R port lost a day to that.
    """
    return _bare_ref(getattr(ds, "_id", None) or "")


def shard_map(build: bool = True) -> Optional[Dict[str, str]]:
    """Map lowercased table name -> core shard reference, or None.

    Returns None when the map is unavailable for any reason: resolution
    switched off, the metadata table unreadable, no `dataset` column. None
    means "order the shards as before", never an error -- a fetch must not
    start failing because a metadata read did.

    With ``build=False`` the map is returned only if it is already in hand,
    so a caller can resolve for free before deciding to pay for a probe.
    """
    if not resolution_enabled():
        return None

    # Lazy: table_metadata imports datasets, and fetch imports both. Keeping
    # the import inside the call keeps this module free of that ordering.
    from .table_metadata import _get_meta_dataset, _meta_cache_key
    from .datasets import _cache_version, _dataset_version_tag
    from .cache import metadata_cache

    key = _meta_cache_key("metadata", "main")

    # The free path makes no request of its own -- not even the one that reads
    # the metadata version. It costs two, and paying them on a fetch whose
    # very next lookup is going to hit would make resolution a net loss on the
    # commonest fetch there is. A map built in this process is reused, and
    # otherwise the whole metadata frame is taken if some earlier filter() or
    # info() already paid for it, version check and all skipped: an out-of-date
    # frame can only put the shards in a worse order, never shorten the list,
    # and the probe before it is what catches that.
    if not build:
        if _state["map"] is not _UNSET:
            return _state["map"]
        cached = metadata_cache.get(key)
        return _map_from_frame(cached) if cached is not None else None

    try:
        version = _cache_version(_dataset_version_tag(_get_meta_dataset()))
    except Exception as e:  # pragma: no cover - a metadata blip, not a fetch error
        logger.debug("Shard resolution: could not resolve the metadata version (%s)", e)
        return None

    if _state["map"] is not _UNSET and _state["map_version"] == version:
        return _state["map"]

    frame = None
    try:
        cached = metadata_cache.get(key, version)
        if cached is not None:
            frame = cached
        else:
            frame = _get_meta_dataset().table("metadata").to_pandas_dataframe(
                variables=["table", "dataset"], progress=False
            )
    except Exception as e:
        logger.debug("Shard resolution: could not read the metadata map (%s)", e)
        frame = None

    if frame is None:
        # Remember the failure for this metadata version: a map that cannot be
        # built must not be retried on every fetch.
        _state["map"] = None
        _state["map_version"] = version
        return None

    mapping = _map_from_frame(frame)
    _state["map"] = mapping
    _state["map_version"] = version
    return mapping


def _clean(value: Any) -> Optional[str]:
    """A usable lowercased cell value, or None.

    A missing cell arrives as None in some reads and as a float NaN in others,
    and `str(nan)` is the perfectly usable-looking string "nan" -- which is how
    a column of missing shard names would become a map that confidently points
    every table at a dataset called "nan". Both are rejected here, along with
    the empty string. `value != value` is the NaN test that needs no import.
    """
    if value is None or value != value:
        return None
    text = str(value).strip().lower()
    if not text or text == "nan":
        return None
    return text


def _shard_ordinal(ref: str) -> int:
    """Position of a shard in its family: `..._7` is 7, an unsuffixed one is 1.

    Only used to break a tie, below. Shard references are named by the
    uploader's convention (`item_response_warehouse`, `..._2`, ... `..._7`),
    and an unparseable name sorts first so a rename can only cost a tie-break,
    never a wrong answer.
    """
    tail = ref.rsplit("_", 1)[-1]
    return int(tail) if tail.isdigit() else 1


def _map_from_frame(frame: Any) -> Optional[Dict[str, str]]:
    """Build the name -> shard mapping from a metadata frame.

    A name listed against two shards resolves to the newer one, matching what
    the shard search itself does (newest-first, first hit wins) -- so a
    shadowed table reads the same way with resolution on as off.
    """
    try:
        if "table" not in frame.columns or "dataset" not in frame.columns:
            return None
        mapping: Dict[str, str] = {}
        for table, dataset in zip(frame["table"], frame["dataset"]):
            name = _clean(table)
            shard = _clean(dataset)
            shard = _bare_ref(shard) if shard else shard
            if name is None or shard is None:
                continue
            held = mapping.get(name)
            if held is not None and _shard_ordinal(held) >= _shard_ordinal(shard):
                continue
            mapping[name] = shard
        return mapping or None
    except Exception as e:  # pragma: no cover - a shape surprise, not a fetch error
        logger.debug("Shard resolution: unusable metadata frame (%s)", e)
        return None


def _is_core_list(ds_list: List[Any]) -> bool:
    """Whether these handles are the core response warehouses.

    The `dataset` column only names core shards, so the map can say nothing
    about the conjoint, rankings or item-text lists. They are told apart by
    their references rather than by a `source` argument, because `fetch()`
    is handed a dataset list and never learns which source it came from --
    and a check on the handles cannot drift from the list that was passed.
    """
    from ...config import MAIN_REFS
    from .shard_discovery import effective_refs

    try:
        core = {_bare_ref(ref) for _user, ref in effective_refs(MAIN_REFS, "warehouse")}
    except Exception:  # pragma: no cover - discovery failure, not a fetch error
        core = {_bare_ref(ref) for _user, ref in MAIN_REFS}
    return any(_shard_key(ds) in core for ds in ds_list)


def shard_first(
    ds_list: List[Any],
    name: str,
    build: bool = False,
) -> List[Any]:
    """Return `ds_list` with the shard the metadata names for `name` first.

    Returns the list unchanged whenever resolution cannot help: a dataset
    list that is not the core warehouses, a name the map does not carry, a
    shard not in this session's list. Nothing is ever dropped, so every
    caller's not-found path is reached exactly when it was before.
    """
    if not isinstance(name, str) or not name:
        return ds_list
    if len(ds_list) < 2 or not resolution_enabled():
        return ds_list
    if not _is_core_list(ds_list):
        return ds_list

    mapping = shard_map(build=build)
    if not mapping:
        return ds_list

    wanted = mapping.get(name.strip().lower())
    if not wanted:
        return ds_list

    for i, ds in enumerate(ds_list):
        if _shard_key(ds) == wanted:
            if i == 0:
                return ds_list
            return [ds_list[i]] + ds_list[:i] + ds_list[i + 1:]
    return ds_list


def shard_first_after_miss(ds_list: List[Any], name: str) -> List[Any]:
    """`shard_first`, paying to build the map: the probe has already missed.

    This is the other half of probe-first. The newest shard has been tried
    and did not have the table, so reading the map now is cheaper than the
    not-found lookups it replaces. Written to be passed straight to
    `_search_datasets(..., on_miss=...)`.
    """
    return shard_first(ds_list, name, build=True)
