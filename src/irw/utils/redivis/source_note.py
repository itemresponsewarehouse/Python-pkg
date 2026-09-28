"""Credit note for tables IRW took from an intermediary such as openESM.

A table the IRW found through another collection carries that collection's key
in the biblio column ``Source_via`` (ben-domingue/irw#2421). When such a table
is fetched, this prints the collection's own note and citation, once per
session per collection -- not once per table, so fetching thirty openESM
tables prints it once.

A credit line must never break or slow a download meaningfully, so:

* the lookup is one small server-side query, not a download of the whole
  biblio table, and its answer is kept for a week in the IRW cache folder as
  ``source_via.csv`` -- a fresh session otherwise pays ~4 s opening the
  metadata dataset. The R package reads and writes the same file;
* the query is not retried, and any failure -- no network, no ``Source_via``
  column in an older release, anything -- means no note, silently. A failure
  is cached too, so it costs one session a week, not every session;
* only the ``main`` source is looked up (every such table is there today).

Silence it with ``irw.disable_source_note()`` or ``IRW_SOURCE_NOTE=0``.
"""
import csv
import os
import time
import warnings
from typing import Dict, Iterable, Optional

# The package's copy of metadata/aggregators.csv in ben-domingue/irw, which is
# the source of truth: keep the two in step. A key biblio names but this does
# not know still gets the one-line note, without a citation.
AGGREGATORS: Dict[str, Dict[str, str]] = {
    "openESM": {
        "note": ("These data were found via openESM, where additional "
                 "metadata for this dataset are available."),
        "citation": (
            "Siepe, B. S., Haslbeck, J. M. B., Kloft, M., Büchner, A., "
            "Zhang, Y., Fried, E. I., & Heck, D. W. (2026). Introducing "
            "openESM: A database of openly available experience sampling "
            "datasets. Behavior Research Methods, 58(8), 240. "
            "https://doi.org/10.3758/s13428-026-03112-y"
        ),
    },
}

_state = {"enabled": True, "shown": set(), "lookup": None}

CACHE_FILE = "source_via.csv"
CACHE_TTL_SECONDS = 7 * 24 * 3600


def disable_source_note() -> None:
    """Suppress the once-per-session note on tables found via openESM and similar."""
    _state["enabled"] = False


def _enabled() -> bool:
    env = os.environ.get("IRW_SOURCE_NOTE", "").strip().lower()
    return _state["enabled"] and env not in ("0", "false", "no", "off")


def _cache_path():
    from .disk_cache import cache_root
    return cache_root() / CACHE_FILE


def _read_cached() -> Optional[Dict[str, Dict[str, Optional[str]]]]:
    """The cached lookup if the file is younger than a week, else None."""
    try:
        path = _cache_path()
        if time.time() - path.stat().st_mtime > CACHE_TTL_SECONDS:
            return None
        with open(path, newline="", encoding="utf-8") as f:
            return {r["table"].lower(): {"via": r["via"], "url": r["url"] or None}
                    for r in csv.DictReader(f)}
    except Exception:
        return None


def _write_cached(lookup: Dict[str, Dict[str, Optional[str]]]) -> None:
    try:
        path = _cache_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_name(f"{path.name}.tmp-{os.getpid()}")
        with open(tmp, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["table", "via", "url"])
            for table, hit in sorted(lookup.items()):
                w.writerow([table, hit["via"], hit["url"] or ""])
        os.replace(tmp, path)
    except Exception:
        pass


def _load_lookup() -> Dict[str, Dict[str, Optional[str]]]:
    """lowercased table -> {"via": key, "url": URL__for_data_}; {} on any failure."""
    cached = _read_cached()
    if cached is not None:
        return cached
    lookup = _query_lookup()
    _write_cached(lookup)
    return lookup


def _query_lookup() -> Dict[str, Dict[str, Optional[str]]]:
    try:
        import redivis
        from .table_metadata import _get_meta_dataset
        from ...config import SOURCE_META_TABLES

        table = _get_meta_dataset().table(SOURCE_META_TABLES["main"]["biblio"])
        sql = (f"SELECT `table`, Source_via, URL__for_data_ "
               f"FROM `{table.qualified_reference}` "
               f"WHERE Source_via IS NOT NULL AND TRIM(Source_via) != ''")
        df = redivis.query(sql).to_pandas_dataframe(progress=False)
        return {
            str(r["table"]).lower(): {
                "via": str(r["Source_via"]).strip(),
                "url": None if r["URL__for_data_"] is None
                or str(r["URL__for_data_"]).strip() in ("", "NA", "nan")
                else str(r["URL__for_data_"]).strip(),
            }
            for _, r in df.iterrows()
        }
    except Exception:
        return {}


def _message(table: str, via: str, url: Optional[str]) -> str:
    entry = AGGREGATORS.get(via)
    if entry is None:
        lines = [f"Note: '{table}' was found via {via}."]
    else:
        lines = [f"Note: '{table}': {entry['note']}"]
    if url:
        lines.append(f"See {url}")
    if entry is not None:
        lines.append(f"Please also cite: {entry['citation']}")
    lines.append("(shown once per session per source; silence with "
                 "irw.disable_source_note() or IRW_SOURCE_NOTE=0)")
    return "\n".join(lines)


def _source_note(tables: Iterable[str], source: str = "main") -> None:
    """Emit the credit note for any fetched table that came via an intermediary."""
    try:
        if source != "main" or not _enabled():
            return
        if _state["lookup"] is None:
            _state["lookup"] = _load_lookup()
        for table in tables:
            hit = _state["lookup"].get(str(table).lower())
            if hit is None or hit["via"] in _state["shown"]:
                continue
            _state["shown"].add(hit["via"])
            warnings.warn(_message(table, hit["via"], hit["url"]),
                          UserWarning, stacklevel=3)
    except Exception:
        return
