"""Utilities for IRW item text availability on Redivis (internal)."""

from typing import Any, Dict, List, Optional, Set
import warnings

from ...config import ITEMTEXT_REFS
from .shard_discovery import effective_refs
from .cache import metadata_cache
from .datasets import (
    _cache_version,
    _dataset_table_list,
    _datasets_version_tag,
    _init_datasets_from_refs,
)
from .pins import _pins_fingerprint


def _itemtext_datasets_cache_key() -> str:
    """Cache key that changes when ITEMTEXT_REFS is updated (e.g. new shard).

    Mirrors `_main_datasets_cache_key()`: the refs are *in* the key, so a config
    change is a cache miss by construction rather than something to remember to
    invalidate. The session's version pins are in the key for the same reason.
    """
    refs = effective_refs(ITEMTEXT_REFS, "item text shard")
    return ("itemtext_datasets:" + "|".join(f"{u}/{r}" for u, r in refs)
            + _pins_fingerprint(refs))


def _get_itemtext_datasets() -> List[Any]:
    """Return the item text shards, newest first (cached).

    Redivis caps a dataset at 1000 tables, so item text is a shard list. The
    order matters: newest-first means a table present in more than one shard
    resolves to its most recent copy, matching the R package and the core
    warehouses. `skip_unavailable` keeps a freshly created shard with no
    released version from taking down item text for every read-only user.
    """
    cache_key = _itemtext_datasets_cache_key()
    cached = metadata_cache.get(cache_key)
    if cached is not None:
        return cached

    datasets = list(reversed(
        _init_datasets_from_refs(
            effective_refs(ITEMTEXT_REFS, "item text shard"), skip_unavailable=True
        )
    ))
    metadata_cache.set(cache_key, datasets)
    return datasets


def _get_itemtext_table(base_name: str) -> Optional[Any]:
    """Find `{base_name}__items` in the newest shard that has it.

    Searching rather than asking one remembered dataset is the point: "listing
    found it in shard 2, fetch asked shard 1" is exactly the shadowing bug
    sharding would otherwise introduce.

    `base_name` is resolved against the stored names first, so a caller may pass
    the response table's own casing -- see `_itemtext_name_index`.
    """
    from .tables import _get_table, _search_datasets

    stored = _itemtext_name_index().get(base_name.lower(), base_name)
    result, _last_other, invalid = _search_datasets(
        _get_itemtext_datasets(),
        lambda ds: _get_table(ds, f"{stored}__items"),
    )
    if invalid is not None:
        raise invalid
    return result


def _itemtext_name_index() -> Dict[str, str]:
    """Map lowercased base name -> the name actually stored on Redivis.

    Item text tables are lower-cased on upload while many IRW response tables
    are mixed case, so `HEARD_Roch_2022_K6` is stored as
    `heard_roch_2022_k6__items`.

    This is hardening, not a bug fix: Redivis' own table lookup turns out to be
    case-insensitive, so `ds.table("HEARD_Roch_2022_K6__items")` resolves today
    (verified 2026-09-05 -- both spellings return the same 30-row table). That
    behaviour is undocumented and not ours to rely on. Resolving explicitly
    matches what the R client already does (`.resolve_itemtext_table_name`) and
    makes the resolution visible where it can be tested.

    Populated newest-shard-first, so a name present in more than one shard --
    including two that differ only in case -- resolves to its most recent copy.

    Keyed on the shards' current version tags, not just on ITEMTEXT_REFS. The
    refs alone make a *config* change a cache miss; they say nothing about a
    *release*, and a release is what withdraws item text. Without the version a
    long-running process kept resolving and serving wording that a rights
    ruling had removed (issue #51).
    """
    datasets = _get_itemtext_datasets()
    index_key = _itemtext_datasets_cache_key() + ":index"
    version = _cache_version(_datasets_version_tag(datasets))
    cached = metadata_cache.get(index_key, version)
    if cached is not None:
        return cached

    index: Dict[str, str] = {}
    for ds in datasets:
        for t in _dataset_table_list(ds):
            name = getattr(t, "name", "") or ""
            if name.endswith("__items"):
                base = name[: -len("__items")]
                index.setdefault(base.lower(), base)

    metadata_cache.set(index_key, index, version)
    return index


def _list_itemtext_tables() -> Set[str]:
    """Return set of base table names that have item text available.

    The union across every shard, lowercased for case-insensitive matching.
    Built from the same listing pass as `_itemtext_name_index`.
    """
    return set(_itemtext_name_index())



_DISCLAIMER = (
    "Note: IRW item text is reconstructed from published sources using a largely\n"
    "automated pipeline and is provided for research purposes only. We make no\n"
    "guarantee as to its accuracy, completeness, or alignment with the `item`\n"
    "identifiers in the response data; verify against the original source.\n"
    "Inclusion here implies no license to reuse an instrument; copyright remains\n"
    "with the original rights holders.\n"
    "See https://itemresponsewarehouse.org/itemtext_issues.html\n"
    "(silence with irw.utils.redivis.item_text.disable_itemtext_disclaimer())"
)

_disclaimer_state = {"shown": False, "enabled": True}


def disable_itemtext_disclaimer() -> None:
    """Suppress the once-per-session item text disclaimer."""
    _disclaimer_state["enabled"] = False


def _itemtext_disclaimer() -> None:
    """Emit the item text disclaimer once per session."""
    if _disclaimer_state["shown"] or not _disclaimer_state["enabled"]:
        return
    _disclaimer_state["shown"] = True
    warnings.warn(_DISCLAIMER, UserWarning, stacklevel=3)


# --- licence on the wording itself -------------------------------------------
#
# Most item text carries no licence of its own (see _DISCLAIMER). A minority of
# tables republish wording their source released under an open licence
# (CC BY-SA 4.0, CC BY-NC 4.0, ...), and that licence has to travel with the
# data, not only sit on the Redivis page. The record is the IRW repository's
# itemtext/itemtext_licences.csv, fetched once per session.

_ITEMTEXT_LICENCES_URL = (
    "https://raw.githubusercontent.com/ben-domingue/irw/main/"
    "itemtext/itemtext_licences.csv"
)
_LICENCE_COLUMNS = ("table", "licence", "licence_url", "attribution", "changes", "basis")
_licence_state: Dict[str, Any] = {"table": None, "failed": False, "noticed": set()}


def _itemtext_licences() -> Optional[Dict[str, Dict[str, str]]]:
    """The wording-licence record, keyed by lowercase table name.

    None when it could not be fetched (warned once per session): a missing
    record must not read as "no licence", so callers get None, not {}.
    """
    if _licence_state["table"] is not None:
        return _licence_state["table"]
    if _licence_state["failed"]:
        return None
    try:
        import csv
        import io
        import requests  # a dependency of redivis
        response = requests.get(_ITEMTEXT_LICENCES_URL, timeout=(5, 15))
        response.raise_for_status()
        rows = list(csv.DictReader(io.StringIO(response.content.decode("utf-8-sig"))))
        if not rows or any(c not in rows[0] for c in _LICENCE_COLUMNS):
            raise ValueError("unexpected columns in itemtext_licences.csv")
    except Exception as error:
        _licence_state["failed"] = True
        warnings.warn(
            "Could not fetch the item-text licence record from "
            f"{_ITEMTEXT_LICENCES_URL} ({type(error).__name__}); wording licences "
            "will not be reported this session.",
            UserWarning, stacklevel=3,
        )
        return None
    table = {r["table"].lower(): {c: r[c] for c in _LICENCE_COLUMNS} for r in rows}
    _licence_state["table"] = table
    return table


def itemtext_licence(table_name: str) -> Optional[Dict[str, str]]:
    """Return the licence record for a table's item wording, or None.

    None means either that the wording carries no licence of its own (the
    usual case: inclusion implies no licence to reuse the instrument) or that
    the record could not be fetched, which is warned about separately.
    """
    table = _itemtext_licences()
    if not table:
        return None
    return table.get(table_name.lower())


def _itemtext_licence_notice(table_name: str) -> Optional[Dict[str, str]]:
    """Warn once per table with its wording licence, if it has one."""
    hit = itemtext_licence(table_name)
    if hit is None:
        return None
    if table_name.lower() not in _licence_state["noticed"]:
        _licence_state["noticed"].add(table_name.lower())
        warnings.warn(
            f"Item wording licence for {hit['table']}: {hit['licence']} ({hit['licence_url']})\n"
            f"  Source: {hit['attribution']}\n"
            f"  Changes: {hit['changes']}\n"
            "  This licence covers the item text as published here; it implies no "
            "licence to reuse the original source instrument.",
            UserWarning, stacklevel=3,
        )
    return hit
