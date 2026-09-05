"""Utilities for IRW item text availability on Redivis (internal)."""

from typing import Any, Dict, List, Optional, Set
import warnings

from ...config import ITEMTEXT_REFS
from .cache import metadata_cache
from .datasets import _init_datasets_from_refs


def _itemtext_datasets_cache_key() -> str:
    """Cache key that changes when ITEMTEXT_REFS is updated (e.g. new shard).

    Mirrors `_main_datasets_cache_key()`: the refs are *in* the key, so a config
    change is a cache miss by construction rather than something to remember to
    invalidate.
    """
    return "itemtext_datasets:" + "|".join(f"{u}/{r}" for u, r in ITEMTEXT_REFS)


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
        _init_datasets_from_refs(ITEMTEXT_REFS, skip_unavailable=True)
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
    """
    index_key = _itemtext_datasets_cache_key() + ":index"
    cached = metadata_cache.get(index_key)
    if cached is not None:
        return cached

    index: Dict[str, str] = {}
    for ds in _get_itemtext_datasets():
        # Use cached table list if available. `_init_dataset` sets `_id`, so
        # this key is per-shard and stays correct as shards are added.
        ds_id = getattr(ds, "_id", None) or getattr(ds, "name", None)
        cache_key = f"dataset_tables:{ds_id}" if ds_id else None

        cached_table_list = None
        if cache_key:
            cached_table_list = metadata_cache.get(cache_key)

        if cached_table_list is None:
            tables = ds.list_tables()
            if cache_key:
                metadata_cache.set(cache_key, list(tables))
        else:
            tables = cached_table_list

        for t in tables:
            name = getattr(t, "name", "") or ""
            if name.endswith("__items"):
                base = name[: -len("__items")]
                index.setdefault(base.lower(), base)

    metadata_cache.set(index_key, index)
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
