"""Utilities for IRW item text availability on Redivis (internal)."""

from typing import Any, List, Optional, Set
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
    """
    from .tables import _get_table, _search_datasets

    result, _last_other, invalid = _search_datasets(
        _get_itemtext_datasets(),
        lambda ds: _get_table(ds, f"{base_name}__items"),
    )
    if invalid is not None:
        raise invalid
    return result


def _list_itemtext_tables() -> Set[str]:
    """Return set of base table names that have item text available.

    The union across every shard. Item text tables are named as
    "{base}__items"; we strip the suffix and return the base names in lowercase
    for case-insensitive matching.
    """
    names_key = _itemtext_datasets_cache_key() + ":names"
    cached = metadata_cache.get(names_key)
    if cached is not None:
        return cached

    available: Set[str] = set()
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
                available.add(name[: -len("__items")].lower())

    metadata_cache.set(names_key, available)
    return available



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
