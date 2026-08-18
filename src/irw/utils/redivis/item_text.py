"""Utilities for IRW item text availability on Redivis (internal)."""

from typing import Set
import warnings

import redivis

from ...config import ITEMTEXT_REF
from .cache import metadata_cache


def _get_itemtext_dataset():
    """Return the Redivis dataset object for IRW item text metadata (cached)."""
    cached = metadata_cache.get("itemtext_dataset")
    if cached is not None:
        return cached

    dataset = redivis.user(ITEMTEXT_REF[0]).dataset(ITEMTEXT_REF[1])
    dataset.get()
    metadata_cache.set("itemtext_dataset", dataset)
    return dataset


def _list_itemtext_tables() -> Set[str]:
    """Return set of base table names that have item text available.

    Item text tables are named as "{base}__items"; we strip the suffix and
    return the base names in lowercase for case-insensitive matching.
    """
    cached = metadata_cache.get("itemtext_tables")
    if cached is not None:
        return cached

    ds = _get_itemtext_dataset()
    
    # Use cached table list if available
    ds_id = getattr(ds, "_id", None) or getattr(ds, "name", None)
    cache_key = f"dataset_tables:{ds_id}" if ds_id else None
    
    cached_table_list = None
    if cache_key:
        cached_table_list = metadata_cache.get(cache_key)
    
    if cached_table_list is None:
        tables = ds.list_tables()
        # Cache the table list
        if cache_key:
            metadata_cache.set(cache_key, list(tables))
    else:
        tables = cached_table_list
    
    available: Set[str] = set()
    for t in tables:
        name = getattr(t, "name", "") or ""
        if name.endswith("__items"):
            base = name[: -len("__items")]
            available.add(base.lower())

    metadata_cache.set("itemtext_tables", available)
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
