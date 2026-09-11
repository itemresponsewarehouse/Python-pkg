"""Table-level metadata utilities for main IRW dataset."""

import pandas as pd
from typing import Any
from ...config import META_REF, META_TABLES
from .cache import metadata_cache
from .datasets import (
    _cache_version,
    _dataset_table_list,
    _dataset_version_tag,
    _datasets_version_tag,
    _init_dataset,
    _init_main_datasets,
    _main_datasets_cache_key,
)
from .pins import _pins_fingerprint


def _get_meta_dataset() -> Any:
    """Get the IRW metadata dataset object.

    The handle is cached for the life of the process, which is fine: it is a
    handle, not data. What must not be trusted is the `properties` frozen on
    it at `.get()` time -- see `_dataset_version_tag`, which is how every
    caller below resolves the current version.
    """
    # Opened through `_init_dataset` so a session version pin applies, and
    # keyed on the pin so a pinned handle is a different object from the
    # current one (`properties` never refetches, so sharing would be wrong).
    cache_key = "meta_dataset" + _pins_fingerprint([META_REF])
    cached_dataset = metadata_cache.get(cache_key)
    if cached_dataset is not None:
        return cached_dataset

    dataset = _init_dataset(*META_REF)

    metadata_cache.set(cache_key, dataset)
    return dataset


def _get_existing_tables() -> set[str]:
    """Get set of existing table names from main IRW datasets.

    Keyed on the warehouses' current version tags: this set is what filters
    every metadata frame down to tables that still exist, so if it cannot
    notice a release then neither can they (issue #51).
    """
    ds_list = _init_main_datasets()
    cache_key = "existing_tables:" + _main_datasets_cache_key()
    version = _cache_version(_datasets_version_tag(ds_list))
    cached_tables = metadata_cache.get(cache_key, version)
    if cached_tables is not None:
        return cached_tables

    existing_tables = set()
    for ds in ds_list:
        for tbl in _dataset_table_list(ds):
            existing_tables.add(tbl.name.lower())

    metadata_cache.set(cache_key, existing_tables, version)
    return existing_tables


def get_metadata_table() -> pd.DataFrame:
    """
    Get the IRW metadata table (precomputed stats for each table).
    
    Returns
    -------
    pd.DataFrame
        Metadata information for IRW tables.
    """
    dataset = _get_meta_dataset()
    latest_version_tag = _cache_version(_dataset_version_tag(dataset))
    
    # Check cache
    cached_data = metadata_cache.get("metadata", latest_version_tag)
    if cached_data is not None:
        return cached_data
    
    # Fetch new data
    table = dataset.table(META_TABLES["metadata"])
    metadata_df = table.to_pandas_dataframe()
    
    # Cache the result
    metadata_cache.set("metadata", metadata_df, latest_version_tag)
    
    return metadata_df


def get_tags_table() -> pd.DataFrame:
    """
    Get the IRW tags table (measurement information for each table).
    
    Returns
    -------
    pd.DataFrame
        Tags information for IRW tables.
    """
    dataset = _get_meta_dataset()
    latest_version_tag = _cache_version(_dataset_version_tag(dataset))
    
    # Check cache
    cached_data = metadata_cache.get("tags", latest_version_tag)
    if cached_data is not None:
        return cached_data
    
    # Fetch tags table
    table = dataset.table(META_TABLES["tags"])
    tags_df = table.to_pandas_dataframe()
    
    # Clean the data
    tags_df = tags_df.replace("NA", pd.NA)
    
    # Filter to existing tables only
    existing_tables = _get_existing_tables()
    tags_df["table_lower"] = tags_df["table"].str.lower()
    filtered_tags = tags_df[tags_df["table_lower"].isin(existing_tables)].copy()
    filtered_tags.drop("table_lower", axis=1, inplace=True)
    
    # Cache the result
    metadata_cache.set("tags", filtered_tags, latest_version_tag)
    
    return filtered_tags


def get_collections_table() -> pd.DataFrame:
    """
    Get the IRW collections registry: one row per collection.

    Note this table has NO `table` column, so unlike every other metadata table
    here it is deliberately NOT filtered to existing tables -- doing so would
    empty it.

    Returns
    -------
    pd.DataFrame
        Columns: collection, label, kind, definition, rule, coverage, basis,
        n_tables, maintainer, added.
    """
    dataset = _get_meta_dataset()
    latest_version_tag = _cache_version(_dataset_version_tag(dataset))

    cached_data = metadata_cache.get("collections", latest_version_tag)
    if cached_data is not None:
        return cached_data

    table = dataset.table(META_TABLES["collections"])
    df = table.to_pandas_dataframe()
    df = df.replace("NA", pd.NA)
    if "n_tables" in df.columns:
        df["n_tables"] = pd.to_numeric(df["n_tables"], errors="coerce").astype("Int64")

    metadata_cache.set("collections", df, latest_version_tag)
    return df


def get_collection_members_table() -> pd.DataFrame:
    """
    Get IRW collection membership: one row per (table, collection) pair.

    Long format -- `table` repeats, by design, because a table can belong to
    many collections at once. Filtered to tables that currently exist.

    Returns
    -------
    pd.DataFrame
        Columns: table, collection, basis.
    """
    dataset = _get_meta_dataset()
    latest_version_tag = _cache_version(_dataset_version_tag(dataset))

    cached_data = metadata_cache.get("collection_members", latest_version_tag)
    if cached_data is not None:
        return cached_data

    table = dataset.table(META_TABLES["collection_members"])
    df = table.to_pandas_dataframe()

    existing_tables = _get_existing_tables()
    df["table_lower"] = df["table"].str.lower()
    filtered = df[df["table_lower"].isin(existing_tables)].copy()
    filtered.drop("table_lower", axis=1, inplace=True)

    metadata_cache.set("collection_members", filtered, latest_version_tag)
    return filtered


def get_biblio_table() -> pd.DataFrame:
    """
    Get the IRW bibliography table (bibliography info for each table).
    
    Returns
    -------
    pd.DataFrame
        Bibliography information for IRW tables.
    """
    dataset = _get_meta_dataset()
    latest_version_tag = _cache_version(_dataset_version_tag(dataset))
    
    # Check cache
    cached_data = metadata_cache.get("biblio", latest_version_tag)
    if cached_data is not None:
        return cached_data
    
    # Fetch biblio table
    table = dataset.table(META_TABLES["biblio"])
    biblio_df = table.to_pandas_dataframe()
    
    # Filter to existing tables only
    existing_tables = _get_existing_tables()
    biblio_df["table_lower"] = biblio_df["table"].str.lower()
    filtered_biblio = biblio_df[biblio_df["table_lower"].isin(existing_tables)].copy()
    filtered_biblio.drop("table_lower", axis=1, inplace=True)
    
    # Cache the result
    metadata_cache.set("biblio", filtered_biblio, latest_version_tag)
    
    return filtered_biblio
    
def _table_info() -> pd.DataFrame:
    """
    Internal function to get comprehensive information about all IRW tables.
    
    Returns a DataFrame with statistics, tags, and bibliography information
    for all tables in the main IRW dataset.
    
    Uses cached individual tables (metadata, tags, biblio) which are already
    cached with version checking. The combined result is also cached.
    
    Returns
    -------
    pandas.DataFrame
        Combined table information with columns from stats, tags, and biblio.
    """
    dataset = _get_meta_dataset()
    latest_version_tag = _cache_version(_dataset_version_tag(dataset))
    
    # Check if we have cached combined metadata with version check
    cached_combined = metadata_cache.get("combined_metadata", latest_version_tag)
    if cached_combined is not None:
        return cached_combined
    
    # Get stats, tags, and biblio metadata sources (these use cached versions)
    stats_df = get_metadata_table()
    tags_df = get_tags_table()
    biblio_df = get_biblio_table()
    
    # Join stats and tags on the 'table' column
    result = stats_df.merge(tags_df, on="table", how="left")
    
    # Add all biblio fields
    if not biblio_df.empty:
        result = result.merge(biblio_df, on="table", how="left")

    # Collections (issue #1633). collection_members is LONG -- a plain merge
    # would multiply rows per table. Group to a list first, then join one
    # column, so `collections` is list-valued and _apply_tag_filter handles it
    # unchanged (it already accepts list/tuple row values).
    try:
        members_df = get_collection_members_table()
    except Exception:
        members_df = pd.DataFrame(columns=["table", "collection"])
    if not members_df.empty:
        colls = (members_df.groupby("table")["collection"]
                 .apply(list).rename("collections"))
        result = result.merge(colls, left_on="table", right_index=True, how="left")
        result["collections"] = result["collections"].apply(
            lambda v: v if isinstance(v, list) else []
        )
    else:
        result["collections"] = [[] for _ in range(len(result))]

    # Cache the combined result with version checking
    metadata_cache.set("combined_metadata", result, latest_version_tag)
    
    return result