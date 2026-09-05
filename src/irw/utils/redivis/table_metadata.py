"""Table-level metadata utilities for main IRW dataset."""

import pandas as pd
import redivis
from typing import Any
from ...config import META_REF, META_TABLES
from .cache import metadata_cache
from .datasets import _init_main_datasets, _main_datasets_cache_key


def _get_meta_dataset() -> Any:
    """Get the IRW metadata dataset object."""
    cached_dataset = metadata_cache.get("meta_dataset")
    if cached_dataset is not None:
        return cached_dataset
    
    dataset = redivis.user(META_REF[0]).dataset(META_REF[1])
    dataset.get()
    
    metadata_cache.set("meta_dataset", dataset)
    return dataset


def _get_existing_tables() -> set[str]:
    """Get set of existing table names from main IRW datasets."""
    cache_key = "existing_tables:" + _main_datasets_cache_key()
    cached_tables = metadata_cache.get(cache_key)
    if cached_tables is not None:
        return cached_tables
    
    ds_list = _init_main_datasets()
    existing_tables = set()
    
    for ds in ds_list:
        # Use cached table list if available
        ds_id = getattr(ds, "_id", None) or getattr(ds, "name", None)
        ds_cache_key = f"dataset_tables:{ds_id}" if ds_id else None
        
        cached_table_list = None
        if ds_cache_key:
            cached_table_list = metadata_cache.get(ds_cache_key)
        
        if cached_table_list is None:
            tables = ds.list_tables()
            # Cache the table list
            if ds_cache_key:
                metadata_cache.set(ds_cache_key, list(tables))
        else:
            tables = cached_table_list
        
        for tbl in tables:
            existing_tables.add(tbl.name.lower())
    
    metadata_cache.set(cache_key, existing_tables)
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
    latest_version_tag = dataset.properties.get("version", {}).get("tag")
    
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
    latest_version_tag = dataset.properties.get("version", {}).get("tag")
    
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
    latest_version_tag = dataset.properties.get("version", {}).get("tag")

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
    latest_version_tag = dataset.properties.get("version", {}).get("tag")

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
    latest_version_tag = dataset.properties.get("version", {}).get("tag")
    
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
    latest_version_tag = dataset.properties.get("version", {}).get("tag")
    
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