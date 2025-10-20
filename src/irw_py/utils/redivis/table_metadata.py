"""Table-level metadata utilities for main IRW dataset."""

import pandas as pd
import redivis
from typing import Dict, Any
from ...config import META_REF, META_TABLES
from .cache import metadata_cache
from .datasets import _init_main_datasets


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
    cached_tables = metadata_cache.get("existing_tables")
    if cached_tables is not None:
        return cached_tables
    
    ds_list = _init_main_datasets()
    existing_tables = set()
    
    for ds in ds_list:
        tables = ds.list_tables()
        for tbl in tables:
            existing_tables.add(tbl.name.lower())
    
    metadata_cache.set("existing_tables", existing_tables)
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
    
    Returns
    -------
    pandas.DataFrame
        Combined table information with columns from stats, tags, and biblio.
    """
    # Check if we have cached combined metadata
    cached_combined = metadata_cache.get("combined_metadata")
    if cached_combined is not None:
        return cached_combined
    
    # Get stats, tags, and biblio metadata sources
    stats_df = get_metadata_table()
    tags_df = get_tags_table()
    biblio_df = get_biblio_table()
    
    # Join stats and tags on the 'table' column
    result = stats_df.merge(tags_df, on="table", how="left")
    
    # Add all biblio fields
    if not biblio_df.empty:
        result = result.merge(biblio_df, on="table", how="left")
    
    # Cache the combined result for faster access
    metadata_cache.set("combined_metadata", result)
    
    return result