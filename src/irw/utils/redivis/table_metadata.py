"""Table-level metadata utilities for every IRW table source.

Each source (main, nom, sim, comp) has its own metadata and bibliography tables
in irw_meta, and main and nom also have a tags table -- see SOURCE_META_TABLES
in config.py. Cache keys for main are the bare kind ("metadata", "tags", ...);
every other source's key carries the source, so the four never share a frame.
"""

from typing import Any, Dict, List

import pandas as pd
from ...config import (
    COLLECTION_SOURCES,
    META_REF,
    META_TABLES,
    SOURCE_META_TABLES,
    SOURCES,
    TAG_SOURCES,
)
from .cache import metadata_cache
from .datasets import (
    _cache_version,
    _dataset_table_list,
    _dataset_version_tag,
    _datasets_cache_key,
    _datasets_version_tag,
    _init_comp_dataset,
    _init_dataset,
    _init_main_datasets,
    _init_nom_dataset,
    _init_sim_dataset,
    _main_datasets_cache_key,
)
from .pins import _pins_fingerprint


def _check_source(source: str) -> Dict[str, str]:
    """The metadata table names for `source`, or ValueError for an unknown one."""
    if source not in SOURCES:
        raise ValueError(
            f"Unknown source '{source}'. Must be one of: "
            + ", ".join(f"'{s}'" for s in SOURCES)
        )
    return SOURCE_META_TABLES[source]


def _check_tag_source(source: str) -> None:
    """Refuse a source that has no tags table (Rpkg's .irw_tags_for_source)."""
    _check_source(source)
    if source not in TAG_SOURCES:
        raise ValueError(
            f"Tags are not available for source='{source}'. Tagged sources: "
            + ", ".join(f"'{s}'" for s in TAG_SOURCES) + "."
        )


def _check_collection_source(source: str) -> None:
    """Refuse a source that has no collections (Rpkg's .irw_collection_sources)."""
    _check_source(source)
    if source not in COLLECTION_SOURCES:
        raise ValueError(
            "`collection` is only available for source in "
            + ", ".join(f"'{s}'" for s in COLLECTION_SOURCES) + "."
        )


def _meta_cache_key(kind: str, source: str) -> str:
    """Cache key for one metadata frame. Main keeps its historical bare keys."""
    return kind if source == "main" else f"{kind}:{source}"


def _source_datasets(source: str) -> List[Any]:
    """The Redivis dataset handles whose tables make up `source`."""
    _check_source(source)
    if source == "main":
        return _init_main_datasets()
    if source == "nom":
        return [_init_nom_dataset()]
    if source == "sim":
        return [_init_sim_dataset()]
    return [_init_comp_dataset()]


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


def _get_existing_tables(source: str = "main") -> set[str]:
    """Get set of existing table names, lowercased, for one source.

    Keyed on the source's current version tags: this set is what filters
    every metadata frame down to tables that still exist, so if it cannot
    notice a release then neither can they (issue #51).
    """
    ds_list = _source_datasets(source)
    if source == "main":
        cache_key = "existing_tables:" + _main_datasets_cache_key()
    else:
        cache_key = f"existing_tables:{source}:" + _datasets_cache_key(ds_list)
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


def get_metadata_table(source: str = "main") -> pd.DataFrame:
    """
    Get the IRW metadata table (precomputed stats for each table).

    Parameters
    ----------
    source : str, default "main"
        Table source: "main", "nom", "sim" or "comp".

    Returns
    -------
    pd.DataFrame
        Metadata information for the source's tables.
    """
    table_name = _check_source(source)["metadata"]
    cache_key = _meta_cache_key("metadata", source)
    dataset = _get_meta_dataset()
    latest_version_tag = _cache_version(_dataset_version_tag(dataset))
    
    # Check cache
    cached_data = metadata_cache.get(cache_key, latest_version_tag)
    if cached_data is not None:
        return cached_data
    
    # Fetch new data
    table = dataset.table(table_name)
    metadata_df = table.to_pandas_dataframe()
    
    # Cache the result
    metadata_cache.set(cache_key, metadata_df, latest_version_tag)
    
    return metadata_df


def get_tags_table(source: str = "main") -> pd.DataFrame:
    """
    Get the IRW tags table (measurement information for each table).

    Parameters
    ----------
    source : str, default "main"
        Table source. Only "main" and "nom" are tagged; "sim" and "comp" have
        no tags by design and raise ValueError rather than return an empty
        frame.

    Returns
    -------
    pd.DataFrame
        Tags information for the source's tables.
    """
    _check_tag_source(source)
    table_name = SOURCE_META_TABLES[source]["tags"]
    cache_key = _meta_cache_key("tags", source)
    dataset = _get_meta_dataset()
    latest_version_tag = _cache_version(_dataset_version_tag(dataset))
    
    # Check cache
    cached_data = metadata_cache.get(cache_key, latest_version_tag)
    if cached_data is not None:
        return cached_data
    
    # Fetch tags table
    table = dataset.table(table_name)
    tags_df = table.to_pandas_dataframe()
    
    # Clean the data
    tags_df = tags_df.replace("NA", pd.NA)
    
    # Filter to existing tables only
    existing_tables = _get_existing_tables(source)
    tags_df["table_lower"] = tags_df["table"].str.lower()
    filtered_tags = tags_df[tags_df["table_lower"].isin(existing_tables)].copy()
    filtered_tags.drop("table_lower", axis=1, inplace=True)
    
    # Cache the result
    metadata_cache.set(cache_key, filtered_tags, latest_version_tag)
    
    return filtered_tags


def _merge_on_table(left: pd.DataFrame, right: pd.DataFrame) -> pd.DataFrame:
    """Left-join `right` onto `left` by table name, ignoring case.

    The metadata, tags and biblio tables do not agree on the case of a name:
    against live irw_meta, an exact join on `table` dropped the tags of 308
    main tables and the bibliography of 30. `left` keeps its own `table`
    column; `right`'s is dropped so no `table_x`/`table_y` pair appears.
    """
    if "table" not in right.columns:
        return left
    right = right.copy()
    right["_table_key"] = right["table"].astype(str).str.lower()
    right = right.drop(columns=["table"]).drop_duplicates(subset=["_table_key"])
    left = left.copy()
    left["_table_key"] = left["table"].astype(str).str.lower()
    merged = left.merge(right, on="_table_key", how="left")
    return merged.drop(columns=["_table_key"])


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


def get_biblio_table(source: str = "main") -> pd.DataFrame:
    """
    Get the IRW bibliography table (bibliography info for each table).

    Parameters
    ----------
    source : str, default "main"
        Table source: "main", "nom", "sim" or "comp".

    Returns
    -------
    pd.DataFrame
        Bibliography information for the source's tables.
    """
    table_name = _check_source(source)["biblio"]
    cache_key = _meta_cache_key("biblio", source)
    dataset = _get_meta_dataset()
    latest_version_tag = _cache_version(_dataset_version_tag(dataset))
    
    # Check cache
    cached_data = metadata_cache.get(cache_key, latest_version_tag)
    if cached_data is not None:
        return cached_data
    
    # Fetch biblio table
    table = dataset.table(table_name)
    biblio_df = table.to_pandas_dataframe()
    
    # Filter to existing tables only
    existing_tables = _get_existing_tables(source)
    biblio_df["table_lower"] = biblio_df["table"].str.lower()
    filtered_biblio = biblio_df[biblio_df["table_lower"].isin(existing_tables)].copy()
    filtered_biblio.drop("table_lower", axis=1, inplace=True)
    
    # Cache the result
    metadata_cache.set(cache_key, filtered_biblio, latest_version_tag)
    
    return filtered_biblio
    
def _table_info(source: str = "main") -> pd.DataFrame:
    """
    Internal function to get comprehensive information about all tables in a source.
    
    Returns a DataFrame with statistics, tags (for a tagged source), and
    bibliography information for all tables in the source.
    
    Uses cached individual tables (metadata, tags, biblio) which are already
    cached with version checking. The combined result is also cached.

    Parameters
    ----------
    source : str, default "main"
        Table source: "main", "nom", "sim" or "comp".
    
    Returns
    -------
    pandas.DataFrame
        Combined table information with columns from stats, tags, and biblio.
        Only a collection source (main) carries the `collections` column.
    """
    _check_source(source)
    cache_key = _meta_cache_key("combined_metadata", source)
    dataset = _get_meta_dataset()
    latest_version_tag = _cache_version(_dataset_version_tag(dataset))
    
    # Check if we have cached combined metadata with version check
    cached_combined = metadata_cache.get(cache_key, latest_version_tag)
    if cached_combined is not None:
        return cached_combined
    
    # Get stats, tags, and biblio metadata sources (these use cached versions)
    result = get_metadata_table(source)
    if source in TAG_SOURCES:
        result = _merge_on_table(result, get_tags_table(source))
    
    # Add all biblio fields
    biblio_df = get_biblio_table(source)
    if not biblio_df.empty:
        result = _merge_on_table(result, biblio_df)

    if source not in COLLECTION_SOURCES:
        # No `collections` column at all, rather than an all-empty one: an
        # empty list per row would let filter(collection=...) match nothing
        # and look like an answer. filter() refuses the argument up front.
        metadata_cache.set(cache_key, result, latest_version_tag)
        return result

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
    metadata_cache.set(cache_key, result, latest_version_tag)
    
    return result