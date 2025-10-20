"""List available IRW tables (basic and enriched variants)."""

from typing import List, Dict, Any
import logging
import pandas as pd
from ..utils.redivis.table_metadata import _table_info
from ..utils.redivis.item_text import _list_itemtext_tables
from ..utils.redivis.cache import metadata_cache

# =====================
# Module-level constants
# =====================

logger = logging.getLogger(__name__)

EXCLUDE_COLUMNS = {
    "name",
    "name_lower",
    "table",
    "numRows",
    "variableCount",
}

RENAME_MAP = {
    # Tag normalization
    'child_age__for_child_focused_studies_': 'child_age',
    'primary_language_s_': 'language',
    'dataset': 'source_redivis_dataset',
    # Biblio normalization
    'Description': 'description',
    'Reference_x': 'reference',
    'DOI__for_paper_': 'doi',
    'URL__for_data_': 'url',
    'Derived_License': 'license',
    'BibTex': 'bibtex',
}

STATS_SET = {
    'n_responses',
    'n_participants',
    'n_items',
    'responses_per_participant',
    'responses_per_item',
    'density',
}

TAGS_SET = {
    'construct_type',
    'construct_name',
    'age_range',
    'child_age',
    'sample',
    'item_format',
    'measurement_tool',
    'n_categories',
    'variables',
    'language',
    'source_redivis_dataset',
    'has_item_text',
}


# =====================
# Helper functions
# =====================

def _build_base_table_list(datasets: List[Any]) -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []
    for ds in datasets:
        try:
            tables = ds.list_tables()
        except Exception:
            continue
        for t in tables:
            rows.append({"name": getattr(t, "name", None)})
    if not rows:
        return pd.DataFrame(columns=["name"])  
    out = pd.DataFrame(rows)
    return out.sort_values("name", kind="stable").reset_index(drop=True)


def _merge_metadata(base: pd.DataFrame, datasets: List[Any]) -> pd.DataFrame:
    # Cache key based on dataset names
    ds_names = "|".join(sorted([(getattr(ds, "name", None) or "").lower() for ds in datasets]))
    cache_key = f"list_tables:{ds_names}"
    cached = metadata_cache.get(cache_key)
    if cached is not None:
        return cached.copy()

    metadata = _table_info()
    if metadata.empty:
        return base

    metadata['name_lower'] = metadata['table'].str.lower()
    base['name_lower'] = base['name'].str.lower()

    merged = base.merge(
        metadata,
        left_on='name_lower',
        right_on='name_lower',
        how='left',
        suffixes=('', '_meta')
    )
    merged = merged.drop(columns=['name_lower', 'table'], errors='ignore')

    # Select all useful columns
    result_columns = ['name']
    for col in merged.columns:
        if col not in EXCLUDE_COLUMNS:
            result_columns.append(col)

    result = merged[result_columns].copy()
    result = result.drop_duplicates(subset=['name']).reset_index(drop=True)

    # Rename for readability
    result = result.rename(columns={k: v for k, v in RENAME_MAP.items() if k in result.columns})

    # Cache and return
    metadata_cache.set(cache_key, result)
    return result.copy()


def _compute_item_text_flag(df: pd.DataFrame) -> pd.DataFrame:
    try:
        itemtext_available = _list_itemtext_tables()
        df['has_item_text'] = df['name'].str.lower().isin(itemtext_available)
    except Exception:
        df['has_item_text'] = False
    return df


def _order_columns(df: pd.DataFrame) -> pd.DataFrame:
    core_cols = ['name']
    stats_cols, tags_cols, biblio_cols = [], [], []
    for col in df.columns:
        if col in core_cols:
            continue
        if col in STATS_SET:
            stats_cols.append(col)
        elif col in TAGS_SET:
            tags_cols.append(col)
        else:
            biblio_cols.append(col)
    ordered_cols = [c for c in core_cols if c in df.columns] + stats_cols + tags_cols + biblio_cols
    return df[ordered_cols]


def list_tables(datasets: List[Any]) -> pd.DataFrame:
    """
    List available tables with integrated IRW stats, tags, biblio, and item-text availability.

    Parameters
    ----------
    datasets : List[Any]
        Redivis dataset objects to enumerate tables from.

    Returns
    -------
    pandas.DataFrame
        One row per table. Columns are grouped in the following order:

        - Core: name
        - Stats: n_responses, n_participants, n_items, responses_per_participant,
          responses_per_item, density
        - Tags: construct_type, construct_name, age_range, child_age, sample,
          item_format, measurement_tool, n_categories, variables, language,
          source_redivis_dataset, has_item_text
        - Bib: reference, doi, url, license, bibtex

    Examples
    --------
    >>> from irw_py.operations.list_tables import list_tables
    >>> from irw_py import IRW
    >>> irw = IRW()
    >>> tables = list_tables(irw._datasets)
    >>> large = tables[tables["n_responses"] > 10_000]
    >>> english = tables[tables["language"] == "English"]
    """
    # Base table list from Redivis
    out = _build_base_table_list(datasets)
    
    # Get metadata and merge
    try:
        # Merge metadata and post-process
        result = _merge_metadata(out, datasets)
        result = _compute_item_text_flag(result)
        result = _order_columns(result)
        return result
        
    except Exception as e:
        # If metadata merge fails, return basic table info
        logger.info(f"list_tables metadata merge failed; returning basic info: {e}")
        return out


def list_tables_basic(datasets: List[Any]) -> pd.DataFrame:
    """
    List available tables with only Redivis base properties.

    Intended for datasets without IRW metadata (e.g., IRWSim, IRWComp).

    Returns
    -------
    pandas.DataFrame
        Columns: name, numRows, variableCount
    """
    rows: List[Dict[str, Any]] = []
    for ds in datasets:
        try:
            tables = ds.list_tables()
        except Exception:
            continue

        for t in tables:
            props = getattr(t, "properties", {}) or {}
            rows.append(
                {
                    "name": getattr(t, "name", None),
                    "numRows": props.get("numRows"),
                    "variableCount": props.get("variableCount"),
                }
            )

    if not rows:
        return pd.DataFrame(columns=["name", "numRows", "variableCount"]) 

    out = pd.DataFrame(rows)
    out = out.sort_values("name", kind="stable").reset_index(drop=True)
    return out
