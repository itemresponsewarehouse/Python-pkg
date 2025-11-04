"""
Simplified IRW API - all functions at module level.

Usage:
    import irw
    
    # Database operations
    irw.list_tables()
    irw.filter(construct_type="Affective/mental health")
    irw.info()  # Database info
    irw.info("agn_kay_2025")  # Table info
    
    # Table operations
    irw.fetch("agn_kay_2025")
    irw.itemtext("agn_kay_2025")
    irw.save_bibtex("agn_kay_2025")
    irw.download("agn_kay_2025")
    irw.long2resp(df)  # df is a DataFrame from fetch()
"""

from __future__ import annotations
from typing import Optional, Union, Dict, List, Literal
import pandas as pd
from .utils.redivis import _init_main_datasets, _init_sim_dataset, _init_comp_dataset
from .utils.redivis.item_text import _list_itemtext_tables
from .utils.long2resp import long2resp as _long2resp
from .operations.fetch import fetch as _fetch
from .operations.list_tables import list_tables as _list_tables, list_tables_basic
from .operations.info import info_for
from .operations.filter import filter_tables
from .operations.filter_info import get_filters as _get_filters_func, describe_filter as _describe_filter
from .utils.table_helpers import (
    _get_table_info_dict,
    _get_table_bibtex,
    _get_table_itemtext,
    _format_table_info
)


def _get_datasets(source: str = "main"):
    """Get datasets for the given source."""
    if source == "main":
        return _init_main_datasets()
    elif source == "sim":
        return [_init_sim_dataset()]
    elif source == "comp":
        return [_init_comp_dataset()]
    else:
        raise ValueError(f"Unknown source '{source}'. Must be one of: 'main', 'sim', 'comp'")


def list_tables(source: str = "main", include_metadata: bool = False) -> pd.DataFrame:
    """
    List available IRW tables.
    
    Parameters
    ----------
    source : str, default "main"
        Dataset source to use. Options: "main", "sim", "comp"
    include_metadata : bool, default False
        If True and source="main", return enriched IRW metadata.
        If False, return basic Redivis properties.
        
    Returns
    -------
    pandas.DataFrame
        Table listing with metadata.
    """
    datasets = _get_datasets(source)
    if source == "main" and include_metadata:
        return _list_tables(datasets)
    else:
        return list_tables_basic(datasets)


def filter(**kwargs) -> pd.Series:
    """
    Filter IRW tables based on metadata criteria.
    
    Only works for main IRW datasets.
    
    Parameters
    ----------
    **kwargs
        Filter parameters (n_responses, construct_type, etc.)
        
    Returns
    -------
    pandas.Series
        Sorted Series of table names that match filters.
    """
    datasets = _get_datasets("main")
    return filter_tables(datasets, **kwargs)


def info(table_name: Optional[str] = None, source: str = "main", return_dict: bool = False) -> Union[None, Dict]:
    """
    Get information about the IRW database or a specific table.
    
    Parameters
    ----------
    table_name : str, optional
        If provided, returns info for that table. If None, returns database info.
    source : str, default "main"
        Dataset source (only used for database info).
    return_dict : bool, default False
        If True and table_name provided, returns dictionary instead of formatted string.
        
    Returns
    -------
    None, dict, or str
        Database info prints and returns None.
        Table info prints and returns None (unless return_dict=True, then returns dict).
    """
    if table_name is None:
        # Database info
        datasets = _get_datasets(source)
        title = {
            "main": "IRW Database Information",
            "sim": "IRW Simulation Database Information",
            "comp": "IRW Competition Database Information"
        }.get(source, "IRW Database Information")
        return info_for(datasets, title=title)
    else:
        # Table info
        if source != "main":
            raise ValueError(f"Table info is only available for main IRW datasets (source='main'). Current source is '{source}'.")
        
        result = _get_table_info_dict(table_name)
        
        if not result:
            message = f"No metadata available for table: {table_name}"
            print(message)
            return None if not return_dict else {}
        
        formatted_message = _format_table_info(table_name, result)
        print(formatted_message)
        
        if return_dict:
            return result
        else:
            return None  # Already printed, don't return string


def fetch(
    table_name: Union[str, List[str]], 
    source: str = "main", 
    *, 
    dedup: bool = False,
    wide: bool = False
) -> Union[pd.DataFrame, Dict[str, Optional[pd.DataFrame]]]:
    """
    Fetch one or more IRW tables.
    
    Parameters
    ----------
    table_name : str or list of str
        Single table name or list of table names.
    source : str, default "main"
        Dataset source to use.
    dedup : bool, default False
        Apply deduplication to responses.
    wide : bool, default False
        If True, automatically convert to wide-format response matrix using long2resp().
        Only works for single table fetch. For multiple tables, use long2resp() separately.
        
    Returns
    -------
    pandas.DataFrame or dict[str, pandas.DataFrame]
        Single table returns DataFrame, multiple return dict.
        If wide=True, returns wide-format response matrix instead of long format.
    """
    datasets = _get_datasets(source)
    result = _fetch(datasets, table_name, dedup=dedup)
    
    # Handle single DataFrame result
    if isinstance(result, pd.DataFrame):
        # If wide=True, convert to response matrix
        if wide:
            return _long2resp(result, wave=None, id_density_threshold=0.1, agg_method="mean")
        return result
    
    # Handle dict of DataFrames
    elif isinstance(result, dict):
        out = {}
        for k, v in result.items():
            if v is not None:
                # If wide=True, convert each table
                if wide:
                    out[k] = _long2resp(v, wave=None, id_density_threshold=0.1, agg_method="mean")
                else:
                    out[k] = v
            else:
                out[k] = None
        return out
    
    else:
        return result


def itemtext(table_name: str) -> Union[pd.DataFrame, str]:
    """
    Get item-level text for a table.
    
    Parameters
    ----------
    table_name : str
        Name of the IRW table.
        
    Returns
    -------
    pandas.DataFrame or str
        DataFrame with item text if available, otherwise message string.
    """
    return _get_table_itemtext(table_name)


def save_bibtex(
    table_names: Union[str, List[str]], 
    output_file: Optional[str] = None
) -> List[str]:
    """
    Get/save BibTeX entries for one or more IRW tables.
    
    Updates the BibTeX key to match the table name. Attempts to fetch BibTeX
    from the bibliography table first, then falls back to DOI-based lookup if needed.
    
    If output_file is provided, saves entries to file. Otherwise, returns entries.
    
    Parameters
    ----------
    table_names : str or list of str
        Single table name or list of table names for which BibTeX entries are generated.
    output_file : str, optional
        File path to save BibTeX entries. If None, returns entries instead.
        
    Returns
    -------
    list of str
        List of valid BibTeX entries.
        
    Examples
    --------
    >>> import irw
    >>> 
    >>> # Get BibTeX for a single table
    >>> bibtex = irw.save_bibtex("agn_kay_2025")
    >>> 
    >>> # Save BibTeX for multiple tables to file
    >>> irw.save_bibtex(["agn_kay_2025", "pks_probability"], "refs.bib")
    """
    import re
    import urllib.request
    from urllib.error import URLError
    
    # Normalize input to list
    if isinstance(table_names, str):
        table_names = [table_names]
    
    # Initialize lists
    valid_entries = []
    missing_tables = []
    missing_doi_tables = []
    
    # Fetch the full biblio table once (uses cache)
    from .utils.redivis.table_metadata import get_biblio_table
    from .utils.redivis.table_metadata import _get_existing_tables
    
    biblio_df = get_biblio_table()
    existing_tables = _get_existing_tables()
    
    # Process each table name
    for table_name in table_names:
        # Check if table exists in IRW
        if table_name.lower() not in existing_tables:
            missing_tables.append(table_name)
            continue
        
        # Get the corresponding row from biblio (case-insensitive)
        biblio_entry = biblio_df[biblio_df['table'].str.lower() == table_name.lower()]
        if biblio_entry.empty:
            missing_doi_tables.append(table_name)
            continue
        
        # Get BibTeX and DOI from the row
        bibtex = biblio_entry.iloc[0].get('BibTex') if 'BibTex' in biblio_entry.columns else None
        doi = biblio_entry.iloc[0].get('DOI__for_paper_') if 'DOI__for_paper_' in biblio_entry.columns else None
        
        # Clean BibTeX
        if bibtex is not None and pd.notna(bibtex):
            bibtex = str(bibtex).strip()
        else:
            bibtex = None
        
        # Step 1: Try manual BibTeX
        if bibtex and '@' in bibtex:
            # Valid BibTeX found
            pass
        else:
            # Step 2: Try DOI-based BibTeX
            bibtex = None
            if doi is not None and pd.notna(doi):
                doi_str = str(doi).strip()
                if doi_str:
                    try:
                        # Fetch BibTeX from DOI.org
                        url = f"https://doi.org/{doi_str}"
                        req = urllib.request.Request(
                            url,
                            headers={'Accept': 'application/x-bibtex'}
                        )
                        with urllib.request.urlopen(req, timeout=10) as response:
                            fetched_bibtex = response.read().decode('utf-8').strip()
                            if fetched_bibtex and '@' in fetched_bibtex:
                                bibtex = fetched_bibtex
                    except (URLError, Exception):
                        pass
        
        # Step 3: If all else fails
        if not bibtex or '@' not in bibtex:
            missing_doi_tables.append(table_name)
            continue
        
        # Step 4: Replace BibTeX key with table name
        # Pattern: @type{key, -> @type{table_name,
        bibtex = re.sub(
            r'@(\w+)\{[^,]+,',
            f'@\\1{{{table_name},',
            bibtex,
            count=1
        )
        
        valid_entries.append(bibtex)
    
    # Remove duplicates while preserving order
    if valid_entries:
        seen = set()
        unique_entries = []
        for entry in valid_entries:
            if entry not in seen:
                seen.add(entry)
                unique_entries.append(entry)
        
        if output_file:
            # Save to file
            with open(output_file, 'w', encoding='utf-8') as f:
                for entry in unique_entries:
                    f.write(entry + '\n\n')
        
        return unique_entries
    
    return valid_entries


def download(table_name: str, path: Optional[str] = None, overwrite: bool = False) -> str:
    """
    Download a table using Redivis's native download method.
    
    Parameters
    ----------
    table_name : str
        Name of the IRW table.
    path : str, optional
        Path to save file. If None, saves in current directory with table name.
    overwrite : bool, default False
        Whether to overwrite existing file.
        
    Returns
    -------
    str
        Path to saved file.
    """
    from .utils.redivis.tables import _get_table
    import os
    
    # Try to find table in main, sim, comp (using cached datasets)
    for source in ["main", "sim", "comp"]:
        datasets = _get_datasets(source)
        for ds in (datasets if isinstance(datasets, list) else [datasets]):
            try:
                table_obj = _get_table(ds, table_name)
                if hasattr(table_obj, 'download'):
                    table_obj.download(path=path, overwrite=overwrite)
                    if path is None:
                        path = os.path.join(os.getcwd(), table_name)
                    return path
            except Exception:
                continue
    
    raise ValueError(f"Table '{table_name}' not found or does not support downloading.")


def long2resp(
    df: pd.DataFrame,
    wave: Optional[int] = None,
    id_density_threshold: Optional[float] = 0.1,
    agg_method: Literal["mean", "mode", "median", "first"] = "mean"
) -> pd.DataFrame:
    """
    Convert IRW long-format data to wide-format response matrix.
    
    Parameters
    ----------
    df : pandas.DataFrame
        Long-format DataFrame with columns: id, item, resp (and optionally wave).
        Typically obtained from irw.fetch().
    wave : int, optional
        Filter by wave. Defaults to most frequent wave if None.
    id_density_threshold : float, optional
        Minimum response density (0.0-1.0). None to disable. Default 0.1.
    agg_method : str, default "mean"
        How to handle multiple id-item pairs: "mean", "mode", "median", "first".
        
    Returns
    -------
    pandas.DataFrame
        Wide-format response matrix where rows are ids and columns are items.
        
    Examples
    --------
    >>> import irw
    >>> 
    >>> # Fetch data
    >>> df = irw.fetch("agn_kay_2025")
    >>> 
    >>> # Convert to wide format
    >>> resp_matrix = irw.long2resp(df)
    """
    return _long2resp(df, wave=wave, id_density_threshold=id_density_threshold, agg_method=agg_method)


def get_filters() -> List[str]:
    """
    Get list of available filter parameter names.
    
    Returns
    -------
    list[str]
        List of all available filter parameter names.
        
    Examples
    --------
    >>> import irw
    >>> filters = irw.get_filters()
    >>> print(filters)  # ['n_responses', 'n_participants', ...]
    """
    return _get_filters_func()


def describe_filter(filter_name: str) -> Optional[Dict]:
    """
    Describe a filter and show available values.
    
    Parameters
    ----------
    filter_name : str
        Name of the filter to describe.
        
    Returns
    -------
    dict or None
        Dictionary with 'description' and 'values', or None if not found.
    """
    return _describe_filter(_get_datasets("main"), filter_name)


def list_tables_with_itemtext() -> List[str]:
    """
    List tables that have item-level text available.
    
    Returns
    -------
    list of str
        Sorted list of table names with item text.
    """
    available_tables = _list_itemtext_tables()
    return sorted(list(available_tables))

