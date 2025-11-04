"""Fetch operations for IRW data.

This module contains all functionality related to fetching data from IRW datasets.
"""

from typing import Iterable, Union, Dict, Optional, Any, List
import warnings
import logging
import numpy as np
import pandas as pd

from ..utils.redivis import _get_table, _classify_error, _format_error

logger = logging.getLogger(__name__)


def fetch(datasets: List[Any], name: Union[str, Iterable[str], pd.Series], *, dedup: bool = False) -> Union[pd.DataFrame, Dict[str, Optional[pd.DataFrame]], None]:
    """
    Fetch one or more IRW tables.
    
    Parameters
    ----------
    datasets : List[Any]
        List of Redivis dataset objects to search through.
    name : str | Iterable[str] | pandas.Series
        Single table id → returns DataFrame or None.
        Multiple (list, Series, etc.) → returns dict[name -> DataFrame | None].
        Can also pass a pandas Series (e.g., from filter() method).
    dedup : bool, default False
        After fetch, keep one row per (id,item[,wave]).
    
    Returns
    -------
    pd.DataFrame | dict[str, pd.DataFrame | None] | None
        Single table name returns DataFrame or None.
        Multiple table names return dict mapping names to DataFrame or None.
    
    Raises
    ------
    ValueError
        If name is empty.
    TypeError
        If name is not a string, iterable of strings, or pandas Series.
    
    Examples
    --------
    >>> import irw
    >>> from irw.operations.fetch import fetch
    >>> from irw.utils.redivis import _init_main_datasets
    >>> 
    >>> # Fetch a single table
    >>> datasets = _init_main_datasets()
    >>> df = fetch(datasets, "agn_kay_2025")
    >>> 
    >>> # Fetch multiple tables
    >>> tables = fetch(datasets, ["agn_kay_2025", "pks_probability"])
    >>> 
    >>> # Fetch from filter results (pandas Series)
    >>> filtered = irw.filter(construct_type="Affective/mental health")
    >>> tables = fetch(datasets, filtered)
    >>> 
    >>> # Fetch with deduplication
    >>> df_dedup = fetch(datasets, "agn_kay_2025", dedup=True)
    """
    # Handle pandas Series
    if isinstance(name, pd.Series):
        if name.empty:
            raise ValueError("name Series cannot be empty")
        name = name.tolist()
    
    # Check if empty (after Series conversion)
    if isinstance(name, str):
        if not name:
            raise ValueError("name cannot be empty")
        return _fetch_one_table(datasets, name, dedup=dedup)
    
    # Validate iterable
    if not name:
        raise ValueError("name cannot be empty")
    
    try:
        name_list = list(name)
        if not name_list:
            raise ValueError("name iterable cannot be empty")
    except TypeError:
        raise TypeError("name must be a string, iterable of strings, or pandas Series")

    out: Dict[str, Optional[pd.DataFrame]] = {}
    for nm in name_list:
        if not isinstance(nm, str):
            raise TypeError(f"All names must be strings, got {type(nm)}")
        key = str(nm)
        out[key] = _fetch_one_table(datasets, key, dedup=dedup)
    return out


def _fetch_one_table(datasets: List[Any], name: str, *, dedup: bool) -> Optional[pd.DataFrame]:
    """
    Internal: fetch a single table using the provided datasets.
    """
    if not isinstance(name, str) or name == "":
        logger.warning(f"Table '{name}' cannot be fetched due to an invalid format.")
        return None

    last_err: Exception | None = None
    for ds in datasets:
        try:
            tbl = _get_table(ds, name)
            df = tbl.to_pandas_dataframe()

            # --- inline transforms needed only for fetch() ---

            # Coerce 'resp' to numeric (exact R-style warning text)
            if "resp" in df.columns:
                s = df["resp"]

                def missing_like(x) -> bool:
                    if x is None:
                        return True
                    if isinstance(x, float) and pd.isna(x):
                        return True
                    if isinstance(x, str) and x.strip().upper() in {"", "NA", "NAN", "NULL"}:
                        return True
                    return False

                if pd.api.types.is_object_dtype(s) or pd.api.types.is_string_dtype(s):
                    coerced = pd.to_numeric(s.where(s.map(missing_like), s), errors="coerce")
                    bad = (~s.map(missing_like)) & coerced.isna()
                    if bad.any():
                        warnings.warn(
                            f"In dataset '{name}': 'resp' column contained non-numeric values that could not be coerced. Some NAs were introduced.",
                            RuntimeWarning,
                            stacklevel=2,
                        )
                    df = df.copy()
                    df["resp"] = coerced

            # Dedup without pandas GroupBy.apply warning
            if dedup:
                if "date" in df.columns:
                    logger.info(f"Deduplication skipped for dataset '{name}': 'date' column detected (timestamped responses).")
                elif {"id", "item"}.issubset(df.columns):
                    keys = ["id", "item"] + (["wave"] if "wave" in df.columns else [])
                    msg = (
                        f"Deduplicated dataset '{name}': one response randomly retained per (id, item, wave) group."
                        if "wave" in df.columns
                        else f"Deduplicated dataset '{name}': one response randomly retained per (id, item) pair."
                    )
                    n0 = len(df)
                    rng = np.random.RandomState(42)
                    groups = df.groupby(keys, dropna=False).indices  # dict[group_key -> ndarray of row indices]
                    if groups:
                        chosen_idx = [rng.choice(ix) for ix in groups.values()]
                        df = df.loc[sorted(chosen_idx)].reset_index(drop=True)
                        if len(df) < n0:
                            logger.info(msg)
                        else:
                            logger.info(f"Deduplication not needed for dataset '{name}': no duplicate responses found.")
                    else:
                        logger.info(f"Deduplication not needed for dataset '{name}': no duplicate responses found.")
                # else: if missing id/item, silently skip dedup (matches R spirit)

            return pd.DataFrame(df)

        except Exception as e:
            last_err = e
            kind = _classify_error(e)
            if kind == "invalid_request":
                logger.warning(f"Table '{name}' cannot be fetched due to an invalid format.")
                return None
            # not_found/transient/unknown → try next dataset
            continue

    # After searching all datasets
    if last_err is None or _classify_error(last_err) == "not_found":
        logger.warning(f"Table '{name}' does not exist in the IRW database.")
    else:
        logger.error(f"Error fetching dataset '{name}' : {_format_error(last_err)}")
    return None
