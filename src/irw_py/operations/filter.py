"""Filter IRW tables based on metadata criteria."""

from typing import List, Optional, Union
import re
import pandas as pd
import numpy as np
from ..operations.list_tables import list_tables


def _apply_numeric_filter(
    df: pd.DataFrame, 
    column: str, 
    value: Optional[Union[float, int, List[Optional[Union[float, int]]]]]
) -> pd.DataFrame:
    """Apply numeric filter (exact value or range).
    
    Supports:
    - Single value: exact match
    - List of length 1: exact match
    - List of length 2: range [min, max], where None means infinity
    """
    if value is None or column not in df.columns:
        return df
    
    if isinstance(value, (int, float)):
        # Exact match
        mask = df[column] == value
    elif isinstance(value, list) and len(value) == 1:
        # Single value in list
        mask = df[column] == value[0]
    elif isinstance(value, list) and len(value) == 2:
        # Range [min, max], where None means infinity
        min_val, max_val = value[0], value[1]
        if min_val is None:
            min_val = -np.inf
        if max_val is None or max_val == float('inf') or max_val == np.inf:
            max_val = np.inf
        mask = (df[column] >= min_val) & (df[column] <= max_val)
    else:
        return df
    
    return df[mask].copy()


def _apply_tag_filter(
    df: pd.DataFrame,
    column: str,
    values: Optional[Union[str, List[str]]]
) -> pd.DataFrame:
    """Apply tag-based filter (exact match, can be multiple values).
    
    Handles comma-separated values in tag columns (e.g., "value1, value2").
    Matches if any value in the comma-separated list matches any filter value.
    """
    if values is None or column not in df.columns:
        return df
    
    if isinstance(values, str):
        values = [values]
    
    # Build mask checking each row
    mask = pd.Series(False, index=df.index)
    
    for idx in df.index:
        row_value = df.loc[idx, column]
        
        if pd.isna(row_value):
            continue
        
        # Handle comma-separated values in tag column
        if isinstance(row_value, str):
            # Split on comma and strip whitespace
            tag_list = [v.strip() for v in row_value.split(',') if v.strip()]
        elif isinstance(row_value, (list, tuple)):
            tag_list = [str(v).strip() for v in row_value if v]
        else:
            tag_list = [str(row_value).strip()]
        
        # Check if any value in tag_list matches any filter value
        matches = any(tag in values for tag in tag_list)
        mask.loc[idx] = matches
    
    return df[mask].copy()


def _apply_variable_filter(
    df: pd.DataFrame,
    variables: Optional[Union[str, List[str]]]
) -> pd.DataFrame:
    """Apply variable presence filter (exact names or prefix matching).
    
    ALL variables in the filter list must be present (AND logic).
    Variables containing '_' are treated as prefix matches.
    """
    if variables is None or 'variables' not in df.columns:
        return df
    
    if isinstance(variables, str):
        variables = [variables]
    
    # Build mask - ALL variables must match (AND logic)
    mask = pd.Series(True, index=df.index)
    
    # Check each row's variables column
    for idx in df.index:
        row_vars = df.loc[idx, 'variables']
        
        # Handle different data types
        if pd.isna(row_vars):
            mask.loc[idx] = False
            continue
        
        if isinstance(row_vars, (list, tuple)):
            # If it's a list/tuple, check directly
            var_list = [str(v).lower() for v in row_vars]
        elif isinstance(row_vars, str):
            # R uses pipe-separated: "var1|var2|var3"
            var_list = [v.strip().lower() for v in re.split(r'\|\s*', str(row_vars)) if v.strip()]
        else:
            # Convert to string and try pipe-separated
            var_str = str(row_vars).lower()
            var_list = [v.strip() for v in re.split(r'\|\s*', var_str) if v.strip()]
        
        # Check if ALL filter variables match
        all_match = True
        for var in variables:
            var_lower = var.lower()
            
            # Check if variable contains '_' (R logic: any underscore means prefix match)
            if '_' in var_lower:
                # Prefix matching - remove trailing underscore if present
                var_prefix = var_lower.rstrip('_')
                matches = any(v.startswith(var_prefix) for v in var_list)
            else:
                # Exact match
                matches = var_lower in var_list
            
            if not matches:
                all_match = False
                break
        
        mask.loc[idx] = all_match
    
    return df[mask].copy()


def _apply_longitudinal_filter(
    df: pd.DataFrame,
    longitudinal: Optional[bool]
) -> pd.DataFrame:
    """Apply longitudinal filter."""
    if longitudinal is None or 'longitudinal' not in df.columns:
        return df
    
    mask = df['longitudinal'] == longitudinal
    return df[mask].copy()


def filter_tables(
    datasets: List,
    n_responses: Optional[Union[float, int, List[Optional[Union[float, int]]]]] = None,
    n_categories: Optional[Union[float, int, List[Optional[Union[float, int]]]]] = None,
    n_participants: Optional[Union[float, int, List[Optional[Union[float, int]]]]] = None,
    n_items: Optional[Union[float, int, List[Optional[Union[float, int]]]]] = None,
    responses_per_participant: Optional[Union[float, int, List[Optional[Union[float, int]]]]] = None,
    responses_per_item: Optional[Union[float, int, List[Optional[Union[float, int]]]]] = None,
    density: Optional[Union[float, int, List[Optional[Union[float, int]]]]] = [0.5, 1],
    var: Optional[Union[str, List[str]]] = None,
    age_range: Optional[Union[str, List[str]]] = None,
    child_age: Optional[Union[str, List[str]]] = None,
    construct_type: Optional[Union[str, List[str]]] = None,
    construct_name: Optional[Union[str, List[str]]] = None,
    sample: Optional[Union[str, List[str]]] = None,
    measurement_tool: Optional[Union[str, List[str]]] = None,
    item_format: Optional[Union[str, List[str]]] = None,
    language: Optional[Union[str, List[str]]] = None,
    longitudinal: Optional[bool] = None,
    license: Optional[Union[str, List[str]]] = None,
) -> pd.Series:
    """
    Filter IRW tables based on metadata criteria.
    
    Returns the names of datasets in the Item Response Warehouse (IRW) that match
    user-specified metadata, tag values, variable presence, and license criteria.
    
    This function only works for the main IRW database (source="main").
    
    Parameters
    ----------
    n_responses : float, int, or list of length 1 or 2, optional
        Filter by total number of responses.
        - Single value: exact match (e.g., n_responses=1000)
        - List of length 2: range [min, max] (e.g., n_responses=[1000, None] for >= 1000)
    
    n_categories : float, int, or list of length 1 or 2, optional
        Filter by number of unique response categories.
        - List of length 2: range [min, max], use None for infinity (e.g., [3, None] for >= 3)
    
    n_participants : float, int, or list of length 1 or 2, optional
        Filter by number of unique participants.
        - List of length 2: range [min, max], use None for infinity (e.g., [500, None] for >= 500)
    
    n_items : float, int, or list of length 1 or 2, optional
        Filter by number of unique items.
        - List of length 2: range [min, max], use None for infinity (e.g., [10, 50] for between 10-50)
    
    responses_per_participant : float, int, or list of length 1 or 2, optional
        Filter by average responses per participant.
        - List of length 2: range [min, max], use None for infinity
    
    responses_per_item : float, int, or list of length 1 or 2, optional
        Filter by average responses per item.
        - List of length 2: range [min, max], use None for infinity
    
    density : float, int, list of length 1 or 2, or None, optional
        Filter by matrix density (proportion of cells with valid responses).
        A density of 1 means every person responded to every item (100% of cells have valid responses).
        Lower density indicates that some individuals did not respond to all items.
        Default is [0.5, 1] to exclude sparse matrices.
        - List of length 2: range [min, max], use None for infinity (e.g., density=[0.5, None] for >= 0.5)
        - Set to None to disable density filtering
    
    var : str or list of str, optional
        Filter datasets by presence of variables.
        - Exact names: "rt", "wave"
        - Prefix matching: "cov_" (matches any variable starting with "cov_")
    
    age_range : str or list of str, optional
        Filter by participant age group (e.g., "Adult (18+)").
    
    child_age : str or list of str, optional
        Filter by child age subgroup.
    
    construct_type : str or list of str, optional
        Filter by high-level construct category (e.g., "Affective/mental health").
    
    construct_name : str or list of str, optional
        Filter by specific construct (e.g., "Big Five").
    
    sample : str or list of str, optional
        Filter by sample type or recruitment method (e.g., "Educational", "Clinical").
    
    measurement_tool : str or list of str, optional
        Filter by instrument type (e.g., "Survey/questionnaire").
    
    item_format : str or list of str, optional
        Filter by item format (e.g., "Likert Scale/selected response").
    
    language : str or list of str, optional
        Filter by language used (e.g., "eng").
    
    longitudinal : bool or None, optional
        Filter longitudinal datasets.
        - True: include only datasets flagged as longitudinal
        - False: exclude datasets flagged as longitudinal
        - None: no filter (default)
    
    license : str or list of str, optional
        Filter datasets by license (e.g., "CC BY 4.0").
    
    Returns
    -------
    pandas.Series
        A sorted Series of dataset names (table names) that match all specified filters.
        Returns empty Series if no matches are found.
    
    Examples
    --------
    >>> from irw_py import IRW
    >>> irw = IRW()  # source="main" by default
    >>> 
    >>> # Numeric filters
    >>> filtered = irw.filter(n_responses=[1000, None], n_items=[10, 50])  # >= 1000 responses, 10-50 items
    >>> 
    >>> # Variable presence
    >>> filtered = irw.filter(var="rt")
    >>> filtered = irw.filter(var=["wave", "cov_"])
    >>> 
    >>> # Tag metadata filtering
    >>> filtered = irw.filter(construct_type="Affective/mental health", sample="Educational")
    >>> 
    >>> # License filtering
    >>> filtered = irw.filter(license="CC BY 4.0")
    >>> 
    >>> # Filter by response category complexity
    >>> filtered = irw.filter(n_categories=2)  # binary
    >>> filtered = irw.filter(n_categories=[3, 5])  # small multi-category
    """
    # Get all tables with metadata
    df = list_tables(datasets)
    
    if df.empty or 'name' not in df.columns:
        return pd.Series([], dtype=str, name='name')
    
    # Apply numeric filters
    df = _apply_numeric_filter(df, 'n_responses', n_responses)
    df = _apply_numeric_filter(df, 'n_categories', n_categories)
    df = _apply_numeric_filter(df, 'n_participants', n_participants)
    df = _apply_numeric_filter(df, 'n_items', n_items)
    df = _apply_numeric_filter(df, 'responses_per_participant', responses_per_participant)
    df = _apply_numeric_filter(df, 'responses_per_item', responses_per_item)
    
    # Apply density filter (with default handling)
    # Default is [0.5, 1] to exclude sparse matrices
    if density is not None:
        df_before_density = df.copy()
        df = _apply_numeric_filter(df, 'density', density)
        
        # Show message if default density removed datasets
        # Check if density equals default [0.5, 1]
        is_default_density = (
            isinstance(density, list) and 
            len(density) == 2 and 
            density[0] == 0.5 and 
            density[1] == 1
        )
        
        num_removed = len(df_before_density) - len(df)
        if is_default_density and num_removed > 0:
            import warnings
            warnings.warn(
                f"Note: Default density filter (0.5-1) removed {num_removed} dataset(s). "
                f"Set density=None to disable.",
                UserWarning,
                stacklevel=2
            )
    
    # Apply variable presence filter
    df = _apply_variable_filter(df, var)
    
    # Apply tag filters (using cleaned-up column names from list_tables)
    if age_range is not None:
        df = _apply_tag_filter(df, 'age_range', age_range)
    if child_age is not None:
        df = _apply_tag_filter(df, 'child_age', child_age)
    if construct_type is not None:
        df = _apply_tag_filter(df, 'construct_type', construct_type)
    if construct_name is not None:
        df = _apply_tag_filter(df, 'construct_name', construct_name)
    if sample is not None:
        df = _apply_tag_filter(df, 'sample', sample)
    if measurement_tool is not None:
        df = _apply_tag_filter(df, 'measurement_tool', measurement_tool)
    if item_format is not None:
        df = _apply_tag_filter(df, 'item_format', item_format)
    if language is not None:
        df = _apply_tag_filter(df, 'language', language)
    
    # Apply longitudinal filter
    df = _apply_longitudinal_filter(df, longitudinal)
    
    # Apply license filter
    df = _apply_tag_filter(df, 'license', license)
    
    # Return sorted Series of table names
    if df.empty:
        return pd.Series([], dtype=str, name='name')
    
    result = df['name'].sort_values().reset_index(drop=True)
    return result

