"""Functions to explore available filter options."""

from typing import List, Dict, Any, Optional
import pandas as pd
from ..utils.redivis.table_metadata import get_metadata_table, get_tags_table, get_biblio_table
from ..operations.list_tables import _build_base_table_list


# Filter descriptions
FILTER_DESCRIPTIONS = {
    'n_responses': 'Total number of responses in the dataset',
    'n_categories': 'Number of unique response categories',
    'n_participants': 'Number of unique participants (id)',
    'n_items': 'Number of unique items',
    'responses_per_participant': 'Average number of responses per participant',
    'responses_per_item': 'Average number of responses per item',
    'density': 'Matrix density (proportion of non-missing values)',
    'var': 'Variable presence filter - filter by presence of specific variables',
    'age_range': 'Participant age group (e.g., "Adult (18+)")',
    'child_age': 'Child age subgroup (for child-focused studies)',
    'construct_type': 'High-level construct category (e.g., "Affective/mental health")',
    'construct_name': 'Specific construct name (e.g., "Big Five")',
    'sample': 'Sample type or recruitment method (e.g., "Educational", "Clinical")',
    'measurement_tool': 'Instrument type (e.g., "Survey/questionnaire")',
    'item_format': 'Item format (e.g., "Likert Scale/selected response")',
    'language': 'Primary language used (e.g., "eng")',
    'longitudinal': 'Whether the dataset is longitudinal (has wave or date variables)',
    'license': 'Dataset license type (e.g., "CC BY 4.0")'
}


def get_filters() -> List[str]:
    """
    Get the list of available filter argument names for the filter() method.
    
    Returns
    -------
    list of str
        List of all available filter parameter names that can be used with filter().
        
    Examples
    --------
    >>> from irw_py import IRW
    >>> irw = IRW()
    >>> 
    >>> # See what filters are available
    >>> filters = irw.get_filters()
    >>> print(filters)
    """
    return list(FILTER_DESCRIPTIONS.keys())


def describe_filter(datasets: List, filter_name: str) -> Optional[Dict[str, Any]]:
    """
    Describe a specific filter and show its available values/statistics.
    
    For numeric filters, returns summary statistics (min, max, mean, median, etc.).
    For categorical filters, returns value_counts Series.
    For special filters (var, longitudinal), returns descriptive information.
    
    **Performance Note:** This function only loads the necessary metadata (stats, tags, or
    bibliography) based on the filter type. For example, requesting 'n_responses' only loads
    the stats table, not tags or bibliography. All loaded data is cached for subsequent calls.
    If you plan to explore multiple filters across different types, you can also call
    list_tables(include_metadata=True) once and then access the DataFrame columns directly.
    
    Parameters
    ----------
    datasets : List
        Redivis dataset objects (typically from IRW._datasets).
    filter_name : str
        Name of the filter to describe (e.g., 'n_responses', 'construct_type', 'var').
        
    Returns
    -------
    dict or None
        Dictionary with keys:
        - 'description': str describing what the filter does
        - 'info': Additional information about available values:
                  - For numeric filters: dict with summary statistics (min, max, mean, median, etc.)
                  - For categorical filters: pandas.Series with value_counts
                  - For special filters: dict with descriptive information
        - None if filter not found or not available
        
    Examples
    --------
    >>> from irw_py import IRW
    >>> irw = IRW()
    >>> 
    >>> # Describe numeric filter (only loads stats metadata)
    >>> info = irw.describe_filter('n_responses')
    >>> print(info['description'])  # What the filter does
    >>> print(info['info'])  # Summary statistics (min, max, mean, etc.)
    >>> 
    >>> # Describe categorical filter (only loads tags metadata)
    >>> info = irw.describe_filter('construct_type')
    >>> print(info['description'])  # What the filter does
    >>> print(info['info'])  # value_counts Series
    >>> 
    >>> # Describe variable filter
    >>> info = irw.describe_filter('var')
    >>> print(info['description'])
    >>> print(info['info'])  # Descriptive information
    """
    # Check if filter name is valid
    if filter_name not in FILTER_DESCRIPTIONS:
        return None
    
    description = FILTER_DESCRIPTIONS[filter_name]
    result = {'description': description}
    
    # Get base table list (needed for merging)
    base = _build_base_table_list(datasets)
    if base.empty:
        return None
    
    # Numeric filters - only need stats/metadata table
    numeric_filters = [
        'n_responses',
        'n_participants',
        'n_items',
        'responses_per_participant',
        'responses_per_item',
        'density'
    ]
    
    if filter_name in numeric_filters:
        metadata_df = get_metadata_table()
        if metadata_df.empty:
            return None
        
        # Merge with base to get all tables
        metadata_df['name_lower'] = metadata_df['table'].str.lower()
        base['name_lower'] = base['name'].str.lower()
        merged = base.merge(metadata_df, left_on='name_lower', right_on='name_lower', how='left')
        
        if filter_name not in merged.columns:
            return None
        series = merged[filter_name].dropna()
        if series.empty:
            return None
        result['info'] = {
            'min': float(series.min()),
            'max': float(series.max()),
            'mean': float(series.mean()),
            'median': float(series.median()),
            'std': float(series.std()),
            'count': len(series),
            'non_null_count': len(series),
            'null_count': len(merged) - len(series)
        }
        return result
    
    # n_categories is in tags table, not metadata table
    if filter_name == 'n_categories':
        tags_df = get_tags_table()
        if tags_df.empty:
            return None
        
        # Merge with base
        tags_df['name_lower'] = tags_df['table'].str.lower()
        base['name_lower'] = base['name'].str.lower()
        merged = base.merge(tags_df, left_on='name_lower', right_on='name_lower', how='left')
        
        if 'n_categories' not in merged.columns:
            return None
        series = merged['n_categories'].dropna()
        if series.empty:
            return None
        result['info'] = {
            'min': float(series.min()),
            'max': float(series.max()),
            'mean': float(series.mean()),
            'median': float(series.median()),
            'std': float(series.std()),
            'count': len(series),
            'non_null_count': len(series),
            'null_count': len(merged) - len(series)
        }
        return result
    
    # Categorical/tag filters - only need tags table
    categorical_filters = [
        'construct_type',
        'construct_name',
        'age_range',
        'child_age',
        'sample',
        'item_format',
        'measurement_tool',
        'language'
    ]
    
    if filter_name in categorical_filters:
        tags_df = get_tags_table()
        if tags_df.empty:
            return None
        
        # Handle column name mapping
        col_name = filter_name
        if filter_name == 'child_age':
            col_name = 'child_age__for_child_focused_studies_'
        elif filter_name == 'language':
            col_name = 'primary_language_s_'
        
        # Merge with base
        tags_df['name_lower'] = tags_df['table'].str.lower()
        base['name_lower'] = base['name'].str.lower()
        merged = base.merge(tags_df, left_on='name_lower', right_on='name_lower', how='left')
        
        if col_name not in merged.columns:
            return None
        value_counts = merged[col_name].value_counts(dropna=True)
        if value_counts.empty:
            return None
        # Sort by count (descending), then by value (ascending)
        result_df = value_counts.reset_index()
        result_df.columns = ['value', 'count']
        result_df = result_df.sort_values(['count', 'value'], ascending=[False, True])
        result['info'] = pd.Series(result_df['count'].values, index=result_df['value'].values, name=filter_name)
        return result
    
    # License filter - only need bibliography table
    if filter_name == 'license':
        biblio_df = get_biblio_table()
        if biblio_df.empty:
            return None
        
        # Merge with base
        biblio_df['name_lower'] = biblio_df['table'].str.lower()
        base['name_lower'] = base['name'].str.lower()
        merged = base.merge(biblio_df, left_on='name_lower', right_on='name_lower', how='left')
        
        if 'Derived_License' not in merged.columns:
            return None
        value_counts = merged['Derived_License'].value_counts(dropna=True)
        if value_counts.empty:
            return None
        # Sort by count (descending), then by value (ascending)
        result_df = value_counts.reset_index()
        result_df.columns = ['value', 'count']
        result_df = result_df.sort_values(['count', 'value'], ascending=[False, True])
        result['info'] = pd.Series(result_df['count'].values, index=result_df['value'].values, name='license')
        return result
    
    # Special filters
    if filter_name == 'var':
        # Need tags table for variables column
        tags_df = get_tags_table()
        if tags_df.empty:
            return None
        
        tags_df['name_lower'] = tags_df['table'].str.lower()
        base['name_lower'] = base['name'].str.lower()
        merged = base.merge(tags_df, left_on='name_lower', right_on='name_lower', how='left')
        
        if 'variables' not in merged.columns:
            return None
        has_vars = merged['variables'].notna().sum()
        result['info'] = {
            'available': has_vars > 0,
            'datasets_with_variables': int(has_vars),
            'total_datasets': len(merged),
            'usage': 'Filter by variable presence. Use exact names (e.g., "rt", "wave") or prefix matching (e.g., "cov_"). All specified variables must be present (AND logic).'
        }
        return result
    
    if filter_name == 'longitudinal':
        # Need metadata table for longitudinal flag
        metadata_df = get_metadata_table()
        if metadata_df.empty:
            return None
        
        metadata_df['name_lower'] = metadata_df['table'].str.lower()
        base['name_lower'] = base['name'].str.lower()
        merged = base.merge(metadata_df, left_on='name_lower', right_on='name_lower', how='left')
        
        if 'longitudinal' not in merged.columns:
            return None
        long_counts = merged['longitudinal'].value_counts(dropna=True)
        result['info'] = {
            'available': True,
            'longitudinal_count': int(long_counts.get(True, 0)),
            'non_longitudinal_count': int(long_counts.get(False, 0)),
            'total': len(merged),
            'usage': 'Filter by longitudinal status. Use True (only longitudinal), False (exclude longitudinal), or None (no filter).'
        }
        return result
    
    return None
