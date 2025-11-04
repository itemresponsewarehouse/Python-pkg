"""Helper functions for table operations that only need table_name (not data)."""

from typing import Optional, Dict, Any, Union
import pandas as pd
from ..utils.redivis.table_metadata import _table_info
from ..utils.redivis.item_text import _get_itemtext_dataset, _list_itemtext_tables
from ..utils.redivis.tables import _get_table


def _get_table_metadata(table_name: str) -> Dict[str, Any]:
    """Get metadata dictionary for a table by name."""
    # Get combined metadata table
    metadata_df = _table_info()
    
    if metadata_df.empty:
        return {}
    
    # Find metadata for this table (case-insensitive match)
    table_lower = table_name.lower()
    metadata_df['table_lower'] = metadata_df['table'].str.lower()
    table_metadata = metadata_df[metadata_df['table_lower'] == table_lower]
    
    if table_metadata.empty:
        return {}
    
    # Convert first matching row to dict
    row = table_metadata.iloc[0]
    # Convert to dict, excluding table_lower if present
    return {k: v for k, v in row.to_dict().items() if k != 'table_lower'}


def _get_table_info_dict(table_name: str) -> Dict[str, Any]:
    """Get structured info dictionary for a table."""
    metadata_dict = _get_table_metadata(table_name)
    
    if not metadata_dict:
        return {}
    
    # Organize metadata into structured dict
    # Note: column names are raw from merged tables (before RENAME_MAP)
    return {
        'stats': {
            'n_responses': metadata_dict.get('n_responses'),
            'n_categories': metadata_dict.get('n_categories'),
            'n_participants': metadata_dict.get('n_participants'),
            'n_items': metadata_dict.get('n_items'),
            'responses_per_participant': metadata_dict.get('responses_per_participant'),
            'responses_per_item': metadata_dict.get('responses_per_item'),
            'density': metadata_dict.get('density'),
            'variables': metadata_dict.get('variables'),
        },
        'tags': {
            'age_range': metadata_dict.get('age_range'),
            'child_age': metadata_dict.get('child_age__for_child_focused_studies_'),  # Raw column name
            'sample': metadata_dict.get('sample'),
            'construct_type': metadata_dict.get('construct_type'),
            'construct_name': metadata_dict.get('construct_name'),
            'measurement_tool': metadata_dict.get('measurement_tool'),
            'item_format': metadata_dict.get('item_format'),
            'language': metadata_dict.get('primary_language_s_'),  # Raw column name
        },
        'biblio': {
            'description': metadata_dict.get('Description'),  # Raw column name
            'reference': metadata_dict.get('Reference_x'),  # Raw column name
            'doi': metadata_dict.get('DOI__for_paper_'),  # Raw column name
            'url': metadata_dict.get('URL__for_data_'),  # Raw column name
            'license': metadata_dict.get('Derived_License'),  # Raw column name
            'bibtex': metadata_dict.get('BibTex'),  # Raw column name
        },
        'item_text': metadata_dict.get('has_item_text', False),
        'dataset': metadata_dict.get('dataset'),  # Raw column name (no rename needed)
    }


def _get_table_bibtex(table_name: str) -> Optional[str]:
    """Get BibTeX citation for a table by name."""
    metadata_dict = _get_table_metadata(table_name)
    return metadata_dict.get('BibTex') if metadata_dict else None  # Raw column name


def _get_table_itemtext(table_name: str) -> Union[pd.DataFrame, str]:
    """Get item-level text for a table by name."""
    # Check itemtext availability using cached itemtext_tables
    available_tables = _list_itemtext_tables()
    if table_name.lower() not in available_tables:
        return f"Item-level text is not available for table '{table_name}'."
    
    # Item text is available, fetch it
    try:
        # Get itemtext dataset
        itemtext_dataset = _get_itemtext_dataset()
        
        # Item text tables are named as "{table_name}__items"
        itemtext_table_name = f"{table_name}__items"
        
        # Get the table
        itemtext_table = _get_table(itemtext_dataset, itemtext_table_name)
        
        # Fetch as DataFrame
        itemtext_df = itemtext_table.to_pandas_dataframe()
        
        return itemtext_df
        
    except Exception:
        # If fetching fails, return message
        return f"Item-level text is not available for table '{table_name}'."


def _format_table_info(table_name: str, info_dict: Dict[str, Any]) -> str:
    """Format table info as a string."""
    lines = []
    lines.append(f"\n{'='*60}")
    lines.append(f"IRW Table: {table_name}")
    lines.append(f"{'='*60}")
    
    # Description section (if available)
    biblio = info_dict.get('biblio', {})
    description = biblio.get('description')
    if description:
        lines.append("\nDescription:")
        lines.append("-" * 60)
        # Wrap long descriptions
        desc_lines = str(description).split('\n')
        for line in desc_lines:
            if len(line) <= 60:
                lines.append(f"  {line}")
            else:
                # Word wrap for long lines
                words = line.split()
                current_line = []
                current_length = 0
                for word in words:
                    if current_length + len(word) + 1 <= 56:  # 60 - 4 for "  "
                        current_line.append(word)
                        current_length += len(word) + 1
                    else:
                        if current_line:
                            lines.append(f"  {' '.join(current_line)}")
                        current_line = [word]
                        current_length = len(word)
                if current_line:
                    lines.append(f"  {' '.join(current_line)}")
    
    # Stats section
    stats = info_dict.get('stats', {})
    lines.append("\nSummary Statistics:")
    lines.append("-" * 60)
    n_responses = stats.get('n_responses')
    lines.append(f"  Responses: {n_responses:,}" if n_responses is not None else "  Responses: N/A")
    n_participants = stats.get('n_participants')
    lines.append(f"  Participants: {n_participants:,}" if n_participants is not None else "  Participants: N/A")
    n_items = stats.get('n_items')
    lines.append(f"  Items: {n_items:,}" if n_items is not None else "  Items: N/A")
    n_categories = stats.get('n_categories')
    lines.append(f"  Categories: {n_categories}" if n_categories is not None else "  Categories: N/A")
    density = stats.get('density')
    lines.append(f"  Density: {density:.3f}" if density is not None else "  Density: N/A")
    responses_per_participant = stats.get('responses_per_participant')
    lines.append(f"  Responses per participant: {responses_per_participant:.2f}" if responses_per_participant is not None else "  Responses per participant: N/A")
    responses_per_item = stats.get('responses_per_item')
    lines.append(f"  Responses per item: {responses_per_item:.2f}" if responses_per_item is not None else "  Responses per item: N/A")
    variables = stats.get('variables')
    lines.append(f"  Variables: {variables}" if variables else "  Variables: N/A")
    
    # Tags section
    tags = info_dict.get('tags', {})
    lines.append("\nMeasurement Information:")
    lines.append("-" * 60)
    construct_type = tags.get('construct_type')
    lines.append(f"  Construct Type: {construct_type}" if construct_type else "  Construct Type: N/A")
    construct_name = tags.get('construct_name')
    lines.append(f"  Construct Name: {construct_name}" if construct_name else "  Construct Name: N/A")
    sample = tags.get('sample')
    lines.append(f"  Sample: {sample}" if sample else "  Sample: N/A")
    age_range = tags.get('age_range')
    lines.append(f"  Age Range: {age_range}" if age_range else "  Age Range: N/A")
    child_age = tags.get('child_age')
    lines.append(f"  Child Age: {child_age}" if child_age else "  Child Age: N/A")
    measurement_tool = tags.get('measurement_tool')
    lines.append(f"  Measurement Tool: {measurement_tool}" if measurement_tool else "  Measurement Tool: N/A")
    item_format = tags.get('item_format')
    lines.append(f"  Item Format: {item_format}" if item_format else "  Item Format: N/A")
    language = tags.get('language')
    lines.append(f"  Language: {language}" if language else "  Language: N/A")
    
    # Biblio section
    biblio = info_dict.get('biblio', {})
    lines.append("\nBibliography:")
    lines.append("-" * 60)
    reference = biblio.get('reference')
    lines.append(f"  Reference: {reference}" if reference else "  Reference: N/A")
    doi = biblio.get('doi')
    lines.append(f"  DOI: {doi}" if doi else "  DOI: N/A")
    url = biblio.get('url')
    lines.append(f"  URL: {url}" if url else "  URL: N/A")
    license = biblio.get('license')
    lines.append(f"  License: {license}" if license else "  License: N/A")
    bibtex = biblio.get('bibtex')
    if bibtex:
        lines.append(f"  BibTeX: Available (use save_bibtex() to save)")
    else:
        lines.append(f"  BibTeX: N/A")
    
    # Additional info
    lines.append("\nAdditional Information:")
    lines.append("-" * 60)
    if info_dict.get('item_text'):
        lines.append("  Item-level text: Available (use itemtext() to retrieve)")
    else:
        lines.append("  Item-level text: Not available")
    dataset = info_dict.get('dataset')
    lines.append(f"  Source Dataset: {dataset}" if dataset else "  Source Dataset: N/A")
    
    lines.append(f"\n{'='*60}\n")
    
    return "\n".join(lines)

