# Available Methods for irw

## Database Operations

- **`list_tables(source="main", include_metadata=False)`** - List available tables
- **`filter(...)`** - Filter tables by metadata criteria (main IRW only)
- **`info()`** - Print database information summary
- **`info(table_name, return_dict=False)`** - Get metadata information for a table
- **`get_filters()`** - Get list of available filter parameter names
  - Returns: `list[str]`
- **`describe_filter(filter_name)`** - Describe a filter and show available values
- **`list_tables_with_itemtext()`** - List tables with item-level text available

## Table Operations

- **`fetch(table_name, source="main", dedup=False, wide=False)`** - Fetch one or more IRW tables
  - Single table → returns `pandas.DataFrame`
  - Multiple tables → returns `dict[str, pandas.DataFrame]`
  - `wide=True`: Automatically convert to wide-format response matrix

- **`itemtext(table_name)`** - Get item-level text
- **`save_bibtex(table_names, output_file=None)`** - Get/save BibTeX citations for one or more tables
  - Supports single table name or list of table names
  - If `output_file` is provided, saves to file. Otherwise, returns entries.
  - Automatically fetches from bibliography table or DOI if needed
  - Updates BibTeX keys to match table names
  - Returns list of saved BibTeX entries
- **`download(table_name, path=None, overwrite=False)`** - Download table using Redivis's native download
- **`long2resp(df, wave=None, id_density_threshold=0.1, agg_method="mean")`** - Convert long-format DataFrame to response matrix
  - Takes a DataFrame (from `fetch()`) and converts to wide format

## Example Workflow
See `examples/example.py` for a complete workflow example.
