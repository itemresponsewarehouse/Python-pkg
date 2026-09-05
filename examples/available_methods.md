# Available Methods for irw

## Database Operations

- **`list_tables(source="main", include_metadata=False)`** - List available tables
  - `source`: `"main"`, `"sim"`, `"comp"` or `"nom"` (nominal responses)
- **`filter(...)`** - Filter tables by metadata criteria (main IRW only)
- **`info()`** - Print database information summary
- **`info(table_name, return_dict=False)`** - Get metadata information for a table
- **`get_filters()`** - Get list of available filter parameter names
  - Returns: `list[str]`
- **`describe_filter(filter_name)`** - Describe a filter and show available values
- **`list_tables_with_itemtext()`** - List tables with item-level text available

## Table Operations

- **`fetch(table_name, source="main", dedup=False, wide=False)`** - Fetch one or more IRW tables
  - `source`: `"main"`, `"sim"`, `"comp"` or `"nom"` (nominal responses)
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


## Collections

```python
import irw

irw.collections()                      # 22 collections: kind, definition, coverage, n_tables
irw.collections(kind="instrument")

tabs = irw.collection("depression")    # -> list of table names, prints coverage
dat  = irw.fetch(tabs)                 # dict keyed by table name

irw.filter(collection="rct")                                  # 178
irw.filter(collection="depression", n_participants=[500, None])
irw.filter(collection=["rct", "response_time"])               # union, not intersection

# Cross-collection: math assessments administered in booklets
sorted(set(irw.collection("math", quiet=True)) &
       set(irw.collection("multistage", quiet=True)))

irw.collection_members(tables="frac20")   # what collections is this table in?
```

Note the `coverage` column. Collections derived from tags searched only ~62% of
tables, so they are not exhaustive — `irw.collection()` says so when that
applies.
