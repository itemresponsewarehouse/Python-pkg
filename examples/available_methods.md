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
- **`collections(kind=None)`** - List IRW collections: labelled groupings of tables
  - Returns: `pandas.DataFrame` with `kind`, `definition`, `coverage`, `n_tables`
- **`collection(name, quiet=False)`** - Get the table names in one collection
  - Returns: `list[str]`; prints the collection's coverage unless `quiet=True`
- **`collection_members(tables=None, collection=None)`** - Look up collection membership
  - By table (which collections is this table in?) or by collection
- **`version(date=None)`** - Which IRW version is live, and the Redivis version of every dataset in it
  - `version()` - the newest IRW version and its dataset pins
  - `version("2026-08-01")` - what was live on that date
  - Returns: `pandas.DataFrame`; the IRW version number is in `.attrs["irw_version"]`

See `## Collections` below for worked examples of the collection methods.

## Table Operations

- **`fetch(table_name, source="main", dedup=False, wide=False)`** - Fetch one or more IRW tables
  - `source`: `"main"`, `"sim"`, `"comp"` or `"nom"` (nominal responses)
  - Single table → returns `pandas.DataFrame`
  - Multiple tables → returns `dict[str, pandas.DataFrame]`
  - `wide=True`: Automatically convert to wide-format response matrix

- **`table_sets(table_name, source="main", per_item=False)`** - Item set, response set and row count of a table, without downloading it
  - Runs server-side aggregate queries, so it does not count against the Redivis export quota
  - Returns `dict` with `table`, `n_rows`, `items`, `resp`, and `per_item` (a `pandas.DataFrame` when `per_item=True`)
  - `"NA"` and empty responses are excluded from `resp`, matching `fetch()`

- **`itemtext(table_name)`** - Get item-level text
- **`save_bibtex(table_names, output_file=None)`** - Get/save BibTeX citations for one or more tables
  - Supports single table name or list of table names
  - If `output_file` is provided, saves to file. Otherwise, returns entries.
  - Automatically fetches from bibliography table or DOI if needed
  - Updates BibTeX keys to match table names
  - Returns list of saved BibTeX entries
- **`download(table_name, path=None, overwrite=False)`** - Download table using Redivis's native download
- **`long2resp(df, wave=None, id_density_threshold=0.1, agg_method=None, check_resp=False, resp_col="resp")`** - Convert long-format DataFrame to response matrix
  - Takes a DataFrame (from `fetch()`) and converts to wide format
  - `resp_col`: the response column, e.g. `"text"` for nominal labels
  - `check_resp=True`: returns `(wide, checks)`, with `checks` as `check_resp()` returns it
- **`resp2long(x, id=True)`** - Convert a wide response matrix (DataFrame or array) back to long format
  - `id=False`: generate positional ids `1..n` for a matrix with no `id` column
- **`check_resp(x, min_count=5, min_prop=0.01, resp_col="resp")`** - Flag single-category items and sparse categories in long data
  - Returns: `dict` with `single_category_items` (list) and `sparse_category_items` (dict of DataFrames)
- **`recode(df, cols=("id", "item"), prefix=None)`** - Replace identifiers with short codes (`P0001`, `I0001`)
  - Returns: `(DataFrame, key)`; codes are only meaningful relative to their key
- **`decode(df, key, cols=None)`** - Restore original identifiers, in long format or `long2resp()` output
- **`covariates(df, cols=None, align=None)`** - Person-level columns, one row per `id`
  - `cols=None` detects columns that take one value within every id, and names the ones it rejects
  - `align=long2resp(df)` puts the rows in the matrix's order; ids absent from `df` give NA rows

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
