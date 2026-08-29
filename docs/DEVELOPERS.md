# Developer notes

Internal documentation for contributors to the `irw` Python package.

## Adding a main IRW Redivis warehouse

Main IRW response tables can span multiple Redivis datasets ("warehouses"). To add another one (e.g. a 7th warehouse), **only update `MAIN_REFS` in `src/irw/config.py`**:

```python
MAIN_REFS: ClassVar[Tuple[Tuple[str, str], ...]] = (
    ("datapages", "item_response_warehouse:as2e"),
    ("datapages", "item_response_warehouse_2:epbx"),
    ("datapages", "item_response_warehouse_3:5xaj"),
    ("datapages", "item_response_warehouse_4:980f"),
    ("datapages", "item_response_warehouse_5:3ykx"),
    ("datapages", "item_response_warehouse_6:xxxx"),  # new warehouse
)
```

Each entry is `(redivis_user, dataset_ref)`, where `dataset_ref` is the Redivis dataset slug (e.g. `item_response_warehouse_6:xxxx`).

No other code changes are needed. The package automatically:

- initializes every warehouse listed in `MAIN_REFS`
- lists tables from all of them (`list_tables`, `filter`, `info`, etc.)
- searches them **newest first** when fetching or downloading (`fetch`,
  `download`), so a table present in more than one warehouse resolves to its
  most recent copy. List `MAIN_REFS` oldest-to-newest; `_order_main_datasets()`
  reverses it. This matches the R package's `.irw_order_datasources()`.
- skips a warehouse it cannot open, with a logged warning, instead of failing
  every lookup. A newly created warehouse has no released version yet and is
  unreadable with a read-only token, so **publish a release on Redivis before
  shipping a new `MAIN_REFS` entry** or its tables will silently be missing. If
  no warehouse opens at all, `_init_datasets_from_refs` raises.

### Verify the change

```bash
pip install pytest

# Fast, offline checks (mocked Redivis)
python -m pytest tests/test_main_refs.py -v

# Live Redivis checks (network + auth required)
RUN_REDIVIS_TESTS=1 python -m pytest tests/test_redivis_integration.py -v
```

After releasing or sharing the update, ask users to **restart their Python session** so in-process caches pick up the new warehouse list.


## Collections (issue #1633)

Labelled groupings of IRW tables — study designs (`rct`, `q_matrix`), instrument
families (`big_five`), constructs (`depression`). A table can be in several.

Two Redivis tables in `irw_meta`, registered in `config.py`'s `META_TABLES`:
`collections:va83` (the registry, one row per collection) and
`collection_members:j7rp` (long, one row per `(table, collection)`).

Public surface: `irw.collections()`, `irw.collection(name)`,
`irw.collection_members()`, and `collection=` on `irw.filter()`.

Three things to know before changing any of it:

- **`get_collections_table()` is deliberately not filtered to existing tables.**
  The registry has no `table` column; filtering would empty it.
- **Membership is joined into `_table_info()` as a *list* column**, not merged
  directly — a plain merge on a long table multiplies rows per table.
  `_apply_tag_filter` handles list values, so no new filter primitive exists.
  (It did not handle them until 2026-08-29: `pd.isna()` raises on a list, which
  made the `isinstance(row_value, (list, tuple))` branch unreachable. Fixed;
  `tests/test_collections.py` pins both the list and string paths.)
- **`n_tables` is recomputed** in `irw.collections()` from live membership
  rather than read from the published registry column, which is a build-time
  count taken before live filtering.

Adding a collection needs no change here at all — it is one line in
`src/collections/registry.csv` in the main repo. That is the point of the long
format. See `Rpkg/inst/developer/collections.md`.
