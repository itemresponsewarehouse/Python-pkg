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
