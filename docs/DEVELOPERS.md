# Developer notes

Internal documentation for contributors to the `irw` Python package.

## Adding a main IRW Redivis warehouse

Main IRW response tables can span multiple Redivis datasets ("warehouses"). To add another one (e.g. a 4th warehouse), **only update `MAIN_REFS` in `src/irw/config.py`**:

```python
MAIN_REFS: ClassVar[Tuple[Tuple[str, str], ...]] = (
    ("datapages", "item_response_warehouse:as2e"),
    ("datapages", "item_response_warehouse_2:epbx"),
    ("datapages", "item_response_warehouse_3:5xaj"),
    ("datapages", "item_response_warehouse_4:xxxx"),  # new warehouse
)
```

Each entry is `(redivis_user, dataset_ref)`, where `dataset_ref` is the Redivis dataset slug (e.g. `item_response_warehouse_4:xxxx`).

No other code changes are needed. The package automatically:

- initializes every warehouse listed in `MAIN_REFS`
- lists tables from all of them (`list_tables`, `filter`, `info`, etc.)
- searches them in order when fetching or downloading (`fetch`, `download`)

### Verify the change

```bash
pip install pytest

# Fast, offline checks (mocked Redivis)
python -m pytest tests/test_main_refs.py -v

# Live Redivis checks (network + auth required)
RUN_REDIVIS_TESTS=1 python -m pytest tests/test_redivis_integration.py -v
```

After releasing or sharing the update, ask users to **restart their Python session** so in-process caches pick up the new warehouse list.
