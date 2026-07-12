"""Live Redivis integration tests (require network + Redivis auth).

Run manually:
    RUN_REDIVIS_TESTS=1 python -m pytest tests/test_redivis_integration.py -v
"""

import os
import warnings

import pytest

pytestmark = pytest.mark.skipif(
    not os.environ.get("RUN_REDIVIS_TESTS"),
    reason="Set RUN_REDIVIS_TESTS=1 to run live Redivis integration tests.",
)

import irw
from irw.config import MAIN_REFS
from irw.utils.redivis.cache import metadata_cache
from irw.utils.redivis.datasets import _init_main_datasets


@pytest.fixture(autouse=True)
def clear_cache():
    metadata_cache.clear()
    yield
    metadata_cache.clear()


def test_all_main_warehouses_initialize():
    datasets = _init_main_datasets()
    assert len(datasets) == len(MAIN_REFS)
    ids = [getattr(ds, "_id", None) for ds in datasets]
    assert ids == [ref for _, ref in MAIN_REFS]


def test_list_tables_includes_all_warehouses():
    per_warehouse = {}
    for ds in _init_main_datasets():
        short = getattr(ds, "_id", "").split(":")[0]
        per_warehouse[short] = {t.name for t in ds.list_tables()}

    listed = set(irw.list_tables()["name"])
    union = set().union(*per_warehouse.values())
    assert listed == union
    assert len(listed) > 0


def test_fetch_table_from_warehouse_one():
    df = irw.fetch("COACH_Chen_2022_PHQ9")
    assert df is not None
    assert len(df) > 0
    assert {"id", "item", "resp"}.issubset(df.columns)


def test_fetch_table_unique_to_warehouse_three():
    datasets = _init_main_datasets()
    w1, w2, w3 = [{t.name for t in ds.list_tables()} for ds in datasets]
    only_w3 = w3 - w1 - w2
    if not only_w3:
        pytest.skip("No warehouse-3-only tables available to test.")

    table_name = sorted(only_w3)[0]
    df = irw.fetch(table_name)
    assert df is not None
    assert len(df) > 0


def test_fetch_missing_table_warns_cleanly():
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        result = irw.fetch("irw_integration_test_table_does_not_exist_xyz")

    assert result is None
    assert len(caught) == 1
    msg = str(caught[0].message)
    assert msg == "Table 'irw_integration_test_table_does_not_exist_xyz' does not exist in the IRW database."
    assert "warehouse" not in msg.lower()
