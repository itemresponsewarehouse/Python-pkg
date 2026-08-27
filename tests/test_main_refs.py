"""Tests for config-driven main warehouse discovery (no Redivis network calls)."""

from unittest.mock import MagicMock, patch

import pytest

from irw.config import MAIN_REFS
from irw.utils.redivis.cache import metadata_cache
from irw.utils.redivis.datasets import (
    _datasets_cache_key,
    _init_datasets_from_refs,
    _init_main_datasets,
    _main_datasets_cache_key,
    _order_main_datasets,
)
from irw.utils.redivis.tables import _classify_error, _retry_transient, _search_datasets


@pytest.fixture(autouse=True)
def clear_cache():
    metadata_cache.clear()
    yield
    metadata_cache.clear()


def test_main_refs_cache_key_grows_with_each_warehouse(monkeypatch):
    monkeypatch.setattr(
        "irw.utils.redivis.datasets.MAIN_REFS",
        (
            ("datapages", "item_response_warehouse:as2e"),
            ("datapages", "item_response_warehouse_2:epbx"),
            ("datapages", "item_response_warehouse_3:5xaj"),
            ("datapages", "item_response_warehouse_4:abcd"),
        ),
    )
    key = _main_datasets_cache_key()
    assert "item_response_warehouse_4:abcd" in key
    assert key != _datasets_cache_key([])


@patch("irw.utils.redivis.datasets._init_dataset")
def test_init_main_datasets_uses_every_main_ref(mock_init_dataset):
    refs = (
        ("user_a", "warehouse_a:aaa"),
        ("user_b", "warehouse_b:bbb"),
        ("user_c", "warehouse_c:ccc"),
        ("user_d", "warehouse_d:ddd"),
    )
    mock_init_dataset.side_effect = [MagicMock(_id=ref) for _, ref in refs]

    with patch("irw.utils.redivis.datasets.MAIN_REFS", refs):
        datasets = _init_main_datasets()

    assert len(datasets) == 4
    assert mock_init_dataset.call_args_list == [((user, ref),) for user, ref in refs]


def test_init_datasets_from_refs_returns_one_handle_per_ref():
    refs = (("u1", "ds1:a"), ("u2", "ds2:b"))
    with patch("irw.utils.redivis.datasets._init_dataset") as mock_init:
        mock_init.side_effect = [MagicMock(_id=ref) for _, ref in refs]
        datasets = _init_datasets_from_refs(refs)
    assert len(datasets) == 2
    assert mock_init.call_count == 2


def test_search_datasets_tries_each_warehouse_until_success():
    first = MagicMock(_id="warehouse_1:a")
    second = MagicMock(_id="warehouse_2:b")
    third = MagicMock(_id="warehouse_3:c")
    calls = []

    def callback(ds):
        calls.append(getattr(ds, "_id"))
        if ds is third:
            return "found"
        raise Exception("Not found: missing table")

    result, last_other, invalid_request = _search_datasets([first, second, third], callback)

    assert result == "found"
    assert last_other is None
    assert invalid_request is None
    assert calls == ["warehouse_1:a", "warehouse_2:b", "warehouse_3:c"]


def test_search_datasets_returns_none_when_missing_everywhere():
    datasets = [MagicMock(_id="warehouse_1:a"), MagicMock(_id="warehouse_2:b")]

    def callback(_ds):
        raise Exception("Not found: missing table")

    result, last_other, invalid_request = _search_datasets(datasets, callback)

    assert result is None
    assert last_other is None
    assert invalid_request is None


def test_config_main_refs_is_non_empty_tuple():
    assert isinstance(MAIN_REFS, tuple)
    assert len(MAIN_REFS) >= 1
    assert all(len(ref) == 2 for ref in MAIN_REFS)


def test_classify_truncated_redivis_read_as_transient():
    err = Exception(
        "Expected to be able to read 159752 bytes for message body, got 12443"
    )
    assert _classify_error(err) == "transient"


def test_retry_transient_retries_then_succeeds(monkeypatch):
    calls = {"n": 0}

    def flaky():
        calls["n"] += 1
        if calls["n"] < 3:
            raise Exception("Expected to be able to read 100 bytes for message body, got 10")
        return "ok"

    monkeypatch.setattr("irw.utils.redivis.tables.time.sleep", lambda _s: None)
    assert _retry_transient(flaky, max_attempts=3) == "ok"
    assert calls["n"] == 3



# --- search order: newest warehouse first (matches the R package) -------------


def test_order_main_datasets_reverses_to_newest_first():
    oldest, middle, newest = (
        MagicMock(_id="item_response_warehouse:as2e"),
        MagicMock(_id="item_response_warehouse_5:3ykx"),
        MagicMock(_id="item_response_warehouse_6:fpe6"),
    )
    ordered = _order_main_datasets([oldest, middle, newest])
    assert [ds._id for ds in ordered] == [
        "item_response_warehouse_6:fpe6",
        "item_response_warehouse_5:3ykx",
        "item_response_warehouse:as2e",
    ]


@patch("irw.utils.redivis.datasets._init_dataset")
def test_init_main_datasets_returns_newest_first(mock_init_dataset):
    refs = (
        ("datapages", "warehouse_a:aaa"),
        ("datapages", "warehouse_b:bbb"),
        ("datapages", "warehouse_c:ccc"),
    )
    mock_init_dataset.side_effect = [MagicMock(_id=ref) for _, ref in refs]

    with patch("irw.utils.redivis.datasets.MAIN_REFS", refs):
        datasets = _init_main_datasets()

    # Opened oldest-to-newest (config order) but searched newest-first.
    assert mock_init_dataset.call_args_list == [((u, r),) for u, r in refs]
    assert [ds._id for ds in datasets] == [
        "warehouse_c:ccc",
        "warehouse_b:bbb",
        "warehouse_a:aaa",
    ]


def test_duplicate_table_resolves_to_newest_warehouse():
    """A table in two warehouses must come from the newer one."""
    old_ds = MagicMock(_id="warehouse_old:aaa")
    new_ds = MagicMock(_id="warehouse_new:bbb")

    def callback(ds):
        return f"rows-from-{ds._id}"

    result, _, _ = _search_datasets(_order_main_datasets([old_ds, new_ds]), callback)
    assert result == "rows-from-warehouse_new:bbb"


# --- an unreleased/unreadable warehouse must not break every lookup -----------


def test_init_datasets_from_refs_skips_unavailable_warehouse(caplog):
    refs = (("datapages", "warehouse_ok:aaa"), ("datapages", "warehouse_unreleased:bbb"))

    def fake_init(user, ref):
        if "unreleased" in ref:
            raise Exception("[403 insufficient_scope] missing required scope: data.edit")
        return MagicMock(_id=ref)

    with patch("irw.utils.redivis.datasets._init_dataset", side_effect=fake_init):
        datasets = _init_datasets_from_refs(refs, skip_unavailable=True)

    assert [ds._id for ds in datasets] == ["warehouse_ok:aaa"]
    assert "warehouse_unreleased:bbb" in caplog.text


def test_init_datasets_from_refs_raises_when_no_warehouse_opens():
    refs = (("datapages", "warehouse_a:aaa"), ("datapages", "warehouse_b:bbb"))

    with patch(
        "irw.utils.redivis.datasets._init_dataset",
        side_effect=Exception("unauthorized: bad token"),
    ):
        with pytest.raises(RuntimeError, match="No IRW warehouse could be opened"):
            _init_datasets_from_refs(refs, skip_unavailable=True)


def test_init_datasets_from_refs_still_raises_by_default():
    """Non-main sources (sim/comp/nom) have a single dataset; failure is fatal."""
    refs = (("bdomingu", "irw_simsyn:0btg"),)

    with patch(
        "irw.utils.redivis.datasets._init_dataset", side_effect=Exception("boom")
    ):
        with pytest.raises(Exception, match="boom"):
            _init_datasets_from_refs(refs)
