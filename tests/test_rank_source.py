"""Offline coverage for the `rank` (rankings) source.

Rpkg gained source = "rank" in itemresponsewarehouse/Rpkg#198; the pipeline's
parity check (ben-domingue/irw metadata/check_config_parity.py) requires this
package to declare the same dataset. No network: _init_dataset is patched.
Standard: ben-domingue/irw data/rankings/README.md (tracking issue #3024).
"""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

import irw
from irw.config import RANK_REFS, SOURCES
from irw.utils.redivis.cache import metadata_cache
from irw.utils.redivis import datasets as ds_mod
from irw.utils.redivis.datasets import _init_rank_datasets
from irw.utils.redivis import table_metadata

DATA = Path(__file__).parent / "data"


@pytest.fixture(autouse=True)
def clear_cache():
    metadata_cache.clear()
    yield
    metadata_cache.clear()


def test_rank_refs_match_the_r_package_spec():
    """Rpkg/R/redivis-config.R: .irw_datasource_specs$rank, oldest to newest."""
    assert RANK_REFS[0] == ("datapages", "irw_rank:93f5")
    assert all(ref.startswith("irw_rank") for _user, ref in RANK_REFS)
    assert "rank" in SOURCES


@patch("irw.utils.redivis.datasets._init_dataset")
def test_init_rank_datasets_opens_every_shard_and_caches(mock_init_dataset):
    mock_init_dataset.side_effect = lambda user, ref: f"ds:{ref}"
    first = _init_rank_datasets()
    assert first == [f"ds:{ref}" for _user, ref in reversed(RANK_REFS)]
    assert _init_rank_datasets() is first
    assert mock_init_dataset.call_count == len(RANK_REFS)


# rank is a shard list from day one (the family will pass Redivis' 1000-table
# cap), with the semantics MAIN_REFS has: every shard opened, searched
# newest-first, and an unreleased shard skipped with a warning.

TWO_SHARDS = (("datapages", "irw_rank:93f5"), ("datapages", "irw_rank_2:zzzz"))


def test_rank_shards_are_searched_newest_first(monkeypatch):
    monkeypatch.setattr(ds_mod, "RANK_REFS", TWO_SHARDS)
    monkeypatch.setattr(ds_mod, "_init_dataset", lambda user, ref: f"ds:{ref}")
    assert _init_rank_datasets() == ["ds:irw_rank_2:zzzz", "ds:irw_rank:93f5"]


def test_an_unreleased_rank_shard_is_skipped_not_fatal(monkeypatch, caplog):
    monkeypatch.setattr(ds_mod, "RANK_REFS", TWO_SHARDS)

    def init(user, ref):
        if ref.startswith("irw_rank_2"):
            raise RuntimeError("403 insufficient_scope: no released version")
        return f"ds:{ref}"

    monkeypatch.setattr(ds_mod, "_init_dataset", init)
    with caplog.at_level("WARNING"):
        assert _init_rank_datasets() == ["ds:irw_rank:93f5"]
    assert "irw_rank_2" in caplog.text


def test_list_tables_flags_a_name_present_in_two_rank_shards(monkeypatch):
    import importlib
    lt = importlib.import_module("irw.operations.list_tables")

    class _T:
        def __init__(self, name): self.name = name

    newest, oldest = object(), object()
    listing = {newest: [_T("shared_2025"), _T("zeta_2026")], oldest: [_T("alpha_2024"), _T("shared_2025")]}
    monkeypatch.setattr(lt, "_dataset_table_list", lambda ds: listing[ds])
    with pytest.warns(UserWarning, match="more than one rank shard: shared_2025"):
        out = lt._build_base_table_list([newest, oldest], "rank")
    assert list(out.name) == ["alpha_2024", "shared_2025", "zeta_2026"]


@patch("irw.api._init_rank_datasets")
def test_get_datasets_dispatches_rank(mock_init_rank):
    ds = MagicMock()
    mock_init_rank.return_value = [ds]
    from irw.api import _get_datasets
    assert _get_datasets("rank") == [ds]


@patch("irw.utils.redivis.table_metadata._init_rank_datasets")
@patch("irw.utils.redivis.table_metadata._init_comp_dataset")
def test_source_datasets_never_falls_through_to_comp(mock_comp, mock_rank):
    mock_rank.return_value = ["rank-ds"]
    assert table_metadata._source_datasets("rank") == ["rank-ds"]
    mock_comp.assert_not_called()


def test_rank_meta_tables():
    assert table_metadata._meta_table("rank", "metadata") == "rank_metadata"
    assert table_metadata._meta_table("rank", "biblio") == "rank_biblio"
    with pytest.raises(ValueError, match="Tags are not available"):
        table_metadata._check_tag_source("rank")


def test_rank_shards_are_pinnable():
    from irw.utils.redivis.pins import _pinnable_refs
    assert "irw_rank" in _pinnable_refs()


def test_rank_filters_take_license_and_name_the_rest(monkeypatch):
    from irw.operations.filter import filter_tables, _check_filters_for_source
    import irw.operations.filter as fmod
    listing = pd.DataFrame({"name": ["a_rank", "b_rank"], "n_respondents": [200, 5000],
                            "method": ["irv", "stv"], "license": ["CC0 1.0", "CC BY 4.0"]})
    monkeypatch.setattr(fmod, "list_tables", lambda datasets, source="core", **kw: listing)
    assert list(filter_tables([], source="rank")) == ["a_rank", "b_rank"]
    assert list(filter_tables([], source="rank", license="CC BY 4.0")) == ["b_rank"]
    with pytest.raises(ValueError, match="no column for them: n_items"):
        filter_tables([], source="rank", n_items=[5, 10])
    with pytest.raises(ValueError, match="only available when source='conj'"):
        _check_filters_for_source("rank", {"outcome": "choice"})
    with pytest.raises(ValueError):
        _check_filters_for_source("rank", {"construct_type": "x"})


def test_fetch_dedup_leaves_a_rankings_table_alone(monkeypatch):
    import irw.api as api
    seen = {}

    def fake_fetch(datasets, table_name, dedup, max_rows, columns):
        seen["dedup"] = dedup
        return pd.DataFrame({"id": [1, 1], "task": 1, "item": ["c01", "c01"], "resp": [1, 2]})

    monkeypatch.setattr(api, "_get_datasets", lambda source: [])
    monkeypatch.setattr(api, "_fetch", fake_fetch)
    monkeypatch.setattr(api, "_source_note", lambda names, source: None)
    with pytest.warns(UserWarning, match="duplicate marks as recorded"):
        out = irw.fetch("x_rank", source="rank", dedup=True)
    assert seen["dedup"] is False
    assert len(out) == 2


def test_info_labels_a_rank_table():
    from irw.utils.table_helpers import _format_aux_table_info
    text = _format_aux_table_info("eurovision_2016_final", {"source": "rank", "biblio": {}})
    assert "(rankings, source='rank')" in text


# rank_compress(): the helper data/rankings/README.md promises. Its fixture and
# expected outputs are shared byte-for-byte with the R package's tests
# (tests/testthat/fixtures/rank_fixture*.csv there), so the two helpers are
# held to the same answer.

def _fixture(name):
    return pd.read_csv(DATA / name)


def test_rank_compress_drops_later_repeats_closes_gaps_and_renumbers():
    out = irw.rank_compress(_fixture("rank_fixture.csv"))
    expected = _fixture("rank_fixture_compressed.csv")
    assert list(out.columns) == list(expected.columns)
    pd.testing.assert_frame_equal(out, expected, check_dtype=False)
    assert pd.api.types.is_integer_dtype(out["resp"])


def test_rank_compress_keep_repeats_only_renumbers():
    out = irw.rank_compress(_fixture("rank_fixture.csv"), drop_repeats=False)
    pd.testing.assert_frame_equal(out, _fixture("rank_fixture_keep_repeats.csv"), check_dtype=False)


def test_rank_compress_without_task_and_on_a_clean_ballot():
    d = pd.DataFrame({"id": [2, 1, 1], "item": ["c01", "c02", "c01"], "resp": [1, 2, 1]})
    out = irw.rank_compress(d)
    assert list(out["id"]) == [1, 1, 2]
    assert list(out["resp"]) == [1, 2, 1]
    assert "task" not in out.columns


def test_rank_compress_refuses_what_is_not_a_rankings_table():
    with pytest.raises(ValueError, match="missing resp"):
        irw.rank_compress(pd.DataFrame({"id": [1], "item": ["a"]}))
    with pytest.raises(ValueError, match="positive integer"):
        irw.rank_compress(pd.DataFrame({"id": [1], "item": ["a"], "resp": [0]}))
    with pytest.raises(ValueError, match="positive integer"):
        irw.rank_compress(pd.DataFrame({"id": [1], "item": ["a"], "resp": [1.5]}))
    with pytest.raises(ValueError, match="True or False"):
        irw.rank_compress(pd.DataFrame({"id": [1], "item": ["a"], "resp": [1]}), drop_repeats=None)
    empty = irw.rank_compress(pd.DataFrame({"id": [], "item": [], "resp": []}))
    assert len(empty) == 0
