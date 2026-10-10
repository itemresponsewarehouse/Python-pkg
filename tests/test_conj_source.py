"""Offline coverage for the `conj` (conjoint) source.

Rpkg gained source = "conj" in itemresponsewarehouse/Rpkg#186; the pipeline's
parity check (ben-domingue/irw metadata/check_config_parity.py) requires this
package to declare the same dataset. No network: _init_dataset is patched.
"""

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

import irw
from irw.config import CONJ_REFS, SOURCES
from irw.utils.redivis.cache import metadata_cache
from irw.utils.redivis import datasets as ds_mod
from irw.utils.redivis.datasets import _init_conj_datasets
from irw.utils.redivis import table_metadata


@pytest.fixture(autouse=True)
def clear_cache():
    metadata_cache.clear()
    yield
    metadata_cache.clear()


def test_conj_refs_match_the_r_package_spec():
    """Rpkg/R/redivis-config.R: .irw_datasource_specs$conj, oldest to newest."""
    assert CONJ_REFS[0] == ("datapages", "irw_conjoint:5wjx")
    assert all(ref.startswith("irw_conjoint") for _user, ref in CONJ_REFS)
    assert "conj" in SOURCES


@patch("irw.utils.redivis.datasets._init_dataset")
def test_init_conj_datasets_opens_every_shard_and_caches(mock_init_dataset):
    mock_init_dataset.side_effect = lambda user, ref: f"ds:{ref}"
    first = _init_conj_datasets()
    assert first == [f"ds:{ref}" for _user, ref in reversed(CONJ_REFS)]
    assert _init_conj_datasets() is first
    assert mock_init_dataset.call_count == len(CONJ_REFS)


# conj is a shard list (irw_conjoint is near Redivis' 1000-table cap) with the
# semantics MAIN_REFS has: every shard opened, searched newest-first, and an
# unreleased shard skipped with a warning rather than taking the source down.

TWO_SHARDS = (("datapages", "irw_conjoint:5wjx"), ("datapages", "irw_conjoint_2:zzzz"))


def test_conj_shards_are_searched_newest_first(monkeypatch):
    monkeypatch.setattr(ds_mod, "CONJ_REFS", TWO_SHARDS)
    monkeypatch.setattr(ds_mod, "_init_dataset", lambda user, ref: f"ds:{ref}")
    assert _init_conj_datasets() == ["ds:irw_conjoint_2:zzzz", "ds:irw_conjoint:5wjx"]


def test_an_unreleased_conj_shard_is_skipped_not_fatal(monkeypatch, caplog):
    monkeypatch.setattr(ds_mod, "CONJ_REFS", TWO_SHARDS)

    def init(user, ref):
        if ref.startswith("irw_conjoint_2"):
            raise RuntimeError("403 insufficient_scope: no released version")
        return f"ds:{ref}"

    monkeypatch.setattr(ds_mod, "_init_dataset", init)
    with caplog.at_level("WARNING"):
        assert _init_conj_datasets() == ["ds:irw_conjoint:5wjx"]
    assert "irw_conjoint_2" in caplog.text


def test_list_tables_flags_a_name_present_in_two_conj_shards(monkeypatch):
    import importlib
    lt = importlib.import_module("irw.operations.list_tables")

    class _T:
        def __init__(self, name): self.name = name

    newest, oldest = object(), object()
    listing = {newest: [_T("shared_2025"), _T("zeta_2026")], oldest: [_T("alpha_2024"), _T("shared_2025")]}
    monkeypatch.setattr(lt, "_dataset_table_list", lambda ds: listing[ds])
    with pytest.warns(UserWarning, match="more than one conj shard: shared_2025"):
        out = lt._build_base_table_list([newest, oldest], "conj")
    assert list(out.name) == ["alpha_2024", "shared_2025", "zeta_2026"]
    # Main keeps resolving silently to the newest copy.
    import warnings as _w
    with _w.catch_warnings():
        _w.simplefilter("error")
        lt._build_base_table_list([newest, oldest], "main")


@patch("irw.api._init_conj_datasets")
def test_get_datasets_dispatches_conj(mock_init_conj):
    ds = MagicMock()
    mock_init_conj.return_value = [ds]
    from irw.api import _get_datasets
    assert _get_datasets("conj") == [ds]


@patch("irw.utils.redivis.table_metadata._init_conj_datasets")
@patch("irw.utils.redivis.table_metadata._init_comp_dataset")
def test_source_datasets_never_falls_through_to_comp(mock_comp, mock_conj):
    """Before conj existed, any source not main/nom/sim got the competitions dataset."""
    mock_conj.return_value = ["conj-ds"]
    assert table_metadata._source_datasets("conj") == ["conj-ds"]
    mock_comp.assert_not_called()


def test_conj_metadata_is_conj_metadata():
    assert table_metadata._meta_table("conj", "metadata") == "conj_metadata"


def test_metadata_is_public_and_takes_a_source(monkeypatch):
    import irw.api as api
    seen = []
    monkeypatch.setattr(api, "_get_metadata_table", lambda source="main": seen.append(source) or pd.DataFrame({"table": ["t"]}))
    assert "metadata" in irw.__all__
    assert list(irw.metadata(source="conj").table) == ["t"]
    assert irw.metadata().shape == (1, 1)
    assert seen == ["conj", "main"]


def _fake_conj(monkeypatch):
    import irw.utils.redivis.table_metadata as tm
    meta = pd.DataFrame({
        "table": ["a_us_choice", "b_pooled_both", "c_gb_rating", "d_named"],
        "n_respondents": [300, 18000, 900, 2000], "n_attributes": [4, 9, 6, 11],
        "outcomes": ["choice", "choice;rating", "rating", "choice_neighbor;rating_neighbor"],
        "country": ["US", "AT;DE;GB", "GB", "TR"]})
    bib = pd.DataFrame({"table": list(meta.table),
                        "Derived_License": ["CC0 1.0", "CC0 1.0", "CC BY 4.0", "CC0 1.0"]})
    monkeypatch.setattr(tm, "get_metadata_table", lambda source="main": meta)
    monkeypatch.setattr(tm, "get_biblio_table", lambda source="main": bib)


def test_conj_filters(monkeypatch):
    from irw.operations.filter import filter_tables
    _fake_conj(monkeypatch)

    def f(**kw):
        return list(filter_tables([], source="conj", **kw))

    assert f() == ["a_us_choice", "b_pooled_both", "c_gb_rating", "d_named"]
    assert f(outcome="rating") == ["b_pooled_both", "c_gb_rating", "d_named"]
    assert f(outcome=["choice", "rating"]) == ["b_pooled_both", "d_named"]
    assert f(country="gb") == ["b_pooled_both", "c_gb_rating"]
    assert f(n_respondents=[1000, None]) == ["b_pooled_both", "d_named"]
    assert f(n_attributes=9) == ["b_pooled_both"]
    assert f(license="CC BY 4.0") == ["c_gb_rating"]
    assert f(country="US", outcome="rating") == []
    with pytest.raises(ValueError, match="choice"):
        f(outcome="vote")


def test_conj_and_other_filters_do_not_cross():
    from irw.operations.filter import _check_filters_for_source
    from irw.operations.filter_info import get_filters
    with pytest.raises(ValueError, match="not available for source='conj'"):
        _check_filters_for_source("conj", {"n_items": [5, 10]})
    with pytest.raises(ValueError, match="only available when source='conj'"):
        _check_filters_for_source("main", {"country": "US"})
    with pytest.raises(ValueError, match="only available when source='conj'"):
        _check_filters_for_source("comp", {"outcome": "choice"})
    _check_filters_for_source("conj", {"country": "US", "license": "CC0 1.0"})
    assert get_filters("conj") == ["license", "n_respondents", "n_attributes", "outcome", "country"]
    assert "country" not in get_filters("main") and "outcome" not in get_filters("nom")



def test_conj_biblio_is_conj_biblio():
    assert table_metadata._meta_table("conj", "biblio") == "conj_biblio"


def _conj_df():
    return pd.DataFrame({
        "id": [1, 1, 1, 1, 2, 2, 2, 2], "task": [1, 1, 2, 2] * 2, "profile": [1, 2] * 4,
        "choice": [1, 0, 0, 1, 1, 0, 0, 0], "rating": [5, 3, None, 6, 7, 2, 4, 4],
        "attr_party": ["Democrat", "Republican"] * 4, "cov_age": [30] * 4 + [40] * 4,
    })


def test_conj_long_stacks_outcomes():
    long = irw.conj_long(_conj_df())
    assert list(long.columns[:5]) == ["id", "item", "resp", "trial_task", "trial_profile"]
    assert {"trial_attr_party", "cov_age"} <= set(long.columns)
    assert (long["item"] == "choice").sum() == 8
    assert (long["item"] == "rating").sum() == 7          # the missing rating is dropped
    assert not long.duplicated(["id", "item", "trial_task", "trial_profile"]).any()


def test_conj_long_named_outcomes_and_errors():
    d = _conj_df().assign(choice_effective=lambda x: x["choice"])
    assert set(irw.conj_long(d)["item"]) == {"choice", "rating", "choice_effective"}
    assert set(irw.conj_long(d, outcomes=["rating"])["item"]) == {"rating"}
    with pytest.raises(ValueError, match="Not outcome columns"):
        irw.conj_long(d, outcomes=["resp"])
    with pytest.raises(ValueError, match="missing profile"):
        irw.conj_long(d.drop(columns="profile"))
