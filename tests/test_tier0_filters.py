"""The tier-0 psychometric filters (ben-domingue/irw#1710).

Four columns that describe how a table behaves rather than how big it is:
difficulty_spread, mean_item_rest_r, mean_abs_item_rest_r, prop_items_extreme.
They are classical and model-free, published per table by the IRW pipeline's
stage 19, and they exist on core's metadata only.

Two properties are worth pinning rather than assuming:

- These are the first numeric filters whose column can be NaN -- a table whose
  responses are all one value has no difficulty spread, a single-item table has
  no rest score. pandas comparisons against NaN are False, so such a table drops
  out of a range filter; R's equivalent needed an explicit is.na() guard, and a
  reader comparing the two packages should see the behaviour asserted here.
- Signed and absolute mean item-rest correlation are both exposed because IRW
  does not harmonise response direction. A reverse-keyed scale reads near zero
  signed and fine absolute, and the gap between them is the only way to tell it
  apart from a scale whose items genuinely do not cohere.

No network: every Redivis read is a synthetic frame.
"""

import pandas as pd
import pytest

from irw.operations import filter as filter_mod
from irw.operations import filter_info
from irw.utils.redivis.cache import metadata_cache

TIER0 = ("difficulty_spread", "mean_item_rest_r", "mean_abs_item_rest_r",
         "prop_items_extreme")


@pytest.fixture(autouse=True)
def clean_cache():
    metadata_cache.clear()
    yield
    metadata_cache.clear()


@pytest.fixture
def listing(monkeypatch):
    """core's catalogue, with one table per interesting tier-0 shape."""
    frame = pd.DataFrame({
        "name": ["coherent", "reverse_keyed", "floor", "constant"],
        "n_responses": [1000, 1000, 1000, 1000],
        "n_items": [10, 10, 10, 10],
        "density": [0.9, 0.9, 0.9, 0.9],
        "difficulty_spread": [0.6, 0.5, 0.2, float("nan")],
        "mean_item_rest_r": [0.55, 0.01, 0.30, float("nan")],
        "mean_abs_item_rest_r": [0.55, 0.42, 0.30, float("nan")],
        "prop_items_extreme": [0.0, 0.0, 0.8, float("nan")],
        "has_item_text": [False, False, False, False],
    })
    monkeypatch.setattr(filter_mod, "list_tables",
                        lambda datasets, source="core": frame.copy())
    return frame


def test_filters_select_on_psychometric_properties(listing):
    assert filter_mod.filter_tables([], mean_abs_item_rest_r=[0.4, None]).tolist() == \
        ["coherent", "reverse_keyed"]
    assert filter_mod.filter_tables([], prop_items_extreme=0).tolist() == \
        ["coherent", "reverse_keyed"]
    assert filter_mod.filter_tables([], difficulty_spread=[0.55, None]).tolist() == ["coherent"]


def test_the_signed_absolute_gap_finds_a_reverse_keyed_scale(listing):
    got = filter_mod.filter_tables([], mean_item_rest_r=[None, 0.1],
                                   mean_abs_item_rest_r=[0.3, None])
    assert got.tolist() == ["reverse_keyed"]


def test_a_table_with_no_tier0_value_is_dropped_not_returned(listing):
    got = filter_mod.filter_tables([], difficulty_spread=[0, 1]).tolist()
    assert "constant" not in got
    assert sorted(got) == ["coherent", "floor", "reverse_keyed"]


def test_tier0_filters_are_refused_for_sources_without_them(monkeypatch):
    frame = pd.DataFrame({"name": ["nom_a"], "n_items": [5]})
    monkeypatch.setattr(filter_mod, "list_tables",
                        lambda datasets, source="core": frame.copy())
    with pytest.raises(ValueError, match="mean_abs_item_rest_r"):
        filter_mod.filter_tables([], source="nom", mean_abs_item_rest_r=[0.4, None])
    with pytest.raises(ValueError, match="difficulty_spread"):
        filter_mod.filter_tables([], source="comp", difficulty_spread=[0, 1])
    with pytest.raises(ValueError, match="prop_items_extreme"):
        filter_mod.filter_tables([], source="conj", prop_items_extreme=0)


def test_core_metadata_without_the_columns_errors_rather_than_widening(monkeypatch):
    """An irw_meta version older than 2026-10-21. Every filter here is a no-op
    when its column is absent, which would return the whole catalogue as if it
    had been narrowed."""
    frame = pd.DataFrame({"name": ["a", "b"], "n_items": [5, 6], "density": [0.9, 0.9]})
    monkeypatch.setattr(filter_mod, "list_tables",
                        lambda datasets, source="core": frame.copy())
    with pytest.raises(filter_mod.IRWMetadataUnavailable, match="difficulty_spread"):
        filter_mod.filter_tables([], difficulty_spread=[0, 1])


@pytest.mark.parametrize("name", TIER0)
def test_every_tier0_filter_is_documented(name):
    """describe_filter() must answer for a filter filter() accepts -- the
    'the list is a lie' failure tests/test_filter_info.py guards generally."""
    assert name in filter_info.FILTER_DESCRIPTIONS
    assert name in filter_mod.NUMERIC_FILTERS
