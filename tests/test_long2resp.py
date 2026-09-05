"""Offline coverage for long2resp().

This is the function that turns the long format every fetch() returns into the
wide response matrix people actually model -- step 4 of the workflow in
CLAUDE.md -- and it had no tests at all. The precedent is the filter() bug in
0.1.0, which silently returned 4,229 of 4,230 tables for every query because
nothing asserted otherwise.

No network: long2resp() takes a DataFrame and returns a DataFrame.
"""

import numpy as np
import pandas as pd
import pytest

from irw.utils.long2resp import long2resp


def _long(rows):
    return pd.DataFrame(rows, columns=["id", "item", "resp"])


# --- shape -----------------------------------------------------------------

def test_pivots_to_one_row_per_id_and_one_column_per_item():
    df = _long([
        ("p1", "q1", 1), ("p1", "q2", 0),
        ("p2", "q1", 0), ("p2", "q2", 1),
    ])
    wide = long2resp(df, id_density_threshold=None)

    assert list(wide.columns) == ["id", "q1", "q2"]
    assert len(wide) == 2
    assert wide.set_index("id").loc["p1", "q2"] == 0


def test_missing_required_columns_raise_and_name_what_is_missing():
    with pytest.raises(ValueError, match="item"):
        long2resp(pd.DataFrame({"id": ["p1"], "resp": [1]}))


def test_a_date_column_is_refused_rather_than_silently_dropped():
    df = _long([("p1", "q1", 1)])
    df["date"] = "2026-01-01"
    with pytest.raises(ValueError, match="date"):
        long2resp(df)


# --- item names ------------------------------------------------------------

def test_an_item_name_containing_item_underscore_survives_the_round_trip():
    """Regression: `myitem_x` came back as `myx`.

    long2resp prefixes items with `item_` so numeric ids do not become numeric
    column labels, then strips it after the pivot. The strip used
    `col.replace("item_", "")`, which removes every occurrence, so the prefix
    and the item's own text were both eaten.
    """
    df = _long([
        ("p1", "myitem_x", 1), ("p1", "q1", 0),
        ("p2", "myitem_x", 0), ("p2", "q1", 1),
    ])
    wide = long2resp(df, id_density_threshold=None)

    assert "myitem_x" in wide.columns
    assert "myx" not in wide.columns


def test_an_item_named_item_x_does_not_merge_with_an_item_named_x():
    """Regression: `a` and `item_a` were pivoted as one item.

    The prefix was applied only to items that did not already start with
    `item_`, so `a` became `item_a` and a genuine `item_a` was left alone.
    Both then keyed the same column and agg_method averaged responses from two
    different questions -- reported to the user as duplicate (id, item) pairs
    in their own data, not as a collision long2resp had created.
    """
    df = _long([
        ("p1", "item_a", 1), ("p1", "a", 0),
        ("p2", "item_a", 1), ("p2", "a", 1),
    ])
    wide = long2resp(df, id_density_threshold=None)

    assert "a" in wide.columns
    assert "item_a" in wide.columns
    assert "item_item_a" not in wide.columns  # the doubled name stays internal

    wide = wide.set_index("id")
    assert wide.loc["p1", "item_a"] == 1
    assert wide.loc["p1", "a"] == 0        # was 0.5, the mean of the two
    assert wide.loc["p2", "item_a"] == 1
    assert wide.loc["p2", "a"] == 1


def test_numeric_item_ids_become_string_columns_not_integers():
    df = _long([("p1", 3, 1), ("p1", 7, 0), ("p2", 3, 0), ("p2", 7, 1)])
    wide = long2resp(df, id_density_threshold=None)

    assert list(wide.columns) == ["id", "3", "7"]


# --- duplicate (id, item) pairs -------------------------------------------

@pytest.mark.parametrize("agg_method, expected", [
    ("mean", 2.0),
    ("median", 1.0),
    ("first", 1.0),
    ("mode", 1.0),
])
def test_duplicate_id_item_pairs_are_aggregated_per_agg_method(agg_method, expected):
    """p1 answered q1 three times: 1, 1, 4. mean=2, median=1, first=1, mode=1."""
    df = _long([
        ("p1", "q1", 1), ("p1", "q1", 1), ("p1", "q1", 4),
        ("p2", "q1", 3),
    ])
    wide = long2resp(df, id_density_threshold=None, agg_method=agg_method)

    assert wide.set_index("id").loc["p1", "q1"] == expected


def test_an_unknown_agg_method_raises():
    df = _long([("p1", "q1", 1)])
    with pytest.raises(ValueError, match="agg_method"):
        long2resp(df, id_density_threshold=None, agg_method="maximum")


# --- missing cells and non-numeric responses -------------------------------

def test_a_respondent_who_never_saw_an_item_gets_nan_not_a_dropped_row():
    df = _long([("p1", "q1", 1), ("p1", "q2", 0), ("p2", "q1", 1)])
    wide = long2resp(df, id_density_threshold=None).set_index("id")

    assert len(wide) == 2
    assert np.isnan(wide.loc["p2", "q2"])


def test_non_numeric_responses_become_nan():
    df = _long([("p1", "q1", "yes"), ("p1", "q2", 1), ("p2", "q1", 0), ("p2", "q2", 1)])
    wide = long2resp(df, id_density_threshold=None).set_index("id")

    assert np.isnan(wide.loc["p1", "q1"])
    assert wide.loc["p2", "q1"] == 0


# --- density filtering -----------------------------------------------------

def test_sparse_ids_are_dropped_at_the_default_threshold():
    """p_sparse answered 1 of 20 items -- density 0.05, under the 0.1 default."""
    rows = [("p_dense", f"q{i}", 1) for i in range(20)]
    rows += [("p_sparse", "q0", 1)]
    wide = long2resp(_long(rows))

    assert list(wide["id"]) == ["p_dense"]


def test_threshold_none_keeps_everyone():
    rows = [("p_dense", f"q{i}", 1) for i in range(20)]
    rows += [("p_sparse", "q0", 1)]
    wide = long2resp(_long(rows), id_density_threshold=None)

    assert set(wide["id"]) == {"p_dense", "p_sparse"}


# --- waves -----------------------------------------------------------------

def test_wave_defaults_to_the_most_frequent_one():
    df = _long([
        ("p1", "q1", 1), ("p1", "q2", 1), ("p2", "q1", 1),
        ("p3", "q1", 0),
    ])
    df["wave"] = [1, 1, 1, 2]
    wide = long2resp(df, id_density_threshold=None)

    assert set(wide["id"]) == {"p1", "p2"}


def test_an_explicit_wave_selects_that_wave():
    df = _long([
        ("p1", "q1", 1), ("p1", "q2", 1), ("p2", "q1", 1),
        ("p3", "q1", 0),
    ])
    df["wave"] = [1, 1, 1, 2]
    wide = long2resp(df, wave=2, id_density_threshold=None)

    assert set(wide["id"]) == {"p3"}


def test_wave_is_not_a_column_of_the_response_matrix():
    df = _long([("p1", "q1", 1), ("p2", "q1", 0)])
    df["wave"] = 1
    wide = long2resp(df, id_density_threshold=None)

    assert "wave" not in wide.columns
