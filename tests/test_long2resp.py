"""Offline coverage for long2resp().

This is the function that turns the long format every fetch() returns into the
wide response matrix people actually model -- step 4 of the workflow in
CLAUDE.md -- and it had no tests at all. The precedent is the filter() bug in
0.1.0, which silently returned 4,229 of 4,230 tables for every query because
nothing asserted otherwise.

Also covers its inverse, resp2long() (#60), and check_resp() (#61), which
share the module and the item-naming convention from #31.

No network: all three take a DataFrame and return plain pandas objects.
"""

import numpy as np
import pandas as pd
import pytest

from irw.utils.long2resp import check_resp, long2resp, resp2long


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


def test_a_labelled_wave_is_filtered_never_pooled():
    """The defect this guards: skipping the filter does not return every wave,
    it averages them. Under the default agg_method of "mean" a pre and a post
    answer on a 1-5 scale came back as their mean, which is in nobody's data."""
    df = _long([("p1", "q1", 1), ("p1", "q1", 5), ("p2", "q1", 2), ("p2", "q1", 4)])
    df["wave"] = ["pre", "post", "pre", "post"]
    wide = long2resp(df, wave="post", id_density_threshold=None)

    assert sorted(wide["q1"]) == [4, 5]


def test_a_wave_that_is_absent_raises():
    df = _long([("p1", "q1", 1), ("p2", "q1", 0)])
    df["wave"] = ["pre", "post"]
    with pytest.raises(ValueError, match="not present in this table"):
        long2resp(df, wave="followup", id_density_threshold=None)


def test_an_all_missing_wave_column_raises():
    df = _long([("p1", "q1", 1), ("p2", "q1", 0)])
    df["wave"] = None
    with pytest.raises(ValueError, match="missing on every row"):
        long2resp(df, id_density_threshold=None)


def test_a_numeric_wave_argument_matches_a_string_column():
    df = _long([("p1", "q1", 1), ("p2", "q1", 0)])
    df["wave"] = ["1", "2"]
    wide = long2resp(df, wave=1, id_density_threshold=None)

    assert set(wide["id"]) == {"p1"}


def test_the_occasion_choice_is_recorded_on_the_frame():
    """`print` alone is invisible to the MCP, which drops package stdout."""
    df = _long([("p1", "q1", 1), ("p2", "q1", 0)])
    df["wave"] = [1, 2]
    wide = long2resp(df, id_density_threshold=None)

    assert any("most frequent wave" in m for m in wide.attrs["irw_messages"])


def test_wave_is_not_a_column_of_the_response_matrix():
    df = _long([("p1", "q1", 1), ("p2", "q1", 0)])
    df["wave"] = 1
    wide = long2resp(df, id_density_threshold=None)

    assert "wave" not in wide.columns


# --- resp_col (#61) --------------------------------------------------------

def test_resp_col_widens_a_text_column_and_keeps_the_labels():
    """Nominal responses live in a text column. Before resp_col, long2resp
    could only read `resp`, and averaging labels would have made them NaN."""
    df = _long([("p1", "q1", 1), ("p1", "q2", 2), ("p2", "q1", 2), ("p2", "q2", 1)])
    df["text"] = ["red", "blue", "blue", "red"]
    wide = long2resp(df, id_density_threshold=None, resp_col="text").set_index("id")

    assert wide.loc["p1", "q1"] == "red"
    assert wide.loc["p2", "q2"] == "red"


def test_a_text_column_defaults_to_first_for_duplicates():
    df = _long([("p1", "q1", 0), ("p1", "q1", 0), ("p2", "q1", 0)])
    df["text"] = ["red", "blue", "green"]
    wide = long2resp(df, id_density_threshold=None, resp_col="text").set_index("id")

    assert wide.loc["p1", "q1"] == "red"


def test_mean_on_a_column_with_no_numbers_raises_instead_of_returning_nan():
    df = _long([("p1", "q1", 0), ("p2", "q1", 1)])
    df["text"] = ["red", "blue"]
    with pytest.raises(ValueError, match="numeric"):
        long2resp(df, id_density_threshold=None, resp_col="text", agg_method="mean")


def test_a_missing_resp_col_raises_and_names_it():
    df = _long([("p1", "q1", 1)])
    with pytest.raises(ValueError, match="answer"):
        long2resp(df, resp_col="answer")


def test_resp_col_does_not_need_a_resp_column():
    df = pd.DataFrame({"id": ["p1", "p2"], "item": ["q1", "q1"], "answer": [3, 4]})
    wide = long2resp(df, id_density_threshold=None, resp_col="answer").set_index("id")

    assert wide.loc["p2", "q1"] == 4


def test_the_numeric_default_is_unchanged_for_numeric_strings():
    """An object column of numeric strings is still averaged, as it always was."""
    df = _long([("p1", "q1", "1"), ("p1", "q1", "4"), ("p2", "q1", "2")])
    wide = long2resp(df, id_density_threshold=None).set_index("id")

    assert wide.loc["p1", "q1"] == 2.5


def test_an_already_missing_response_is_not_reported_as_a_conversion_failure(capsys):
    df = _long([("p1", "q1", 1), ("p1", "q2", np.nan), ("p2", "q1", 0), ("p2", "q2", 1)])
    long2resp(df, id_density_threshold=None)

    assert "could not be converted" not in capsys.readouterr().out


# --- check_resp (#61) ------------------------------------------------------

def _polytomous(item, counts):
    """Long rows for one item: `counts` maps response category -> n."""
    rows, person = [], 0
    for category, n in counts.items():
        for _ in range(n):
            rows.append((f"p{person}", item, category))
            person += 1
    return rows


def test_check_resp_flags_an_item_with_one_observed_category():
    df = _long(_polytomous("flat", {1: 10}) + _polytomous("ok", {0: 10, 1: 10}))
    checks = check_resp(df)

    assert checks["single_category_items"] == ["flat"]
    assert checks["sparse_category_items"] == {}


def test_missing_responses_are_not_a_category():
    """An item answered 1 by everyone who answered is single-category, however
    many people skipped it."""
    rows = _polytomous("q", {1: 10}) + [("px", "q", np.nan), ("py", "q", np.nan)]
    assert check_resp(_long(rows))["single_category_items"] == ["q"]


def test_check_resp_flags_a_sparse_category_by_count():
    df = _long(_polytomous("q", {0: 50, 1: 50, 2: 3}))
    sparse = check_resp(df)["sparse_category_items"]

    assert list(sparse) == ["q"]
    assert list(sparse["q"].columns) == ["resp", "count", "prop"]
    assert sparse["q"]["resp"].tolist() == [2]
    assert sparse["q"]["count"].tolist() == [3]
    assert sparse["q"]["prop"].iloc[0] == pytest.approx(3 / 103)


def test_check_resp_flags_a_sparse_category_by_proportion_alone():
    """The rule is count < min_count OR prop < min_prop. 6 of 1006 clears the
    count threshold of 5 but is under 1%."""
    df = _long(_polytomous("q", {0: 500, 1: 500, 2: 6}))
    sparse = check_resp(df)["sparse_category_items"]

    assert sparse["q"]["resp"].tolist() == [2]


def test_a_rare_category_of_a_dichotomous_item_is_not_sparse():
    """Sparse categories are a polytomous check, as in R: a dichotomous item
    answered correctly by 2 of 100 is a hard item."""
    df = _long(_polytomous("q", {0: 98, 1: 2}))
    assert check_resp(df)["sparse_category_items"] == {}


def test_check_resp_thresholds_are_configurable():
    df = _long(_polytomous("q", {0: 50, 1: 50, 2: 3}))
    assert check_resp(df, min_count=2)["sparse_category_items"] == {}


def test_check_resp_reads_a_custom_response_column():
    df = _long(_polytomous("q", {0: 20}))
    df["answer"] = ["a"] * 10 + ["b"] * 9 + ["c"]
    checks = check_resp(df, resp_col="answer")

    assert checks["single_category_items"] == []
    assert checks["sparse_category_items"]["q"]["resp"].tolist() == ["c"]


def test_check_resp_reports_item_names_as_the_wide_columns_spell_them():
    df = _long(_polytomous(3, {1: 10}))
    assert check_resp(df)["single_category_items"] == ["3"]


def test_check_resp_refuses_wide_data_and_says_how_to_check_it():
    wide = pd.DataFrame({"id": ["p1", "p2"], "q1": [1, 0]})
    with pytest.raises(ValueError, match="resp2long"):
        check_resp(wide)


def test_check_resp_missing_resp_col_raises():
    with pytest.raises(ValueError, match="answer"):
        check_resp(_long([("p1", "q1", 1)]), resp_col="answer")


def test_long2resp_returns_the_frame_alone_unless_checks_are_asked_for():
    df = _long(_polytomous("q", {0: 10, 1: 10}))
    assert isinstance(long2resp(df, id_density_threshold=None), pd.DataFrame)


def test_long2resp_check_resp_returns_the_diagnostics_alongside_the_matrix():
    """Returned, not attached: DataFrame.attrs does not survive most pandas
    operations, so a diagnostic stored there would vanish the first time the
    user subset the matrix."""
    df = _long(_polytomous("flat", {1: 10}) + _polytomous("q", {0: 50, 1: 50, 2: 3}))
    wide, checks = long2resp(df, id_density_threshold=None, check_resp=True)

    assert isinstance(wide, pd.DataFrame)
    assert checks["single_category_items"] == ["flat"]
    assert list(checks["sparse_category_items"]) == ["q"]
    # every flagged item is a column of the matrix it came with
    for item in checks["single_category_items"] + list(checks["sparse_category_items"]):
        assert item in wide.columns


def test_long2resp_check_resp_keeps_item_a_and_a_apart():
    """The diagnostics are keyed by the restored names, so the #31 collision
    cannot come back through them."""
    rows = [(f"p{i}", "a", 1) for i in range(10)]
    rows += [(f"p{i}", "item_a", i % 2) for i in range(10)]
    _, checks = long2resp(_long(rows), id_density_threshold=None, check_resp=True)

    assert checks["single_category_items"] == ["a"]


def test_long2resp_check_resp_checks_the_aggregated_responses():
    """p1 answered 1 then 2; the matrix holds the mean, 1.5, so the check sees
    one category -- the one in the matrix -- not two."""
    df = _long([("p1", "q", 1), ("p1", "q", 2)])
    _, checks = long2resp(df, id_density_threshold=None, check_resp=True)

    assert checks["single_category_items"] == ["q"]


# --- resp2long (#60) -------------------------------------------------------

def test_resp2long_returns_irw_long_format_item_major():
    wide = pd.DataFrame({"id": ["p1", "p2"], "q1": [1, 0], "q2": [0, 1]})
    long = resp2long(wide)

    assert list(long.columns) == ["id", "item", "resp"]
    assert long["item"].tolist() == ["q1", "q1", "q2", "q2"]
    assert long["id"].tolist() == ["p1", "p2", "p1", "p2"]
    assert long["resp"].tolist() == [1, 0, 0, 1]


def test_the_round_trip_recovers_the_original_responses():
    df = _long([
        ("p1", "q1", 1), ("p1", "q2", 0), ("p1", "myitem_x", 1),
        ("p2", "q1", 0), ("p2", "q2", 1), ("p2", "myitem_x", 0),
    ])
    back = resp2long(long2resp(df, id_density_threshold=None))

    key = ["id", "item"]
    expected = df.sort_values(key).reset_index(drop=True)
    got = back.sort_values(key).reset_index(drop=True)
    assert got[key].equals(expected[key])
    assert (got["resp"] == expected["resp"]).all()


def test_the_round_trip_does_not_merge_item_a_into_a():
    """#60 asks for the `item_` prefix to be stripped on the way back. It must
    not be: long2resp already returns the original names (#30, #31), so a strip
    would turn a genuine `item_a` into `a` and collide the two again."""
    df = _long([
        ("p1", "item_a", 1), ("p1", "a", 0),
        ("p2", "item_a", 1), ("p2", "a", 1),
    ])
    back = resp2long(long2resp(df, id_density_threshold=None))

    assert set(back["item"]) == {"a", "item_a"}
    assert not back.duplicated(subset=["id", "item"]).any()
    p1 = back[back["id"] == "p1"].set_index("item")["resp"]
    assert p1["item_a"] == 1 and p1["a"] == 0


def test_a_missing_cell_becomes_a_row_with_a_missing_response():
    wide = pd.DataFrame({"id": ["p1", "p2"], "q1": [1.0, np.nan]})
    long = resp2long(wide)

    assert len(long) == 2
    assert long["resp"].isna().sum() == 1


def test_resp2long_requires_an_id_column_by_default():
    with pytest.raises(ValueError, match="id=False"):
        resp2long(pd.DataFrame({"q1": [1, 0]}))


def test_id_false_generates_positional_ids_and_treats_every_column_as_an_item():
    long = resp2long(pd.DataFrame({"q1": [1, 0, 1], "q2": [0, 0, 1]}), id=False)

    assert long["id"].tolist() == [1, 2, 3, 1, 2, 3]
    assert set(long["item"]) == {"q1", "q2"}


def test_id_false_uses_an_existing_id_column_and_says_so(capsys):
    wide = pd.DataFrame({"id": ["p1", "p2"], "q1": [1, 0]})
    long = resp2long(wide, id=False)

    assert long["id"].tolist() == ["p1", "p2"]
    assert "id" not in set(long["item"])
    assert "ignored" in capsys.readouterr().out


def test_resp2long_accepts_a_numpy_array():
    long = resp2long(np.array([[1, 0], [0, 1], [1, 1]]), id=False)

    assert len(long) == 6
    assert long["item"].tolist() == [1, 1, 1, 2, 2, 2]
    assert long["id"].tolist() == [1, 2, 3, 1, 2, 3]


def test_resp2long_keeps_a_nullable_integer_dtype():
    wide = pd.DataFrame({"id": ["p1", "p2"], "q1": pd.array([1, None], dtype="Int64"),
                         "q2": pd.array([0, 1], dtype="Int64")})
    long = resp2long(wide)

    assert str(long["resp"].dtype) == "Int64"
    assert long["resp"].isna().sum() == 1


def test_resp2long_rejects_a_frame_with_no_item_columns():
    with pytest.raises(ValueError, match="No item columns"):
        resp2long(pd.DataFrame({"id": ["p1"]}))


def test_resp2long_rejects_what_is_not_a_frame_or_matrix():
    with pytest.raises(ValueError, match="DataFrame"):
        resp2long([[1, 0], [0, 1]])


# Occasion columns other than `wave` (review R-25, ben-domingue/irw#3223). The
# standard defines nine ways a response can repeat; the reshape handled one,
# and the other eight were dropped from the frame and then averaged together by
# the default agg_method="mean".

def _rater_frame():
    return pd.DataFrame({
        "id": ["p1"] * 4 + ["p2"] * 4,
        "item": ["q1", "q2"] * 4,
        "rater": ["r1", "r1", "r2", "r2"] * 2,
        "resp": [1, 1, 5, 5, 2, 2, 4, 4],
    })


def test_a_repeated_rater_is_neither_pooled_nor_chosen_for_the_caller():
    # The two raters disagree by four points, so their mean is 3, which neither
    # gave. The first version of this fix kept the most frequent rater instead;
    # on R's swmd_mokken, a 651-rater panel, that returned one respondent.
    with pytest.raises(ValueError, match=r"keys repeated id\+item responses \(2 values\)"):
        long2resp(_rater_frame(), id_density_threshold=None)

    with pytest.raises(ValueError, match=r"occasion=\{'rater': 'r1'\}"):
        long2resp(_rater_frame(), id_density_threshold=None)


def test_an_explicit_agg_method_pools_the_rater_panel_and_names_the_column():
    wide = long2resp(_rater_frame(), agg_method="mode", id_density_threshold=None)

    # Both raters' rows are still there to be aggregated, so both ids survive.
    assert len(wide) == 2
    assert wide.attrs["irw_occasion_pooled"] == ["rater"]
    assert wide.attrs["irw_occasion"] == {}
    assert any("Aggregating across rater (2 values)" in m
               for m in wide.attrs["irw_messages"])


def test_the_occasion_argument_chooses_for_any_occasion_column():
    wide = long2resp(_rater_frame(), occasion={"rater": "r2"},
                     id_density_threshold=None)

    assert wide.loc[wide["id"] == "p1", ["q1", "q2"]].to_numpy().ravel().tolist() == [5, 5]
    assert wide.attrs["irw_occasion"] == {"rater": "r2"}


def test_an_occasion_not_in_the_table_raises():
    with pytest.raises(ValueError, match="is not present in this table"):
        long2resp(_rater_frame(), occasion={"rater": "r9"})


def test_an_occasion_column_empty_on_every_row_raises():
    df = _rater_frame()
    df["rater"] = None
    with pytest.raises(ValueError, match="missing on every row"):
        long2resp(df)


def test_trial_columns_count_as_occasions():
    df = pd.DataFrame({
        "id": ["p1"] * 4, "item": ["q1", "q2"] * 2,
        "trial_number": [1, 1, 2, 2], "resp": [0, 0, 1, 1],
    })
    with pytest.raises(ValueError, match=r"Column 'trial_number' keys repeated"):
        long2resp(df, id_density_threshold=None)

    second = long2resp(df, occasion={"trial_number": 2}, id_density_threshold=None)
    assert second[["q1", "q2"]].to_numpy().ravel().tolist() == [1, 1]
    assert second.attrs["irw_occasion"] == {"trial_number": "2"}


def test_a_column_that_does_not_key_a_repeat_is_left_alone():
    # `subtest` can key a repeat, and here it does not: each id+item appears
    # once and the subtests hold different items. Filtering to one subtest
    # would throw away items rather than choose an occasion.
    df = pd.DataFrame({
        "id": ["p1"] * 4 + ["p2"] * 4,
        "item": ["a1", "a2", "b1", "b2"] * 2,
        "subtest": ["a", "a", "b", "b"] * 2,
        "resp": [1, 0, 1, 0, 0, 1, 0, 1],
    })
    wide = long2resp(df, id_density_threshold=None)

    assert sorted(c for c in wide.columns if c != "id") == ["a1", "a2", "b1", "b2"]
    assert wide.attrs["irw_occasion"] == {}


def test_an_explicit_occasion_is_honoured_even_when_nothing_repeats():
    df = pd.DataFrame({
        "id": ["p1"] * 4 + ["p2"] * 4,
        "item": ["a1", "a2", "b1", "b2"] * 2,
        "subtest": ["a", "a", "b", "b"] * 2,
        "resp": [1, 0, 1, 0, 0, 1, 0, 1],
    })
    wide = long2resp(df, occasion={"subtest": "a"}, id_density_threshold=None)

    assert sorted(c for c in wide.columns if c != "id") == ["a1", "a2"]


def test_occasion_and_wave_cannot_contradict_each_other():
    df = pd.DataFrame({"id": ["p1", "p1"], "item": ["q1", "q1"],
                       "wave": [1, 2], "resp": [0, 1]})
    with pytest.raises(ValueError, match="disagree"):
        long2resp(df, wave=1, occasion={"wave": 2})


def test_occasion_names_must_be_columns_the_table_has_an_occasion_for():
    with pytest.raises(ValueError, match="no occasion for"):
        long2resp(_rater_frame(), occasion={"session": "1"})
    with pytest.raises(TypeError, match="mapping"):
        long2resp(_rater_frame(), occasion="r1")
    with pytest.raises(ValueError, match="single value"):
        long2resp(_rater_frame(), occasion={"rater": ["r1", "r2"]})


def test_the_chosen_occasion_reaches_the_mcp_disclosures():
    # The MCP server drops stdout, so attrs is the only channel (R-19).
    wide = long2resp(_rater_frame(), occasion={"rater": "r1"},
                     id_density_threshold=None)
    messages = " ".join(wide.attrs["irw_messages"])

    assert "rater" in messages
    assert "Keeping only responses from rater r1" in messages

    # A pooled occasion has to reach the same channel: the frame is narrower
    # than the table either way, and that is the fact an agent must report.
    pooled = long2resp(_rater_frame(), agg_method="mode", id_density_threshold=None)
    assert "Aggregating across rater" in " ".join(pooled.attrs["irw_messages"])


# ---- column order (review R-23, ben-domingue/irw#3221) ----------------------
#
# Python sorted lexicographically (item1, item10, item2) and R ordered columns
# by first appearance in the input rows, which is not even stable: Redivis does
# not guarantee row order, so the same table fetched twice could hand back its
# columns differently. Both clients sort naturally now, from one key definition
# written the same way in both.

def test_item_columns_come_back_in_natural_order():
    df = pd.DataFrame({
        "id": ["p1"] * 4,
        "item": ["item10", "item2", "item1", "item20"],
        "resp": [1, 2, 3, 4],
    })
    wide = long2resp(df, id_density_threshold=None)
    assert [c for c in wide.columns if c != "id"] == ["item1", "item2", "item10", "item20"]
    assert wide.columns[0] == "id"


def test_the_order_does_not_depend_on_the_row_order():
    rows = [("item10", 1), ("item2", 2), ("item1", 3)]
    orders = []
    for perm in ([0, 1, 2], [2, 1, 0], [1, 0, 2]):
        df = pd.DataFrame({
            "id": ["p1"] * 3,
            "item": [rows[i][0] for i in perm],
            "resp": [rows[i][1] for i in perm],
        })
        orders.append(list(long2resp(df, id_density_threshold=None).columns))
    assert orders[0] == orders[1] == orders[2]


def test_the_natural_key_is_the_one_rpkg_reproduces():
    """Pinned because Rpkg's `.irw_natural_key()` must build the same string:
    digit runs left-padded to 20, everything else lowercased, raw name as the
    tie-break."""
    from irw.utils.long2resp import _natural_key

    assert _natural_key("item2") == ("item" + "0" * 19 + "2", "item2")
    assert _natural_key("Item2")[0] == _natural_key("item2")[0]
    assert _natural_key("Item2")[1] != _natural_key("item2")[1]
    names = ["item10", "Item3", "item1", "q2", "q10", "a"]
    assert sorted(names, key=_natural_key) == [
        "a", "item1", "Item3", "item10", "q2", "q10",
    ]
    # A digit run too long to be an item number is left alone rather than
    # padded, and still sorts deterministically.
    long_run = "x" + "9" * 25
    assert _natural_key(long_run)[0] == long_run
