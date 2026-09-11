"""Person-level covariates must be detected by R's rule and line up with the matrix.

Port of `irw_covariates()` (issue #58). The failure this function exists to
prevent is silent: a covariate matched onto a wide response matrix in the wrong
order misassigns people to groups with no error. So the alignment tests assert
row-for-row agreement with `long2resp()` output whose order differs from the
long data, not just that the right ids are present.

The detection rule is checked against the case a naive `groupby().first()`
gets wrong -- a column present on some of a person's rows and missing on
others -- and the printed message is checked to name what was rejected.

No network: nothing here touches Redivis.
"""

import numpy as np
import pandas as pd
import pytest

import irw
from irw.operations.covariates import covariates
from irw.utils.long2resp import long2resp


@pytest.fixture
def long_df():
    # ids deliberately out of sorted order, so first-appearance order and
    # long2resp()'s sorted order disagree.
    return pd.DataFrame(
        {
            "id": [3, 3, 1, 1, 2, 2],
            "item": ["i1", "i2", "i1", "i2", "i1", "i2"],
            "resp": [1, 0, 1, 1, 0, 1],
            "cov_group": ["A", "A", "B", "B", "A", "A"],
            "cov_age": [30, 30, 41, 41, 25, 25],
        }
    )


# --- detection ------------------------------------------------------------

def test_one_row_per_id_in_order_of_first_appearance(long_df):
    out = covariates(long_df)
    assert list(out.columns) == ["id", "cov_group", "cov_age"]
    assert out["id"].tolist() == [3, 1, 2]
    assert out["cov_group"].tolist() == ["A", "B", "A"]
    assert out["cov_age"].tolist() == [30, 41, 25]


def test_response_level_columns_are_never_candidates(long_df):
    # rater and rt are constant within id here, and still not returned.
    df = long_df.assign(rater="r1", rt=1.5, source_table="t")
    out = covariates(df)
    assert list(out.columns) == ["id", "cov_group", "cov_age"]


def test_partly_missing_column_counts_as_varying(long_df, capsys):
    df = long_df.assign(cov_score=[1.0, np.nan, 2.0, 2.0, 3.0, 3.0])
    out = covariates(df)
    assert "cov_score" not in out.columns
    assert "cov_score" in capsys.readouterr().out
    # The shortcut this rule exists to disagree with: first() skips the NaN
    # and reports id 3 as 1.0.
    assert df.groupby("id")["cov_score"].first()[3] == 1.0


def test_column_missing_on_every_row_of_a_person_is_person_level(long_df):
    df = long_df.assign(cov_score=[np.nan, np.nan, 2.0, 2.0, None, None])
    out = covariates(df)
    assert out["cov_score"].isna().tolist() == [True, False, True]


def test_message_names_the_rejected_columns(long_df, capsys):
    df = long_df.assign(cov_x=[1, 2, 1, 1, 1, 1], cov_y=list("abcdef"))
    covariates(df)
    printed = capsys.readouterr().out
    assert "Not person-level (varies within id), so not returned: cov_x, cov_y" in printed


def test_no_message_when_nothing_is_rejected(long_df, capsys):
    covariates(long_df)
    assert capsys.readouterr().out == ""


def test_wave_is_person_level_only_when_each_person_has_one_wave(long_df, capsys):
    one_wave_each = long_df.assign(wave=[1, 1, 2, 2, 1, 1])
    assert "wave" in covariates(one_wave_each).columns

    capsys.readouterr()
    repeated = long_df.assign(wave=[1, 2, 1, 1, 1, 1])
    assert "wave" not in covariates(repeated).columns
    assert "wave" in capsys.readouterr().out


def test_ids_only_when_no_covariates(long_df, capsys):
    out = covariates(long_df[["id", "item", "resp"]])
    assert list(out.columns) == ["id"]
    assert out["id"].tolist() == [3, 1, 2]
    assert "No person-level covariates found" in capsys.readouterr().out


# --- explicit cols --------------------------------------------------------

def test_explicit_cols_select_and_order(long_df):
    out = covariates(long_df, cols=["cov_age", "cov_group"])
    assert list(out.columns) == ["id", "cov_age", "cov_group"]


def test_a_single_column_name_is_accepted(long_df):
    assert list(covariates(long_df, cols="cov_age").columns) == ["id", "cov_age"]


def test_explicit_varying_column_is_an_error(long_df):
    df = long_df.assign(cov_x=[1, 2, 1, 1, 1, 1])
    with pytest.raises(ValueError, match="not person-level.*cov_x"):
        covariates(df, cols=["cov_group", "cov_x"])


def test_explicit_missing_column_is_an_error(long_df):
    with pytest.raises(ValueError, match="Missing required IRW columns: cov_nope"):
        covariates(long_df, cols=["cov_nope"])


def test_naming_id_does_not_duplicate_it(long_df):
    assert list(covariates(long_df, cols=["id", "cov_age"]).columns) == ["id", "cov_age"]


@pytest.mark.parametrize("bad", [[1, 2], [None]])
def test_non_string_cols_are_an_error(long_df, bad):
    with pytest.raises(ValueError, match="`cols` must be"):
        covariates(long_df, cols=bad)


def test_df_must_be_a_frame_with_id(long_df):
    with pytest.raises(ValueError, match="pandas DataFrame"):
        covariates(long_df.to_dict())
    with pytest.raises(ValueError, match="Missing required IRW columns: id"):
        covariates(long_df.drop(columns="id"))


# --- align ----------------------------------------------------------------

def test_align_to_long2resp_output_matches_row_for_row(long_df, capsys):
    wide = long2resp(long_df, id_density_threshold=None)
    assert wide["id"].tolist() != covariates(long_df)["id"].tolist()
    out = covariates(long_df, align=wide)
    assert out["id"].tolist() == wide["id"].tolist()
    lookup = long_df.drop_duplicates("id").set_index("id")["cov_group"]
    assert out["cov_group"].tolist() == [lookup[i] for i in wide["id"]]


def test_align_to_a_frame_indexed_by_id(long_df):
    wide = long2resp(long_df, id_density_threshold=None).set_index("id")
    out = covariates(long_df, align=wide)
    assert out["id"].tolist() == wide.index.tolist()


@pytest.mark.parametrize(
    "ids",
    [[2, 3, 1], (2, 3, 1), np.array([2, 3, 1]), pd.Series([2, 3, 1]), pd.Index([2, 3, 1])],
)
def test_align_to_a_sequence_of_ids(long_df, ids):
    out = covariates(long_df, align=ids)
    assert out["id"].tolist() == [2, 3, 1]
    assert out["cov_age"].tolist() == [25, 30, 41]


def test_absent_ids_give_na_rows_not_dropped_rows(long_df, capsys):
    out = covariates(long_df, align=[1, 99, 3])
    assert out["id"].tolist() == [1, 99, 3]
    assert out.loc[1, ["cov_group", "cov_age"]].isna().all()
    assert out.loc[2, "cov_group"] == "A"
    assert "1 id(s) in `align` were not found in `df`" in capsys.readouterr().out


def test_na_rows_keep_integer_columns_integer(long_df):
    out = covariates(long_df, align=[1, 99])
    assert str(out["cov_age"].dtype) == "Int64"
    assert out["cov_age"].iloc[0] == 41


def test_repeated_ids_in_align_repeat_the_row(long_df):
    out = covariates(long_df, align=[3, 3])
    assert out["cov_group"].tolist() == ["A", "A"]


def test_type_mismatch_is_named_when_nothing_matches(long_df, capsys):
    out = covariates(long_df, align=["1", "2"])
    assert out["cov_group"].isna().all()
    assert "No id matched" in capsys.readouterr().out


def test_frame_without_id_column_or_index_is_an_error(long_df):
    # A default RangeIndex must not be read as ids.
    wide = long2resp(long_df, id_density_threshold=None).drop(columns="id")
    with pytest.raises(ValueError, match="neither an `id` column nor an index"):
        covariates(long_df, align=wide)


def test_two_dimensional_array_is_an_error(long_df):
    with pytest.raises(ValueError, match="2-D array"):
        covariates(long_df, align=np.zeros((3, 2)))


def test_unsupported_align_is_an_error(long_df):
    with pytest.raises(ValueError, match="`align` must be"):
        covariates(long_df, align={1, 2})


def test_exported_at_top_level():
    assert irw.covariates is covariates
    assert "covariates" in irw.__all__
