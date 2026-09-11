"""recode() must give deterministic codes, and decode() must undo it exactly.

Ports of `irw_recode()` and `irw_decode()` (issue #59). The checks fall into
three groups:

- **Codes.** Assigned in code-point order of the unique non-missing values,
  zero-padded to `max(4, len(str(n_unique)))`. R forces a locale-independent
  radix sort to get this; Python's string comparison is already code-point
  order, and the test below asserts it rather than trusting it.
- **Round trip.** `decode(*recode(df))` gives back `df`, dtypes included, in
  long format and through `long2resp()`.
- **The key travels with the frame, not on it.** The deliberate divergence
  from R, which hides the key in an attribute that most operations drop.

No network: nothing here touches Redivis.
"""

import warnings

import numpy as np
import pandas as pd
import pytest

from irw.operations.recode import decode, recode
from irw.utils.long2resp import long2resp


def _long(ids, items, resp=None):
    return pd.DataFrame({
        "id": ids,
        "item": items,
        "resp": resp if resp is not None else [1] * len(ids),
    })


# --- codes ----------------------------------------------------------------

def test_codes_follow_code_point_order_not_locale_order():
    """A locale-aware sort puts 'a' next to 'A' and 'é' next to 'e'. Code-point
    order puts every uppercase ASCII letter first, then lowercase, then the
    accented letters -- which is what R's radix sort gives too."""
    items = ["é", "b", "Z", "a", "É", "B", "e"]
    _, key = recode(_long(list(range(7)), items), cols="item")
    assert key["original"].tolist() == ["B", "Z", "a", "b", "e", "É", "é"]
    assert key["original"].tolist() == sorted(items, key=lambda s: [ord(c) for c in s])
    assert key["code"].tolist() == [f"I000{i}" for i in range(1, 8)]


def test_codes_do_not_depend_on_row_order():
    df = _long(["r", "q", "p", "q"], ["x2", "x1", "x3", "x2"])
    shuffled = df.sample(frac=1, random_state=3).reset_index(drop=True)
    _, key = recode(df)
    _, key_shuffled = recode(shuffled)
    assert key.equals(key_shuffled)


def test_integer_ids_sort_as_strings_like_the_r_version():
    """R recodes `as.character(x)`, so 10 sorts before 2."""
    _, key = recode(_long([2, 10, 1], ["a", "a", "a"]), cols="id")
    assert key["original"].tolist() == [1, 10, 2]


def test_default_prefixes_and_overrides():
    df = _long(["u", "v"], ["x", "y"]).assign(wave=[1, 2])
    recoded, _ = recode(df, cols=["id", "item", "wave"], prefix={"item": "Q"})
    assert recoded["id"].tolist() == ["P0001", "P0002"]
    assert recoded["item"].tolist() == ["Q0001", "Q0002"]
    assert recoded["wave"].tolist() == ["W0001", "W0002"]


@pytest.mark.parametrize("n_unique, expected_first", [
    (9999, "I0001"),
    (10000, "I00001"),
])
def test_code_width_grows_so_large_tables_do_not_collide(n_unique, expected_first):
    items = [f"item{i}" for i in range(n_unique)]
    recoded, key = recode(_long([1] * n_unique, items), cols="item")
    assert key["code"].iloc[0] == expected_first
    assert key["code"].is_unique
    assert recoded["item"].nunique() == n_unique


def test_missing_values_stay_missing_and_are_not_coded():
    df = _long(["a", None, "b", np.nan], ["x", "x", "x", "x"])
    recoded, key = recode(df, cols="id")
    assert recoded["id"].isna().tolist() == [False, True, False, True]
    assert key["original"].tolist() == ["a", "b"]


def test_a_column_with_no_values_is_left_unchanged_with_a_warning():
    df = _long(["a", "b"], [None, None])
    with pytest.warns(UserWarning, match="'item' has no non-missing values"):
        recoded, key = recode(df)
    assert recoded["item"].isna().all()
    assert recoded["id"].tolist() == ["P0001", "P0002"]
    assert set(key["column"]) == {"id"}


def test_the_input_frame_is_not_modified():
    df = _long(["a", "b"], ["x", "y"])
    before = df.copy()
    recode(df)
    assert df.equals(before)


def test_a_categorical_column_is_recoded_by_value():
    df = _long(pd.Categorical(["b", "a", "b"], categories=["b", "a", "unused"]),
               ["x", "x", "x"])
    recoded, key = recode(df, cols="id")
    assert recoded["id"].tolist() == ["P0002", "P0001", "P0002"]
    assert key["original"].tolist() == ["a", "b"]


# --- round trip -----------------------------------------------------------

def test_decode_restores_values_and_dtypes():
    df = pd.DataFrame({
        "id": [101, 101, 7, 7],
        "item": ["Q_β", "Q_α", "Q_β", "Q_α"],
        "resp": [1, 0, 1, 1],
    })
    recoded, key = recode(df)
    decoded = decode(recoded, key)
    assert decoded["id"].tolist() == df["id"].tolist()
    assert decoded["item"].tolist() == df["item"].tolist()
    assert decoded["id"].dtype == df["id"].dtype
    assert decoded["resp"].equals(df["resp"])


def test_decode_undoes_long2resp_column_names():
    df = _long([1, 1, 2, 2], ["x", "y", "x", "y"], [1, 0, 1, 1])
    recoded, key = recode(df)
    wide = long2resp(recoded, id_density_threshold=None)
    assert list(wide.columns) == ["id", "I0001", "I0002"]
    decoded = decode(wide, key)
    assert list(decoded.columns) == ["id", "x", "y"]
    assert decoded["id"].tolist() == [1, 2]


def test_decode_keeps_the_item_prefix_r_writes_on_wide_columns():
    _, key = recode(_long([1, 2], ["x", "y"]), cols="item")
    wide = pd.DataFrame({"id": [1], "item_I0001": [1], "item_I0002": [0], "other": [5]})
    assert list(decode(wide, key).columns) == ["id", "item_x", "item_y", "other"]


def test_a_key_read_back_from_csv_still_decodes(tmp_path):
    recoded, key = recode(_long(["a", "b"], ["x", "y"]))
    path = tmp_path / "key.csv"
    key.to_csv(path, index=False)
    decoded = decode(recoded, pd.read_csv(path))
    assert decoded["item"].tolist() == ["x", "y"]


def test_keys_from_separate_calls_combine_with_concat():
    df = _long(["a", "b"], ["x", "y"])
    step1, key_id = recode(df, cols="id")
    step2, key_item = recode(step1, cols="item")
    decoded = decode(step2, pd.concat([key_id, key_item], ignore_index=True))
    assert decoded[["id", "item"]].equals(df[["id", "item"]])


def test_decode_can_be_limited_to_named_columns():
    recoded, key = recode(_long(["a", "b"], ["x", "y"]))
    decoded = decode(recoded, key, cols="id")
    assert decoded["id"].tolist() == ["a", "b"]
    assert decoded["item"].tolist() == ["I0001", "I0002"]


# --- the key travels with the frame ---------------------------------------

def test_recode_returns_the_key_rather_than_hiding_it_on_the_frame():
    """R attaches the key as an attribute that most operations drop. Here it is
    returned, so nothing about the frame has to survive for decode() to work."""
    recoded, key = recode(_long(["a", "b"], ["x", "y"]))
    assert list(key.columns) == ["column", "original", "code"]
    assert "irw_recode_key" not in recoded.attrs
    # An operation that drops attrs changes nothing.
    subset = recoded[recoded["item"] == "I0002"].reset_index(drop=True)
    assert decode(subset, key)["id"].tolist() == ["b"]


def test_codes_from_another_key_are_reported_not_silently_kept():
    """Codes are only meaningful relative to their key -- the likeliest misuse."""
    recoded, _ = recode(_long(["a", "b", "c"], ["x", "y", "z"]))
    _, other_key = recode(_long(["a"], ["x"]))
    with pytest.warns(UserWarning, match="2 value\\(s\\) in 'id' were not found"):
        decoded = decode(recoded, other_key, cols="id")
    assert decoded["id"].tolist() == ["a", "P0002", "P0003"]


def test_a_fully_matched_decode_is_silent():
    recoded, key = recode(_long(["a", "b"], ["x", "y"]))
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        decode(recoded, key)


# --- refusals -------------------------------------------------------------

def test_a_missing_column_is_refused():
    with pytest.raises(ValueError, match="Missing required IRW columns: wave"):
        recode(_long(["a"], ["x"]), cols=["id", "wave"])


def test_a_column_named_twice_is_refused():
    """Recoding twice in one call would recode the codes."""
    with pytest.raises(ValueError, match="more than once"):
        recode(_long(["a"], ["x"]), cols=["id", "id"])


@pytest.mark.parametrize("prefix", [["Q"], {"item": 1}])
def test_a_malformed_prefix_is_refused(prefix):
    with pytest.raises(ValueError, match="prefix"):
        recode(_long(["a"], ["x"]), prefix=prefix)


def test_a_malformed_key_is_refused():
    with pytest.raises(ValueError, match="'key' must be a DataFrame"):
        decode(_long(["a"], ["x"]), pd.DataFrame({"code": ["P0001"]}))


def test_decode_with_nothing_to_match_is_refused():
    _, key = recode(_long(["a"], ["x"]), cols="id")
    with pytest.raises(ValueError, match="Nothing to decode"):
        decode(pd.DataFrame({"resp": [1]}), key)
