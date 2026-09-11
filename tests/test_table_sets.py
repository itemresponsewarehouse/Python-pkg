"""Offline coverage for irw.table_sets(), the port of Rpkg/R/aggregate.R.

The function exists so that set questions about a table do not export the
table, so the assertions that matter are on what reached Redivis: SQL queries,
never a read of the table itself. A test that only checked the returned sets
would pass just as well against an implementation that fetched every row and
computed them locally -- the thing #55 is meant to replace.

No network: Redivis is replaced by a fake that answers each query by shape.
"""

import os
import warnings
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

import irw
from irw.operations import table_sets as ts
from irw.operations.table_sets import _coerce_resp_set, table_sets


REF = "datapages.item_response_warehouse:as2e:v52_0.t:abcd"


class _Variable:
    def __init__(self, name):
        self.name = name


class _Table:
    def __init__(self, variables=("id", "item", "resp")):
        self.qualified_reference = REF
        self._variables = variables
        self.read = False

    def get(self):
        return None

    def list_variables(self):
        return [_Variable(v) for v in self._variables]

    def to_pandas_dataframe(self, *args, **kwargs):
        self.read = True
        raise AssertionError("table_sets() must not read the table")


class _Dataset:
    def __init__(self, table=None, missing=False):
        self._table = table
        self._missing = missing
        self.asked = 0

    def table(self, name):
        self.asked += 1
        if self._missing:
            raise Exception("not_found")
        return self._table


class _FakeRedivis:
    """Answers the four query shapes table_sets() sends, and records them."""

    def __init__(self, *, n=10, items=("q2", "q1"), resp=("2", "1"), per_item=None, error=None):
        self.sql = []
        self._n = n
        self._items = list(items)
        self._resp = list(resp)
        self._per_item = per_item
        self._error = error

    def query(self, sql):
        self.sql.append(sql)
        q = MagicMock()
        if self._error is not None:
            q.to_pandas_dataframe.side_effect = self._error
        elif "GROUP BY item" in sql:
            q.to_pandas_dataframe.return_value = self._per_item
        elif "COUNT(*) AS n FROM" in sql:
            q.to_pandas_dataframe.return_value = pd.DataFrame({"n": [self._n]})
        elif "DISTINCT CAST(item" in sql:
            q.to_pandas_dataframe.return_value = pd.DataFrame({"item": self._items})
        elif "SELECT DISTINCT TRIM(CAST(resp" in sql:
            q.to_pandas_dataframe.return_value = pd.DataFrame({"resp": self._resp})
        else:
            raise AssertionError(f"unexpected query: {sql}")
        return q


@pytest.fixture
def fake():
    f = _FakeRedivis()
    with patch.object(ts, "redivis", f):
        yield f


# --- the queries -------------------------------------------------------------

def test_answers_come_from_queries_and_the_table_is_never_read(fake):
    tbl = _Table()
    out = table_sets([_Dataset(tbl)], "t")
    assert tbl.read is False
    assert out == {"table": REF, "n_rows": 10, "items": ["q1", "q2"],
                   "resp": [1, 2], "per_item": None}
    assert len(fake.sql) == 3
    assert all(f"`{REF}`" in sql for sql in fake.sql)


def test_the_resp_query_filters_the_na_token_and_empty_strings(fake):
    """IRW stores a missing response as the literal "NA"; without the filter
    the distinct set gains a phantom level fetch() never shows."""
    table_sets([_Dataset(_Table())], "t")
    resp_sql = next(s for s in fake.sql if "DISTINCT TRIM" in s)
    assert "NOT IN ('NA', '')" in resp_sql
    assert "resp IS NOT NULL" in resp_sql


def test_per_item_runs_one_extra_grouped_query_with_the_same_filter():
    per = pd.DataFrame({"item": ["q2", "q1"], "n": [4, 5], "resp_min": [0.0, 1.0],
                        "resp_max": [1.0, 3.0], "n_resp_levels": [2, 3]})
    f = _FakeRedivis(per_item=per)
    with patch.object(ts, "redivis", f):
        out = table_sets([_Dataset(_Table())], "t", per_item=True)
    assert len(f.sql) == 4
    assert "NOT IN ('NA', '')" in f.sql[-1]
    # Sorted locally: read streams do not promise ORDER BY's row order.
    assert out["per_item"]["item"].tolist() == ["q1", "q2"]
    assert out["per_item"]["n"].tolist() == [5, 4]


def test_missing_columns_give_none_rather_than_a_failing_query(fake):
    out = table_sets([_Dataset(_Table(variables=("id", "resp")))], "t", per_item=True)
    assert out["items"] is None
    assert out["per_item"] is None
    assert not any("CAST(item" in s for s in fake.sql)


# --- table resolution --------------------------------------------------------

def test_resolution_walks_shards_past_not_found_like_fetch(fake):
    first, second = _Dataset(missing=True), _Dataset(_Table())
    out = table_sets([first, second], "t")
    assert first.asked == 1 and second.asked == 1
    assert out["table"] == REF


def test_a_table_in_no_shard_raises_does_not_exist(fake):
    with pytest.raises(ValueError, match="does not exist in the IRW database"):
        table_sets([_Dataset(missing=True), _Dataset(missing=True)], "nope")
    assert fake.sql == []


def test_quota_during_lookup_raises_the_quota_message(fake):
    ds = _Dataset()
    ds.table = MagicMock(side_effect=Exception("You cannot export more than 5TB of data"))
    with pytest.raises(ValueError, match="export quota"):
        table_sets([ds], "t")


@pytest.mark.parametrize("bad", ["", None, ["a", "b"]])
def test_name_must_be_one_non_empty_string(bad):
    with pytest.raises(ValueError):
        table_sets([_Dataset(_Table())], bad)


# --- query errors go through the #21/#38 classification ----------------------

def test_quota_on_a_query_is_reported_as_quota_not_a_raw_exception():
    f = _FakeRedivis(error=Exception("RESOURCE_EXHAUSTED: rate limit"))
    with patch.object(ts, "redivis", f):
        with pytest.raises(RuntimeError) as info:
            table_sets([_Dataset(_Table())], "t")
    assert "quota" in str(info.value).lower()
    assert "invalid format" not in str(info.value)


def test_auth_on_a_query_tells_the_user_to_sign_in():
    f = _FakeRedivis(error=Exception("401 Unauthorized"))
    with patch.object(ts, "redivis", f):
        with pytest.raises(RuntimeError, match="authentication failed"):
            table_sets([_Dataset(_Table())], "t")


def test_other_query_errors_are_sanitized():
    f = _FakeRedivis(error=Exception("Syntax error near item_response_warehouse:as2e"))
    with patch.object(ts, "redivis", f):
        with pytest.raises(RuntimeError) as info:
            table_sets([_Dataset(_Table())], "t")
    assert "error occurred while querying" in str(info.value)
    assert "item_response_warehouse:as2e" not in str(info.value)


def test_transient_query_failures_are_retried(monkeypatch):
    monkeypatch.setattr("irw.utils.redivis.tables.time.sleep", lambda s: None)
    f = _FakeRedivis()
    real_query = f.query
    calls = {"n": 0}

    def flaky(sql):
        calls["n"] += 1
        if calls["n"] == 1:
            q = MagicMock()
            q.to_pandas_dataframe.side_effect = Exception("Read timed out")
            return q
        return real_query(sql)

    f.query = flaky
    with patch.object(ts, "redivis", f):
        out = table_sets([_Dataset(_Table())], "t")
    assert out["n_rows"] == 10


# --- resp coercion -----------------------------------------------------------

def test_all_numeric_values_come_back_as_sorted_numbers():
    assert _coerce_resp_set(["10", "2", "1"]) == [1, 2, 10]
    assert _coerce_resp_set(["0.5", "1"]) == [0.5, 1.0]


def test_one_non_numeric_value_keeps_the_whole_set_as_strings():
    assert _coerce_resp_set(["2", "b", "1"]) == ["1", "2", "b"]


def test_nom_is_always_character_even_when_every_value_parses():
    """Nominal responses are category labels, not numbers."""
    assert _coerce_resp_set(["2", "10", "1"], source="nom") == ["1", "10", "2"]


def test_spellings_of_the_same_number_collapse():
    assert _coerce_resp_set(["1", "1.0", "2"]) == [1.0, 2.0]


def test_empty_set_is_empty():
    assert _coerce_resp_set([]) == []
    assert _coerce_resp_set([], source="nom") == []


def test_nom_source_reaches_the_coercion():
    f = _FakeRedivis(resp=("2", "1"))
    with patch.object(ts, "redivis", f):
        out = table_sets([_Dataset(_Table())], "t", source="nom")
    assert out["resp"] == ["1", "2"]


# --- public surface ----------------------------------------------------------

def test_exported_and_dispatches_source():
    assert "table_sets" in irw.__all__
    with patch("irw.api._get_datasets") as get, patch("irw.api._table_sets") as inner:
        get.return_value = ["ds"]
        irw.table_sets("t", source="nom", per_item=True)
    get.assert_called_once_with("nom")
    inner.assert_called_once_with(["ds"], "t", source="nom", per_item=True)


def test_quota_message_points_at_table_sets():
    from irw.utils.redivis.tables import _terminal_error_message

    msg = _terminal_error_message(Exception("export quota exceeded"), "t")
    assert "irw.table_sets()" in msg


# --- live ----------------------------------------------------------------------

@pytest.mark.skipif(
    os.getenv("RUN_REDIVIS_TESTS") != "1",
    reason="live Redivis checks; set RUN_REDIVIS_TESTS=1",
)
def test_live_table_sets_on_a_small_table():
    """COACH_Chen_2022_PHQ9: 122,645 rows. Queries only; nothing is exported."""
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        out = irw.table_sets("COACH_Chen_2022_PHQ9", per_item=True)
    assert out["n_rows"] > 0
    assert "coach_chen_2022_phq9" in out["table"].lower()
    assert out["items"] and all(isinstance(i, str) for i in out["items"])
    assert out["resp"] and "NA" not in out["resp"]
    assert sorted(out["per_item"]["item"].tolist()) == out["items"]
    assert int(out["per_item"]["n"].sum()) <= out["n_rows"]
