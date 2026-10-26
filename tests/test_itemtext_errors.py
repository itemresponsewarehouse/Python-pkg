"""A failed item-text fetch must not be reported as "IRW has no item text".

`itemtext()` returns `Union[pd.DataFrame, str]`, so callers tell the two cases
apart by type and a `str` means absence. The function used to end in a bare
`except Exception` that returned the absence sentence for network timeouts,
quota exhaustion and auth failures alike, so a Redivis hiccup silently became
a factual claim about the corpus (issue #42). Only a minority of IRW tables
have item text, which is what made the wrong answer plausible enough that
nobody investigated it.

No network: every dataset, table and error here is synthetic.
"""

import importlib

import pandas as pd
import pytest

from irw.utils import table_helpers


ABSENT = "Item-level text is not available"


class _Table:
    """Stand-in for a Redivis table whose download does something specific."""

    def __init__(self, outcome):
        self._outcome = outcome
        self.calls = 0

    def to_pandas_dataframe(self, *args, **kwargs):
        self.calls += 1
        if isinstance(self._outcome, Exception):
            raise self._outcome
        return self._outcome


@pytest.fixture
def has_item_text(monkeypatch):
    """Every table in these tests is one the corpus DOES have item text for."""
    monkeypatch.setattr(
        table_helpers, "_list_itemtext_tables", lambda: {"a_table"}
    )


def _install(monkeypatch, outcome):
    table = _Table(outcome)
    monkeypatch.setattr(
        table_helpers, "_get_itemtext_table", lambda name: table
    )
    return table


# --- the bug -------------------------------------------------------------

@pytest.mark.parametrize("error", [
    ConnectionError("connection reset by peer"),
    TimeoutError("read timed out"),
    OSError("pyarrow: IO error"),
])
def test_a_transient_failure_raises_rather_than_claiming_absence(
    monkeypatch, has_item_text, error
):
    _install(monkeypatch, error)
    with pytest.raises(Exception) as caught:
        table_helpers._get_table_itemtext("a_table")
    assert ABSENT not in str(caught.value)


def test_quota_exhaustion_is_named_not_swallowed(monkeypatch, has_item_text):
    """Redivis reports the 30-day export cap as `invalid_request` (issue #21).

    It reached this function as a bare exception and left as "no item text".
    """
    err = Exception({
        "status": 400,
        "error": "invalid_request",
        "error_description": (
            "You cannot export more than 5TB of data within a 30 day period."
        ),
    })
    _install(monkeypatch, err)
    with pytest.raises(RuntimeError, match="quota"):
        table_helpers._get_table_itemtext("a_table")


def test_auth_failure_is_named_not_swallowed(monkeypatch, has_item_text):
    err = Exception({"status": 401, "error": "unauthorized"})
    _install(monkeypatch, err)
    with pytest.raises(RuntimeError, match="authentication"):
        table_helpers._get_table_itemtext("a_table")


def test_a_lookup_failure_propagates(monkeypatch, has_item_text):
    """The shard search re-raises terminal errors; nothing above may eat them."""
    def boom(name):
        raise RuntimeError("shard search failed")

    monkeypatch.setattr(table_helpers, "_get_itemtext_table", boom)
    with pytest.raises(RuntimeError, match="shard search failed"):
        table_helpers._get_table_itemtext("a_table")


# --- what must still be absence -----------------------------------------

def test_a_table_with_no_item_text_still_returns_the_sentence(monkeypatch):
    monkeypatch.setattr(table_helpers, "_list_itemtext_tables", lambda: set())
    result = table_helpers._get_table_itemtext("a_table")
    assert isinstance(result, str) and ABSENT in result


def test_a_stale_index_entry_still_returns_the_sentence(
    monkeypatch, has_item_text
):
    """Index says yes, shard search says no -- the shape a withdrawal leaves
    behind in a process holding a pre-release cache (issue #51). Absence is
    the right answer for the caller."""
    monkeypatch.setattr(table_helpers, "_get_itemtext_table", lambda name: None)
    result = table_helpers._get_table_itemtext("a_table")
    assert isinstance(result, str) and ABSENT in result


def test_a_successful_fetch_still_returns_the_frame(monkeypatch, has_item_text):
    frame = pd.DataFrame({"item": ["q1"], "item_text": ["How are you?"]})
    _install(monkeypatch, frame)
    result = table_helpers._get_table_itemtext("a_table")
    assert isinstance(result, pd.DataFrame)
    assert result.equals(frame)


# --- retry ---------------------------------------------------------------

def test_a_transient_download_error_is_retried_before_it_raises(
    monkeypatch, has_item_text
):
    """fetch() retries transient read failures; item text now does too, so a
    single hiccup is not surfaced to the user at all."""
    monkeypatch.setattr("time.sleep", lambda s: None)
    table = _install(monkeypatch, ConnectionError("connection reset"))
    with pytest.raises(ConnectionError):
        table_helpers._get_table_itemtext("a_table")
    assert table.calls == 3


def test_a_terminal_error_is_not_retried(monkeypatch, has_item_text):
    """Quota applies to the account, so trying twice more just wastes time."""
    monkeypatch.setattr("time.sleep", lambda s: None)
    table = _install(monkeypatch, Exception({
        "status": 400,
        "error": "invalid_request",
        "error_description": "You cannot export more than 5TB of data.",
    }))
    with pytest.raises(RuntimeError):
        table_helpers._get_table_itemtext("a_table")
    assert table.calls == 1


# ---- list_tables()' has_item_text column -----------------------------------
#
# The same defect, in the one place it survived R-18: `_compute_item_text_flag`
# answered a failed listing with False for EVERY table at once, so a transient
# error made `list_tables()` report a corpus with no item text and
# `filter(has_item_text=True)` come back empty, silently. A boolean column has
# no room for "unknown", so it propagates (ben-domingue/irw#3229).

def test_a_failed_itemtext_listing_does_not_flag_the_corpus_as_textless(monkeypatch):
    # `irw.operations.list_tables` is shadowed by the function of the same name
    # the package re-exports, so reach the module explicitly.
    lt = importlib.import_module("irw.operations.list_tables")

    def boom():
        raise RuntimeError("HTTP 504 Gateway Timeout")

    monkeypatch.setattr(lt, "_list_itemtext_tables", boom)
    df = pd.DataFrame({"name": ["a", "b"]})
    with pytest.raises(RuntimeError, match="504"):
        lt._compute_item_text_flag(df)
    assert "has_item_text" not in df.columns


def test_a_successful_listing_still_flags_by_membership(monkeypatch):
    lt = importlib.import_module("irw.operations.list_tables")

    monkeypatch.setattr(lt, "_list_itemtext_tables", lambda: {"a"})
    out = lt._compute_item_text_flag(pd.DataFrame({"name": ["A", "b"]}))
    assert out["has_item_text"].tolist() == [True, False]
