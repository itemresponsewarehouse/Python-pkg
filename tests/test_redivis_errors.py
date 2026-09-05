"""Offline coverage for Redivis error classification.

_classify_error() used to collapse everything Redivis returns with an
`invalid_request` code into one bucket, and all three call sites then told the
user their table had an invalid FORMAT. Redivis reports export-quota
exhaustion that way, so a user who had hit the 30-day cap was sent looking for
a problem in the warehouse that does not exist. Authentication failure was not
classified at all and surfaced as a raw exception. See issue #21.

Ported from Rpkg/R/redivis-errors.R, which already drew these distinctions.

No network: every error is synthetic.
"""

import pytest

from irw.utils.redivis.tables import (
    _classify_error,
    _sanitize_error,
    _terminal_error_message,
    _TERMINAL_ERROR_KINDS,
    _search_datasets,
)


# --- classification --------------------------------------------------------

def test_quota_is_not_reported_as_an_invalid_request():
    """The whole point of the issue: Redivis sends quota AS invalid_request."""
    err = Exception({
        "status": 400,
        "error": "invalid_request",
        "error_description": "You cannot export more than 5TB of data within a 30 day period.",
    })
    assert _classify_error(err) == "quota"


@pytest.mark.parametrize("text", [
    "You cannot export more than 5TB of data",
    "exported 6TB within the past 30 days",
    "Export quota exceeded",
    "rate limit exceeded",
    "Too Many Requests",
    "RESOURCE_EXHAUSTED",
    "HTTP 429 returned",
])
def test_quota_phrasings_all_classify_as_quota(text):
    assert _classify_error(Exception(text)) == "quota"


@pytest.mark.parametrize("text", [
    "401 Unauthorized",
    "user is unauthenticated",
    "permission_denied",
    "access_denied",
    "Forbidden",
    "invalid_grant",
    "invalid_token",
    "You must be logged in",
])
def test_auth_failures_are_classified(text):
    assert _classify_error(Exception(text)) == "auth"


def test_a_genuine_invalid_request_is_still_invalid_request():
    err = Exception({"status": 400, "error": "invalid_request",
                     "error_description": "Table has no exportable columns"})
    assert _classify_error(err) == "invalid_request"


def test_not_found_still_means_try_the_next_datasource():
    assert _classify_error(Exception({"error": "not_found"})) == "not_found"
    assert _classify_error(Exception("Table not found")) == "not_found"


def test_the_transient_path_is_unchanged():
    """Python's retry works and R's does not; this issue must not disturb it."""
    err = Exception("Expected to be able to read 159752 bytes for message body, got 12443")
    assert _classify_error(err) == "transient"


def test_an_unrecognized_error_is_unknown():
    assert _classify_error(Exception("something else entirely")) == "unknown"


# --- search control flow ---------------------------------------------------

def test_quota_and_auth_stop_the_search_rather_than_trying_every_warehouse():
    assert set(_TERMINAL_ERROR_KINDS) == {"quota", "auth", "invalid_request"}


@pytest.mark.parametrize("text", ["export quota exceeded", "401 Unauthorized"])
def test_search_datasets_returns_a_terminal_error_immediately(text):
    calls = {"n": 0}

    def callback(ds):
        calls["n"] += 1
        raise Exception(text)

    result, last_other, terminal = _search_datasets([1, 2, 3], callback)
    assert result is None and last_other is None
    assert terminal is not None
    assert calls["n"] == 1  # did not go on to the other two warehouses


def test_search_datasets_still_walks_past_not_found():
    calls = {"n": 0}

    def callback(ds):
        calls["n"] += 1
        raise Exception("not_found")

    result, last_other, terminal = _search_datasets([1, 2, 3], callback)
    assert (result, last_other, terminal) == (None, None, None)
    assert calls["n"] == 3


# --- message text ----------------------------------------------------------

def test_quota_message_names_the_quota_and_not_the_table():
    err = Exception("You cannot export more than 5TB of data within a 30 day period.")
    msg = _terminal_error_message(err, "agn_kay_2025")
    assert "quota" in msg.lower()
    assert "account-wide" in msg
    assert "invalid format" not in msg


def test_auth_message_tells_the_user_to_sign_in():
    msg = _terminal_error_message(Exception("401 Unauthorized"), "agn_kay_2025")
    assert "authentication failed" in msg.lower()
    assert "invalid format" not in msg


def test_a_real_invalid_format_still_says_invalid_format():
    err = Exception({"error": "invalid_request", "error_description": "bad table"})
    msg = _terminal_error_message(err, "agn_kay_2025")
    assert "invalid format" in msg
    assert "agn_kay_2025" in msg


# --- sanitization ----------------------------------------------------------

@pytest.mark.parametrize("ref", [
    "item_response_warehouse:as2e",
    "item_response_warehouse_6:fpe6",
    "irw_nominal:614n",
    "irw_text_2:ae47",
])
def test_internal_dataset_refs_are_stripped_from_user_facing_text(ref):
    cleaned = _sanitize_error(f"Error fetching datapages.{ref} for table x")
    assert ref not in cleaned
    assert "IRW" in cleaned


def test_sanitize_collapses_whitespace_and_tolerates_empty():
    assert _sanitize_error("  a\n\n  b  ") == "a b"
    assert _sanitize_error(None) == ""
    assert _sanitize_error("") == ""
