"""The credential gate: no credential and no terminal is an error, not a wait.

The behaviour under test is the one the review measured as four minutes of
silence before a kill (ben-domingue/irw#3221): the Redivis SDK's fallback for a
missing credential is a browser login, and in a script nobody completes it. R
has answered this with ``irw_has_credentials()`` since 1.3.0, so these tests
also pin the parity -- the predicate is public, and the failure is a named
error rather than a hang.
"""

import json
from pathlib import Path

import pytest

from irw.utils.redivis.auth import (
    IRWCredentialsUnavailable,
    credential_state,
    has_credentials,
    require_credentials,
)


@pytest.fixture(autouse=True)
def _no_ambient_credentials(monkeypatch, tmp_path):
    """Neither the developer's token nor their cached login reaches these tests."""
    monkeypatch.delenv("REDIVIS_API_TOKEN", raising=False)
    monkeypatch.delenv("IRW_ALLOW_INTERACTIVE_LOGIN", raising=False)
    monkeypatch.setattr(Path, "home", classmethod(lambda cls: tmp_path))


def _write_credentials(tmp_path, **fields):
    directory = tmp_path / ".redivis"
    directory.mkdir(exist_ok=True)
    (directory / "python_credentials").write_text(json.dumps(fields))


def _noninteractive(monkeypatch):
    """A session with no terminal: a cron job, a CI runner, an MCP server."""
    monkeypatch.setattr("irw.utils.redivis.auth._is_interactive", lambda: False)


def test_a_token_is_enough(monkeypatch):
    monkeypatch.setenv("REDIVIS_API_TOKEN", "x")
    assert credential_state() == ("ok", None)
    assert has_credentials() is True


def test_no_credential_at_all_is_missing():
    state, message = credential_state()
    assert state == "missing"
    assert "REDIVIS_API_TOKEN" in message
    assert has_credentials() is False


def test_a_live_cached_login_is_ok(tmp_path):
    import time

    _write_credentials(tmp_path, access_token="t", expires_at=time.time() + 3600)
    assert has_credentials() is True


def test_an_expired_cached_login_is_not_ok(tmp_path):
    import time

    _write_credentials(tmp_path, access_token="t", expires_at=time.time() - 3600)
    state, message = credential_state()
    assert state == "expired"
    assert "expired" in message
    assert has_credentials() is False


def test_an_unreadable_credential_file_is_named_as_such(tmp_path):
    directory = tmp_path / ".redivis"
    directory.mkdir()
    (directory / "python_credentials").write_text("not json at all")
    assert credential_state()[0] == "unreadable"


def test_a_credential_with_no_expiry_is_trusted(tmp_path):
    """Matches the SDK's own guard, which wants `expires_at` before distrusting."""
    _write_credentials(tmp_path, access_token="t")
    assert has_credentials() is True


def test_require_credentials_raises_where_no_login_could_complete(monkeypatch):
    _noninteractive(monkeypatch)
    with pytest.raises(IRWCredentialsUnavailable) as error:
        require_credentials()
    assert error.value.state == "missing"
    assert "REDIVIS_API_TOKEN" in str(error.value)


def test_require_credentials_stays_out_of_the_way_interactively(monkeypatch):
    """A person at a prompt can finish the browser login, so let the SDK ask."""
    monkeypatch.setattr("irw.utils.redivis.auth._is_interactive", lambda: True)
    require_credentials()


def test_the_gate_can_be_opted_out_of(monkeypatch):
    _noninteractive(monkeypatch)
    monkeypatch.setenv("IRW_ALLOW_INTERACTIVE_LOGIN", "1")
    require_credentials()


def test_the_error_says_how_to_bypass_it(monkeypatch):
    _noninteractive(monkeypatch)
    with pytest.raises(IRWCredentialsUnavailable) as error:
        require_credentials()
    assert "IRW_ALLOW_INTERACTIVE_LOGIN" in str(error.value)


def test_opening_a_dataset_stops_before_the_sdk_is_reached(monkeypatch):
    """The guard sits at the one point every irw call opens a dataset through.

    `redivis.user` is replaced with something that fails loudly, so the test
    distinguishes "stopped by the gate" from "stopped by the SDK": only the
    first is the behaviour we want.
    """
    from irw.utils.redivis import datasets

    _noninteractive(monkeypatch)

    def explode(*args, **kwargs):  # pragma: no cover - must not run
        raise AssertionError("the SDK was reached despite having no credential")

    monkeypatch.setattr(datasets.redivis, "user", explode)
    with pytest.raises(IRWCredentialsUnavailable):
        datasets._init_dataset("datapages", "item_response_warehouse")


def test_the_predicate_is_public():
    """Parity with R, where `irw_has_credentials()` is exported and documented."""
    import irw

    assert irw.has_credentials is not None
    assert "has_credentials" in irw.__all__
    assert "IRWCredentialsUnavailable" in irw.__all__


def test_the_mcp_server_reads_the_same_state(monkeypatch, tmp_path):
    """One classification, two wordings -- the MCP copy is gone (#2157, #3221)."""
    import time

    from irw.mcp import IRWMCPError, PackageBackend

    _write_credentials(tmp_path, access_token="t", expires_at=time.time() - 3600)
    with pytest.raises(IRWMCPError) as error:
        PackageBackend().ensure_ready()
    assert error.value.code == "authentication_required"
    assert "expired" in error.value.message
