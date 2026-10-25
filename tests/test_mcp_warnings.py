"""The package's warning filters must survive a tool call.

irw/__init__.py ignores two UserWarnings on purpose: Redivis's "No reference
id was provided for the table" advice (reference ids are reminted on every
release, and pinning one is exactly what broke filter() before 0.1.0) and the
pkg_resources deprecation. A simplefilter("always") inside IRWTools._call put
itself in front of those filters, so a session's first response carried the
pinned-id advice once per catalogue table, and an assistant relayed it as an
IRW data problem. Flagged in the review of #41; these are the regression tests.

A warning the package did not silence must still reach the client, and on
every call rather than only the first: catch_warnings() invalidates the
once-per-location registry on entry, which is what makes record=True enough
on its own.
"""

import importlib
import warnings

import pytest

import irw
from irw.mcp import IRWTools
from test_mcp import FakeBackend, FakeSource

SILENCED = [
    "No reference id was provided for the table metadata, which may cause "
    "your code to break if the name changes. Consider using the qualified "
    "reference metadata:z4cs",
    "pkg_resources is deprecated as an API.",
]
KEPT = "The package meant to say this."


class NoisyBackend(FakeBackend):
    def list_tables(self):
        for message in SILENCED:
            warnings.warn(message, UserWarning)
        warnings.warn(KEPT, UserWarning)
        return super().list_tables()


@pytest.fixture
def package_filters():
    """pytest wraps collection in catch_warnings(), so the filters that
    irw/__init__.py installed while the test modules were being imported are
    discarded before any test runs. Re-run that module so the test sees the
    filters a real process has, and restore pytest's afterwards."""
    with warnings.catch_warnings():
        importlib.reload(irw)
        yield


def test_silenced_package_warnings_do_not_reach_the_client(package_filters):
    tools = IRWTools(NoisyBackend(), FakeSource())
    result = tools.search_tables(limit=1)
    assert not any("reference id" in w for w in result["warnings"]), result["warnings"]
    assert not any("pkg_resources" in w for w in result["warnings"]), result["warnings"]


def test_a_warning_the_package_did_not_silence_still_reaches_the_client(package_filters):
    tools = IRWTools(NoisyBackend(), FakeSource())
    first = tools.search_tables(limit=1)["warnings"]
    second = tools.search_tables(limit=1)["warnings"]
    assert KEPT in first, first
    assert KEPT in second, second


# ---- long2resp's disclosures must reach the client ----
#
# `_call` runs package code inside redirect_stdout(io.StringIO()) so package
# output cannot corrupt MCP stdio, and that buffer is dropped. long2resp says
# which occasion it chose with `print`, so `fetch_table(wide=true)` on a
# two-wave table used to return one wave's data with nothing to say a choice
# had been made -- the agent-facing path lost the only cue that the reshape is
# lossy. The frame now carries the messages in `attrs` and `_call` forwards
# them.


class ReshapingBackend(FakeBackend):
    """A backend whose wide fetch behaves like long2resp: it chooses a wave."""

    def fetch_table(self, table_name, *, wide, dedup, max_rows=None, columns=None):
        frame = super().fetch_table(
            table_name, wide=wide, dedup=dedup, max_rows=max_rows, columns=columns
        )
        if frame is not None and wide:
            print("Defaulting to the most frequent wave: 1")
            frame.attrs["irw_messages"] = ["Defaulting to the most frequent wave: 1"]
        return frame


def test_the_wave_choice_reaches_the_caller():
    tools = IRWTools(ReshapingBackend(), FakeSource())
    payload = tools.fetch_table("alpha_depression", wide=True)
    assert any("most frequent wave" in w for w in payload["warnings"])


def test_a_frame_without_messages_adds_no_warning():
    tools = IRWTools(ReshapingBackend(), FakeSource())
    payload = tools.fetch_table("alpha_depression", wide=False)
    assert not any("most frequent wave" in w for w in payload["warnings"])


def test_a_bare_print_is_still_swallowed():
    """Forwarding `attrs` rather than the captured stdout keeps this opt-in:
    a print meant for a human at a REPL must not become an MCP warning."""

    class ChattyBackend(FakeBackend):
        def fetch_table(self, table_name, *, wide, dedup, max_rows=None, columns=None):
            print("loading the table, please wait")
            return super().fetch_table(table_name, wide=wide, dedup=dedup,
                                       max_rows=max_rows, columns=columns)

    tools = IRWTools(ChattyBackend(), FakeSource())
    payload = tools.fetch_table("alpha_depression")
    assert not any("please wait" in w for w in payload["warnings"])
