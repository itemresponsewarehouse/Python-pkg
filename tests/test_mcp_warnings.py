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
