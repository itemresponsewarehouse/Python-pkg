"""Shared pytest setup."""

import pytest

from irw.utils.redivis import disk_cache


@pytest.fixture(autouse=True)
def _isolated_disk_cache(tmp_path_factory, monkeypatch):
    """Point the on-disk table cache at a throwaway folder for every test.

    Without this, a test whose fake table carries a hash would write into the
    developer's real cache -- and read from it on the next run.
    """
    monkeypatch.setenv("IRW_CACHE_DIR", str(tmp_path_factory.mktemp("irw-cache")))
    monkeypatch.delenv("IRW_CACHE", raising=False)
    disk_cache.set_enabled(None)
    disk_cache._state["announced"] = True
    yield
    disk_cache.set_enabled(None)


@pytest.fixture(autouse=True)
def _no_source_note(monkeypatch):
    """Keep fetch()'s credit note from querying the live biblio in unit tests.

    tests/test_source_note.py clears this and replaces the lookup itself.
    """
    monkeypatch.setenv("IRW_SOURCE_NOTE", "0")
