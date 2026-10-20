"""Shared pytest setup."""

import pytest

from irw.utils.redivis import disk_cache, shard_discovery, shard_resolution


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
def _no_shard_discovery(monkeypatch):
    """Keep shard discovery from reading the Redivis catalog in unit tests.

    It is on by default in real sessions (see utils/redivis/shard_discovery.py)
    and makes one HTTP request per process. tests/test_shard_discovery.py
    clears this and supplies a fixture catalog.
    """
    monkeypatch.setenv("IRW_SHARD_DISCOVERY", "0")
    shard_discovery.reset_state()
    yield
    shard_discovery.reset_state()


@pytest.fixture(autouse=True)
def _no_shard_map_leak():
    """Clear the resolved shard map between tests.

    It is process state keyed on the metadata version, so a map built by one
    test would otherwise decide another test's shard order.
    """
    shard_resolution.reset_state()
    yield
    shard_resolution.reset_state()


@pytest.fixture(autouse=True)
def _no_source_note(monkeypatch):
    """Keep fetch()'s credit note from querying the live biblio in unit tests.

    tests/test_source_note.py clears this and replaces the lookup itself.
    """
    monkeypatch.setenv("IRW_SOURCE_NOTE", "0")
