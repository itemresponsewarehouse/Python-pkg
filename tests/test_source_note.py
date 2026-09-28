"""The fetch-time credit note for tables found via openESM and the like (ben-domingue/irw#2421).

Offline: the biblio lookup is replaced, so nothing here reaches Redivis. The
rules under test are the ones settled on the issue -- once per session per
source, an off switch, and silence (never an error) when the lookup fails.
"""

import warnings

import pandas as pd
import pytest

import irw
import irw.api as api
from irw.utils.redivis import source_note as sn
LOOKUP = {
    "bailon_2020_covidaffect": {"via": "openESM",
                                "url": "https://zenodo.org/records/22763396"},
    "other_esm": {"via": "openESM", "url": None},
    "gilbert_meta_1": {"via": "gilbert_meta", "url": None},
}


@pytest.fixture(autouse=True)
def fresh_state(monkeypatch):
    monkeypatch.setitem(sn._state, "enabled", True)
    monkeypatch.setitem(sn._state, "shown", set())
    monkeypatch.setitem(sn._state, "lookup", None)
    monkeypatch.delenv("IRW_SOURCE_NOTE", raising=False)
    monkeypatch.setattr(sn, "_load_lookup", lambda: dict(LOOKUP))


def _notes(fn):
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        fn()
    return [str(w.message) for w in caught]


def test_note_names_the_source_and_its_citation():
    notes = _notes(lambda: sn._source_note(["Bailon_2020_CovidAffect"]))
    assert len(notes) == 1
    assert "found via openESM" in notes[0]
    assert "https://zenodo.org/records/22763396" in notes[0]
    assert "10.3758/s13428-026-03112-y" in notes[0]


def test_once_per_session_per_source_not_per_table():
    notes = _notes(lambda: (sn._source_note(["bailon_2020_covidaffect"]),
                            sn._source_note(["other_esm", "gilbert_meta_1"])))
    assert len(notes) == 2
    assert "openESM" in notes[0] and "gilbert_meta" in notes[1]


def test_an_unregistered_source_gets_the_note_without_a_citation():
    notes = _notes(lambda: sn._source_note(["gilbert_meta_1"]))
    assert len(notes) == 1 and "found via gilbert_meta." in notes[0]
    assert "cite" not in notes[0]


def test_tables_with_no_source_via_are_silent():
    assert _notes(lambda: sn._source_note(["environment_ltm"])) == []


@pytest.mark.parametrize("off", ["function", "env"])
def test_off_switch(off, monkeypatch):
    if off == "function":
        irw.disable_source_note()
    else:
        monkeypatch.setenv("IRW_SOURCE_NOTE", "0")
    assert _notes(lambda: sn._source_note(["bailon_2020_covidaffect"])) == []


def test_only_the_main_source_is_looked_up(monkeypatch):
    def boom():
        raise AssertionError("looked up biblio for a non-main source")
    monkeypatch.setattr(sn, "_load_lookup", boom)
    assert _notes(lambda: sn._source_note(["x"], source="nom")) == []


def test_a_failed_lookup_is_silent_and_not_repeated(monkeypatch):
    calls = []

    def failing_query(*a, **k):
        calls.append(1)
        raise RuntimeError("no Source_via column in this release")

    import redivis
    monkeypatch.setattr(redivis, "query", failing_query)
    monkeypatch.setattr(sn, "_load_lookup", _real_load_lookup)
    monkeypatch.setattr("irw.utils.redivis.table_metadata._get_meta_dataset",
                        lambda: _FakeMeta())
    assert _notes(lambda: sn._source_note(["bailon_2020_covidaffect"])) == []
    assert _notes(lambda: sn._source_note(["bailon_2020_covidaffect"])) == []
    assert len(calls) == 1


def test_fetch_prints_the_note_for_the_tables_it_returned(monkeypatch):
    monkeypatch.setattr(api, "_get_datasets", lambda source: [])
    monkeypatch.setattr(api, "_fetch", lambda datasets, name, **k: {
        "bailon_2020_covidaffect": pd.DataFrame({"id": [1]}), "missing": None})
    notes = _notes(lambda: irw.fetch(["bailon_2020_covidaffect", "missing"]))
    assert len(notes) == 1 and "openESM" in notes[0]


# The real loader, captured before the autouse fixture replaces it.
_real_load_lookup = sn._load_lookup


class _FakeTable:
    qualified_reference = "datapages.irw_meta.biblio"


class _FakeMeta:
    def table(self, name):
        return _FakeTable()


def _live_loader(monkeypatch, query):
    """The real loader, with Redivis replaced by `query`."""
    import redivis
    monkeypatch.setattr(redivis, "query", query)
    monkeypatch.setattr(sn, "_load_lookup", _real_load_lookup)
    monkeypatch.setattr("irw.utils.redivis.table_metadata._get_meta_dataset",
                        lambda: _FakeMeta())


class _Result:
    def to_pandas_dataframe(self, progress=False):
        return pd.DataFrame({"table": ["Bailon_2020_CovidAffect"],
                             "Source_via": ["openESM"],
                             "URL__for_data_": ["https://zenodo.org/records/22763396"]})


def test_the_lookup_is_kept_on_disk_for_the_next_session(monkeypatch):
    calls = []
    _live_loader(monkeypatch, lambda sql: calls.append(sql) or _Result())
    assert len(_notes(lambda: sn._source_note(["bailon_2020_covidaffect"]))) == 1
    # A new session: nothing in memory, the file is still there.
    monkeypatch.setitem(sn._state, "lookup", None)
    monkeypatch.setitem(sn._state, "shown", set())
    assert len(_notes(lambda: sn._source_note(["bailon_2020_covidaffect"]))) == 1
    assert len(calls) == 1
    assert "Source_via" in calls[0]


def test_a_failure_is_cached_too_so_later_sessions_do_not_pay_for_it(monkeypatch):
    calls = []

    def failing(sql):
        calls.append(sql)
        raise RuntimeError("no Source_via column in this release")

    _live_loader(monkeypatch, failing)
    sn._source_note(["bailon_2020_covidaffect"])
    monkeypatch.setitem(sn._state, "lookup", None)
    sn._source_note(["bailon_2020_covidaffect"])
    assert len(calls) == 1


def test_a_week_old_cache_is_refreshed(monkeypatch):
    import os
    calls = []
    _live_loader(monkeypatch, lambda sql: calls.append(sql) or _Result())
    _notes(lambda: sn._source_note(["bailon_2020_covidaffect"]))
    old = sn._cache_path().stat().st_mtime - sn.CACHE_TTL_SECONDS - 60
    os.utime(sn._cache_path(), (old, old))
    monkeypatch.setitem(sn._state, "lookup", None)
    _notes(lambda: sn._source_note(["bailon_2020_covidaffect"]))
    assert len(calls) == 2
