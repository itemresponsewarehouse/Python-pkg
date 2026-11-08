"""A bare info() reports every data family, as R's irw_info() does."""
from types import SimpleNamespace

import pytest

import irw
from irw import api
from irw.config import SOURCES

GB = 1024**3


def ds(tables, created=1e12, updated=2e12):
    return SimpleNamespace(properties={"tableCount": tables, "totalNumBytes": GB,
                                       "createdAt": created, "updatedAt": updated})


FAMILIES = {
    "core": [ds(10), ds(5, created=5e11)],
    "nom": [ds(3)], "sim": [ds(2)], "comp": [ds(4)],
    "conj": [ds(6), ds(1)], "rank": [ds(7, updated=3e12)],
}


@pytest.fixture
def opened(monkeypatch):
    seen = []
    def fake(source="core"):
        seen.append(source)
        return FAMILIES[source]
    monkeypatch.setattr(api, "_get_datasets", fake)
    return seen


def test_bare_info_reports_every_family_and_a_grand_total(opened, capsys):
    irw.info()
    out = capsys.readouterr().out
    assert opened == list(SOURCES)
    assert "All Families" in out
    assert "Core           core           15        2.00 GB" in out
    assert "Rankings       rank            7        1.00 GB" in out
    assert "Total Table Count:        38\n" in out
    assert "Total Data Size:          8.00 GB" in out


def test_naming_a_source_reports_that_family_alone(opened, capsys):
    irw.info(source="core")
    out = capsys.readouterr().out
    assert opened == ["core"]
    assert "All Families" not in out
    assert "Total Table Count:        15\n" in out


def test_the_old_alias_still_names_one_family(opened, capsys):
    irw.info(source="main")
    assert opened == ["core"]


def test_an_unavailable_family_is_shown_not_silently_dropped(monkeypatch, capsys):
    def fake(source="core"):
        if source == "conj":
            raise RuntimeError("boom")
        return FAMILIES[source]
    monkeypatch.setattr(api, "_get_datasets", fake)
    with pytest.warns(UserWarning, match="'conj' family: boom"):
        irw.info()
    out = capsys.readouterr().out
    assert "conj" in out and "unavailable" in out
    assert "Total (available only):   31\n" in out


def test_no_family_available_raises_the_original_error(monkeypatch, recwarn):
    # An expired login fails every family; the user needs its one message,
    # which says how to sign in, not six warnings and a generic error.
    def fake(source="core"):
        raise PermissionError("please sign in")
    monkeypatch.setattr(api, "_get_datasets", fake)
    with pytest.raises(PermissionError, match="please sign in"):
        irw.info()
    assert not recwarn.list
