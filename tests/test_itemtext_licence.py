"""itemtext_licence(): the licence on a table's wording travels with the data.

The record is the repository's itemtext_licences.csv; these tests stub the
fetched table so they run offline, and check the three outcomes a caller can
see: a licensed table, an unlicensed one, and a failed fetch (which must not
read as "unlicensed").
"""
import warnings

import pandas as pd
import pytest

import irw
import irw.utils.redivis.item_text as it


FAKE = {
    "iowa_gambling_task": {
        "table": "iowa_gambling_task", "licence": "CC BY-SA 4.0",
        "licence_url": "https://creativecommons.org/licenses/by-sa/4.0/",
        "attribution": "Steingroever et al. (2015)",
        "changes": "Reformatted by IRW to one row per item.", "basis": "deposit",
    }
}


@pytest.fixture(autouse=True)
def _reset_state():
    old = dict(it._licence_state)
    it._licence_state.update({"table": None, "failed": False, "noticed": set()})
    yield
    it._licence_state.update(old)


def test_lookup_is_case_insensitive_and_none_when_unlicensed(monkeypatch):
    monkeypatch.setattr(it, "_itemtext_licences", lambda: FAKE)
    assert irw.itemtext_licence("IOWA_GAMBLING_TASK")["licence"] == "CC BY-SA 4.0"
    assert irw.itemtext_licence("gilbert_meta_49") is None


def test_itemtext_warns_once_and_attaches_the_record(monkeypatch):
    monkeypatch.setattr(it, "_itemtext_licences", lambda: FAKE)
    it.disable_itemtext_disclaimer()
    frame = pd.DataFrame({"item": ["i1"], "item_text": ["t"]})
    monkeypatch.setattr("irw.api._get_table_itemtext", lambda name: frame)
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        out = irw.itemtext("iowa_gambling_task")
        irw.itemtext("iowa_gambling_task")
    assert out.attrs["wording_licence"]["licence"] == "CC BY-SA 4.0"
    msgs = [str(w.message) for w in caught if "wording licence" in str(w.message)]
    assert len(msgs) == 1 and "implies no licence" in msgs[0]


def test_unlicensed_table_gets_no_attr(monkeypatch):
    monkeypatch.setattr(it, "_itemtext_licences", lambda: FAKE)
    it.disable_itemtext_disclaimer()
    frame = pd.DataFrame({"item": ["i1"], "item_text": ["t"]})
    monkeypatch.setattr("irw.api._get_table_itemtext", lambda name: frame)
    out = irw.itemtext("gilbert_meta_49")
    assert "wording_licence" not in out.attrs


def test_failed_fetch_warns_once_and_returns_none(monkeypatch):
    monkeypatch.setattr(it, "_ITEMTEXT_LICENCES_URL", "http://127.0.0.1:9/nope.csv")
    with pytest.warns(UserWarning, match="Could not fetch"):
        assert irw.itemtext_licence("iowa_gambling_task") is None
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        assert irw.itemtext_licence("iowa_gambling_task") is None  # no second warning
    assert it._licence_state["failed"] is True
