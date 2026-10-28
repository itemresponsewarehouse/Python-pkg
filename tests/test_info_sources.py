"""info(table, source=) for every source, as R's irw_info(table, source = ).

Before, info() on a table raised for anything but core. A non-core table's
metadata has its source's own columns, so they are shown as they stand.
"""
import pandas as pd
import pytest

import irw
import irw.utils.redivis.table_metadata as tm
from irw.utils import table_helpers


@pytest.fixture
def fake_conj(monkeypatch):
    meta = pd.DataFrame({"table": ["kreps_2020_covid_vaccine"], "n_respondents": [1971],
                         "outcomes": ["choice;rating"], "country": ["US"]})
    combined = meta.assign(Description="Vaccine conjoint", Reference_x="Kreps et al. (2020)",
                           DOI__for_paper_="10.1/x", URL__for_data_="d",
                           Derived_License="CC0 1.0", BibTex="@article{k,}")
    seen = []
    monkeypatch.setattr(table_helpers, "_table_info",
                        lambda source="core": seen.append(source) or combined)
    monkeypatch.setattr(tm, "get_metadata_table", lambda source="core": meta)
    return seen


def test_info_on_a_conj_table_reads_its_own_source(fake_conj, capsys):
    d = irw.info("kreps_2020_covid_vaccine", source="conj", return_dict=True)
    assert fake_conj == ["conj"]
    assert d["source"] == "conj"
    assert d["stats"] == {"n_respondents": 1971, "outcomes": "choice;rating", "country": "US"}
    assert d["biblio"]["license"] == "CC0 1.0"
    out = capsys.readouterr().out
    assert "(conjoint, source='conj')" in out
    assert "n_respondents: 1,971" in out and "Vaccine conjoint" in out
    assert "save_bibtex(..., source='conj')" in out


def test_info_on_an_unknown_table_raises_and_says_which_source(fake_conj):
    # It used to print a line and return {} -- falsy, and the same answer a
    # table with no metadata gives, so a typo read as a fact about the corpus.
    # R has always errored here (ben-domingue/irw#3221).
    with pytest.raises(irw.IRWTableNotFound, match=r"source='conj'"):
        irw.info("nope", source="conj", return_dict=True)
    with pytest.raises(irw.IRWTableNotFound, match="nope"):
        irw.info("nope", source="conj")


def test_the_mcp_turns_that_into_its_own_not_found(fake_conj):
    """The tool layer's `not_found` names the table and is what an agent can
    act on; an uncaught LookupError would surface as an internal failure."""
    from irw import mcp
    backend = mcp.PackageBackend()
    assert backend.describe_table("nope", source="conj") == {}


def test_core_layout_is_unchanged(monkeypatch):
    combined = pd.DataFrame({"table": ["t"], "n_responses": [10], "construct_type": ["x"]})
    monkeypatch.setattr(table_helpers, "_table_info", lambda source="core": combined)
    d = table_helpers._get_table_info_dict("t")
    assert "source" not in d and d["stats"]["n_responses"] == 10


# ---- info()'s item-text field ----
#
# It used to read `has_item_text` from the merged irw_meta metadata row, where
# there is no such column: `has_item_text` is computed from the item-text
# shards' table index by list_tables(). `info()` therefore printed
# "Item-level text: Not available" for every table in the corpus, including the
# ~2,700 that have it, in the released 0.7.7. These pin the three outcomes
# apart -- has it, does not have it, could not be told -- because the defect
# was precisely that the first and the third were reported as the second.


@pytest.fixture
def fake_core(monkeypatch):
    meta = pd.DataFrame({"table": ["fake_table"], "n_responses": [10], "n_items": [2]})
    combined = meta.assign(Description="d", Reference_x="r", DOI__for_paper_="1",
                           URL__for_data_="u", Derived_License="CC0 1.0", BibTex="@a{b,}",
                           dataset="item_response_warehouse")
    monkeypatch.setattr(table_helpers, "_table_info", lambda source="core": combined)
    return combined


def test_item_text_comes_from_the_itemtext_index(fake_core, monkeypatch, capsys):
    monkeypatch.setattr(table_helpers, "_list_itemtext_tables", lambda: {"fake_table"})
    d = irw.info("fake_table", return_dict=True)
    assert d["item_text"] is True
    assert "Item-level text: Available" in capsys.readouterr().out


def test_absent_item_text_is_reported_as_absent(fake_core, monkeypatch, capsys):
    monkeypatch.setattr(table_helpers, "_list_itemtext_tables", lambda: {"other_table"})
    d = irw.info("fake_table", return_dict=True)
    assert d["item_text"] is False
    assert "Item-level text: Not available" in capsys.readouterr().out


def test_an_unreadable_index_is_not_reported_as_absence(fake_core, monkeypatch, capsys):
    def boom():
        raise RuntimeError("Redivis timed out")
    monkeypatch.setattr(table_helpers, "_list_itemtext_tables", boom)
    d = irw.info("fake_table", return_dict=True)
    assert d["item_text"] is None
    assert "Item-level text: Could not be checked" in capsys.readouterr().out
