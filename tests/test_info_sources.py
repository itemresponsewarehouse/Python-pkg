"""info(table, source=) for every source, as R's irw_info(table, source = ).

Before, info() on a table raised for anything but main. A non-main table's
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
                        lambda source="main": seen.append(source) or combined)
    monkeypatch.setattr(tm, "get_metadata_table", lambda source="main": meta)
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


def test_info_on_an_unknown_table_says_which_source(fake_conj, capsys):
    assert irw.info("nope", source="conj", return_dict=True) == {}
    assert "source='conj'" in capsys.readouterr().out


def test_main_layout_is_unchanged(monkeypatch):
    combined = pd.DataFrame({"table": ["t"], "n_responses": [10], "construct_type": ["x"]})
    monkeypatch.setattr(table_helpers, "_table_info", lambda source="main": combined)
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
def fake_main(monkeypatch):
    meta = pd.DataFrame({"table": ["fake_table"], "n_responses": [10], "n_items": [2]})
    combined = meta.assign(Description="d", Reference_x="r", DOI__for_paper_="1",
                           URL__for_data_="u", Derived_License="CC0 1.0", BibTex="@a{b,}",
                           dataset="item_response_warehouse")
    monkeypatch.setattr(table_helpers, "_table_info", lambda source="main": combined)
    return combined


def test_item_text_comes_from_the_itemtext_index(fake_main, monkeypatch, capsys):
    monkeypatch.setattr(table_helpers, "_list_itemtext_tables", lambda: {"fake_table"})
    d = irw.info("fake_table", return_dict=True)
    assert d["item_text"] is True
    assert "Item-level text: Available" in capsys.readouterr().out


def test_absent_item_text_is_reported_as_absent(fake_main, monkeypatch, capsys):
    monkeypatch.setattr(table_helpers, "_list_itemtext_tables", lambda: {"other_table"})
    d = irw.info("fake_table", return_dict=True)
    assert d["item_text"] is False
    assert "Item-level text: Not available" in capsys.readouterr().out


def test_an_unreadable_index_is_not_reported_as_absence(fake_main, monkeypatch, capsys):
    def boom():
        raise RuntimeError("Redivis timed out")
    monkeypatch.setattr(table_helpers, "_list_itemtext_tables", boom)
    d = irw.info("fake_table", return_dict=True)
    assert d["item_text"] is None
    assert "Item-level text: Could not be checked" in capsys.readouterr().out
