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
