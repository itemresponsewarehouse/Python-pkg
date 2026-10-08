"""save_bibtex(source=): each source reads its own bibliography.

Before this, save_bibtex() read the core biblio only, so a comps, nominal,
simsyn or conjoint table always came back with no entry, while R's
irw_save_bibtex(source = ) found it.
"""
import pandas as pd
import pytest

import irw
import irw.utils.redivis.table_metadata as tm
from irw.config import SOURCES


def _fake(monkeypatch, seen):
    def biblio(source="main"):
        seen.append(("biblio", source))
        return pd.DataFrame({"table": [f"t_{source}"],
                             "BibTex": [f"@article{{K_{source}, title={{T}}}}"],
                             "DOI__for_paper_": [None]})

    def existing(source="main"):
        seen.append(("existing", source))
        return {f"t_{source}"}

    monkeypatch.setattr(tm, "get_biblio_table", biblio)
    monkeypatch.setattr(tm, "_get_existing_tables", existing)


@pytest.mark.parametrize("source", SOURCES)
def test_each_source_reads_its_own_biblio(monkeypatch, source):
    seen = []
    _fake(monkeypatch, seen)
    out = irw.save_bibtex(f"t_{source}", source=source)
    assert out == [f"@article{{t_{source}, title={{T}}}}"]
    assert seen == [("biblio", source), ("existing", source)]


def test_default_source_is_main(monkeypatch):
    seen = []
    _fake(monkeypatch, seen)
    assert len(irw.save_bibtex("t_main")) == 1
    assert seen[0] == ("biblio", "main")


def test_a_table_is_not_found_under_another_source(monkeypatch):
    _fake(monkeypatch, [])
    assert irw.save_bibtex("t_comp", source="main") == []


def test_every_source_has_a_biblio_table():
    for source in SOURCES:
        assert tm._meta_table(source, "biblio")


def test_unknown_source_raises():
    with pytest.raises(ValueError):
        tm._meta_table("nope", "biblio")
