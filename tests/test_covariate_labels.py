"""Covariate value labels (ben-domingue/irw#1775).

Offline: irw_meta's `covariate_labels` table is replaced by a fixture whose
rows are copied from `metadata/covariate_labels.csv` in ben-domingue/irw
(the table that will be uploaded). The table does not exist on Redivis yet, so
the absent-table path is tested too.
"""

import numpy as np
import pandas as pd
import pytest

import irw
from irw.utils.redivis import table_metadata
from irw.utils.redivis.cache import metadata_cache
from irw.utils.redivis.table_metadata import CovariateLabelsUnavailable

WITHHELD = "[institution name withheld]"
_REAL_LOADER = table_metadata.get_covariate_labels_table

LABELS = pd.DataFrame(
    [
        ("cucchi_2018_rfq", "cov_gender", "2", "male"),
        ("cucchi_2018_rfq", "cov_gender", "1", "female"),
        ("cucchi_2018_rfq", "cov_ethnicity", "1", "white british"),
        ("cucchi_2018_rfq", "cov_ethnicity", "12", "black caribbean"),
        ("cucchi_2018_rfq", "cov_ethnicity", "2", "white irish"),
        ("estevez_2021_actitu", "cov_gender", "1", "hombre"),
        ("estevez_2021_actitu", "cov_gender", "2", "mujer"),
        ("estevez_2021_actitu", "cov_grade", "1", "5ºEP"),
        ("estevez_2021_actitu", "cov_grade", "2", "6ºEP"),
    ]
    + [("estevez_2021_actitu", "cov_school", str(i), WITHHELD) for i in range(1, 14)],
    columns=["table", "covariate", "code", "label"],
).astype("string")


@pytest.fixture(autouse=True)
def _labels(monkeypatch):
    monkeypatch.setattr(table_metadata, "get_covariate_labels_table", lambda: LABELS.copy())


def _estevez():
    # Codes ship as floats, the way a Redivis numeric column often arrives.
    return pd.DataFrame({
        "id": [1, 1, 2, 2, 3, 3],
        "item": ["a", "b"] * 3,
        "resp": [1, 0, 1, 1, 0, 0],
        "cov_gender": [1.0, 1.0, 2.0, 2.0, np.nan, np.nan],
        "cov_grade": [1.0, 1.0, 2.0, 2.0, 1.0, 1.0],
        "cov_school": [3, 3, 7, 7, 3, 3],
        "cov_other": [9, 9, 8, 8, 7, 7],
    })


# --- covariate_labels() ----------------------------------------------------

def test_covariate_labels_returns_long_rows_sorted_by_numeric_code():
    out = irw.covariate_labels("cucchi_2018_rfq")
    assert list(out.columns) == ["table", "covariate", "code", "label"]
    assert out["covariate"].tolist() == ["cov_ethnicity"] * 3 + ["cov_gender"] * 2
    # 12 after 2: numeric, not lexical, order.
    assert out["code"].tolist() == ["1", "2", "12", "1", "2"]
    assert out.loc[out["covariate"] == "cov_gender", "label"].tolist() == ["female", "male"]


def test_covariate_labels_is_case_insensitive_and_takes_a_list():
    out = irw.covariate_labels(["CUCCHI_2018_RFQ", "estevez_2021_actitu"])
    assert set(out["table"]) == {"cucchi_2018_rfq", "estevez_2021_actitu"}


def test_covariate_labels_withheld_institutions_are_rows_not_names():
    out = irw.covariate_labels("estevez_2021_actitu")
    school = out[out["covariate"] == "cov_school"]
    assert len(school) == 13
    assert set(school["label"]) == {WITHHELD}


def test_covariate_labels_for_an_unlabelled_table_is_empty_with_a_message(capsys):
    out = irw.covariate_labels("no_such_table")
    assert out.empty
    assert list(out.columns) == ["table", "covariate", "code", "label"]
    assert "No covariate value labels" in capsys.readouterr().out


def test_absent_irw_meta_table_is_a_clear_error_not_an_empty_frame(monkeypatch):
    # The real loader, against an irw_meta whose listing has no such table --
    # every irw_meta version before #1775.
    monkeypatch.setattr(table_metadata, "get_covariate_labels_table", _REAL_LOADER)
    metadata_cache.clear()

    class T:
        def __init__(self, name):
            self.name = name

    class DS:
        def table(self, name):  # must not be reached
            raise AssertionError("read attempted on an absent table")

    monkeypatch.setattr(table_metadata, "_get_meta_dataset", lambda: DS())
    monkeypatch.setattr(table_metadata, "_dataset_version_tag", lambda ds: "v30.0")
    monkeypatch.setattr(
        table_metadata, "_dataset_table_list", lambda ds: [T("metadata"), T("tags")]
    )
    with pytest.raises(CovariateLabelsUnavailable, match="no `covariate_labels` table"):
        irw.covariate_labels("cucchi_2018_rfq")
    with pytest.raises(CovariateLabelsUnavailable):
        irw.covariates(_estevez(), labels=True, table="estevez_2021_actitu")
    # Without labels=, covariates() never touches irw_meta.
    assert irw.covariates(_estevez())["cov_gender"].dtype == float
    metadata_cache.clear()


def test_present_irw_meta_table_is_read_as_text(monkeypatch):
    monkeypatch.setattr(table_metadata, "get_covariate_labels_table", _REAL_LOADER)
    metadata_cache.clear()

    class T:
        name = "covariate_labels"

        def to_pandas_dataframe(self):
            return pd.DataFrame({"table": ["t"], "covariate": ["cov_x"],
                                 "code": [1], "label": ["yes"]})

    class DS:
        def table(self, name):
            assert name == "covariate_labels"
            return T()

    monkeypatch.setattr(table_metadata, "_get_meta_dataset", lambda: DS())
    monkeypatch.setattr(table_metadata, "_dataset_version_tag", lambda ds: "v31.0")
    monkeypatch.setattr(table_metadata, "_dataset_table_list", lambda ds: [T()])
    out = irw.covariate_labels("t")
    assert out["code"].tolist() == ["1"]
    assert out["code"].dtype == "string"
    metadata_cache.clear()


# --- covariates(labels=) ---------------------------------------------------

def test_default_output_is_unchanged():
    out = irw.covariates(_estevez())
    assert out["cov_gender"].tolist()[:2] == [1.0, 2.0]
    assert not isinstance(out["cov_gender"].dtype, pd.CategoricalDtype)


def test_labels_true_decodes_to_categoricals(capsys):
    out = irw.covariates(_estevez(), labels=True, table="estevez_2021_actitu")
    g = out["cov_gender"]
    assert isinstance(g.dtype, pd.CategoricalDtype)
    assert list(g.cat.categories) == ["hombre", "mujer"]
    assert g.tolist()[:2] == ["hombre", "mujer"]
    assert pd.isna(g.iloc[2])                       # missing stays missing
    assert out["cov_grade"].tolist() == ["5ºEP", "6ºEP", "5ºEP"]
    msg = capsys.readouterr().out
    assert "Decoded with source labels: cov_gender, cov_grade" in msg


def test_withheld_institutions_stay_as_codes(capsys):
    out = irw.covariates(_estevez(), labels=True, table="estevez_2021_actitu")
    # Decoding would merge 13 schools into one group.
    assert out["cov_school"].tolist() == [3, 7, 3]
    assert "Left as codes" in capsys.readouterr().out


def test_covariates_without_labels_are_untouched():
    out = irw.covariates(_estevez(), labels=True, table="estevez_2021_actitu")
    assert out["cov_other"].tolist() == [9, 8, 7]


def test_codes_without_a_label_keep_their_code_text_and_are_reported(capsys):
    df = pd.DataFrame({
        "id": [1, 2, 3, 4],
        "item": "a",
        "resp": 1,
        "cov_ethnicity": [1, 12, 16, 2],      # 16 has no label in the fixture
    })
    out = irw.covariates(df, labels=True, table="cucchi_2018_rfq")
    e = out["cov_ethnicity"]
    assert e.tolist() == ["white british", "black caribbean", "16", "white irish"]
    assert list(e.cat.categories) == ["white british", "white irish", "black caribbean", "16"]
    assert "cov_ethnicity (16)" in capsys.readouterr().out


def test_codes_shipped_as_text_decode_too():
    df = pd.DataFrame({"id": [1, 2], "item": "a", "resp": 1,
                       "cov_gender": ["1", "2.0"]})
    out = irw.covariates(df, labels=True, table="cucchi_2018_rfq")
    assert out["cov_gender"].tolist() == ["female", "male"]


def test_same_codes_mean_different_things_in_different_tables():
    df = pd.DataFrame({"id": [1, 2], "item": "a", "resp": 1, "cov_gender": [1, 2]})
    a = irw.covariates(df, labels=True, table="cucchi_2018_rfq")["cov_gender"]
    b = irw.covariates(df, labels=True, table="estevez_2021_actitu")["cov_gender"]
    assert a.tolist() == ["female", "male"]
    assert b.tolist() == ["hombre", "mujer"]


def test_labels_true_needs_a_table():
    with pytest.raises(ValueError, match="pass `table=`"):
        irw.covariates(_estevez(), labels=True)


def test_single_source_table_column_names_the_table():
    df = _estevez().assign(source_table="estevez_2021_actitu")
    out = irw.covariates(df, labels=True)
    assert out["cov_gender"].tolist()[:2] == ["hombre", "mujer"]


def test_rows_from_several_tables_are_refused():
    df = _estevez().assign(source_table=["estevez_2021_actitu"] * 3 + ["cucchi_2018_rfq"] * 3)
    with pytest.raises(ValueError, match="several tables"):
        irw.covariates(df, labels=True)


def test_labels_dataframe_is_used_offline(monkeypatch):
    def boom():
        raise AssertionError("network read with a labels DataFrame")
    monkeypatch.setattr(table_metadata, "get_covariate_labels_table", boom)
    rows = LABELS[LABELS["table"] == "estevez_2021_actitu"]
    out = irw.covariates(_estevez(), labels=rows)
    assert out["cov_gender"].tolist()[:2] == ["hombre", "mujer"]
    with pytest.raises(ValueError, match="several tables"):
        irw.covariates(_estevez(), labels=LABELS)


def test_labels_survive_align():
    out = irw.covariates(_estevez(), labels=True, table="estevez_2021_actitu",
                         align=[3, 2, 99])
    g = out["cov_gender"]
    assert isinstance(g.dtype, pd.CategoricalDtype)
    assert pd.isna(g.iloc[0]) and g.iloc[1] == "mujer" and pd.isna(g.iloc[2])


def test_exports():
    assert "covariate_labels" in irw.__all__
    assert "CovariateLabelsUnavailable" in irw.__all__
