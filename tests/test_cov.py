"""The covariance family (source="cov"): computing a core table's item matrix on
fetch, and turning the layout back into a matrix. Twin of Rpkg test-cov.R.
Standard: ben-domingue/irw data/covariance/README.md."""
from unittest.mock import patch

import numpy as np
import pandas as pd
import pytest

import irw
from irw.utils.cov import cov2list, cov2matrix, cov_compute


def long_frame(n=200, seed=1, k=4):
    rng = np.random.default_rng(seed)
    f = rng.normal(size=n)
    x = np.column_stack([0.7 * f + 0.7 * rng.normal(size=n) for _ in range(k)])
    resp = np.digitize(x, [-0.5, 0.5]) + 1
    items = [f"q{j + 1}" for j in range(k)]
    return pd.DataFrame({"id": np.repeat(np.arange(n), k), "item": np.tile(items, n),
                         "resp": resp.ravel().astype(float)})


def wide_of(df):
    return df.pivot(index="id", columns="item", values="resp")


def test_pearson_matches_pandas_and_the_layout_is_the_lower_triangle():
    df = long_frame()
    out = cov_compute(df)
    assert list(out.columns) == ["stat", "var1", "var2", "value", "n"]
    assert len(out) == 10 and (out["stat"] == "cor").all()
    assert list(zip(out["var1"], out["var2"]))[:3] == [("q1", "q1"), ("q2", "q1"), ("q2", "q2")]
    m = cov2matrix(out)
    pd.testing.assert_frame_equal(m["matrix"], wide_of(df).corr(), check_names=False)
    assert m["n"] == 200 and m["computed"] and m["method"] == "pearson"


def test_method_type_and_use_are_passed_through():
    df = long_frame()
    sp = cov2matrix(cov_compute(df, method="spearman", type="cov"))
    assert sp["type"] == "cov"
    ranks = wide_of(df).rank()
    assert np.allclose(sp["matrix"].to_numpy(), ranks.cov().to_numpy())
    df.loc[[0, 5], "resp"] = np.nan
    pw = cov2matrix(cov_compute(df))
    cc = cov2matrix(cov_compute(df, use="complete"))
    assert len(np.unique(pw["n_pairwise"].to_numpy())) > 1
    assert set(np.unique(cc["n_pairwise"].to_numpy())) == {198}
    with pytest.raises(ValueError, match="not defined"):
        cov_compute(df, method="polychoric", type="cov")


def test_a_user_defined_method_gets_the_wide_array_and_is_checked():
    df = long_frame()
    seen = {}

    def shrunk(x):
        seen["shape"] = x.shape
        r = pd.DataFrame(x).corr().to_numpy()
        return 0.5 * r + 0.5 * np.eye(x.shape[1])

    m = cov2matrix(cov_compute(df, method=shrunk))
    assert seen["shape"] == (200, 4) and m["method"] == "user-defined"
    with pytest.raises(ValueError, match="4 x 4"):
        cov_compute(df, method=lambda x: np.eye(3))
    bad = np.eye(4)
    bad[0, 1] = 0.3
    with pytest.raises(ValueError, match="not symmetric"):
        cov_compute(df, method=lambda x: bad)


def test_polychoric_recovers_a_known_correlation_and_tetrachoric_needs_two_categories():
    rng = np.random.default_rng(3)
    n = 4000
    z = rng.multivariate_normal([0, 0], [[1, 0.6], [0.6, 1]], size=n)
    resp = np.digitize(z, [-0.4, 0.3, 1.1])
    df = pd.DataFrame({"id": np.repeat(np.arange(n), 2), "item": np.tile(["a", "b"], n),
                       "resp": resp.ravel().astype(float)})
    r = cov2matrix(cov_compute(df, method="polychoric"))["matrix"].loc["b", "a"]
    assert abs(r - 0.6) < 0.05
    with pytest.raises(ValueError, match="dichotomous"):
        cov_compute(df, method="tetrachoric")
    df["resp"] = (df["resp"] >= 2).astype(float)
    rt = cov2matrix(cov_compute(df, method="tetrachoric"))["matrix"].loc["b", "a"]
    assert abs(rt - 0.6) < 0.07


def test_repeated_measures_too_many_items_flat_items_and_non_core_tables():
    df = long_frame()
    with pytest.raises(ValueError, match="more than once"):
        cov_compute(pd.concat([df, df.iloc[:1]]))
    with pytest.raises(ValueError, match="max_items"):
        cov_compute(df, max_items=3)
    flat = pd.concat([df, pd.DataFrame({"id": range(200), "item": "q5", "resp": 2.0})])
    with pytest.warns(UserWarning, match="no variation"):
        out = cov_compute(flat)
    assert "q5" not in set(out["var1"])
    with pytest.raises(ValueError, match="core table"):
        cov_compute(pd.DataFrame({"id": [1], "task": [1], "profile": [1]}))


def test_cov2matrix_reads_published_tables_groups_and_moments():
    pub = pd.DataFrame({
        "stat": ["cor", "cor", "cor", "sd", "sd", "cor", "cor", "cor"],
        "var1": ["a", "b", "b", "a", "b", "a", "b", "b"],
        "var2": ["a", "a", "b", None, None, "a", "a", "b"],
        "value": [1, 0.4, 1, 2, 3, 1, 0.6, 1],
        "n": [100] * 5 + [80] * 3,
        "group": ["boys"] * 5 + ["girls"] * 3})
    with pytest.raises(ValueError, match="choose one"):
        cov2matrix(pub)
    b = cov2matrix(pub, group="boys")
    assert b["matrix"].loc["a", "b"] == 0.4
    assert b["sd"].to_dict() == {"a": 2.0, "b": 3.0}
    assert not b["computed"]
    assert cov2matrix(pub, group="girls")["n"] == 80
    with pytest.raises(ValueError, match="missing"):
        cov2matrix(pd.DataFrame({"x": [1]}))


def test_fetch_cov_asks_core_for_id_item_resp_and_computes():
    df = long_frame()
    asked = {}

    def fake_fetch(datasets, name, dedup=False, max_rows=None, columns=None):
        asked.update(name=name, columns=columns, dedup=dedup)
        return df

    with patch("irw.api._get_datasets", return_value=[]) as gd, \
            patch("irw.api._fetch", side_effect=fake_fetch), \
            patch("irw.api._cov_published_names", return_value=set()), \
            patch("irw.api._source_note"):
        out = irw.fetch("toy_table", source="cov", cov_args={"method": "spearman"})
    assert gd.call_args[0][0] == "core"
    assert asked["columns"] == ["id", "item", "resp"]
    assert out.attrs["irw_method"] == "spearman" and out.attrs["irw_table"] == "toy_table"
    with pytest.raises(ValueError, match="biased"):
        irw.fetch("toy_table", source="cov", max_rows=10)
    with pytest.raises(ValueError, match="two different things"):
        irw.fetch("toy_table", source="cov", wide=True)


def test_cov_is_a_registered_source_with_one_dataset():
    from irw.config import COV_REF, SOURCES
    assert "cov" in SOURCES
    assert COV_REF == ("datapages", "irw_cov:4e2q")


def test_a_published_matrix_is_returned_as_published():
    pub = pd.DataFrame({"stat": "cor", "var1": ["a", "b", "b"], "var2": ["a", "a", "b"],
                        "value": [1, 0.3, 1], "n": 145})
    seen = {}

    def fake_get(source="core"):
        seen["source"] = source
        return ["cov-dataset"]

    with patch("irw.api._cov_published_names", return_value={"holzinger_1939_grantwhite9"}), \
            patch("irw.api._get_datasets", side_effect=fake_get), \
            patch("irw.api._fetch", return_value=pub), patch("irw.api._source_note"):
        out = irw.fetch("holzinger_1939_grantwhite9", source="cov")
        assert seen["source"] == "cov"
        pd.testing.assert_frame_equal(out, pub)
        with pytest.warns(UserWarning, match="ignored"):
            irw.fetch("holzinger_1939_grantwhite9", source="cov", cov_args={"method": "spearman"})


def test_cov_metadata_and_biblio_read_their_irw_meta_tables():
    from irw.utils.redivis.table_metadata import _meta_table
    assert _meta_table("cov", "metadata") == "cov_metadata"
    assert _meta_table("cov", "biblio") == "cov_biblio"
    with pytest.raises(ValueError, match="not available for source 'cov'"):
        _meta_table("cov", "tags")


def test_cov2list_returns_every_sample_padded_to_the_union_of_variables():
    pub = pd.DataFrame({
        "stat": ["cor", "cor", "cor", "sd"] + ["cor"] * 6,
        "var1": ["a", "b", "b", "a", "a", "c", "c", "b", "b", "c"],
        "var2": ["a", "a", "b", None, "a", "a", "c", "a", "b", "b"],
        "value": [1, 0.4, 1, 2, 1, 0.3, 1, 0.5, 1, 0.2],
        "n": [100] * 4 + [80] * 6,
        "group": ["Conner, Warren (1999a) 1"] * 4 + ["study (2)"] * 6})
    x = cov2list(pub)
    assert list(x["matrices"]) == ["Conner, Warren (1999a) 1", "study (2)"]
    assert x["n"].to_dict() == {"Conner, Warren (1999a) 1": 100, "study (2)": 80}
    assert x["variables"] == ["a", "b", "c"] and x["type"] == "cor"
    m1 = x["matrices"]["Conner, Warren (1999a) 1"]
    assert list(m1.index) == ["a", "b", "c"] and list(m1.columns) == ["a", "b", "c"]
    assert m1.loc["c"].isna().all() and m1["c"].isna().all()
    assert m1.loc["a", "b"] == 0.4 and x["matrices"]["study (2)"].loc["c", "b"] == 0.2
    assert x["sd"]["Conner, Warren (1999a) 1"].to_dict() == {"a": 2.0}
    assert x["sd"]["study (2)"].empty


def test_cov2list_handles_one_sample_cov_tables_and_mixed_types():
    one = pd.DataFrame({"stat": "cov", "var1": ["a", "b", "b"], "var2": ["a", "a", "b"],
                        "value": [4, 1, 9], "n": 50})
    x = cov2list(one)
    assert list(x["matrices"]) == ["all"] and x["type"] == "cov" and x["n"]["all"] == 50
    g2 = one.assign(stat="cor", value=[1, 0.2, 1], group="g2")
    mixed = pd.concat([one.assign(group="g1"), g2])
    with pytest.raises(ValueError, match="mix correlation and covariance"):
        cov2list(mixed)
    with pytest.raises(ValueError, match="missing"):
        cov2list(pd.DataFrame({"x": [1]}))


def test_unreported_cells_warn_and_complete_drops_the_fewest_variables():
    # ben-domingue/irw#3594 (Ben, 2026-11-18): a cell the source did not report
    # is a row with an empty value. Twin of the Rpkg test.
    import warnings as _w
    from irw.utils.cov import CovBlankWarning, cov2list, complete_drop
    v = ["a", "b", "c", "d"]
    rows = [(v[i], v[j], 1.0 if i == j else 0.3) for i in range(4) for j in range(i + 1)]
    pub = pd.DataFrame([{"stat": "cor", "var1": a, "var2": b, "value": x, "n": 120} for a, b, x in rows])
    pub.loc[(pub.var1 == "c") & (pub.var2 == "a"), "value"] = np.nan
    pub.loc[(pub.var1 == "d") & (pub.var2 == "c"), "value"] = np.nan
    with pytest.warns(CovBlankWarning, match=r"2 cells.*complete=True.*\(c\)"):
        m = cov2matrix(pub)
    assert np.isnan(m["matrix"].loc["c", "a"]) and m["dropped"] == []
    m2 = cov2matrix(pub, complete=True)
    assert m2["dropped"] == ["c"] and list(m2["matrix"].index) == ["a", "b", "d"]
    assert not m2["matrix"].isna().any().any()
    one = pub.copy()
    one.loc[(one.var1 == "d") & (one.var2 == "c"), "value"] = 0.3
    assert cov2matrix(one, complete=True)["dropped"] == ["c"]
    pub["group"] = "s1"
    with _w.catch_warnings():
        _w.simplefilter("error")
        x = cov2list(pub)
    assert np.isnan(x["matrices"]["s1"].loc["c", "a"])
    # same cases as irw_validate.cov.complete_drop and the R twin
    nan = np.nan
    order = list("abcde")
    def mk(blanks):
        a = np.full((5, 5), 0.3)
        for p, q in blanks:
            i, j = order.index(p), order.index(q)
            a[i, j] = a[j, i] = nan
        return a
    assert complete_drop(order, mk([("a", "b")])) == ["b"]
    assert complete_drop(order, mk([("a", "c"), ("c", "e")])) == ["c"]
    assert complete_drop(order, mk([("a", "b"), ("d", "e")])) == ["b", "e"]
    assert complete_drop(order, mk([])) == []
