"""MCP tools reach every data source, not just main.

describe_table, fetch_table and get_citation find a table's source from its
name (names are unique across sources); search_tables takes `source`. A
backend without source_of (written before sources) still serves main.
"""
import numpy as np
import pandas as pd
import pytest

pytest.importorskip("mcp")

from irw.mcp import IRWMCPError, IRWTools  # noqa: E402
from test_mcp import FakeBackend, FakeSource  # noqa: E402


class SourcedBackend(FakeBackend):
    """FakeBackend plus one conjoint table, reachable only with source='conj'."""

    def __init__(self):
        super().__init__()
        self.calls = []
        self.conj = pd.DataFrame({
            "name": ["kreps_2020_covid_vaccine"], "n_respondents": [1971],
            "Description": ["COVID-19 vaccine conjoint"], "Derived_License": ["CC0 1.0"]})

    def source_of(self, table_name):
        return "conj" if table_name == "kreps_2020_covid_vaccine" else (
            "main" if table_name in set(self.tables["name"]) else None)

    def list_tables(self, source="main"):
        self.calls.append(("list_tables", source))
        return self.conj.copy() if source == "conj" else super().list_tables()

    def describe_table(self, table_name, source="main"):
        self.calls.append(("describe_table", source))
        if source == "conj":
            return {"source": "conj", "stats": {"n_respondents": np.int64(1971)}}
        return super().describe_table(table_name)

    def fetch_table(self, table_name, *, wide, dedup, max_rows=None, columns=None, source="main"):
        self.calls.append(("fetch_table", source))
        if source == "conj":
            frame = pd.DataFrame({"id": [1, 1], "task": [1, 1], "profile": [1, 2], "choice": [1, 0]})
            return frame.iloc[:max_rows] if max_rows else frame
        return super().fetch_table(table_name, wide=wide, dedup=dedup, max_rows=max_rows, columns=columns)

    def citation(self, table_name, source="main"):
        self.calls.append(("citation", source))
        return ["@article{kreps_2020_covid_vaccine, title={T}}"] if source == "conj" else []

    # The conj filter list is the package's (irw.get_filters("conj")); its
    # filtering is irw.filter(source="conj", ...), stood in for here.
    CONJ_FILTERS = ["license", "n_respondents", "n_attributes", "outcome", "country",
                    "domain", "choice_object"]

    def filter_names(self, source="main"):
        self.calls.append(("filter_names", source))
        return list(self.CONJ_FILTERS) if source == "conj" else super().filter_names()

    def filter_tables(self, **filters):
        source = filters.pop("source", "main")
        self.calls.append(("filter_tables", source, dict(filters)))
        if source != "conj":
            return super().filter_tables(**filters)
        if filters.get("domain") == "immigration_asylum":
            return pd.Series([], name="name", dtype=str)
        return pd.Series(["kreps_2020_covid_vaccine"], name="name", dtype=str)

    def describe_filter(self, filter_name, source="main"):
        self.calls.append(("describe_filter", source, filter_name))
        if source == "conj":
            return {"description": "What the study is about.",
                    "values": pd.Series([3, 1], index=["health_covid", "candidate_choice"], name=filter_name)}
        return super().describe_filter(filter_name)


@pytest.fixture
def backend():
    return SourcedBackend()


@pytest.fixture
def tools(backend):
    return IRWTools(backend, FakeSource())


def test_describe_table_finds_a_conj_table_and_says_so(tools, backend):
    r = tools.describe_table("kreps_2020_covid_vaccine")
    assert r["source"] == "conj"
    assert r["metadata"]["stats"]["n_respondents"] == 1971
    assert ("describe_table", "conj") in backend.calls


def test_get_citation_for_a_conj_table(tools):
    r = tools.get_citation("kreps_2020_covid_vaccine")
    assert r["source"] == "conj" and r["available"] is True


def test_fetch_table_pages_a_conj_table(tools, backend):
    r = tools.fetch_table("kreps_2020_covid_vaccine", limit=1)
    assert r["source"] == "conj"
    assert r["returned"] == 1
    assert ("fetch_table", "conj") in backend.calls


def test_wide_or_dedup_off_main_is_refused_clearly(tools):
    for kw in ({"wide": True}, {"dedup": True}):
        with pytest.raises(IRWMCPError, match="only available for core IRW tables"):
            tools.fetch_table("kreps_2020_covid_vaccine", **kw)


def test_main_tables_still_resolve_to_main(tools, backend):
    r = tools.describe_table("alpha_depression")
    assert r["source"] == "main"
    assert ("describe_table", "main") in backend.calls


def test_search_tables_takes_a_source(tools, backend):
    r = tools.search_tables("vaccine", source="conj")
    assert r["source"] == "conj"
    assert [t["name"] for t in r["tables"]] == ["kreps_2020_covid_vaccine"]
    assert r["n_untagged_in_catalogue"] is None
    assert ("list_tables", "conj") in backend.calls


def test_search_filters_off_main_go_to_the_package_with_the_source(tools, backend):
    r = tools.search_tables("", filters={"domain": "health_covid", "choice_object": "policy"}, source="conj")
    assert [t["name"] for t in r["tables"]] == ["kreps_2020_covid_vaccine"]
    assert r["filters_applied"] == {"domain": "health_covid", "choice_object": "policy"}
    # the package's own filter, called with the source and with no density default (main-only)
    assert ("filter_tables", "conj", {"domain": "health_covid", "choice_object": "policy"}) in backend.calls
    assert not any(c[0] == "filter_tables" and "density" in c[2] for c in backend.calls)
    assert any("domain" in c and "choice_object" in c for c in r["caveats"])
    assert not any("density" in c for c in r["caveats"])
    # no match is an empty result that still names the source
    empty = tools.search_tables("", filters={"domain": "immigration_asylum"}, source="conj")
    assert empty["tables"] == [] and empty["source"] == "conj" and empty["total"] == 0


def test_filter_names_are_per_source(tools):
    # a conj filter on main, or a main filter on conj, is refused by name
    with pytest.raises(IRWMCPError, match="for source='main'"):
        tools.search_tables("", filters={"domain": "health_covid"})
    with pytest.raises(IRWMCPError, match="for source='conj'"):
        tools.search_tables("", filters={"construct_type": "Cognitive"}, source="conj")


def test_describe_filter_takes_a_source(tools, backend):
    r = tools.describe_filter("domain", source="conj")
    assert r["source"] == "conj" and r["filter"] == "domain"
    assert ("describe_filter", "conj", "domain") in backend.calls
    assert r["available_values"] == ["health_covid", "candidate_choice"]
    assert r["value_counts"] == {"health_covid": 3, "candidate_choice": 1}
    with pytest.raises(IRWMCPError, match="for source='main'"):
        tools.describe_filter("domain")
    with pytest.raises(IRWMCPError, match="source must be one of"):
        tools.describe_filter("domain", source="nope")


def test_unknown_source_is_refused(tools):
    with pytest.raises(IRWMCPError, match="source must be one of"):
        tools.search_tables("x", source="nope")


def test_a_backend_without_source_of_serves_main_unchanged():
    tools = IRWTools(FakeBackend(), FakeSource())
    assert tools.describe_table("alpha_depression")["source"] == "main"
