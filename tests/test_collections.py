"""Collections (issue #1633).

No network and no credentials: every Redivis fetcher is monkeypatched with a
synthetic frame. This is deliberate -- `to_pandas_dataframe()` cannot read any
sizeable table in some environments (a pre-existing pyarrow OSError that also
breaks get_tags_table/get_metadata_table), so integration tests would be flaky
for reasons unrelated to this feature.
"""
import pandas as pd
import pytest

import irw
from irw.operations import filter as filter_mod
from irw.operations import filter_info
from irw.utils.redivis import table_metadata


REGISTRY = pd.DataFrame({
    "collection": ["rct", "big_five", "continuous_response"],
    "label": ["RCT", "Big Five / FFM", "Continuous response"],
    "kind": ["design", "instrument", "design"],
    "definition": ["Carries a treat column.",
                   "Five-factor inventories. (searched the 2,251 tagged tables of 3,650)",
                   "Curated."],
    "rule": ["var:treat", "cname:big.?five", "curated"],
    "coverage": ["metadata-complete", "tagged-subset-only", "curated-only"],
    "basis": ["rule", "rule", "curated"],
    "n_tables": pd.array([99, 99, 99], dtype="Int64"),   # deliberately wrong
    "maintainer": "bd",
    "added": "2026-08-29",
})

MEMBERS = pd.DataFrame({
    "table": ["tab_rct_a", "tab_rct_b", "tab_bf_a", "tab_rct_a", "tab_cont"],
    "collection": ["rct", "rct", "big_five", "big_five", "continuous_response"],
    "basis": ["rule:var:treat", "rule:var:treat", "rule:cname:big.?five",
              "rule:cname:big.?five", "curated:bd"],
})


@pytest.fixture(autouse=True)
def _mock(monkeypatch):
    monkeypatch.setattr(irw.api, "_get_collections_table", lambda: REGISTRY.copy())
    monkeypatch.setattr(irw.api, "_get_collection_members_table", lambda: MEMBERS.copy())
    monkeypatch.setattr(table_metadata, "get_collection_members_table", lambda: MEMBERS.copy())


def test_collections_recomputes_n_tables_from_live_membership():
    # The registry claims 99 for everything; membership says otherwise. The
    # published column is a build-time count and members are live-filtered, so
    # trusting it would promise tables a user cannot fetch.
    reg = irw.collections()
    assert dict(zip(reg["collection"], reg["n_tables"])) == {
        "rct": 2, "big_five": 2, "continuous_response": 1
    }


def test_collections_survives_a_membership_fetch_failure(monkeypatch):
    # The registry is 22 rows and always fetchable; membership is large and can
    # fail (a pyarrow OSError in some environments breaks every sizeable
    # Redivis read). collections() must still return the registry, falling back
    # to the published counts, rather than failing outright.
    def boom():
        raise OSError("simulated arrow failure")
    monkeypatch.setattr(irw.api, "_get_collection_members_table", boom)
    reg = irw.collections()
    assert len(reg) == 3
    assert list(reg["n_tables"]) == [99, 99, 99]   # the published build-time counts


def test_collections_kind_filter():
    assert list(irw.collections(kind="design")["collection"]) == ["continuous_response", "rct"]
    assert list(irw.collections(kind=["instrument"])["collection"]) == ["big_five"]
    with pytest.raises(ValueError, match="Unknown kind"):
        irw.collections(kind="nope")


def test_collection_returns_sorted_tables():
    assert irw.collection("rct", quiet=True) == ["tab_rct_a", "tab_rct_b"]
    assert irw.collection("continuous_response", quiet=True) == ["tab_cont"]


def test_collection_reports_incomplete_coverage(capsys):
    irw.collection("big_five")
    out = capsys.readouterr().out
    assert "tagged-subset-only" in out
    assert "does not search the whole warehouse" in out


def test_collection_does_not_cry_wolf_when_complete(capsys):
    irw.collection("rct")
    assert "does not search the whole warehouse" not in capsys.readouterr().out


def test_collection_quiet_prints_nothing(capsys):
    irw.collection("rct", quiet=True)
    assert capsys.readouterr().out == ""


def test_collection_rejects_unknown_and_suggests():
    with pytest.raises(ValueError, match="big_five"):
        irw.collection("bigfive")          # near miss -> suggestion
    with pytest.raises(ValueError, match="No collection named"):
        irw.collection("zzzzzz")


def test_collection_members_answers_the_inverse_question():
    got = irw.collection_members(tables="tab_rct_a")
    assert list(got["collection"]) == ["big_five", "rct"]
    assert list(irw.collection_members(tables="TAB_RCT_A")["collection"]) == ["big_five", "rct"]
    assert len(irw.collection_members(collection="rct")) == 2


def test_apply_tag_filter_handles_list_valued_column():
    # The whole reason `collections` is stored as a list column: the existing
    # tag-filter primitive already does OR over list values, so no new
    # machinery is needed.
    df = pd.DataFrame({
        "name": ["a", "b", "c"],
        "collections": [["rct", "big_five"], ["rct"], []],
    })
    assert list(filter_mod._apply_tag_filter(df, "collections", "big_five")["name"]) == ["a"]
    assert list(filter_mod._apply_tag_filter(df, "collections", "rct")["name"]) == ["a", "b"]
    # OR within the argument -> union, not intersection
    assert list(filter_mod._apply_tag_filter(
        df, "collections", ["big_five", "rct"])["name"]) == ["a", "b"]


def test_apply_tag_filter_string_behaviour_unchanged():
    # _apply_tag_filter is shared with every other tag filter, so the
    # list-handling fix must not disturb the comma-separated string path.
    df = pd.DataFrame({
        "name": ["a", "b", "c", "d"],
        "construct_type": ["Personality", "Personality, Behavioral", "Behavioral", None],
    })
    assert list(filter_mod._apply_tag_filter(df, "construct_type", "Personality")["name"]) == ["a", "b"]
    assert list(filter_mod._apply_tag_filter(df, "construct_type", "Behavioral")["name"]) == ["b", "c"]
    # NaN never matches.
    assert "d" not in list(filter_mod._apply_tag_filter(df, "construct_type", "Personality")["name"])
    # An absent column is now an error, not a no-op: skipping the filter used to
    # widen the result to every table and look like a legitimate answer.
    with pytest.raises(filter_mod.IRWMetadataUnavailable):
        filter_mod._apply_tag_filter(df, "no_such_column", "x")
    # None values -> unchanged frame
    assert len(filter_mod._apply_tag_filter(df, "construct_type", None)) == 4


def test_describe_filter_knows_about_collection():
    # If FILTER_DESCRIPTIONS is not updated, describe_filter returns None and
    # the filter is invisible to irw.get_filters(). Guard that.
    # describe_filter() returns None for anything absent from this dict, which
    # would make the filter invisible to irw.get_filters(). No network here:
    # exercise the registry, not the value-summarising path.
    assert "collection" in filter_info.FILTER_DESCRIPTIONS
    assert filter_info.FILTER_DESCRIPTIONS["collection"]
    assert "collection" in filter_info.get_filters()


def test_collections_column_is_classified_as_a_tag():
    from irw.operations.list_tables import TAGS_SET
    assert "collections" in TAGS_SET
