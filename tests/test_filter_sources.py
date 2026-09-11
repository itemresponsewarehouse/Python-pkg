"""filter(), describe_filter() and the metadata lookups take a source (issue #28).

Tag lookups were hardwired to main: `get_tags_table()` read irw_meta's `tags`
whatever the caller wanted, and `filter()` had no `source` at all. Every rule
here comes from Rpkg's irw_filter(), irw_tag_options() and
irw_license_options(), and `.irw_tag_sources` / `.irw_collection_sources` in
Rpkg/R/redivis-config.R:

- main and nom are tagged; sim and comp are not, and asking them for tags errors.
- only main has collections; asking any other source errors.
- comp takes n_responses, n_actors and license and nothing else (R's
  irw_filter_comp()), and n_actors exists nowhere but comp.

A refusal is an error, never an empty result, because an empty result reads as
"nothing matched" instead of "wrong question".

No network: every Redivis read is a synthetic frame.
"""

import pandas as pd
import pytest

import irw
from irw.config import COLLECTION_SOURCES, SOURCE_META_TABLES, TAG_SOURCES
from irw.operations import filter as filter_mod
from irw.operations import filter_info
from irw.utils.redivis import table_metadata
from irw.utils.redivis.cache import metadata_cache


@pytest.fixture(autouse=True)
def clean_cache():
    metadata_cache.clear()
    yield
    metadata_cache.clear()


# --- config ----------------------------------------------------------------

def test_source_rules_match_the_r_package():
    """R: .irw_tag_sources <- c("core", "nom"); .irw_collection_sources <- c("core")."""
    assert TAG_SOURCES == ("main", "nom")
    assert COLLECTION_SOURCES == ("main",)


def test_source_meta_tables_are_bare_names_and_match_r():
    for source, tables in SOURCE_META_TABLES.items():
        for kind, ref in tables.items():
            assert ":" not in ref, f"{source}/{kind} pins a Redivis reference id"
    # The table names Rpkg/R/redivis-metadata.R reads.
    assert SOURCE_META_TABLES["nom"] == {
        "metadata": "nominal_metadata", "tags": "nominal_tags", "biblio": "nominal_biblio",
    }
    assert SOURCE_META_TABLES["sim"] == {"metadata": "simsyn_metadata", "biblio": "simsyn_biblio"}
    assert SOURCE_META_TABLES["comp"] == {"metadata": "comps_metadata", "biblio": "comps_biblio"}


# --- get_filters(source) ---------------------------------------------------

def test_get_filters_default_is_unchanged_by_n_actors():
    """The MCP server lists irw.get_filters(); a comp-only name there would be
    a filter it advertises and every main search refuses."""
    names = filter_info.get_filters()
    assert "n_actors" not in names
    assert "collection" in names and "construct_type" in names


def test_get_filters_per_source():
    assert filter_info.get_filters("comp") == ["n_responses", "license", "n_actors"]
    nom = filter_info.get_filters("nom")
    assert "construct_type" in nom and "collection" not in nom and "n_actors" not in nom
    sim = filter_info.get_filters("sim")
    assert not set(filter_mod.TAG_FILTERS) & set(sim)
    assert "collection" not in sim and "license" in sim
    with pytest.raises(ValueError, match="Unknown source"):
        filter_info.get_filters("core")


# --- filter_tables: refusals, before any data is loaded -------------------

@pytest.fixture
def no_listing(monkeypatch):
    def boom(*args, **kwargs):
        raise AssertionError("list_tables must not be reached")
    monkeypatch.setattr(filter_mod, "list_tables", boom)


@pytest.mark.parametrize("source", ["sim", "comp"])
def test_tag_filters_error_for_untagged_sources(no_listing, source):
    match = "Tag filters" if source == "sim" else "not available for source='comp'"
    with pytest.raises(ValueError, match=match):
        filter_mod.filter_tables([], construct_type="Cognitive", source=source)


@pytest.mark.parametrize("source", ["nom", "sim"])
def test_collection_errors_outside_main(no_listing, source):
    with pytest.raises(ValueError, match="`collection` is only available"):
        filter_mod.filter_tables([], collection="rct", source=source)


@pytest.mark.parametrize("source", ["main", "nom", "sim"])
def test_n_actors_is_comp_only(no_listing, source):
    with pytest.raises(ValueError, match="n_actors"):
        filter_mod.filter_tables([], n_actors=[2, 10], source=source)


def test_comp_refuses_everything_but_its_three_filters(no_listing):
    with pytest.raises(ValueError, match="n_items, density"):
        filter_mod.filter_tables([], n_items=10, density=[0.2, 1], source="comp")


def test_an_explicit_density_equal_to_the_default_is_still_supplied(no_listing):
    """R tests missing(density), not its value."""
    with pytest.raises(ValueError, match="density"):
        filter_mod.filter_tables([], density=[0.5, 1], source="comp")


def test_unknown_source_errors(no_listing):
    with pytest.raises(ValueError, match="Unknown source"):
        filter_mod.filter_tables([], source="core")


# --- filter_tables: what each source matches ------------------------------

@pytest.fixture
def listing(monkeypatch):
    """Patch list_tables with one frame per source, recording the source."""
    frames = {}
    calls = []

    def fake(datasets, source="main"):
        calls.append(source)
        return frames[source].copy()

    monkeypatch.setattr(filter_mod, "list_tables", fake)
    return frames, calls


def test_nom_reads_nom_metadata_and_skips_the_density_default(listing):
    """nominal_metadata has no density column. R's irw_filter(source = "nom")
    with its default density errors on the missing column; applying the
    default only where the column exists keeps every nom call usable."""
    frames, calls = listing
    frames["nom"] = pd.DataFrame({
        "name": ["nom_a", "nom_b", "nom_c"],
        "n_items": [5, 20, 30],
        "construct_type": ["Cognitive/educational", "Personality", None],
        "has_item_text": [False, False, False],
        "license": ["CC BY 4.0", "CC0 1.0", "CC BY 4.0"],
    })
    got = filter_mod.filter_tables([], source="nom", construct_type="Cognitive/educational")
    assert got.tolist() == ["nom_a"]
    assert filter_mod.filter_tables([], source="nom", n_items=[10, None]).tolist() == ["nom_b", "nom_c"]
    assert set(calls) == {"nom"}


def test_nom_refuses_a_filter_its_metadata_cannot_answer(listing):
    frames, _ = listing
    frames["nom"] = pd.DataFrame({"name": ["nom_a"], "n_items": [5]})
    with pytest.raises(ValueError, match="source='nom'.*density"):
        filter_mod.filter_tables([], source="nom", density=[0.2, 1])
    with pytest.raises(ValueError, match="var"):
        filter_mod.filter_tables([], source="nom", var="rt")


def test_comp_filters_on_actors_and_ignores_the_density_default(listing):
    frames, _ = listing
    frames["comp"] = pd.DataFrame({
        "name": ["c1", "c2", "c3"],
        "n_responses": [100, 5000, 900],
        "n_actors": [2, 50, 8],
        "license": ["CC BY 4.0", "CC BY 4.0", "CC0 1.0"],
    })
    assert filter_mod.filter_tables([], source="comp", n_actors=[2, 10]).tolist() == ["c1", "c3"]
    assert filter_mod.filter_tables(
        [], source="comp", n_actors=[2, 10], license="CC0 1.0"
    ).tolist() == ["c3"]


def test_main_still_treats_a_missing_column_as_an_outage(listing):
    """Main's metadata always has density, so its absence is a failed load --
    the _require_column guard -- and not a source limitation to skip."""
    frames, _ = listing
    frames["main"] = pd.DataFrame({"name": ["t1"], "n_items": [5]})
    with pytest.raises(filter_mod.IRWMetadataUnavailable, match="density"):
        filter_mod.filter_tables([])


# --- api -------------------------------------------------------------------

def test_api_filter_threads_source(monkeypatch):
    seen = {}
    monkeypatch.setattr(irw.api, "_get_datasets", lambda source: [source])

    def fake_filter_tables(datasets, source="main", **kwargs):
        seen.update(datasets=datasets, source=source, kwargs=kwargs)
        return pd.Series([], dtype=str)

    monkeypatch.setattr(irw.api, "filter_tables", fake_filter_tables)
    irw.filter(source="nom", construct_type="Cognitive/educational")
    assert seen == {
        "datasets": ["nom"], "source": "nom",
        "kwargs": {"construct_type": "Cognitive/educational"},
    }


def test_api_describe_filter_threads_source(monkeypatch):
    seen = {}
    monkeypatch.setattr(irw.api, "_get_datasets", lambda source: [source])

    def fake_describe(datasets, filter_name, source="main"):
        seen.update(datasets=datasets, name=filter_name, source=source)

    monkeypatch.setattr(irw.api, "_describe_filter", fake_describe)
    irw.describe_filter("license", source="comp")
    assert seen == {"datasets": ["comp"], "name": "license", "source": "comp"}


# --- describe_filter(source) ----------------------------------------------

@pytest.fixture
def per_source_loaders(monkeypatch):
    """Loaders that answer differently per source, so a hardwired main lookup
    shows up as main's values."""
    base = pd.DataFrame({"name": ["t1", "t2"]})
    tags = {
        "main": pd.DataFrame({"table": ["t1", "t2"], "construct_type": ["Personality", "Personality"]}),
        "nom": pd.DataFrame({"table": ["T1", "t2"], "construct_type": ["Cognitive/educational", None]}),
    }
    biblio = {
        "main": pd.DataFrame({"table": ["t1", "t2"], "Derived_License": ["CC0 1.0", "CC0 1.0"]}),
        "comp": pd.DataFrame({"table": ["t1", "t2"], "Derived_License": ["CC BY 4.0", "CC BY 4.0"]}),
    }
    metadata = {
        "comp": pd.DataFrame({"table": ["t1", "t2"], "n_responses": [10, 30], "n_actors": [2, 4]}),
    }
    monkeypatch.setattr(filter_info, "_build_base_table_list", lambda ds: base.copy())
    monkeypatch.setattr(filter_info, "get_tags_table", lambda source="main": tags[source].copy())
    monkeypatch.setattr(filter_info, "get_biblio_table", lambda source="main": biblio[source].copy())
    monkeypatch.setattr(filter_info, "get_metadata_table", lambda source="main": metadata[source].copy())


def test_describe_filter_reads_the_sources_tags(per_source_loaders):
    main = filter_info.describe_filter([], "construct_type")["values"]
    nom = filter_info.describe_filter([], "construct_type", source="nom")["values"]
    assert dict(main) == {"Personality": 2}
    # T1 is matched to t1 despite the case difference.
    assert dict(nom) == {"Cognitive/educational": 1}


def test_describe_license_is_irw_license_options_by_source(per_source_loaders):
    assert dict(filter_info.describe_filter([], "license", source="comp")["values"]) == {"CC BY 4.0": 2}


def test_describe_n_actors_for_comp(per_source_loaders):
    values = filter_info.describe_filter([], "n_actors", source="comp")["values"]
    assert values["min"] == 2.0 and values["max"] == 4.0


@pytest.mark.parametrize(
    "name, source, match",
    [
        ("construct_type", "sim", "Tag filters"),
        ("collection", "nom", "`collection` is only available"),
        ("n_actors", "main", "n_actors"),
        ("n_items", "comp", "not available for source='comp'"),
    ],
)
def test_describe_filter_refuses_what_filter_refuses(per_source_loaders, name, source, match):
    with pytest.raises(ValueError, match=match):
        filter_info.describe_filter([], name, source=source)


# --- the metadata layer ----------------------------------------------------

class _FakeMeta:
    """irw_meta with one frame per table name, counting reads."""

    def __init__(self, frames):
        self.frames = frames
        self.reads = []
        self.properties = {"version": {"tag": "v1.0"}}
        self._id = "irw_meta"

    def get(self):
        return self

    def table(self, name):
        meta = self

        class _T:
            @staticmethod
            def to_pandas_dataframe():
                meta.reads.append(name)
                return meta.frames[name].copy()

        return _T()


class _Table:
    def __init__(self, name):
        self.name = name


class _Dataset:
    def __init__(self, ds_id, names):
        self._id = ds_id
        self.name = ds_id
        self._names = names
        self.properties = {"version": {"tag": "v1.0"}}

    def get(self):
        return self

    def list_tables(self):
        return [_Table(n) for n in self._names]


@pytest.fixture
def fake_meta(monkeypatch):
    meta = _FakeMeta({
        "metadata": pd.DataFrame({"table": ["main_a"], "n_items": [1]}),
        "tags": pd.DataFrame({"table": ["main_a"], "construct_type": ["Personality"]}),
        "biblio": pd.DataFrame({"table": ["main_a"], "Derived_License": ["CC0 1.0"]}),
        "nominal_metadata": pd.DataFrame({"table": ["Nom_A", "nom_b"], "n_items": [3, 4]}),
        "nominal_tags": pd.DataFrame({
            "table": ["nom_a", "nom_b", "nom_gone"],
            "construct_type": ["Cognitive/educational", "NA", "Personality"],
        }),
        "nominal_biblio": pd.DataFrame({"table": ["NOM_A", "nom_b"], "Derived_License": ["CC BY 4.0", "CC0 1.0"]}),
        "simsyn_metadata": pd.DataFrame({"table": ["sim_a"], "n_items": [9]}),
        "simsyn_biblio": pd.DataFrame({"table": ["sim_a"], "Derived_License": ["CC0 1.0"]}),
    })
    metadata_cache.set("meta_dataset", meta)
    monkeypatch.setattr(table_metadata, "_init_main_datasets", lambda: [_Dataset("main", ["main_a"])])
    monkeypatch.setattr(table_metadata, "_init_nom_dataset", lambda: _Dataset("nom", ["Nom_A", "nom_b"]))
    monkeypatch.setattr(table_metadata, "_init_sim_dataset", lambda: _Dataset("sim", ["sim_a"]))
    return meta


def test_get_tags_table_reads_nominal_tags_filtered_to_live_nom_tables(fake_meta):
    tags = table_metadata.get_tags_table("nom")
    assert fake_meta.reads == ["nominal_tags"]
    assert list(tags["table"]) == ["nom_a", "nom_b"]  # nom_gone is not in irw_nominal
    assert pd.isna(tags.loc[tags["table"] == "nom_b", "construct_type"]).all()


@pytest.mark.parametrize("source", ["sim", "comp"])
def test_get_tags_table_errors_for_untagged_sources(fake_meta, source):
    with pytest.raises(ValueError, match="Tags are not available"):
        table_metadata.get_tags_table(source)
    assert fake_meta.reads == []


def test_caches_are_keyed_per_source(fake_meta):
    """A main frame must never be served for nom, or the reverse."""
    main = table_metadata.get_metadata_table()
    nom = table_metadata.get_metadata_table("nom")
    assert list(main["table"]) == ["main_a"]
    assert list(nom["table"]) == ["Nom_A", "nom_b"]
    table_metadata.get_metadata_table()
    table_metadata.get_metadata_table("nom")
    assert fake_meta.reads == ["metadata", "nominal_metadata"]


def test_table_info_for_nom_joins_case_insensitively_and_has_no_collections(fake_meta):
    info = table_metadata._table_info("nom")
    row = info.set_index("table").loc["Nom_A"]
    assert row["construct_type"] == "Cognitive/educational"
    assert row["Derived_License"] == "CC BY 4.0"
    assert "collections" not in info.columns
    assert "collection_members" not in fake_meta.reads


def test_table_info_for_sim_reads_no_tags(fake_meta):
    info = table_metadata._table_info("sim")
    assert list(info["table"]) == ["sim_a"]
    assert "nominal_tags" not in fake_meta.reads and "tags" not in fake_meta.reads


def test_main_table_info_no_longer_drops_tags_on_a_case_difference(fake_meta, monkeypatch):
    """Against live irw_meta an exact join on `table` dropped the tags of 308
    main tables, so filter(construct_type=...) could never return them."""
    fake_meta.frames["metadata"] = pd.DataFrame({"table": ["HEARD_Roch_2022"], "n_items": [6]})
    fake_meta.frames["tags"] = pd.DataFrame({"table": ["heard_roch_2022"], "construct_type": ["Affective/mental health"]})
    fake_meta.frames["biblio"] = pd.DataFrame({"table": ["heard_roch_2022"], "Derived_License": ["CC BY 4.0"]})
    monkeypatch.setattr(table_metadata, "_init_main_datasets", lambda: [_Dataset("main", ["HEARD_Roch_2022"])])
    monkeypatch.setattr(table_metadata, "get_collection_members_table", lambda: pd.DataFrame(columns=["table", "collection"]))
    info = table_metadata._table_info()
    assert info.loc[0, "construct_type"] == "Affective/mental health"
    assert info.loc[0, "Derived_License"] == "CC BY 4.0"
    assert list(info["table"]) == ["HEARD_Roch_2022"]
