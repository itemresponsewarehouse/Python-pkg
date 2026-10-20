"""Shard discovery (utils/redivis/shard_discovery.py).

The fixture is the live datapages catalog as captured on 2026-10-20, trimmed to
the fields discovery reads, plus two rows for cases the live catalog does not
currently contain: `item_response_warehouse_9` with no published version, and a
published but non-public `irw_rank_3`. It is the same fixture the R package's
tests use, so the two clients are held to one catalog.
"""

import json
import logging
from pathlib import Path

import pytest

from irw import config
from irw.utils.redivis import shard_discovery as sd

FIXTURE = Path(__file__).parent / "data" / "datapages_catalog.json"


@pytest.fixture
def catalog():
    return sd.parse_catalog(FIXTURE.read_text(encoding="utf-8"))


@pytest.fixture
def discovery_on(monkeypatch):
    monkeypatch.delenv("IRW_SHARD_DISCOVERY", raising=False)
    sd.reset_state()
    yield
    sd.reset_state()


def names(refs):
    return [ref.split(":")[0] for _user, ref in refs]


def test_catalog_parses_reference_name_not_display_name(catalog):
    assert len(catalog) > 20
    row = [e for e in catalog if e.reference == "item_response_warehouse_7"]
    assert len(row) == 1
    # `name` is "Item Response Warehouse 7", which does not resolve.
    assert row[0].reference_id == "ej0d"
    assert row[0].version == "v1_3"
    assert row[0].public is True
    assert row[0].table_count == 761


def test_dataset_without_published_version_is_not_parsed(catalog):
    # Two-field scopedReference. Adopting it would make every open warn.
    assert "item_response_warehouse_9" not in [e.reference for e in catalog]


@pytest.mark.parametrize("text", ["not json at all", '{"results":[]}', "", "[]", '{"results":{}}'])
def test_malformed_response_yields_no_entries(text):
    assert sd.parse_catalog(text) == []


def test_appends_a_core_shard_the_config_has_never_heard_of(catalog, discovery_on, monkeypatch):
    monkeypatch.setattr(sd, "fetch_catalog", lambda: catalog)
    refs = sd.effective_refs(config.MAIN_REFS[:2], "warehouse")
    assert names(refs) == [
        "item_response_warehouse",
        "item_response_warehouse_2",
        "item_response_warehouse_3",
        "item_response_warehouse_4",
        "item_response_warehouse_5",
        "item_response_warehouse_6",
        "item_response_warehouse_7",
    ]
    # Reference ids come from the catalog, so the appended refs resolve.
    assert refs[-1] == ("datapages", "item_response_warehouse_7:ej0d")


def test_no_op_when_the_config_is_already_current(catalog, discovery_on, monkeypatch):
    monkeypatch.setattr(sd, "fetch_catalog", lambda: catalog)
    for refs in (config.MAIN_REFS, config.CONJ_REFS, config.RANK_REFS, config.ITEMTEXT_REFS):
        assert sd.effective_refs(refs) == tuple(refs)


def test_appended_shards_are_ordered_oldest_first_not_lexically(discovery_on):
    # _10 sorts before _2 as text, and lookups search newest-first to resolve a
    # duplicate name to its most recent copy, so the wrong order resolves to
    # the wrong shard.
    catalog = [
        sd.CatalogEntry("item_response_warehouse_10", "aa10", "v1_0", True, 1),
        sd.CatalogEntry("item_response_warehouse_2", "bb02", "v1_0", True, 1),
        sd.CatalogEntry("item_response_warehouse_9", "cc09", "v1_0", True, 1),
    ]
    extra = sd.extra_refs((("datapages", "item_response_warehouse:as2e"),), catalog)
    assert names(extra) == [
        "item_response_warehouse_2",
        "item_response_warehouse_9",
        "item_response_warehouse_10",
    ]


def test_extends_a_family_but_never_invents_one(catalog, discovery_on):
    # The organization also holds wordbank, peekbank, palmerpenguins and so on,
    # and a genuinely new IRW family needs a `source` value and filters, so it
    # has to keep failing loudly instead of being adopted here.
    for refs in (config.MAIN_REFS, config.CONJ_REFS, config.RANK_REFS, config.ITEMTEXT_REFS):
        assert sd.extra_refs(refs, catalog) == ()
    for single in (config.SIM_REF, config.COMP_REF, config.NOM_REF):
        assert sd.extra_refs((single,), catalog) == ()


def test_non_public_shard_is_not_adopted(catalog, discovery_on):
    # irw_rank_3 is published in the fixture but publicAccessLevel is
    # "metadata", so opening it anonymously would fail.
    assert sd.extra_refs((("datapages", "irw_rank:93f5"),), catalog) == ()


def test_base_name_is_matched_exactly(discovery_on):
    catalog = [
        sd.CatalogEntry("irw_text_2", "aa02", "v1_0", True, 1),
        sd.CatalogEntry("irw_text_extra", "bb00", "v1_0", True, 1),
        sd.CatalogEntry("irw_textual", "cc00", "v1_0", True, 1),
        sd.CatalogEntry("irw_text_3", "dd03", "v1_0", True, 1),
    ]
    extra = sd.extra_refs((("datapages", "irw_text:07b6"),), catalog)
    assert names(extra) == ["irw_text_2", "irw_text_3"]


def test_unreadable_catalog_leaves_the_configured_list_untouched(discovery_on, monkeypatch):
    monkeypatch.setattr(sd, "fetch_catalog", lambda: None)
    assert sd.effective_refs(config.MAIN_REFS) == tuple(config.MAIN_REFS)


@pytest.mark.parametrize("value", ["0", "false", "no", "off", "OFF"])
def test_discovery_can_be_switched_off(value, catalog, monkeypatch):
    monkeypatch.setenv("IRW_SHARD_DISCOVERY", value)
    monkeypatch.setattr(sd, "fetch_catalog", lambda: catalog)
    assert sd.discovery_enabled() is False
    assert sd.effective_refs(config.MAIN_REFS[:2]) == tuple(config.MAIN_REFS[:2])


def test_discovery_is_on_by_default(monkeypatch):
    # What makes a released package self-correcting.
    monkeypatch.delenv("IRW_SHARD_DISCOVERY", raising=False)
    assert sd.discovery_enabled() is True


def test_a_discovered_shard_is_said_once(catalog, discovery_on, monkeypatch, caplog):
    monkeypatch.setattr(sd, "fetch_catalog", lambda: catalog)
    with caplog.at_level(logging.WARNING, logger="irw.utils.redivis.shard_discovery"):
        sd.effective_refs(config.MAIN_REFS[:6], "warehouse")
        sd.effective_refs(config.MAIN_REFS[:6], "warehouse")
    said = [r for r in caplog.records if "newer than this version" in r.getMessage()]
    assert len(said) == 1
    assert "item_response_warehouse_7" in said[0].getMessage()


def test_a_single_dataset_source_that_grew_a_shard_is_loud(discovery_on, monkeypatch, caplog):
    # sim/comp/nom hold one dataset here and their initializers return one
    # handle, so an extra shard cannot simply be appended. Make the gap loud
    # rather than silent -- "which families did we wire up" is how the seventh
    # core warehouse came to be missed.
    catalog = [sd.CatalogEntry("irw_simsyn_2", "zz02", "v1_0", True, 3)]
    monkeypatch.setattr(sd, "fetch_catalog", lambda: catalog)
    with caplog.at_level(logging.WARNING, logger="irw.utils.redivis.shard_discovery"):
        sd.warn_if_extended(config.SIM_REF, "simsyn dataset")
    assert any("cannot read" in r.getMessage() for r in caplog.records)


def test_a_discovered_shard_is_pinnable(catalog, discovery_on, monkeypatch):
    monkeypatch.setattr(sd, "fetch_catalog", lambda: catalog)
    monkeypatch.setattr(config, "MAIN_REFS", config.MAIN_REFS[:2])
    from irw.utils.redivis import pins

    # pins.py imported MAIN_REFS by name, so patch it there too.
    monkeypatch.setattr(pins, "MAIN_REFS", config.MAIN_REFS)
    keys = pins._pinnable_refs()
    # Without this, use_version() would honour the pin for every configured
    # shard and read a discovered one at its current release.
    assert "item_response_warehouse_7" in keys
    assert "irw_meta" in keys


def test_catalog_is_fetched_once_per_process_even_when_it_fails(discovery_on, monkeypatch):
    calls = {"n": 0}

    def boom(*args, **kwargs):
        calls["n"] += 1
        raise OSError("no network")

    monkeypatch.setattr(sd.urllib.request, "urlopen", boom)
    assert sd.fetch_catalog() is None
    assert sd.fetch_catalog() is None
    assert calls["n"] == 1


def test_fixture_matches_the_fields_discovery_reads():
    # If the endpoint's shape changes, the fixture must change with it, so
    # pin the field names the parser depends on.
    payload = json.loads(FIXTURE.read_text(encoding="utf-8"))
    row = payload["results"][0]
    assert {"scopedReference", "referenceId", "publicAccessLevel", "tableCount"} <= set(row)
