"""A release must invalidate the caches built on top of it.

`MetadataCache` is an in-process dict with no TTL and an *optional* version
check, and the table-index caches did not use the optional half: they called
`metadata_cache.get(key)` with no version, so once populated they were never
invalidated for the life of the process. After a shard release a long-running
consumer kept resolving and serving tables the release had withdrawn.

That is not a general freshness preference. Item-text withdrawals are how IRW
stops distributing instrument wording it has been ruled it may not distribute
-- on 2026-09-09 the `irw_text` v19.0 -> v20.0 release withdrew 15 tables on
rights rulings -- and the MCP server, a long-running consumer, went on serving
the withdrawn wording while a fresh process resolved the same names to None.

There was a second half to the bug that the fix depends on: `redivis.Dataset`
assigns `properties` in `.get()` and never refetches, and the dataset handles
are themselves cached forever, so the version checks that *were* written
correctly for the metadata frames could not fire either. Both halves are
covered here.

No network: every dataset, table and version here is synthetic.
"""

import pandas as pd
import pytest

from irw.utils.redivis import datasets as ds_mod
from irw.utils.redivis import item_text, table_metadata
from irw.utils.redivis.cache import metadata_cache
from irw.utils.redivis.datasets import (
    _cache_version,
    _dataset_table_list,
    _dataset_version_tag,
    _datasets_version_tag,
    _version_ttl_seconds,
)


@pytest.fixture(autouse=True)
def clean_cache(monkeypatch):
    """Each test starts from an empty cache and refreshes on every lookup."""
    metadata_cache.clear()
    monkeypatch.setenv("IRW_VERSION_TTL_SECONDS", "0")
    yield
    metadata_cache.clear()


class _FakeTable:
    def __init__(self, name):
        self.name = name
        self.properties = {"numRows": 1, "variableCount": 3}


class _FakeDataset:
    """A Redivis dataset handle with the behaviour that caused the bug.

    `properties` is only updated by `.get()`, exactly as `redivis.Dataset`
    does it, so a test that never calls `.get()` sees the stale version -- and
    a fix that reads `properties` off a cached handle sees it forever.
    """

    def __init__(self, ds_id, version, table_names):
        self._id = ds_id
        self.name = ds_id
        self._released_version = version
        self._released_tables = list(table_names)
        self.properties = None
        self.get_calls = 0
        self.list_calls = 0
        self.get()

    def release(self, version, table_names):
        """Publish a new version. Nothing on the handle changes until `.get()`."""
        self._released_version = version
        self._released_tables = list(table_names)

    def get(self):
        self.get_calls += 1
        self.properties = {"version": {"tag": self._released_version}}
        return self

    def list_tables(self):
        self.list_calls += 1
        return [_FakeTable(n) for n in self._released_tables]


# --- the version tag itself ----------------------------------------------

def test_the_tag_comes_from_a_refreshed_handle_not_frozen_properties():
    """The whole reason the old version checks could not fire."""
    ds = _FakeDataset("irw_text", "v19.0", [])
    assert _dataset_version_tag(ds) == "v19.0"

    ds.release("v20.0", [])
    # Reading ds.properties directly is what the old code did, and it is stale.
    assert ds.properties["version"]["tag"] == "v19.0"
    assert _dataset_version_tag(ds) == "v20.0"


def test_the_tag_is_cached_for_the_ttl(monkeypatch):
    monkeypatch.setenv("IRW_VERSION_TTL_SECONDS", "3600")
    ds = _FakeDataset("irw_text", "v19.0", [])
    before = ds.get_calls
    for _ in range(5):
        assert _dataset_version_tag(ds) == "v19.0"
    # One cold refresh, then four hits: the fix costs one small metadata
    # request per dataset per window, not one per lookup.
    assert ds.get_calls == before + 1


def test_a_failed_refresh_keeps_the_last_known_tag():
    """A metadata blip must not drop every cache that depends on the tag."""
    ds = _FakeDataset("irw_text", "v19.0", [])
    assert _dataset_version_tag(ds) == "v19.0"

    def boom():
        raise ConnectionError("redivis unreachable")

    ds.get = boom
    assert _dataset_version_tag(ds) == "v19.0"


def test_an_unknown_tag_never_matches_a_cached_entry():
    """`get(key)` with no version means no check, so None must not pass through."""
    assert _cache_version(None) != _cache_version(None)
    assert _cache_version("v20.0") == "v20.0"


def test_a_group_version_is_unknown_if_any_member_is():
    good = _FakeDataset("shard_1", "v3.0", [])
    bad = _FakeDataset("shard_2", None, [])
    assert _datasets_version_tag([good]) is not None
    assert _datasets_version_tag([good, bad]) is None


@pytest.mark.parametrize("raw,expected", [
    ("0", 0.0),
    ("12.5", 12.5),
    ("not-a-number", 300.0),
    ("-1", 300.0),
])
def test_the_ttl_is_configurable_and_falls_back_safely(monkeypatch, raw, expected):
    monkeypatch.setenv("IRW_VERSION_TTL_SECONDS", raw)
    assert _version_ttl_seconds() == expected


# --- the table list -------------------------------------------------------

def test_a_release_invalidates_the_table_list():
    ds = _FakeDataset("irw_text", "v19.0", ["a__items", "b__items"])
    assert [t.name for t in _dataset_table_list(ds)] == ["a__items", "b__items"]
    assert [t.name for t in _dataset_table_list(ds)] == ["a__items", "b__items"]
    assert ds.list_calls == 1  # served from cache within a version

    ds.release("v20.0", ["a__items"])
    assert [t.name for t in _dataset_table_list(ds)] == ["a__items"]
    assert ds.list_calls == 2


def test_an_unversioned_dataset_is_not_cached():
    """No tag means no way to tell a stale list from a current one."""
    ds = _FakeDataset("fresh_shard", None, ["a__items"])
    _dataset_table_list(ds)
    _dataset_table_list(ds)
    assert ds.list_calls == 2


# --- the reported symptom -------------------------------------------------

def test_a_withdrawal_reaches_a_long_running_process(monkeypatch):
    """The 2026-09-09 case: v19.0 -> v20.0 withdrew 15 item-text tables.

    A process that had already built the index went on resolving them.
    """
    shard_1 = _FakeDataset("irw_text", "v19.0", [
        "cognitive_load_klimova_2023_mlq__items",
        "kept_table__items",
    ])
    shard_2 = _FakeDataset("irw_text_2", "v2.0", ["other_table__items"])
    shards = [shard_2, shard_1]  # newest first, as _get_itemtext_datasets returns
    metadata_cache.set(item_text._itemtext_datasets_cache_key(), shards)

    assert "cognitive_load_klimova_2023_mlq" in item_text._list_itemtext_tables()

    shard_1.release("v20.0", ["kept_table__items"])

    available = item_text._list_itemtext_tables()
    assert "cognitive_load_klimova_2023_mlq" not in available
    assert "kept_table" in available
    assert "other_table" in available


def test_the_index_is_still_cached_within_a_version():
    """The fix must not turn every itemtext() call into a listing pass."""
    shard = _FakeDataset("irw_text", "v20.0", ["a__items"])
    metadata_cache.set(item_text._itemtext_datasets_cache_key(), [shard])

    for _ in range(5):
        item_text._list_itemtext_tables()
    assert shard.list_calls == 1


# --- the metadata frames --------------------------------------------------

class _FakeMetaDataset(_FakeDataset):
    """Metadata dataset whose frames change with the release."""

    def __init__(self, version, rows):
        self._rows = rows
        super().__init__("irw_meta", version, [])

    def release_rows(self, version, rows):
        self._rows = rows
        self.release(version, [])

    def table(self, name):
        rows = self._rows

        class _T:
            @staticmethod
            def to_pandas_dataframe():
                return pd.DataFrame({"table": list(rows)})

        return _T()


def test_a_release_invalidates_the_metadata_frame(monkeypatch):
    """These version checks were written correctly and still could not fire:
    the tag was read off a handle that never refetched."""
    meta = _FakeMetaDataset("v20.0", ["alpha"])
    metadata_cache.set("meta_dataset", meta)

    assert list(table_metadata.get_metadata_table()["table"]) == ["alpha"]

    meta.release_rows("v21.0", ["alpha", "beta"])
    assert list(table_metadata.get_metadata_table()["table"]) == ["alpha", "beta"]


def test_existing_tables_notices_a_release(monkeypatch):
    warehouse = _FakeDataset("irw", "v20.0", ["Table_A", "Table_B"])
    monkeypatch.setattr(ds_mod, "_init_main_datasets", lambda: [warehouse])
    monkeypatch.setattr(table_metadata, "_init_main_datasets", lambda: [warehouse])

    assert table_metadata._get_existing_tables() == {"table_a", "table_b"}

    warehouse.release("v21.0", ["Table_A"])
    assert table_metadata._get_existing_tables() == {"table_a"}
