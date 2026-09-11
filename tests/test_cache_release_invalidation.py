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

import time

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


def test_an_unknown_tag_never_matches_a_cached_entry(monkeypatch):
    """`get(key)` with no version means no check, so None must not pass through."""
    monkeypatch.setenv("IRW_VERSION_TTL_SECONDS", "0")
    assert _cache_version(None) != _cache_version(None)
    assert _cache_version("v20.0") == "v20.0"


def test_an_unknown_tag_is_bounded_by_the_ttl_not_repeated_per_call(monkeypatch):
    """The refill this guards is a Redivis table download, charged against the
    account-wide 30-day export cap. A version that cannot be resolved must not
    turn into one re-download per lookup for the life of the process."""
    monkeypatch.setenv("IRW_VERSION_TTL_SECONDS", "3600")
    assert _cache_version(None) == _cache_version(None)
    # ...and still cannot be mistaken for a real version tag.
    assert _cache_version(None) != _cache_version("v20.0")


def test_an_unknown_tag_stops_matching_in_the_next_window(monkeypatch):
    monkeypatch.setenv("IRW_VERSION_TTL_SECONDS", "100")
    clock = {"t": 0.0}
    monkeypatch.setattr(ds_mod.time, "monotonic", lambda: clock["t"])
    first = _cache_version(None)
    clock["t"] = 99.0
    assert _cache_version(None) == first
    clock["t"] = 101.0
    assert _cache_version(None) != first


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


def test_an_unversioned_dataset_is_listed_once_per_window(monkeypatch):
    """No tag means no way to tell a stale list from a current one, so the list
    cannot be trusted across a release -- but it must still be bounded. Listing
    the six warehouses is ~43 paginated requests; per call, that is sustained
    load on a shared account for as long as the version stays unresolvable."""
    monkeypatch.setenv("IRW_VERSION_TTL_SECONDS", "3600")
    ds = _FakeDataset("fresh_shard", None, ["a__items"])
    for _ in range(5):
        _dataset_table_list(ds)
    assert ds.list_calls == 1

    # The next window re-lists, so an unversioned dataset is refreshed rather
    # than pinned for the life of the process.
    clock = {"t": time.monotonic() + 7200}
    monkeypatch.setattr(ds_mod.time, "monotonic", lambda: clock["t"])
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


# --- a handle's address is frozen at its first .get() (#75) ---------------
#
# The fakes above refresh to the current release on every `.get()`, which is
# not what `redivis.Dataset` does. Its `.get()` replaces `uri` and
# `qualified_reference` with the ones Redivis returns, and those name the
# version current at the time, even for a handle opened with no version:
# `/datasets/datapages.item_response_warehouse_3` becomes
# `/datasets/datapages.item_response_warehouse_3:5xaj:v6_0`. Every later
# `.get()` re-reads v6.0. The fakes below do the same, so a refresh that just
# re-`get()`s the cached handle never sees a release -- which is what #53's
# fix did, and why its tests above could not catch it.


class _Server:
    """What Redivis holds: each dataset's released versions and the current one."""

    def __init__(self):
        self.releases = {}  # ref -> {tag: [table names]}
        self.current = {}   # ref -> tag
        self.gets = 0

    def release(self, ref, tag, table_names):
        self.releases.setdefault(ref, {})[tag] = list(table_names)
        self.current[ref] = tag


class _FreezingDataset:
    def __init__(self, server, ref, version=None):
        self._server = server
        self._ref = ref
        self.name = ref
        self.properties = None
        self.qualified_reference = f"datapages.{ref}" + (f":{version}" if version else "")
        self.scoped_reference = self.qualified_reference.split(".", 1)[1]
        self.uri = f"/datasets/{self.qualified_reference}"

    def _addressed_tag(self):
        # The ref already holds a reference id (`irw_text:abcd`), so the
        # version is whatever follows the ref, not whatever follows a colon.
        suffix = self.qualified_reference[len(f"datapages.{self._ref}"):]
        return suffix[1:] or None

    def get(self):
        self._server.gets += 1
        tag = self._addressed_tag() or self._server.current[self._ref]
        # The freeze: the response's address carries the version it resolved.
        self.qualified_reference = f"datapages.{self._ref}:{tag}"
        self.scoped_reference = f"{self._ref}:{tag}"
        self.uri = f"/datasets/{self.qualified_reference}"
        self.properties = {"version": {"tag": tag}}
        return self

    def list_tables(self):
        tag = self._addressed_tag()
        return [_FakeTable(n) for n in self._server.releases[self._ref][tag]]


class _FreezingRedivis:
    def __init__(self, server):
        self.server = server

    def user(self, _user):
        server = self.server

        class _U:
            @staticmethod
            def dataset(ref, version=None):
                return _FreezingDataset(server, ref, version)

        return _U()


@pytest.fixture
def server(monkeypatch):
    from irw.utils.redivis import pins

    srv = _Server()
    monkeypatch.setattr(ds_mod, "redivis", _FreezingRedivis(srv))
    monkeypatch.setattr(pins, "_PINS", {})
    return srv


def test_the_fake_reproduces_the_frozen_address(server):
    """Guard on the fake itself: without the freeze, the tests below prove nothing."""
    server.release("irw_text:abcd", "v19.0", [])
    ds = ds_mod.redivis.user("datapages").dataset("irw_text:abcd").get()
    server.release("irw_text:abcd", "v20.0", [])
    assert ds.get().properties["version"]["tag"] == "v19.0"


def test_a_cached_unpinned_handle_sees_a_release(server):
    server.release("irw_text:abcd", "v19.0", ["a__items"])
    ds = ds_mod._init_dataset("datapages", "irw_text:abcd")
    assert _dataset_version_tag(ds) == "v19.0"

    server.release("irw_text:abcd", "v20.0", ["a__items"])
    assert _dataset_version_tag(ds) == "v20.0"


def test_the_cached_handle_is_moved_to_the_release(server):
    """A current tag is not enough: tables reached through the handle are
    addressed at its `qualified_reference`, so that has to move too."""
    server.release("irw_text:abcd", "v19.0", ["a__items"])
    ds = ds_mod._init_dataset("datapages", "irw_text:abcd")
    server.release("irw_text:abcd", "v20.0", ["a__items"])

    _dataset_version_tag(ds)
    assert ds.qualified_reference == "datapages.irw_text:abcd:v20.0"
    assert ds.uri.endswith(":v20.0")
    assert ds.properties["version"]["tag"] == "v20.0"


def test_a_withdrawal_reaches_a_long_running_process_through_a_frozen_handle(server):
    """The #51 symptom, with the handle behaving as redivis.Dataset really does."""
    server.release("irw_text:abcd", "v19.0", ["withdrawn__items", "kept__items"])
    ds = ds_mod._init_dataset("datapages", "irw_text:abcd")
    assert [t.name for t in _dataset_table_list(ds)] == ["withdrawn__items", "kept__items"]

    server.release("irw_text:abcd", "v20.0", ["kept__items"])
    assert [t.name for t in _dataset_table_list(ds)] == ["kept__items"]


def test_a_pinned_handle_stays_on_its_version(server):
    """The frozen address is exactly what a pin wants; a release must not move it."""
    from irw.utils.redivis import pins

    server.release("irw_text:abcd", "v19.0", ["a__items"])
    pins._PINS[pins._dataset_key("irw_text:abcd")] = "v19.0"
    ds = ds_mod._init_dataset("datapages", "irw_text:abcd")

    server.release("irw_text:abcd", "v20.0", [])
    assert _dataset_version_tag(ds) == "v19.0"
    assert ds.qualified_reference.endswith(":v19.0")
    assert [t.name for t in _dataset_table_list(ds)] == ["a__items"]


def test_an_unchanged_release_does_not_touch_the_handle(server, monkeypatch):
    server.release("irw_text:abcd", "v19.0", [])
    ds = ds_mod._init_dataset("datapages", "irw_text:abcd")
    before = ds.qualified_reference
    for _ in range(3):
        assert _dataset_version_tag(ds) == "v19.0"
    assert ds.qualified_reference == before


def test_the_probe_is_still_bounded_by_the_ttl(server, monkeypatch):
    monkeypatch.setenv("IRW_VERSION_TTL_SECONDS", "3600")
    server.release("irw_text:abcd", "v19.0", [])
    ds = ds_mod._init_dataset("datapages", "irw_text:abcd")
    before = server.gets
    for _ in range(5):
        _dataset_version_tag(ds)
    assert server.gets == before + 1


def test_a_failed_probe_keeps_the_last_known_tag_and_address(server, monkeypatch):
    server.release("irw_text:abcd", "v19.0", [])
    ds = ds_mod._init_dataset("datapages", "irw_text:abcd")
    assert _dataset_version_tag(ds) == "v19.0"

    class _Down:
        @staticmethod
        def user(_user):
            raise ConnectionError("redivis unreachable")

    monkeypatch.setattr(ds_mod, "redivis", _Down())
    assert _dataset_version_tag(ds) == "v19.0"
    assert ds.qualified_reference.endswith(":v19.0")
