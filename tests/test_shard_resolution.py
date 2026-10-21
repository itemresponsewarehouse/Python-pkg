"""Tests for metadata-driven shard resolution (no Redivis network calls).

What matters here is not only that the right table comes back -- the shard
search already guaranteed that -- but *how many shards were asked*. So every
assertion that claims a saving is made on the recorded lookups, not on the
returned frame: a test that only checked the data would pass against a
resolution that did nothing at all.

The other half is the safety property: resolution may reorder the shard list
and must never shorten it. A dropped shard turns a hit into a silent
not-found, which is strictly worse than the lookups this saves.
"""

import pandas as pd
import pytest

from irw.operations.fetch import fetch
from irw.utils.redivis import shard_resolution
from irw.utils.redivis.cache import metadata_cache
from irw.utils.redivis.shard_resolution import (
    _map_from_frame,
    _shard_ordinal,
    shard_first,
    shard_map,
)
from irw.utils.redivis.tables import _search_datasets

# With their reference ids, as config.MAIN_REFS carries them: the metadata's
# `dataset` column has the bare names, so a resolver that compared the two
# spellings directly would match nothing and silently do nothing.
CORE = [
    "item_response_warehouse:as2e",
    "item_response_warehouse_2:epbx",
    "item_response_warehouse_3:5xaj",
]


class _NotFound(Exception):
    """Reads like a Redivis not-found to `_classify_error`."""

    def __init__(self):
        super().__init__({"status": 404, "error": "not_found"})


class _Table:
    def __init__(self, frame):
        self._frame = frame

    def get(self):
        return None

    def to_pandas_dataframe(self, max_results=None, *, variables=None, **kwargs):
        frame = self._frame
        if variables is not None:
            frame = frame.loc[:, list(variables)]
        return frame.copy()


class _Shard:
    """A fake core shard that records every table lookup made against it."""

    def __init__(self, ref, holds=(), frame=None):
        self._id = ref
        # The display name, which is exactly what must not be used as the
        # shard's identity (Rpkg hit this: `$name` after get() is
        # "Item Response Warehouse 3", which matches no metadata value).
        self.name = ref.split(":")[0].replace(
            "item_response_warehouse", "Item Response Warehouse"
        )
        self.holds = set(holds)
        self.lookups = []
        self._frame = frame if frame is not None else _frame()

    def table(self, name):
        self.lookups.append(name)
        if name not in self.holds:
            raise _NotFound()
        return _Table(self._frame)


def _frame(n=4):
    return pd.DataFrame(
        {
            "id": [f"p{i}" for i in range(n)],
            "item": ["q1"] * n,
            "resp": list(range(n)),
        }
    )


def _bare(ref):
    return ref.split(":")[0]


def _shards(holder=None, which=CORE):
    return [
        _Shard(ref, holds=["t"] if _bare(ref) == holder else []) for ref in which
    ]


def _search_order(ds_list):
    """Newest-first, as `_order_main_datasets` hands the list to fetch()."""
    return list(reversed(ds_list))


@pytest.fixture(autouse=True)
def _clean_state(monkeypatch):
    """Resolution on, with no map and no metadata frame carried between tests."""
    monkeypatch.delenv("IRW_SHARD_RESOLUTION", raising=False)
    shard_resolution.reset_state()
    metadata_cache.clear()
    yield
    shard_resolution.reset_state()
    metadata_cache.clear()


class _MetaDataset:
    """A fake irw_meta that records how often its metadata table was read."""

    def __init__(self, frame):
        self._frame = frame
        self.reads = []

    def table(self, name):
        self.reads.append(name)
        return _Table(self._frame)


def _map_frame(mapping):
    return pd.DataFrame(
        {"table": list(mapping), "dataset": [mapping[k] for k in mapping]}
    )


@pytest.fixture
def meta(monkeypatch):
    """Install a fake irw_meta, so the map can be built without a network.

    `shard_map` reaches the metadata through `table_metadata._get_meta_dataset`
    and `datasets._dataset_version_tag`, both imported inside the call, so the
    patch has to land on those modules rather than on this one.
    """
    from irw.utils.redivis import datasets as datasets_mod
    from irw.utils.redivis import table_metadata as meta_mod

    installed = {}

    def install(mapping):
        ds = _MetaDataset(_map_frame(mapping))
        monkeypatch.setattr(meta_mod, "_get_meta_dataset", lambda: ds)
        monkeypatch.setattr(datasets_mod, "_dataset_version_tag", lambda _ds: "v1.0")
        installed["ds"] = ds
        return ds

    return install


# ---------------------------------------------------------------- the map


def test_map_lowercases_and_drops_unusable_rows():
    frame = pd.DataFrame(
        {
            "table": ["Alpha", "beta", None, "", "delta"],
            "dataset": ["Item_Response_Warehouse", None, "irw_3", "irw_3", float("nan")],
        }
    )
    assert _map_from_frame(frame) == {"alpha": "item_response_warehouse"}


def test_a_name_in_two_shards_resolves_to_the_newer_one():
    frame = pd.DataFrame(
        {
            "table": ["t", "t"],
            "dataset": ["item_response_warehouse_3", "item_response_warehouse"],
        }
    )
    # Row order must not decide it: newest-first search would have found the
    # shard 3 copy, so the map has to say the same thing.
    assert _map_from_frame(frame) == {"t": "item_response_warehouse_3"}


def test_shard_ordinal_reads_the_suffix_and_defaults_to_first():
    assert _shard_ordinal("item_response_warehouse") == 1
    assert _shard_ordinal("item_response_warehouse_7") == 7
    assert _shard_ordinal("something_renamed") == 1


def test_a_frame_without_the_dataset_column_yields_no_map():
    assert _map_from_frame(pd.DataFrame({"table": ["t"]})) is None


def test_map_is_none_when_switched_off(monkeypatch):
    monkeypatch.setenv("IRW_SHARD_RESOLUTION", "0")
    assert shard_map(build=True) is None


# ------------------------------------------------------------- reordering


def test_the_named_shard_is_moved_to_the_front(meta):
    ds_list = _search_order(_shards("item_response_warehouse"))
    meta({"t": "item_response_warehouse"})
    shard_map(build=True)  # as an earlier lookup in the session would
    out = shard_first(ds_list, "t")
    assert [_bare(d._id) for d in out] == [
        "item_response_warehouse",
        "item_response_warehouse_3",
        "item_response_warehouse_2",
    ]


def test_reordering_never_drops_a_shard(meta):
    ds_list = _search_order(_shards("item_response_warehouse_2"))
    meta({"t": "item_response_warehouse_2"})
    shard_map(build=True)
    out = shard_first(ds_list, "t")
    assert sorted(map(id, out)) == sorted(map(id, ds_list))


def test_a_name_absent_from_the_map_leaves_the_order_alone(meta):
    ds_list = _search_order(_shards("item_response_warehouse"))
    meta({"other": "item_response_warehouse"})
    shard_map(build=True)
    assert shard_first(ds_list, "t") is ds_list


def test_a_shard_not_in_this_session_leaves_the_order_alone(meta):
    """A pinned or skipped shard must not vanish from the search."""
    ds_list = _search_order(_shards("item_response_warehouse", which=CORE[:2]))
    meta({"t": "item_response_warehouse_3"})
    shard_map(build=True)
    assert shard_first(ds_list, "t") is ds_list


def test_resolution_is_skipped_for_a_non_core_dataset_list(meta):
    """Only the core shards are mapped by the metadata's `dataset` column."""
    ds_list = [_Shard("irw_conjoint:3b0j"), _Shard("irw_conjoint_2:7wxv")]
    meta({"t": "item_response_warehouse"})
    shard_map(build=True)
    assert shard_first(ds_list, "t") is ds_list


def test_a_version_pinned_handle_still_matches_its_shard(meta):
    """`_id` carries `ref@tag` under a pin; the tag is not part of identity."""
    pinned = _Shard("item_response_warehouse:as2e@v46.0")
    ds_list = [_Shard("item_response_warehouse_2:epbx"), pinned]
    meta({"t": "item_response_warehouse"})
    shard_map(build=True)
    assert shard_first(ds_list, "t")[0] is pinned


def test_the_switch_disables_reordering(meta, monkeypatch):
    monkeypatch.setenv("IRW_SHARD_RESOLUTION", "0")
    ds_list = _search_order(_shards("item_response_warehouse"))
    meta({"t": "item_response_warehouse"})
    assert shard_first(ds_list, "t") is ds_list


# ------------------------------------------------------ the on_miss hook


def test_the_hook_runs_after_the_first_miss_and_only_once():
    ds_list = _search_order(_shards("item_response_warehouse"))
    calls = []

    def hook(tail):
        calls.append([_bare(d._id) for d in tail])
        return list(reversed(tail))

    result, other, terminal = _search_datasets(
        ds_list, lambda ds: ds.table("t"), on_miss=hook
    )
    assert result is not None and other is None and terminal is None
    assert calls == [["item_response_warehouse_2", "item_response_warehouse"]]
    # Newest probed, then the reordered tail: the oldest shard, which has it.
    assert [d.lookups for d in ds_list] == [["t"], [], ["t"]]


def test_the_hook_does_not_run_when_the_first_shard_has_the_table():
    ds_list = _search_order(_shards("item_response_warehouse_3"))
    calls = []
    _search_datasets(ds_list, lambda ds: ds.table("t"), on_miss=calls.append)
    assert calls == []


def test_a_hook_that_drops_a_shard_is_ignored():
    """Safety property: a short list would turn a hit into a not-found."""
    ds_list = _search_order(_shards("item_response_warehouse"))
    result, _other, _terminal = _search_datasets(
        ds_list, lambda ds: ds.table("t"), on_miss=lambda tail: tail[:1]
    )
    assert result is not None
    assert [d.lookups for d in ds_list] == [["t"], ["t"], ["t"]]


def test_a_hook_that_raises_is_ignored():
    ds_list = _search_order(_shards("item_response_warehouse"))

    def hook(tail):
        raise RuntimeError("metadata unreadable")

    result, _other, _terminal = _search_datasets(
        ds_list, lambda ds: ds.table("t"), on_miss=hook
    )
    assert result is not None
    assert [d.lookups for d in ds_list] == [["t"], ["t"], ["t"]]


def test_every_shard_is_still_searched_when_no_one_has_the_table():
    ds_list = _search_order(_shards(None))
    result, other, terminal = _search_datasets(
        ds_list, lambda ds: ds.table("t"), on_miss=lambda tail: list(reversed(tail))
    )
    assert (result, other, terminal) == (None, None, None)
    assert [d.lookups for d in ds_list] == [["t"], ["t"], ["t"]]


# --------------------------------------------------------- through fetch()


def test_a_second_fetch_asks_only_the_right_shard(meta):
    """The saving the feature exists for: map in hand, one lookup, no probe."""
    ds = meta({"t": "item_response_warehouse"})
    first = _search_order(_shards("item_response_warehouse"))
    fetch(first, "t")
    # Newest probed, missed, map built, oldest asked: 2 lookups, not 3.
    assert [d.lookups for d in first] == [["t"], [], ["t"]]
    assert ds.reads == ["metadata"]

    second = _search_order(_shards("item_response_warehouse"))
    out = fetch(second, "t")
    assert isinstance(out, pd.DataFrame) and len(out) == 4
    lookups = {_bare(d._id): d.lookups for d in second}
    assert lookups["item_response_warehouse"] == ["t"]
    assert lookups["item_response_warehouse_3"] == []
    assert lookups["item_response_warehouse_2"] == []
    # And the map is not re-read for it.
    assert ds.reads == ["metadata"]


def test_a_fetch_from_the_newest_shard_never_reads_the_map(meta):
    """Probe-first: a newly uploaded table must not pay for a metadata read."""
    ds = meta({})
    ds_list = _search_order(_shards("item_response_warehouse_3"))
    out = fetch(ds_list, "t")
    assert isinstance(out, pd.DataFrame)
    assert [d.lookups for d in ds_list] == [["t"], [], []]
    assert ds.reads == []


def test_fetch_still_reports_a_table_that_is_in_no_shard(meta):
    ds_list = _search_order(_shards(None))
    # A name the metadata does not list: that one gets the plain not-found.
    # The listed-but-absent case is piece B's, in test_metadata_authority.py.
    meta({"other_table": "item_response_warehouse"})
    with pytest.warns(UserWarning, match="does not exist in the IRW database"):
        assert fetch(ds_list, "t") is None
    assert [d.lookups for d in ds_list] == [["t"], ["t"], ["t"]]


def test_fetch_returns_the_same_frame_with_resolution_off(meta, monkeypatch):
    meta({"t": "item_response_warehouse"})
    on = fetch(_search_order(_shards("item_response_warehouse")), "t")
    monkeypatch.setenv("IRW_SHARD_RESOLUTION", "0")
    off = fetch(_search_order(_shards("item_response_warehouse")), "t")
    pd.testing.assert_frame_equal(on, off)
