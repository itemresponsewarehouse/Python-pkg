"""Tests for the on-disk table cache (no Redivis network calls).

The assertion that matters is on the wire: whether the fake table's download
methods were called. A test that only compared the returned frames would pass
just as well if every fetch exported the table again.
"""

import os
import sys
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import irw
from irw.operations.fetch import fetch
from irw.utils import table_helpers
from irw.utils.redivis import disk_cache


class _Dataset:
    def __init__(self, table, pin=None):
        self._table = table
        self._irw_pin = pin
        table.dataset = self

    def table(self, name):
        return self._table


class _Table:
    """A loaded Redivis table handle that records every download."""

    def __init__(self, frame, *, name="t_one", content_hash="h1", version="v5_0"):
        self._frame = frame
        self.name = name
        self.dataset = None
        self.properties = {
            "name": name,
            "hash": content_hash,
            "qualifiedReference": f"datapages.item_response_warehouse:as2e:{version}.{name}:abcd",
        }
        self.arrow_calls = 0
        self.pandas_calls = []

    def get(self):
        return None

    def to_arrow_table(self, *args, **kwargs):
        self.arrow_calls += 1
        return pa.Table.from_pandas(self._frame, preserve_index=False)

    def to_pandas_dataframe(self, max_results=None, *, variables=None, **kwargs):
        self.pandas_calls.append({"max_results": max_results, "variables": variables})
        frame = self._frame
        if variables is not None:
            frame = frame.loc[:, list(variables)]
        if max_results is not None:
            frame = frame.iloc[:max_results]
        return pa.Table.from_pandas(frame, preserve_index=False).to_pandas(
            types_mapper=pd.ArrowDtype)

    @property
    def downloads(self):
        return self.arrow_calls + len(self.pandas_calls)


def _frame(n=6):
    return pd.DataFrame({
        "id": [f"p{i // 2}" for i in range(n)],
        "item": [f"q{i % 2}" for i in range(n)],
        "resp": [str(i) for i in range(n)],
        "cov_age": [30 + i for i in range(n)],
    })


def _files():
    return sorted(Path(os.environ["IRW_CACHE_DIR"]).rglob("*.parquet"))


def test_second_fetch_reads_disk_and_does_not_export():
    table = _Table(_frame())
    first = fetch([_Dataset(table)], "t_one")
    assert table.downloads == 1
    second = fetch([_Dataset(table)], "t_one")
    assert table.downloads == 1
    pd.testing.assert_frame_equal(first, second)
    assert [p.relative_to(os.environ["IRW_CACHE_DIR"]).as_posix() for p in _files()] == [
        "v1/tables/t_one/h1.parquet"]


def test_cached_frame_matches_an_uncached_fetch():
    uncached = _Table(_frame())
    irw.set_cache(False)
    expected = fetch([_Dataset(uncached)], "t_one")
    irw.set_cache(None)

    table = _Table(_frame())
    fetch([_Dataset(table)], "t_one")
    got = fetch([_Dataset(table)], "t_one")
    pd.testing.assert_frame_equal(got, expected)
    assert list(got.dtypes) == list(expected.dtypes)


def test_resp_is_still_coerced_on_a_hit():
    table = _Table(_frame())
    fetch([_Dataset(table)], "t_one")
    df = fetch([_Dataset(table)], "t_one")
    assert pd.api.types.is_numeric_dtype(df["resp"])


def test_changed_hash_is_a_miss_and_replaces_the_old_copy():
    fetch([_Dataset(_Table(_frame(), content_hash="h1"))], "t_one")
    newer = _Table(_frame(8), content_hash="h2")
    df = fetch([_Dataset(newer)], "t_one")
    assert newer.downloads == 1 and len(df) == 8
    assert [p.name for p in _files()] == ["h2.parquet"]


def test_pinned_write_keeps_the_live_copy():
    fetch([_Dataset(_Table(_frame(), content_hash="h2"))], "t_one")
    fetch([_Dataset(_Table(_frame(), content_hash="h1"), pin="v4.0")], "t_one")
    assert [p.name for p in _files()] == ["h1.parquet", "h2.parquet"]


def test_pushdown_on_a_miss_goes_to_redivis_and_saves_nothing():
    table = _Table(_frame())
    df = fetch([_Dataset(table)], "t_one", max_rows=2, columns=["id", "resp"])
    assert table.pandas_calls == [{"max_results": 2, "variables": ["id", "resp"]}]
    assert table.arrow_calls == 0
    assert list(df.columns) == ["id", "resp"] and len(df) == 2
    assert _files() == []


def test_pushdown_on_a_hit_is_applied_locally():
    table = _Table(_frame())
    fetch([_Dataset(table)], "t_one")
    df = fetch([_Dataset(table)], "t_one", max_rows=3, columns=["item", "id"])
    assert table.downloads == 1
    assert list(df.columns) == ["item", "id"] and len(df) == 3


def test_dedup_on_a_hit_matches_dedup_on_a_miss():
    frame = pd.concat([_frame(), _frame()], ignore_index=True)
    miss = fetch([_Dataset(_Table(frame, content_hash="d"))], "t_one", dedup=True)
    hit = fetch([_Dataset(_Table(frame, content_hash="d"))], "t_one", dedup=True)
    pd.testing.assert_frame_equal(miss, hit)
    assert len(hit) == 6


def test_unreadable_file_is_discarded_and_refetched():
    table = _Table(_frame())
    fetch([_Dataset(table)], "t_one")
    _files()[0].write_bytes(b"not parquet")
    df = fetch([_Dataset(table)], "t_one")
    assert table.downloads == 2 and len(df) == 6
    pq.read_table(_files()[0])  # rewritten whole


def test_write_failure_still_returns_the_data(monkeypatch):
    def boom(*a, **k):
        raise OSError("disk full")
    monkeypatch.setattr(disk_cache.pq, "write_table", boom)
    df = fetch([_Dataset(_Table(_frame()))], "t_one")
    assert len(df) == 6
    assert _files() == []
    assert not list(Path(os.environ["IRW_CACHE_DIR"]).rglob("*.tmp-*"))


def test_table_without_a_hash_is_not_cached():
    table = _Table(_frame())
    del table.properties["hash"]
    fetch([_Dataset(table)], "t_one")
    fetch([_Dataset(table)], "t_one")
    assert table.downloads == 2 and _files() == []


@pytest.mark.parametrize("value", ["0", "false", "OFF"])
def test_env_switch_turns_it_off(monkeypatch, value):
    monkeypatch.setenv("IRW_CACHE", value)
    table = _Table(_frame())
    fetch([_Dataset(table)], "t_one")
    fetch([_Dataset(table)], "t_one")
    assert table.downloads == 2 and _files() == []


def test_set_cache_overrides_env(monkeypatch):
    monkeypatch.setenv("IRW_CACHE", "0")
    irw.set_cache(True)
    table = _Table(_frame())
    fetch([_Dataset(table)], "t_one")
    fetch([_Dataset(table)], "t_one")
    assert table.downloads == 1


def test_file_carries_its_provenance():
    fetch([_Dataset(_Table(_frame(), version="v5_0"))], "t_one")
    meta = pq.read_schema(_files()[0]).metadata
    assert meta[b"irw_table"] == b"t_one"
    assert meta[b"irw_hash"] == b"h1"
    assert meta[b"irw_format"] == b"v1"


def test_cache_info_and_clear_cache():
    fetch([_Dataset(_Table(_frame(), name="t_one", content_hash="a"))], "t_one")
    fetch([_Dataset(_Table(_frame(), name="T_Two", content_hash="b"))], "T_Two")
    info = irw.cache_info()
    assert sorted(info["table"]) == ["T_Two", "t_one"]
    assert set(info["version"]) == {"v5.0"}
    freed = irw.clear_cache("t_two")
    assert freed > 0
    assert list(irw.cache_info()["table"]) == ["t_one"]
    irw.clear_cache()
    assert irw.cache_info().empty
    assert not any((Path(os.environ["IRW_CACHE_DIR"]) / "v1" / "tables").iterdir())


def test_itemtext_is_cached_under_its_own_kind(monkeypatch):
    table = _Table(_frame(), name="t_one__items")
    _Dataset(table)
    monkeypatch.setattr(table_helpers, "_list_itemtext_tables", lambda: {"t_one"})
    monkeypatch.setattr(table_helpers, "_get_itemtext_table", lambda name: table)
    first = table_helpers._get_table_itemtext("t_one")
    second = table_helpers._get_table_itemtext("t_one")
    assert table.downloads == 1
    pd.testing.assert_frame_equal(first, second)
    assert [p.parent.parent.name for p in _files()] == ["itemtext"]
    irw.clear_cache("t_one")  # a table's item text goes with it
    assert _files() == []


@pytest.mark.parametrize("platform, env, expected", [
    ("linux", {"XDG_CACHE_HOME": "/xdg"}, "/xdg/irw"),
    ("linux", {}, "~/.cache/irw"),
    ("darwin", {}, "~/Library/Caches/irw"),
    ("win32", {"LOCALAPPDATA": "C:/Users/u/AppData/Local"}, "C:/Users/u/AppData/Local/irw/Cache"),
])
def test_cache_root_per_platform(monkeypatch, platform, env, expected):
    monkeypatch.delenv("IRW_CACHE_DIR")
    for k in ("XDG_CACHE_HOME", "LOCALAPPDATA"):
        monkeypatch.delenv(k, raising=False)
    for k, v in env.items():
        monkeypatch.setenv(k, v)
    monkeypatch.setattr(sys, "platform", platform)
    assert disk_cache.cache_root() == Path(os.path.expanduser(expected))


def test_cache_dir_override():
    assert irw.cache_dir() == os.environ["IRW_CACHE_DIR"]
