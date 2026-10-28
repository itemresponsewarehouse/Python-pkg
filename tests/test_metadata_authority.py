"""Piece B of ben-domingue/irw#3127: the metadata is the authority.

The core metadata, biblio, tags and collection_members frames are no longer
intersected with a live listing of the warehouses. Two things are asserted
here, and the second is the one the ruling calls load-bearing.

First, that a published row survives when no warehouse lists that table. The
fakes are built to *disagree* on purpose: on agreeing data a row-count check
would pass against the old intersection too. `_get_existing_tables` -- what
the intersection called -- is replaced by a sentinel that fails the test if it
is reached, so the filter coming back is a failure rather than a silent
regression.

Second, that fetching such a table explains itself. A bare not-found on a
table the user has just seen in a listing is the worse half of the trade.
"""

import pandas as pd
import pytest

import irw.utils.redivis.table_metadata as tm
from irw.operations.fetch import fetch
from irw.operations.table_sets import table_sets
from irw.utils.redivis import shard_resolution
from irw.utils.redivis.cache import metadata_cache
from irw.utils.redivis.shard_resolution import (
    listed_in_metadata,
    metadata_only_message,
)

WITHDRAWN = "withdrawn_yesterday"


class _NotFound(Exception):
    def __init__(self):
        super().__init__({"status": 404, "error": "not_found"})


class _Shard:
    """A shard holding nothing, which refuses to be enumerated."""

    def __init__(self, ref="item_response_warehouse:as2e"):
        self._id = ref
        self.name = "Item Response Warehouse"

    def list_tables(self):
        raise AssertionError("the live-table intersection is back")

    def table(self, name):
        raise _NotFound()


class _MetaTable:
    def __init__(self, frame):
        self._frame = frame

    def to_pandas_dataframe(self, max_results=None, *, variables=None, **kwargs):
        frame = self._frame
        if variables is not None:
            frame = frame.loc[:, [c for c in variables if c in frame.columns]]
        return frame.copy()


class _MetaDataset:
    def __init__(self, frames):
        self._frames = frames
        self.reads = []

    def table(self, name):
        self.reads.append(name)
        return _MetaTable(self._frames[name])


@pytest.fixture(autouse=True)
def _clean(monkeypatch):
    monkeypatch.delenv("IRW_SHARD_RESOLUTION", raising=False)
    shard_resolution.reset_state()
    metadata_cache.clear()
    # Reaching this is the regression the change exists to prevent.
    monkeypatch.setattr(
        tm,
        "_get_existing_tables",
        lambda source="core": pytest.fail(
            f"_get_existing_tables({source!r}) was called for a core frame"
        ),
    )
    yield
    shard_resolution.reset_state()
    metadata_cache.clear()


def _frames():
    return {
        "metadata": pd.DataFrame(
            {
                "table": ["live_table", WITHDRAWN],
                "dataset": ["item_response_warehouse"] * 2,
                "n_responses": [10, 20],
            }
        ),
        "biblio": pd.DataFrame(
            {"table": ["live_table", WITHDRAWN], "BibTex": ["@a{}", "@b{}"]}
        ),
        "tags": pd.DataFrame(
            {"table": ["live_table", WITHDRAWN], "Construct": ["x", "y"]}
        ),
        "collection_members": pd.DataFrame(
            {"table": ["live_table", WITHDRAWN], "collection": ["c", "c"]}
        ),
    }


@pytest.fixture
def meta(monkeypatch):
    """A fake irw_meta whose rows no warehouse lists."""
    from irw.utils.redivis import datasets as datasets_mod

    ds = _MetaDataset(_frames())
    monkeypatch.setattr(tm, "_get_meta_dataset", lambda: ds)
    monkeypatch.setattr(datasets_mod, "_dataset_version_tag", lambda _ds: "v9.0")
    return ds


# --------------------------------------------- the frames stand as published


def test_metadata_rows_survive_when_no_warehouse_lists_them(meta):
    out = tm.get_metadata_table("core")
    assert len(out) == 2
    assert WITHDRAWN in set(out["table"])


def test_biblio_and_tags_are_no_longer_intersected(meta):
    assert WITHDRAWN in set(tm.get_biblio_table("core")["table"])
    assert WITHDRAWN in set(tm.get_tags_table("core")["table"])


def test_collection_members_are_no_longer_intersected(meta):
    assert WITHDRAWN in set(tm.get_collection_members_table()["table"])


def test_a_non_core_source_keeps_the_intersection(monkeypatch, meta):
    """The ruling was scoped to the core warehouses."""
    seen = []

    def existing(source="core"):
        seen.append(source)
        return {"live_table"}

    monkeypatch.setattr(tm, "_get_existing_tables", existing)
    meta._frames["nominal_biblio"] = _frames()["biblio"]
    out = tm.get_biblio_table("nom")
    assert set(out["table"]) == {"live_table"}
    assert seen == ["nom"]


def test_known_table_names_reads_the_metadata_not_the_warehouses(meta):
    names = tm._known_table_names("core")
    assert names == {"live_table", WITHDRAWN}
    assert meta.reads.count("metadata") == 1


# ------------------------------------------------------- the explanation


def test_the_message_says_what_happened_and_what_fixes_it():
    msg = metadata_only_message("t")
    assert "is in the IRW metadata but not in the warehouse" in msg
    assert "withdrawn after the metadata was last published" in msg
    assert "catches up on the next pipeline run" in msg


def test_listed_in_metadata_is_true_only_for_a_listed_core_name(meta):
    assert listed_in_metadata(WITHDRAWN) is True
    assert listed_in_metadata("never_existed") is False
    # Other sources have no map, so they can never claim a listing.
    assert listed_in_metadata(WITHDRAWN, source="conj") is False


def test_fetch_explains_a_listed_but_absent_table(meta):
    with pytest.warns(UserWarning, match="in the IRW metadata but not in the warehouse"):
        assert fetch([_Shard(), _Shard("item_response_warehouse_2:epbx")], WITHDRAWN) is None


def test_table_sets_explains_a_listed_but_absent_table(meta):
    with pytest.raises(ValueError, match="in the IRW metadata but not in the warehouse"):
        table_sets([_Shard()], WITHDRAWN)


def test_a_table_in_neither_place_keeps_the_plain_message(meta):
    with pytest.warns(UserWarning, match="does not exist in the IRW database"):
        assert fetch([_Shard()], "never_existed") is None


def test_an_unreadable_metadata_table_keeps_the_plain_message(monkeypatch):
    """Choosing between two messages must never become a second failure."""

    def boom():
        raise RuntimeError("irw_meta unreadable")

    monkeypatch.setattr(tm, "_get_meta_dataset", boom)
    with pytest.warns(UserWarning, match="does not exist in the IRW database"):
        assert fetch([_Shard()], WITHDRAWN) is None
