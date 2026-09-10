"""Session version pins: use_version(), set_version(), get_version(), reset_version().

The pin exists for reproducibility, so every test here is about the ways it can
fail silently rather than loudly:

- a pin that Redivis quietly ignores (it resolves an unknown tag to the current
  release instead of erroring);
- a pinned session served data cached from the current release, or the
  reverse -- `redivis.Dataset.properties` never refetches, handles are cached,
  and the version-tag cache is keyed per dataset;
- a dataset younger than the pinned IRW version falling through to today's
  release and mixing versions into a "reproduced" run.

No network: Redivis and the manifest are both fakes.
"""

import re

import pandas as pd
import pytest

import irw
from irw.config import META_REF
from irw.operations import version as version_mod
from irw.utils.redivis import datasets as ds_mod
from irw.utils.redivis import pins
from irw.utils.redivis import table_metadata
from irw.utils.redivis.cache import metadata_cache
from irw.utils.redivis.pins import ABSENT, IRWVersionUnavailable
from irw.utils.table_helpers import _version_line


# --- a fake Redivis ---------------------------------------------------------
#
# Every dataset has a current tag and a dict of released tags -> table rows.
# `dataset(ref)` with no version resolves to the current release, and so does
# an unrecognised version, exactly as Redivis does; a well-formed tag that was
# never released is not found.

_TAG = re.compile(r"^v[0-9]+\.[0-9]+$")


class _NotFound(Exception):
    def __init__(self, ref):
        super().__init__({"error": "not_found", "error_description": f"Not found: {ref}"})


class _FakeTable:
    def __init__(self, name, rows):
        self.name = name
        self._rows = rows
        self.properties = {"numRows": len(rows)}

    def get(self):
        return self

    def to_pandas_dataframe(self, *args, **kwargs):
        return pd.DataFrame({"table": list(self._rows)})


class _FakeDataset:
    def __init__(self, world, ref, version):
        self._world = world
        self._ref = ref
        self._asked = version
        self.properties = None

    def _resolved(self):
        released, current = self._world[self._ref]
        if self._asked is None or not _TAG.match(self._asked):
            return current
        if self._asked not in released:
            raise _NotFound(f"{self._ref}:{self._asked}")
        return self._asked

    def get(self):
        self.properties = {"version": {"tag": self._resolved()}}
        return self

    def _tables(self):
        released, _ = self._world[self._ref]
        return released[self.properties["version"]["tag"]]

    def list_tables(self):
        return [_FakeTable(n, rows) for n, rows in self._tables().items()]

    def table(self, name):
        tables = self._tables()
        if name not in tables:
            raise _NotFound(name)
        return _FakeTable(name, tables[name])


class _FakeRedivis:
    def __init__(self, world):
        self.world = world
        self.opened = []

    def user(self, _user):
        outer = self

        class _U:
            @staticmethod
            def dataset(ref, version=None):
                outer.opened.append((ref, version))
                return _FakeDataset(outer.world, ref, version)

        return _U()


def _world():
    """Every pinnable dataset at v2.0 now; v1.0 exists for two of them.

    irw_meta's metadata table has one row at v1.0 and two at v2.0, and the
    first warehouse gained `new_table` in v2.0 -- the two differences a pin has
    to make visible.
    """
    world = {}
    for key, (_user, ref) in pins._pinnable_refs().items():
        world[ref] = ({"v2.0": {}}, "v2.0")
    meta = META_REF[1]
    world[meta] = ({
        "v1.0": {"metadata": ["alpha"], "tags": ["alpha"], "biblio": ["alpha"]},
        "v2.0": {"metadata": ["alpha", "beta"], "tags": ["alpha", "beta"],
                 "biblio": ["alpha", "beta"]},
    }, "v2.0")
    world["item_response_warehouse:as2e"] = ({
        "v1.0": {"old_table": ["x"]},
        "v2.0": {"old_table": ["x"], "new_table": ["y"]},
    }, "v2.0")
    return world


MANIFEST_ROWS = [
    # IRW v1: only the first warehouse and irw_meta had been released.
    ("1", "2026-08-01T00:00:00Z", "item_response_warehouse", "v1.0",
     "2026-08-01T00:00:00Z", "exact", ""),
    ("1", "2026-08-01T00:00:00Z", "irw_meta", "v1.0",
     "2026-08-01T00:00:00Z", "bracketed", "2026-08-02T00:00:00Z"),
]
for _key in pins._pinnable_refs():
    MANIFEST_ROWS.append(("2", "2026-09-01T00:00:00Z", _key, "v2.0",
                          "2026-09-01T00:00:00Z", "exact", ""))


@pytest.fixture(autouse=True)
def fake_irw(monkeypatch):
    frame = pd.DataFrame(MANIFEST_ROWS, columns=list(version_mod.COLUMNS), dtype="object")
    frame["irw_version"] = frame["irw_version"].astype(int)
    frame["released"] = [version_mod._parse_utc(v) for v in frame["irw_released_at"]]
    monkeypatch.setattr(version_mod, "_MANIFEST", frame)

    fake = _FakeRedivis(_world())
    monkeypatch.setattr(ds_mod, "redivis", fake)
    # A long TTL is the hard case: a version tag cached under the current
    # release stays "fresh" for the whole test unless keys keep them apart.
    monkeypatch.setenv("IRW_VERSION_TTL_SECONDS", "3600")

    pins._PINS.clear()
    monkeypatch.setattr(pins, "_PINNED_IRW_VERSION", None)
    metadata_cache.clear()
    yield fake
    pins._PINS.clear()
    metadata_cache.clear()


def _meta_rows():
    return list(table_metadata.get_metadata_table()["table"])


# --- use_version() ----------------------------------------------------------

def test_use_version_pins_every_dataset_to_its_own_tag(capsys):
    out = irw.use_version(1)
    assert out.attrs["irw_version"] == 1
    got = pins._pins()
    assert got["item_response_warehouse"] == "v1.0"
    assert got["irw_meta"] == "v1.0"
    # Every other dataset had no release at v1, and must not read "current".
    assert set(got) == set(pins._pinnable_refs())
    assert got["item_response_warehouse_2"] == ABSENT
    assert pd.isna(out.set_index("dataset").loc["item_response_warehouse_2", "version"])
    assert "Reading IRW v1" in capsys.readouterr().out


def test_use_version_with_no_argument_pins_the_newest(capsys):
    out = irw.use_version(quiet=True)
    assert out.attrs["irw_version"] == 2
    assert set(pins._pins().values()) == {"v2.0"}
    assert "item_response_warehouse_2:" not in capsys.readouterr().out


def test_use_version_by_date_warns_when_the_resolution_is_approximate():
    with pytest.warns(UserWarning, match="approximate"):
        irw.use_version(date="2026-08-15", quiet=True)
    assert pins.pinned_irw_version() == (1, "2026-08-01T00:00:00Z")


def test_use_version_by_number_does_not_warn():
    import warnings

    with warnings.catch_warnings():
        warnings.simplefilter("error")
        irw.use_version(1, quiet=True)


def test_use_version_rejects_both_keys_and_unknown_numbers():
    with pytest.raises(ValueError, match="not both"):
        irw.use_version(1, date="2026-08-15")
    with pytest.raises(ValueError, match="no version 99"):
        irw.use_version(99)
    with pytest.raises(ValueError, match="IRW version number"):
        irw.use_version("2026-08-01")
    assert pins._pins() == {}


def test_version_reports_a_numbered_version():
    out = irw.version(version=1)
    assert out.attrs["irw_version"] == 1
    assert set(out["dataset"]) == {"item_response_warehouse", "irw_meta"}


# --- the pin reaches Redivis ------------------------------------------------

def test_a_pinned_dataset_is_opened_at_its_tag(fake_irw):
    irw.set_version("irw_meta", "1.0")
    fake_irw.opened.clear()
    ds = ds_mod._init_dataset(*META_REF)
    assert fake_irw.opened == [(META_REF[1], "v1.0")]
    assert ds.properties["version"]["tag"] == "v1.0"


def test_an_unpinned_dataset_is_opened_without_a_version(fake_irw):
    ds_mod._init_dataset(*META_REF)
    assert fake_irw.opened == [(META_REF[1], None)]


def test_a_dataset_absent_at_the_pinned_version_cannot_be_read():
    irw.use_version(1, quiet=True)
    with pytest.raises(IRWVersionUnavailable, match="reset_version"):
        ds_mod._init_dataset("datapages", "irw_simsyn:0btg")


def test_absent_shards_are_skipped_not_reported_as_failures(monkeypatch):
    irw.use_version(1, quiet=True)
    opened = ds_mod._init_main_datasets()
    assert [d._id for d in opened] == ["item_response_warehouse:as2e@v1.0"]


def test_a_source_with_nothing_released_says_so():
    irw.use_version(1, quiet=True)
    with pytest.raises(IRWVersionUnavailable):
        ds_mod._init_datasets_from_refs(
            (("datapages", "irw_text:07b6"), ("datapages", "irw_text_2:ae47")),
            skip_unavailable=True,
        )


# --- verification: Redivis must not be allowed to ignore a pin ---------------

def test_a_tag_redivis_resolves_to_something_else_is_refused(fake_irw, monkeypatch):
    # Redivis hands back the current release for a version it does not
    # recognise. The pattern check stops "banana"; this stops a tag that passes
    # the pattern but still resolves elsewhere.
    monkeypatch.setattr(pins, "VERSION_PATTERN", pins.re.compile(r".*"))
    with pytest.raises(ValueError, match="could not be resolved"):
        ds_mod._verify_version("datapages", META_REF[1], "banana")


def test_a_tag_that_was_never_released_is_not_found():
    with pytest.raises(ValueError, match="does not exist"):
        irw.set_version("irw_meta", "v9.0")
    assert pins._pins() == {}


def test_set_version_rejects_malformed_tags_and_unknown_datasets():
    with pytest.raises(ValueError, match="version tag"):
        irw.set_version("irw_meta", "banana")
    with pytest.raises(ValueError, match="Unknown IRW dataset"):
        irw.set_version("irw_metadata", "v1.0")


# --- caches: the reason this is not a one-line change -------------------------

def test_a_pinned_session_is_not_served_the_current_release_and_back():
    assert _meta_rows() == ["alpha", "beta"]

    irw.use_version(1, quiet=True)
    assert _meta_rows() == ["alpha"]

    irw.reset_version()
    assert _meta_rows() == ["alpha", "beta"]


def test_pinned_and_current_handles_never_share_a_version_tag_entry():
    """Keys alone must separate them, not just the clear on a pin change.

    The version tag is cached per dataset label for the TTL. If a pinned
    handle had the same label as the current one, it would read the current
    release's tag from that cache, and every frame keyed on the tag would be
    served from the current release.
    """
    current = ds_mod._init_dataset(*META_REF)
    assert ds_mod._dataset_version_tag(current) == "v2.0"

    pins._PINS["irw_meta"] = "v1.0"  # deliberately without clearing the cache
    pinned = ds_mod._init_dataset(*META_REF)

    assert pinned is not current
    assert ds_mod._dataset_label(pinned) != ds_mod._dataset_label(current)
    assert ds_mod._dataset_version_tag(pinned) == "v1.0"
    assert ds_mod._dataset_version_tag(current) == "v2.0"


def test_handle_cache_keys_carry_the_pins():
    before = (ds_mod._main_datasets_cache_key(),)
    pins._PINS["item_response_warehouse"] = "v1.0"
    assert ds_mod._main_datasets_cache_key() not in before
    pins._PINS.clear()
    assert ds_mod._main_datasets_cache_key() in before


def test_a_pin_on_one_warehouse_reaches_the_table_listing():
    names = set(irw.list_tables()["name"])
    assert {"old_table", "new_table"} <= names

    irw.set_version("item_response_warehouse", "v1.0")
    names = set(irw.list_tables()["name"])
    assert "old_table" in names and "new_table" not in names


def test_fetch_under_a_pin_says_the_table_is_missing_from_the_pinned_release():
    irw.use_version(1, quiet=True)
    with pytest.warns(UserWarning, match="pinned version"):
        assert irw.fetch("new_table") is None


# --- get_version(), reset_version(), and what info() says ---------------------

def test_get_version_reports_resolved_tags_and_pins():
    irw.use_version(1, quiet=True)
    out = irw.get_version().set_index("dataset")
    assert out.loc["irw_meta", "version"] == "v1.0"
    assert pd.isna(out.loc["irw_simsyn", "version"])
    assert out["pinned"].all()

    irw.reset_version()
    out = irw.get_version(["irw_meta"]).set_index("dataset")
    assert out.loc["irw_meta", "version"] == "v2.0"
    assert not out.loc["irw_meta", "pinned"]


def test_reset_version_can_unpin_one_dataset(capsys):
    irw.set_version("irw_meta", "v1.0")
    irw.set_version("item_response_warehouse", "v1.0")
    remaining = irw.reset_version("irw_meta")
    assert remaining == {"item_response_warehouse": "v1.0"}
    assert "Unpinned irw_meta" in capsys.readouterr().out


def test_info_names_the_pinned_version_not_the_newest():
    irw.use_version(1, quiet=True)
    assert _version_line() == (
        "IRW version: v1 (released 2026-08-01T00:00:00Z), pinned for this session"
    )


def test_info_stops_claiming_an_irw_version_once_a_pin_moves():
    irw.use_version(1, quiet=True)
    irw.set_version("irw_meta", "v2.0")
    line = _version_line()
    assert "v1" not in line.split("(")[0]
    assert "pinned individually" in line and "irw_meta v2.0" in line


def test_info_reports_the_newest_version_when_unpinned():
    assert _version_line() == "IRW version: v2 (released 2026-09-01T00:00:00Z)"
