"""Offline tests for irw.version(). No network.

The manifest is downloaded once per session and cached in the module. Every
test here installs a fixture in that cache instead, so nothing goes out.

    python3 -m pytest tests/test_version.py
"""

import datetime as dt

import pandas as pd
import pytest

from irw.operations import version as mod

FIXTURE_ROWS = [
    # IRW v1: the shard alone, with the overwritten kind of date.
    ("1", "2024-01-01T00:00:00Z", "item_response_warehouse", "v1.0",
     "2024-01-01T00:00:00Z", "bracketed", "2026-07-01T00:00:00Z"),
    ("1", "2024-01-01T00:00:00Z", "irw_meta", "v1.0",
     "2024-01-01T00:00:00Z", "exact", ""),
    # IRW v2: the shard moves, irw_meta is carried forward unchanged.
    ("2", "2026-07-01T00:00:00Z", "item_response_warehouse", "v2.0",
     "2026-07-01T00:00:00Z", "bracketed", "2026-09-01T00:00:00Z"),
    ("2", "2026-07-01T00:00:00Z", "irw_meta", "v1.0",
     "2024-01-01T00:00:00Z", "exact", ""),
    # IRW v3: irw_meta moves.
    ("3", "2026-08-15T00:00:00Z", "item_response_warehouse", "v2.0",
     "2026-07-01T00:00:00Z", "bracketed", "2026-09-01T00:00:00Z"),
    ("3", "2026-08-15T00:00:00Z", "irw_meta", "v2.0",
     "2026-08-15T00:00:00Z", "exact", ""),
]


@pytest.fixture(autouse=True)
def manifest(monkeypatch):
    frame = pd.DataFrame(FIXTURE_ROWS, columns=list(mod.COLUMNS), dtype="object")
    frame["irw_version"] = frame["irw_version"].astype(int)
    frame["released"] = [mod._parse_utc(v) for v in frame["irw_released_at"]]
    monkeypatch.setattr(mod, "_MANIFEST", frame)
    yield frame


def test_no_argument_reports_the_newest_version():
    out = mod.version()
    assert out.attrs["irw_version"] == 3
    assert set(out["dataset"]) == {"item_response_warehouse", "irw_meta"}
    assert out.loc[out["dataset"] == "irw_meta", "version"].iloc[0] == "v2.0"


def test_a_date_reports_the_version_live_then():
    with pytest.warns(UserWarning):
        out = mod.version("2026-08-01")
    assert out.attrs["irw_version"] == 2
    # The shard was on v2.0 that day, but irw_meta was still on v1.0.
    assert out.loc[out["dataset"] == "irw_meta", "version"].iloc[0] == "v1.0"


def test_a_date_lands_on_the_version_in_force_not_the_next_one():
    with pytest.warns(UserWarning):
        assert mod.version("2026-08-14 23:59:59").attrs["irw_version"] == 2
    with pytest.warns(UserWarning):
        assert mod.version("2026-08-15").attrs["irw_version"] == 3


def test_a_date_before_the_corpus_existed_raises():
    with pytest.raises(ValueError, match="did not exist yet"):
        mod.version("2020-01-01")


def test_a_date_lookup_on_an_overwritten_timestamp_warns():
    # The tag itself may be wrong here: a later version could already have been
    # released inside the bracket. That is what the warning is for.
    with pytest.warns(UserWarning, match="approximate"):
        mod.version("2026-08-01")


def test_asking_for_the_newest_version_does_not_warn():
    # Same bracketed row, weaker claim: v3's pins are exactly what v3 held, so
    # warning here would overstate the problem.
    import warnings

    with warnings.catch_warnings():
        warnings.simplefilter("error")
        mod.version()


def test_approximate_column_marks_the_bracketed_rows():
    out = mod.version()
    approx = dict(zip(out["dataset"], out["approximate"]))
    assert approx["item_response_warehouse"] is True
    assert approx["irw_meta"] is False


@pytest.mark.parametrize("given,expected", [
    ("2026-08-01", dt.datetime(2026, 8, 1, tzinfo=dt.timezone.utc)),
    ("2026-08-01 12:30", dt.datetime(2026, 8, 1, 12, 30, tzinfo=dt.timezone.utc)),
    ("2026-08-01T12:30:00Z", dt.datetime(2026, 8, 1, 12, 30, tzinfo=dt.timezone.utc)),
    (dt.date(2026, 8, 1), dt.datetime(2026, 8, 1, tzinfo=dt.timezone.utc)),
])
def test_as_utc_reads_the_forms_a_person_would_type(given, expected):
    assert mod._as_utc(given) == expected


def test_a_naive_datetime_is_read_as_utc_not_local():
    # Silently applying the machine's timezone would make the same script
    # resolve to different versions on different laptops.
    naive = dt.datetime(2026, 8, 1, 12, 0)
    assert mod._as_utc(naive) == dt.datetime(2026, 8, 1, 12, tzinfo=dt.timezone.utc)


def test_as_utc_rejects_what_it_cannot_read():
    with pytest.raises(ValueError, match="Could not read"):
        mod._as_utc("last tuesday")
    with pytest.raises(TypeError):
        mod._as_utc(20260801)


def test_a_manifest_with_unexpected_columns_is_rejected():
    # Guards against a schema change in the irw repo silently producing wrong
    # pins in an old install.
    broken = pd.DataFrame(FIXTURE_ROWS,
                          columns=["irw_version", "irw_released_at", "dataset",
                                   "tag", "redivis_released_at", "precision",
                                   "redivis_released_before"])
    with pytest.raises(RuntimeError, match="expected columns"):
        mod._check_columns(broken)


# --- current_version(): the quiet stamp info() uses ------------------------

def test_current_version_reports_the_newest_version_and_its_date():
    assert mod.current_version() == (3, "2026-08-15T00:00:00Z")


def test_current_version_does_not_print():
    """version() prints a summary line as a side effect; info() must not."""
    import io
    import contextlib

    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        mod.current_version()
    assert buf.getvalue() == ""


def test_current_version_returns_none_when_the_manifest_is_unreachable(monkeypatch):
    """Offline, info() should still print everything else it knows."""
    monkeypatch.setattr(mod, "_MANIFEST", None)
    monkeypatch.setattr(
        mod, "load_manifest",
        lambda refresh=False: (_ for _ in ()).throw(RuntimeError("no network")),
    )
    assert mod.current_version() is None
