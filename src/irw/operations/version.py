"""Which IRW version was live, and what every dataset was pinned to.

IRW is eleven Redivis datasets, each versioned independently -- `irw_meta` is
at v19.x while `irw_simsyn` has had two releases ever -- so no Redivis version
describes the corpus and a paper has had nothing to cite. The version manifest
in the `irw` repository supplies one: a record of the released version of every
dataset at every point in the corpus' history, with the IRW version number
incrementing whenever any of them is published.

    irw.version()               # the newest IRW version and its eleven pins
    irw.version("2026-08-01")   # what was live on 1 August 2026

The manifest is downloaded once per session rather than shipped inside the
package. It is rewritten daily as datasets are published, so a copy baked in at
build time would tell someone on a three-month-old install that the newest IRW
version is whatever it was in June -- wrong rather than merely stale.
"""

from __future__ import annotations

import csv
import datetime as dt
import io
import urllib.error
import urllib.request
import warnings
from typing import Optional, Union

import pandas as pd

#: The path is ``metadata/``, not ``src/metadata/``: the ``irw`` repository's
#: root is the directory that appears as ``src/`` in a local working copy.
MANIFEST_URL = (
    "https://raw.githubusercontent.com/ben-domingue/irw/main/"
    "metadata/version_manifest.tsv"
)

COLUMNS = (
    "irw_version", "irw_released_at", "dataset", "redivis_tag",
    "redivis_released_at", "precision", "redivis_released_before",
)

BRACKETED = "bracketed"

#: Session cache. One request per process, like the R package's.
_MANIFEST: Optional[pd.DataFrame] = None

_DATE_FORMATS = (
    "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d %H:%M", "%Y-%m-%d",
)


def _parse_utc(text: str) -> Optional[dt.datetime]:
    if not text:
        return None
    return dt.datetime.strptime(text, "%Y-%m-%dT%H:%M:%SZ").replace(
        tzinfo=dt.timezone.utc)


def _as_utc(when: Union[str, dt.date, dt.datetime]) -> dt.datetime:
    """Coerce what someone would actually type into a UTC instant.

    A bare date means the start of that day, so ``version("2026-08-01")``
    answers "what was live when that day began".
    """
    if isinstance(when, dt.datetime):
        if when.tzinfo is None:
            return when.replace(tzinfo=dt.timezone.utc)
        return when.astimezone(dt.timezone.utc)
    if isinstance(when, dt.date):
        return dt.datetime(when.year, when.month, when.day, tzinfo=dt.timezone.utc)
    if not isinstance(when, str):
        raise TypeError(
            "'date' must be a date string, date, or datetime -- "
            f"got {type(when).__name__}."
        )
    text = when.strip()
    for fmt in _DATE_FORMATS:
        try:
            return dt.datetime.strptime(text, fmt).replace(tzinfo=dt.timezone.utc)
        except ValueError:
            continue
    raise ValueError(
        f"Could not read {when!r} as a date. "
        'Try "2026-08-01" or "2026-08-01 12:00:00".'
    )


def _check_columns(frame: pd.DataFrame) -> None:
    """Refuse a manifest whose schema has moved on.

    The file is written by the `irw` repository and read by an installed
    package, so the two can drift. Guessing at renamed columns would produce
    plausible-looking wrong pins, which is worse than not answering.
    """
    found = tuple(frame.columns[:len(COLUMNS)])
    if found != COLUMNS:
        raise RuntimeError(
            "The IRW version manifest does not have the expected columns. "
            "This package may be out of date; please report it at "
            "https://github.com/itemresponsewarehouse/Python-pkg/issues\n"
            f"  expected: {list(COLUMNS)}\n  found:    {list(found)}"
        )


def load_manifest(refresh: bool = False) -> pd.DataFrame:
    """Download the manifest, or return this session's cached copy."""
    global _MANIFEST
    if _MANIFEST is not None and not refresh:
        return _MANIFEST

    try:
        with urllib.request.urlopen(MANIFEST_URL, timeout=30) as response:
            body = response.read().decode("utf-8")
    except (urllib.error.URLError, TimeoutError, OSError) as exc:
        raise RuntimeError(
            f"Could not download the IRW version manifest from\n  {MANIFEST_URL}\n"
            f"({exc}). Check your internet connection and try again."
        ) from exc

    rows = list(csv.DictReader(io.StringIO(body), delimiter="\t"))
    frame = pd.DataFrame(rows, dtype="object")
    if frame.empty:
        raise RuntimeError(f"The IRW version manifest at {MANIFEST_URL} is empty.")
    _check_columns(frame)

    frame["irw_version"] = frame["irw_version"].astype(int)
    frame["released"] = [_parse_utc(v) for v in frame["irw_released_at"]]
    _MANIFEST = frame
    return frame


def version(date: Optional[Union[str, dt.date, dt.datetime]] = None
            ) -> pd.DataFrame:
    """Report an IRW version and the Redivis version of every dataset in it.

    With no argument, the newest IRW version. With a date, the version that was
    live then -- which is how you recover what an analysis run months ago was
    actually reading.

    **Dates before 2026-07-21 are approximate.** Redivis overwrote its own
    release timestamps for the older warehouse shards during a platform
    migration: 142 of the corpus' 332 released versions claim to have been
    released inside one 80-minute window that day. For those the manifest
    records the earliest date the version could have been live and marks the
    row ``bracketed``. A date lookup landing on one warns, because the *tag*
    may then be wrong too -- a later version could already have been released
    inside the bracket. IRW version numbers themselves are always exact; only
    the mapping from a date to a version is affected.

    Args:
        date: Optional date or time, as ``"2026-08-01"``,
            ``"2026-08-01 12:00:00"``, or a ``date``/``datetime``.

    Returns:
        A DataFrame with columns ``dataset``, ``version``, ``released_at`` and
        ``approximate``, one row per dataset. The IRW version number is in
        ``.attrs["irw_version"]``.

    Examples:
        >>> irw.version()                      # doctest: +SKIP
        >>> irw.version("2026-08-01")          # doctest: +SKIP
    """
    manifest = load_manifest()

    if date is None:
        number = int(manifest["irw_version"].max())
        as_of = False
    else:
        when = _as_utc(date)
        live = manifest.loc[[r <= when for r in manifest["released"]], "irw_version"]
        if live.empty:
            earliest = min(manifest["released"]).strftime("%Y-%m-%d")
            raise ValueError(
                f"IRW has no version from before {earliest}; "
                "the corpus did not exist yet."
            )
        number = int(live.max())
        as_of = True

    rows = manifest[manifest["irw_version"] == number]
    out = pd.DataFrame({
        "dataset": rows["dataset"].tolist(),
        "version": rows["redivis_tag"].tolist(),
        "released_at": rows["redivis_released_at"].tolist(),
        "approximate": [p == BRACKETED for p in rows["precision"]],
    })
    released_at = rows["irw_released_at"].iloc[0]
    out.attrs["irw_version"] = number
    out.attrs["irw_released_at"] = released_at

    print(f"IRW v{number} (released {released_at}), {len(out)} dataset(s).")

    # The two caveats are different and must not be confused. Asked for a
    # version, a bracketed row means only that we cannot date it -- the pins
    # are exactly what that version held. Asked what was live on a *date*, the
    # same row means the pin itself may be wrong.
    approx = int(out["approximate"].sum())
    if approx and as_of:
        warnings.warn(
            f"This is approximate. {approx} of {len(out)} pins rest on a "
            "release date that Redivis overwrote, so for those the version tag "
            "may be wrong as well: a later release could already have been "
            "live. Cite an IRW version number rather than a date.",
            UserWarning,
            stacklevel=2,
        )
    elif approx:
        print(f"  {approx} of {len(out)} release dates are approximate "
              "(Redivis overwrote them). The pins are exact; only their dates "
              "are lower bounds.")

    return out
