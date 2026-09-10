"""Which IRW version was live, and what every dataset was pinned to.

IRW is eleven Redivis datasets, each versioned independently -- `irw_meta` is
at v19.x while `irw_simsyn` has had two releases ever -- so no Redivis version
describes the corpus and a paper has had nothing to cite. The version manifest
in the `irw` repository supplies one: a record of the released version of every
dataset at every point in the corpus' history, with the IRW version number
incrementing whenever any of them is published.

    irw.version()               # the newest IRW version and its eleven pins
    irw.version("2026-08-01")   # what was live on 1 August 2026
    irw.use_version(332)        # read the corpus as v332 held it (issue #3)

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
from typing import Dict, List, Optional, Tuple, Union

import pandas as pd

from ..utils.redivis.datasets import _init_dataset, _verify_version
from ..utils.redivis.pins import (
    ABSENT,
    _normalize_version,
    _pinnable_refs,
    _pins,
    _replace_pins,
)

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


def current_version() -> Optional[Tuple[int, str]]:
    """The newest IRW version and its release date, or None if unavailable.

    A quiet, non-raising counterpart to ``version()`` for callers that want to
    stamp output with a version but must not fail or print because of it --
    ``info()`` is the case this exists for. The manifest is cached for the
    session, so the cost is one download per process, not one per call.
    """
    try:
        manifest = load_manifest()
        number = int(manifest["irw_version"].max())
        released = manifest.loc[
            manifest["irw_version"] == number, "irw_released_at"
        ].iloc[0]
        return number, str(released)
    except Exception:
        # Offline, or the manifest moved. The caller's own output is worth more
        # than the version stamp, so degrade rather than raise.
        return None


def _as_irw_version(number: object) -> int:
    """Coerce a user-supplied IRW version number: ``332`` or ``"332"``.

    Anything else is a mistake worth naming. A date silently read as a version
    number would pin a session to the wrong corpus.
    """
    if isinstance(number, str) and number.strip().isdigit():
        number = int(number.strip())
    if isinstance(number, float) and number.is_integer():
        number = int(number)
    if (isinstance(number, bool) or not isinstance(number, int)
            or not 1 <= number <= 1_000_000):
        raise ValueError(
            f"'version' must be a single IRW version number, e.g. 332; got "
            f"{number!r}. To look up a date instead, name the argument: "
            'date="2026-08-01".'
        )
    return number


def _select(version: Optional[Union[int, str]] = None,
            date: Optional[Union[str, dt.date, dt.datetime]] = None
            ) -> Tuple[int, pd.DataFrame, bool]:
    """Resolve a version number or a date to one IRW version's manifest rows.

    The single place that turns "which point in history" into rows, so that
    ``version()`` and ``use_version()`` cannot disagree about what a date or a
    number means. Returns ``(number, rows, as_of)``, where ``as_of`` says the
    version was reached from a date -- the case that can be wrong.
    """
    if version is not None and date is not None:
        raise ValueError(
            "Give either 'version' or 'date', not both. An IRW version number "
            "is exact; a date has to be resolved to one."
        )
    manifest = load_manifest()

    if version is not None:
        number = _as_irw_version(version)
        if number not in set(manifest["irw_version"]):
            raise ValueError(
                f"IRW has no version {number}. Released versions run from "
                f"{int(manifest['irw_version'].min())} to "
                f"{int(manifest['irw_version'].max())}."
            )
        as_of = False
    elif date is None:
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
    return number, rows, as_of


def _report_approximate(precision: pd.Series, as_of: bool) -> None:
    """Report the two different meanings of a bracketed row.

    The caveats are different and must not be confused. Asked for a version,
    a bracketed row means only that we cannot date it -- the pins are exactly
    what that version held. Asked what was live on a *date*, the same row means
    the pin itself may be wrong.
    """
    approx = int(sum(p == BRACKETED for p in precision))
    total = len(precision)
    if approx and as_of:
        warnings.warn(
            f"This is approximate. {approx} of {total} pins rest on a "
            "release date that Redivis overwrote, so for those the version tag "
            "may be wrong as well: a later release could already have been "
            "live. Cite an IRW version number rather than a date.",
            UserWarning,
            stacklevel=3,
        )
    elif approx:
        print(f"  {approx} of {total} release dates are approximate "
              "(Redivis overwrote them). The pins are exact; only their dates "
              "are lower bounds.")


def version(date: Optional[Union[str, dt.date, dt.datetime]] = None,
            version: Optional[Union[int, str]] = None) -> pd.DataFrame:
    """Report an IRW version and the Redivis version of every dataset in it.

    With no argument, the newest IRW version. With a ``version`` number,
    exactly what that version held. With a date, the version that was live
    then -- which is how you recover what an analysis run months ago was
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
        version: Optional IRW version number, e.g. ``332``. Mutually exclusive
            with ``date``.

    Returns:
        A DataFrame with columns ``dataset``, ``version``, ``released_at`` and
        ``approximate``, one row per dataset. The IRW version number is in
        ``.attrs["irw_version"]``.

    Examples:
        >>> irw.version()                      # doctest: +SKIP
        >>> irw.version(version=332)           # doctest: +SKIP
        >>> irw.version("2026-08-01")          # doctest: +SKIP
    """
    number, rows, as_of = _select(version=version, date=date)

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
    _report_approximate(rows["precision"], as_of)

    return out


# =====================
# Session pins
# =====================
#
# Ported from Rpkg/R/manifest.R (irw_use_version) and Rpkg/R/version.R
# (irw_set_version, irw_get_version, irw_reset_version). The state and the
# rules live in utils/redivis/pins.py; every dataset handle is opened through
# utils/redivis/datasets._init_dataset, which is where a pin takes effect.


def use_version(version: Optional[Union[int, str]] = None,
                date: Optional[Union[str, dt.date, dt.datetime]] = None,
                quiet: bool = False) -> pd.DataFrame:
    """Read the whole corpus at one IRW version for the rest of the session.

    Pins every IRW dataset -- response warehouses, metadata and item text --
    to the Redivis version that one IRW version held, so ``fetch()``,
    ``list_tables()``, ``filter()``, ``info()`` and ``itemtext()`` return the
    same data whenever the script is re-run, even after IRW has been corrected
    or extended. Each dataset is pinned to its own tag: one IRW version means a
    different Redivis version in every shard. A dataset that had no release at
    that IRW version is pinned to nothing and cannot be read, rather than
    falling through to today's release; a table only it holds is not found.

    Called with no arguments it pins the newest version: run it at the start of
    a project and record the number it prints. Pinning by date warns when the
    date resolution is approximate; pinning by number never has to.

    Args:
        version: IRW version number, e.g. ``332``. Defaults to the newest.
        date: Date or time to pin the corpus as it was then. Mutually
            exclusive with ``version``.
        quiet: Suppress the per-dataset listing.

    Returns:
        A DataFrame of ``dataset`` and ``version`` as pinned (missing for a
        dataset not yet released then), with the IRW version number in
        ``.attrs["irw_version"]``.

    Examples:
        >>> irw.use_version(332)               # doctest: +SKIP
        >>> df = irw.fetch("gilbert_meta_12")  # v332's copy   # doctest: +SKIP
        >>> irw.reset_version()                # doctest: +SKIP
    """
    number, rows, as_of = _select(version=version, date=date)
    refs = _pinnable_refs()

    unknown = sorted(set(rows["dataset"]) - set(refs))
    if unknown:
        warnings.warn(
            f"IRW v{number} contains {len(unknown)} dataset(s) this version of "
            f"the package does not know about: {', '.join(unknown)}. They "
            "cannot be pinned or read here; update the package.",
            UserWarning,
            stacklevel=2,
        )

    tags = dict(zip(rows["dataset"], rows["redivis_tag"]))
    pins: Dict[str, str] = {}
    for key, (user, ref) in refs.items():
        tag = _normalize_version(tags.get(key))
        if tag is None:
            # Absent from this IRW version, or a tag in a form we do not
            # recognise. Both must block reads rather than fall through.
            pins[key] = ABSENT
        else:
            _verify_version(user, ref, tag)
            pins[key] = tag

    released_at = str(rows["irw_released_at"].iloc[0])
    _replace_pins(pins, irw_version=(number, released_at))

    out = pd.DataFrame({
        "dataset": list(pins),
        "version": [None if t == ABSENT else t for t in pins.values()],
    })
    out.attrs["irw_version"] = number

    absent = sum(t == ABSENT for t in pins.values())
    print(f"Reading IRW v{number} (released {released_at}) for this session: "
          f"{len(pins) - absent} dataset(s) pinned"
          + (f", {absent} not yet released then" if absent else "") + ".")
    if not quiet:
        for key, tag in pins.items():
            print(f"  {key}: {'not released yet' if tag == ABSENT else tag}")
    _report_approximate(rows["precision"], as_of)

    return out


def set_version(dataset: str, version: str) -> Dict[str, str]:
    """Pin one IRW dataset to a released Redivis version for this session.

    Every lookup that reads the dataset honours the pin -- ``fetch()``,
    ``filter()``, ``list_tables()``, metadata and item text. A table that does
    not exist in the pinned version is not found rather than silently fetched
    from the current release. To pin the whole corpus at once, use
    ``use_version()``; ``get_version()`` shows the tags currently in use.

    Args:
        dataset: Dataset name, e.g. ``"item_response_warehouse"``.
        version: Released version tag, e.g. ``"v32.0"`` (the ``v`` is optional).

    Returns:
        The pins now in effect, as ``{dataset: tag}``.
    """
    refs = _pinnable_refs()
    if not isinstance(dataset, str) or not dataset:
        raise ValueError(
            "'dataset' must be a single dataset name, e.g. "
            '"item_response_warehouse".'
        )
    if dataset not in refs:
        raise ValueError(
            f"Unknown IRW dataset: {dataset!r}. Pinnable datasets are: "
            f"{', '.join(refs)}."
        )
    tag = _normalize_version(version)
    if tag is None:
        raise ValueError(
            f"'version' must be a released version tag such as \"v32.0\"; got "
            f"{version!r}. Use irw.get_version() to see the versions in use."
        )

    user, ref = refs[dataset]
    _verify_version(user, ref, tag)

    pins = _pins()
    pins[dataset] = tag
    _replace_pins(pins)
    print(f"Pinned {dataset} to {tag} for this session.")
    return _pins()


def get_version(dataset: Optional[Union[str, List[str]]] = None) -> pd.DataFrame:
    """Report the Redivis version each IRW dataset is being read at.

    Asks Redivis for each dataset's resolved tag, so it costs one small request
    per dataset. Record these tags to make an analysis reproducible; replay
    them with ``set_version()``, or cite the IRW version from ``version()``.

    Args:
        dataset: A dataset name or list of names. Defaults to every dataset.

    Returns:
        A DataFrame with columns ``dataset``, ``version`` (missing when the
        dataset is pinned to a version it had not yet released, or could not
        be reached) and ``pinned``.
    """
    refs = _pinnable_refs()
    if dataset is not None:
        wanted = [dataset] if isinstance(dataset, str) else list(dataset)
        unknown = [d for d in wanted if d not in refs]
        if unknown:
            raise ValueError(
                f"Unknown IRW dataset(s): {unknown}. Known datasets are: "
                f"{', '.join(refs)}."
            )
        refs = {d: refs[d] for d in wanted}

    pins = _pins()
    versions: List[Optional[str]] = []
    for key, (user, ref) in refs.items():
        if pins.get(key) == ABSENT:
            # Nothing to ask Redivis about.
            versions.append(None)
            continue
        try:
            ds = _init_dataset(user, ref)
            tag = ((ds.properties or {}).get("version") or {}).get("tag")
        except Exception:
            tag = None
        versions.append(tag)

    return pd.DataFrame({
        "dataset": list(refs),
        "version": versions,
        "pinned": [key in pins for key in refs],
    })


def reset_version(dataset: Optional[Union[str, List[str]]] = None) -> Dict[str, str]:
    """Remove version pins: one dataset, several, or (by default) all of them.

    Unpinned datasets read the current release again.

    Returns:
        The pins still in effect, as ``{dataset: tag}``.
    """
    pins = _pins()
    if dataset is None:
        dropped = list(pins)
        pins = {}
    else:
        wanted = [dataset] if isinstance(dataset, str) else list(dataset)
        dropped = [d for d in wanted if d in pins]
        pins = {k: v for k, v in pins.items() if k not in wanted}

    _replace_pins(pins)
    if dropped:
        print(f"Unpinned {', '.join(dropped)}; using the current release.")
    else:
        print("No IRW version pins were set.")
    return _pins()
