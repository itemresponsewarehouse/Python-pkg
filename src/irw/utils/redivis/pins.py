"""Session version pins: which Redivis version each IRW dataset is read at.

IRW is twelve Redivis datasets, each versioned independently. A pin fixes one
of them to a released version tag for the rest of the process, so a script
re-run next year reads the data it read today. `irw.use_version()` pins every
dataset at once to what one IRW version held; `irw.set_version()` pins one.
This module is only the state and the rules -- the public functions live in
`operations/version.py`, and every dataset handle is opened through
`datasets._init_dataset`, which is the one place a pin takes effect.

Ported from `Rpkg/R/version.R`. Two things the R package learned the hard way
carry over unchanged:

- **Redivis resolves an unrecognised version to the current release.**
  `dataset(ref, version="banana")` silently returns the latest data rather than
  failing (verified 2026-09-10 against `item_response_warehouse_3`), which would
  defeat the pin without a sound. Tags are checked against a pattern before
  they are sent, and the tag Redivis resolves to is compared with the tag asked
  for.
- **A dataset that did not exist at the pinned IRW version is not unpinned.**
  Unpinned means "current release", which would quietly mix today's data into
  a run meant to reproduce an old one. It is pinned to `ABSENT` instead, and
  opening it raises `IRWVersionUnavailable`.

A pin change clears every in-process cache (see `_replace_pins`), and the pins
are also part of every dataset-handle cache key and handle label (see
`_pins_fingerprint`), so a pinned session can never be served a handle, a
table list, or a version tag cached from the current release, or the reverse.
"""

import re
from typing import Dict, Iterable, List, Optional, Tuple

from ...config import COMP_REF, ITEMTEXT_REFS, MAIN_REFS, META_REF, NOM_REF, SIM_REF
from .cache import metadata_cache

#: Accepted form of a Redivis version tag: ``v32.0`` or ``32.0``.
VERSION_PATTERN = re.compile(r"^v?[0-9]+\.[0-9]+$")

#: Pin value for a dataset that had no release at the pinned IRW version.
#: Deliberately not a valid tag, so `set_version()` cannot be talked into it.
ABSENT = "<none>"

#: dataset key -> normalised tag (or ABSENT). Empty means "current release".
_PINS: Dict[str, str] = {}

#: The IRW version `use_version()` last pinned, as (number, released_at, pins).
#: The pins snapshot is what makes it honest: once `set_version()` moves one
#: dataset, the session no longer reads that IRW version, and `pinned_irw_version()`
#: stops claiming it does.
_PINNED_IRW_VERSION: Optional[Tuple[int, str, Dict[str, str]]] = None


class IRWVersionUnavailable(RuntimeError):
    """A pinned dataset had no released version at the pinned IRW version."""


def _dataset_key(ds_ref: str) -> str:
    """Pin key for a dataset ref: the part before the hash.

    ``item_response_warehouse:as2e`` -> ``item_response_warehouse``, which is
    also how the version manifest names datasets.
    """
    return ds_ref.split(":", 1)[0]


def _pinnable_refs() -> Dict[str, Tuple[str, str]]:
    """Every IRW dataset that can be pinned, keyed by dataset name.

    The response sources plus the metadata dataset and every item-text shard,
    so a pinned session is reproducible in its metadata as well as its data.
    Order follows R's `.irw_pinnable_specs()`: sources, then meta, then text.
    """
    refs: List[Tuple[str, str]] = [
        *MAIN_REFS, SIM_REF, COMP_REF, NOM_REF, META_REF, *ITEMTEXT_REFS,
    ]
    return {_dataset_key(ref): (user, ref) for user, ref in refs}


def _normalize_version(version: object) -> Optional[str]:
    """Normalise a tag to Redivis' ``vN.N`` form, or None if it is not one."""
    if not isinstance(version, str):
        return None
    text = version.strip()
    if not VERSION_PATTERN.match(text):
        return None
    return text if text.startswith("v") else f"v{text}"


def _pins() -> Dict[str, str]:
    """A copy of the pins in effect."""
    return dict(_PINS)


def _pinned_version(ds_ref: str) -> Optional[str]:
    """The pin for one dataset ref, or None when it reads the current release."""
    return _PINS.get(_dataset_key(ds_ref))


def _pins_fingerprint(refs: Iterable[Tuple[str, str]]) -> str:
    """Cache-key suffix naming the pins that apply to `refs`; empty if none.

    Empty when nothing is pinned, so an unpinned session keeps exactly the
    cache keys it had before pinning existed.
    """
    parts = []
    for _user, ref in refs:
        tag = _pinned_version(ref)
        if tag is not None:
            parts.append(f"{_dataset_key(ref)}={tag}")
    return f"@pins[{','.join(parts)}]" if parts else ""


def _replace_pins(pins: Dict[str, str],
                  irw_version: Optional[Tuple[int, str]] = None) -> None:
    """Install a new set of pins and drop every cache built under the old ones.

    Clearing by the whole cache rather than by an enumerated list is R's rule
    (`.irw_clear_all_datasource_caches`): a cache added later cannot be left
    stale behind a pin. The keys are pin-aware as well, but some composite
    frames -- tags and biblio filtered to the tables that exist -- are keyed
    only on the metadata dataset's tag, so pinning one warehouse alone would
    otherwise leave them describing the current release.

    The version manifest is not in this cache: it records every IRW version,
    so it stays true whatever is pinned.
    """
    global _PINNED_IRW_VERSION
    _PINS.clear()
    _PINS.update(pins)
    # Only use_version() names an IRW version. A later set_version() or
    # partial reset keeps the record; pinned_irw_version() compares snapshots,
    # so it stops claiming the number the moment the pins no longer match it.
    if irw_version is not None:
        _PINNED_IRW_VERSION = (irw_version[0], irw_version[1], dict(pins))
    metadata_cache.clear()


def pinned_irw_version() -> Optional[Tuple[int, str]]:
    """The IRW version this session is pinned to, if it still reads exactly that.

    None when nothing is pinned, and None after `set_version()` or a partial
    `reset_version()` has moved a pin away from what `use_version()` set.
    """
    if _PINNED_IRW_VERSION is None:
        return None
    number, released, snapshot = _PINNED_IRW_VERSION
    if snapshot != _PINS:
        return None
    return number, released


def _absent_message(ds_ref: str) -> str:
    return (
        f"{_dataset_key(ds_ref)} had no released version at the IRW version "
        "pinned by irw.use_version(), so it cannot be read in this session. "
        "Use irw.reset_version() to return to the current release."
    )


def _pinned_not_found_hint(datasets: Iterable[object]) -> str:
    """Extra text for a table missing from pinned handles; empty if unpinned.

    "Never existed in IRW" and "not in the release you pinned" are the same
    Redivis not-found error and need entirely different fixes. Ported from R's
    `.irw_pinned_not_found_message`, but read off the handles that were
    actually searched rather than off a source name.
    """
    pinned = []
    for ds in datasets:
        tag = getattr(ds, "_irw_pin", None)
        ref = getattr(ds, "_id", None)
        if isinstance(tag, str) and isinstance(ref, str):
            pinned.append(f"{_dataset_key(ref)} {tag}")
    if not pinned:
        return ""
    return (
        f" It does not exist in the pinned version(s): {', '.join(pinned)}. "
        "It may have been added in a later release; use irw.reset_version() "
        "to read the current release, or irw.list_tables() to see what the "
        "pinned version holds."
    )
