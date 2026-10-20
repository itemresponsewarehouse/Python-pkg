"""Finding shards that were created after this package was released.

The shard lists in ``irw.config`` are literals in package source, so corpus
growth and package releases are coupled. When a new shard is seeded, every
installed copy of the package is blind to it -- and blind *silently*: a shard
absent from the ref list is never opened, never listed and never warned about,
so ``list_tables()``, ``metadata()`` and ``filter()`` simply come back short.
That is what happened with the seventh core warehouse, which released on
2026-10-15 with 761 tables that no released client could see
(ben-domingue/irw#3091).

So the ref lists are treated as a floor. Once per process the Redivis
organization catalog is read and any shard that continues a family already in
the config is appended.

Three properties make this safe to do by default:

* It only ever *extends* a family already in the config, by matching the
  family's base name plus a numeric suffix. A new data family is deliberately
  not adopted: that needs a ``source`` value, filters and metadata wiring, so
  it must keep failing loudly as it does today.
* It only appends, never removes or reorders. A process with no network, or one
  that opts out, behaves exactly as before.
* A shard is adopted only if the catalog shows it has a published version and
  is publicly readable, which are the two conditions for opening it without a
  warning.

Opt out with ``IRW_SHARD_DISCOVERY=0``.

This mirrors ``R/shard-discovery.R`` in the R package. The two clients declare
the same shard lists, so a fix in one and not the other just moves the gap --
``irw_nominal`` was reachable from R and invisible from Python for months
(ben-domingue/irw#1733).
"""

from __future__ import annotations

import json
import logging
import os
import re
import urllib.request
from typing import Any, Dict, List, NamedTuple, Optional, Sequence, Tuple

logger = logging.getLogger(__name__)

CATALOG_ORGANIZATION = "datapages"

# A documented REST endpoint (https://docs.redivis.com/api/rest-api/datasets/list)
# that answers without credentials for public datasets. 1000 is the maximum it
# accepts and far above the ~32 datasets the organization holds, so one request
# is the whole catalog and there is no paging to get wrong.
CATALOG_URL = (
    f"https://redivis.com/api/v1/organizations/{CATALOG_ORGANIZATION}"
    "/datasets?maxResults=1000"
)

# Bounded on purpose. Discovery is an improvement on the configured list, not a
# dependency of it, so a slow or unreachable catalog must cost a few seconds
# once and then get out of the way -- hence the timeout plus the cached
# failure below.
CATALOG_TIMEOUT_SECONDS = 10

Ref = Tuple[str, str]


class CatalogEntry(NamedTuple):
    """One dataset as the organization catalog describes it."""

    reference: str
    reference_id: str
    version: str
    public: bool
    table_count: Optional[int]


# Module-level rather than in metadata_cache: the catalog is not warehouse
# state, it must survive a cache clear, and a failure must be remembered so a
# process with no network pays for one timeout and not one per call. The
# sentinel distinguishes "not looked yet" from "looked, nothing there".
_UNSET = object()
_state: Dict[str, Any] = {"catalog": _UNSET, "announced": set()}


def reset_state() -> None:
    """Forget the cached catalog and announcements (tests)."""
    _state["catalog"] = _UNSET
    _state["announced"] = set()


def discovery_enabled() -> bool:
    """Whether shard discovery is switched on."""
    value = os.environ.get("IRW_SHARD_DISCOVERY", "").strip().lower()
    if not value:
        return True
    return value not in {"0", "false", "no", "off"}


def parse_catalog(text: str) -> List[CatalogEntry]:
    """Parse the catalog response into entries discovery can use.

    The reference *name* is the first field of ``scopedReference``, not
    ``name``: ``name`` is the display name and does not resolve (``irw_rank``
    shows as "IRW_rank", the seventh core warehouse as "Item Response
    Warehouse 7"). A ``scopedReference`` carries its version as a third field
    only once a version has been published, so requiring three fields is also
    the released-or-not test.
    """
    try:
        payload = json.loads(text)
    except (ValueError, TypeError):
        return []
    results = payload.get("results") if isinstance(payload, dict) else None
    if not isinstance(results, list):
        return []

    entries: List[CatalogEntry] = []
    for item in results:
        if not isinstance(item, dict):
            continue
        ref = item.get("scopedReference")
        if not isinstance(ref, str):
            continue
        parts = ref.split(":")
        # name:refid:version. Two fields means no published version yet.
        if len(parts) != 3:
            continue
        count = item.get("tableCount")
        entries.append(
            CatalogEntry(
                reference=parts[0],
                reference_id=parts[1],
                version=parts[2],
                public=item.get("publicAccessLevel") == "data",
                table_count=count if isinstance(count, int) else None,
            )
        )
    return entries


def fetch_catalog() -> Optional[List[CatalogEntry]]:
    """The organization's dataset catalog, read once per process.

    Never raises: discovery has to leave the package exactly as it was when the
    catalog cannot be read.
    """
    if _state["catalog"] is not _UNSET:
        return _state["catalog"]

    catalog: Optional[List[CatalogEntry]] = None
    try:
        # A fixed https URL, defined above; nothing here is caller-supplied.
        with urllib.request.urlopen(CATALOG_URL, timeout=CATALOG_TIMEOUT_SECONDS) as response:
            if response.status == 200:
                entries = parse_catalog(response.read().decode("utf-8"))
                catalog = entries or None
    except Exception as exc:  # any failure at all means "no catalog"
        logger.debug("Shard discovery could not read the dataset catalog: %s", exc)
        catalog = None

    _state["catalog"] = catalog
    return catalog


def _reference_name(ref: str) -> str:
    """``item_response_warehouse_7:ej0d`` -> ``item_response_warehouse_7``."""
    return ref.split(":")[0]


def family_base(refs: Sequence[Ref]) -> Optional[str]:
    """Base name of a family of shards.

    The oldest ref names the family: the core warehouses start at
    ``item_response_warehouse`` and continue ``_2``, ``_3``, ...; item text
    starts at ``irw_text``; conjoint at ``irw_conjoint``.
    """
    if not refs:
        return None
    return re.sub(r"_\d+$", "", _reference_name(refs[0][1]))


def extra_refs(refs: Sequence[Ref], catalog: Optional[Sequence[CatalogEntry]]) -> Tuple[Ref, ...]:
    """Shards of this family that are live but absent from ``refs``.

    Ordered oldest-first by numeric suffix to match how every ref list is
    declared. Lexical order would put ``_10`` before ``_2``, and since lookups
    search newest-first to resolve a duplicate name to its most recent copy,
    that would resolve to the wrong shard.
    """
    if not refs or not catalog:
        return ()
    base = family_base(refs)
    if not base:
        return ()

    # ``base`` or ``base_<digits>`` and nothing else, so a different family that
    # merely starts with the same characters cannot match (``irw_textual`` is
    # not an ``irw_text`` shard). The base is escaped because it is data.
    pattern = re.compile(rf"^{re.escape(base)}(_\d+)?$")
    configured = {_reference_name(ref) for _user, ref in refs}

    found = []
    for entry in catalog:
        if not entry.public or entry.reference in configured:
            continue
        if not pattern.match(entry.reference):
            continue
        suffix = entry.reference[len(base):]
        index = int(suffix.lstrip("_")) if suffix else 1
        found.append((index, entry))

    found.sort(key=lambda pair: pair[0])
    return tuple(
        (CATALOG_ORGANIZATION, f"{entry.reference}:{entry.reference_id}")
        for _index, entry in found
    )


def _announce(new: Sequence[Ref], noun: str, usable: bool) -> None:
    """Tell the user once that their irw is behind the warehouse."""
    names = [_reference_name(ref) for _user, ref in new]
    unseen = [name for name in names if name not in _state["announced"]]
    if not unseen:
        return
    _state["announced"].update(unseen)
    listed = ", ".join(unseen)
    if usable:
        logger.warning(
            "Found %d IRW %s(s) newer than this version of irw: %s. They are "
            "included here. Upgrading irw also brings the documentation and "
            "filters that shipped with them.",
            len(unseen), noun, listed,
        )
    else:
        logger.warning(
            "IRW has %d %s(s) this version of irw cannot read: %s. Upgrade irw "
            "to reach them.",
            len(unseen), noun, listed,
        )


def effective_refs(refs: Sequence[Ref], noun: str = "warehouse") -> Tuple[Ref, ...]:
    """Configured refs plus any live shard that continues the family.

    The single seam discovery acts through: every initializer and every cache
    key goes through here, so the configured and effective lists cannot drift
    apart.
    """
    refs = tuple(refs)
    if not refs or not discovery_enabled():
        return refs
    extra = extra_refs(refs, fetch_catalog())
    if not extra:
        return refs
    _announce(extra, noun, usable=True)
    return refs + extra


def warn_if_extended(ref: Ref, noun: str) -> None:
    """Warn when a single-dataset source has grown a shard this client cannot hold.

    ``sim``, ``comp`` and ``nom`` are one dataset each here, and their
    initializers return one handle, so a second shard cannot simply be
    appended the way it can for the lists. None of the three is anywhere near
    the 1000-table cap, so this is not expected -- but "which families did we
    remember to wire up" is exactly how the seventh core warehouse came to be
    missed, so the gap is made loud rather than left silent.
    """
    if not discovery_enabled():
        return
    extra = extra_refs((ref,), fetch_catalog())
    if extra:
        _announce(extra, noun, usable=False)
