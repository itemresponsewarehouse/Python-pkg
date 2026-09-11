"""Internal Redivis dataset management utilities with lazy loading and caching."""

import logging
import os
import time
from typing import Any, List, Optional, Tuple
from ...config import MAIN_REFS, SIM_REF, COMP_REF, NOM_REF
from .cache import metadata_cache
from .pins import (
    ABSENT,
    IRWVersionUnavailable,
    _absent_message,
    _dataset_key,
    _pinned_version,
    _pins_fingerprint,
)
import redivis

logger = logging.getLogger(__name__)


def _main_datasets_cache_key() -> str:
    """Cache key that changes when MAIN_REFS is updated (e.g. new warehouse added).

    The session's version pins are part of the key, so a pinned session and an
    unpinned one can never share warehouse handles.
    """
    return ("main_datasets:" + "|".join(f"{user}/{ref}" for user, ref in MAIN_REFS)
            + _pins_fingerprint(MAIN_REFS))


def _datasets_cache_key(datasets: List[Any]) -> str:
    """Stable cache key for a list of Redivis dataset handles."""
    labels = sorted(
        (getattr(ds, "_id", None) or getattr(ds, "name", None) or "").lower()
        for ds in datasets
    )
    return "|".join(labels)


def _init_datasets_from_refs(
    refs: Tuple[Tuple[str, str], ...],
    *,
    skip_unavailable: bool = False,
) -> List[Any]:
    """Initialize one Redivis dataset handle per (user, dataset_ref) entry.

    With ``skip_unavailable``, a warehouse that cannot be opened is dropped with
    a warning instead of aborting. A warehouse that exists but has no released
    version yet errors for read-only tokens, and one such warehouse must not
    take down every IRW lookup. If no warehouse opens at all -- e.g. a bad token,
    which fails for all of them -- the error is raised.
    """
    if not skip_unavailable:
        return [_init_dataset(user, ref) for user, ref in refs]

    datasets: List[Any] = []
    failures: List[str] = []
    absent: List[str] = []
    for user, ref in refs:
        # A shard younger than the pinned IRW version is not a failure: it did
        # not exist then, so a reproduced session should not see it. Skipped
        # quietly, unlike an unavailable shard, because absence is the right
        # answer rather than a fault.
        if _pinned_version(ref) == ABSENT:
            absent.append(_dataset_key(ref))
            continue
        try:
            datasets.append(_init_dataset(user, ref))
        except Exception as e:
            failures.append(f"{ref}: {e}")
            logger.warning(
                "Skipping unavailable IRW warehouse %s (%s). Its tables are not "
                "available in this session.",
                ref,
                e,
            )

    if absent:
        logger.info(
            "Skipping %d dataset(s) with no release at the pinned IRW version: %s",
            len(absent),
            ", ".join(absent),
        )
    if not datasets:
        if not failures:
            raise IRWVersionUnavailable(
                "None of " + ", ".join(absent) + " had a released version at "
                "the IRW version pinned by irw.use_version(). Use "
                "irw.reset_version() to return to the current release."
            )
        raise RuntimeError(
            "No IRW warehouse could be opened: " + "; ".join(failures)
        )
    return datasets



# =====================
# Release awareness
# =====================
#
# Every cache below a dataset handle has to be able to notice a release, or a
# long-running process serves what the release withdrew. That is not a general
# freshness preference: item-text withdrawals are how IRW stops distributing
# instrument wording it has been ruled it may not distribute, and a withdrawal
# that does not reach a consumer is not a withdrawal (issue #51).
#
# Noticing a release means asking Redivis for the current version tag, and that
# is where the previous approach quietly failed. `redivis.Dataset.properties`
# is a plain attribute assigned by `.get()` -- it does not refetch. The dataset
# handles are themselves cached here forever, so `ds.properties["version"]`
# returns the tag as of the first call for the life of the process, and the
# version checks in `table_metadata.py` that look correct could never fire.
#
# So the tag has to come from a handle that is actually re-`get()`. That is one
# small metadata request, and doing it per lookup would be wasteful, so it is
# bounded by a TTL: worst-case staleness is the TTL, not the process lifetime.
#
# Re-`get()`-ing the *same* handle is not enough either (issue #75). `.get()`
# replaces the handle's `uri` and `qualified_reference` with the ones Redivis
# returns, and those name the version that was current at the time
# (`item_response_warehouse_3:5xaj:v6_0`) even when no version was asked for.
# Every later `.get()` then re-reads that version, and every table reached
# through the handle is addressed at it. So the refresh asks an unversioned
# reference for the current release, and when that has moved, points the
# cached handle at it in place -- the handle lists and caches holding the
# object then follow without being rebuilt.

# The attributes `redivis.Dataset.get()` rewrites from the response
# (`update_properties` in redivis 0.20.x). Moving a handle to another release
# means moving all of them, or tables would be addressed at the old one.
_HANDLE_ADDRESS_ATTRS = ("properties", "qualified_reference", "scoped_reference", "uri", "name")

_DEFAULT_VERSION_TTL_SECONDS = 300.0


def _version_ttl_seconds() -> float:
    """Seconds a cached version tag is trusted before the handle is refreshed.

    Read from the environment on each call rather than at import so a caller
    can tighten it -- `IRW_VERSION_TTL_SECONDS=0` refreshes on every lookup --
    without restarting. An unparseable or negative value falls back to the
    default rather than disabling caching by accident.
    """
    raw = os.environ.get("IRW_VERSION_TTL_SECONDS")
    if raw is None:
        return _DEFAULT_VERSION_TTL_SECONDS
    try:
        value = float(raw)
    except (TypeError, ValueError):
        logger.warning(
            "Ignoring unparseable IRW_VERSION_TTL_SECONDS=%r; using %s seconds.",
            raw,
            _DEFAULT_VERSION_TTL_SECONDS,
        )
        return _DEFAULT_VERSION_TTL_SECONDS
    if value < 0:
        return _DEFAULT_VERSION_TTL_SECONDS
    return value


def _dataset_label(ds: Any) -> str:
    """Stable per-dataset cache label."""
    return (getattr(ds, "_id", None) or getattr(ds, "name", None) or "").lower()


def _properties_tag(properties: Any) -> Optional[str]:
    return ((properties or {}).get("version") or {}).get("tag")


def _refresh_handle(ds: Any) -> Any:
    """Re-read `ds` from Redivis and return the properties of its current release.

    A pinned handle is re-read as it is: its frozen address is the point. So is
    a handle that did not come through `_init_dataset`, which has no unversioned
    reference to ask. Otherwise the current release is read from a fresh,
    unversioned handle, and `ds` is moved onto it if it has changed.
    """
    user = getattr(ds, "_user", None)
    ref = getattr(ds, "_id", None)
    if getattr(ds, "_irw_pin", None) is not None or not user or not ref:
        ds.get()
        return getattr(ds, "properties", None)

    current = redivis.user(user).dataset(ref)
    current.get()
    old_tag = _properties_tag(getattr(ds, "properties", None))
    new_tag = _properties_tag(current.properties)
    if new_tag != old_tag:
        for attr in _HANDLE_ADDRESS_ATTRS:
            if hasattr(current, attr):
                setattr(ds, attr, getattr(current, attr))
        logger.info("IRW dataset %s moved from %s to %s.", ref, old_tag, new_tag)
    return current.properties


def _dataset_version_tag(ds: Any) -> Optional[str]:
    """Current released version tag for `ds`, refreshed at most once per TTL.

    Returns None when the tag cannot be determined -- an unversioned dataset,
    or a metadata request that failed. None is a cache key like any other: it
    is stable, so it does not cause churn, and it does not match a real tag, so
    a later successful read invalidates.

    A failed refresh keeps the last known tag rather than dropping every cache
    that depends on it. A metadata blip should not turn into a corpus-wide
    refetch on a token that is already rate limited.
    """
    key = f"version_tag:{_dataset_label(ds)}"
    now = time.monotonic()
    stamped = metadata_cache.get(key)
    if stamped is not None and (now - stamped[0]) < _version_ttl_seconds():
        return stamped[1]

    try:
        properties = _refresh_handle(ds)
    except Exception as e:
        logger.debug("Could not refresh version for %s: %s", _dataset_label(ds), e)
        if stamped is not None:
            # Re-stamp so a persistent outage is not one request per lookup.
            metadata_cache.set(key, (now, stamped[1]))
            return stamped[1]
        return None

    tag = _properties_tag(properties)
    metadata_cache.set(key, (now, tag))
    return tag


def _datasets_version_tag(datasets: List[Any]) -> Optional[str]:
    """One cache-version string for a group of datasets, or None if unknown.

    Any shard moving invalidates a cache built across all of them, which is
    what an index spanning shards needs: a table withdrawn from shard 2 has to
    drop out of an index that also covers shard 1.

    None if any member's tag is unknown, because a group version that cannot
    see one shard cannot promise anything about the group.
    """
    parts = []
    for ds in datasets:
        tag = _dataset_version_tag(ds)
        if tag is None:
            return None
        parts.append(f"{_dataset_label(ds)}={tag}")
    return "|".join(parts)


def _cache_version(tag: Optional[str]) -> str:
    """Turn a version tag into a cache version that is never wrongly trusted.

    `MetadataCache.get(key)` with no version means "no check", which is the
    stale-forever behaviour. An unknown tag must therefore not be passed
    through as None: it becomes a value that cannot match anything stored, so
    an unknown version is a miss rather than a free pass.

    But a miss on *every* call is its own hazard, and a worse one. The caches
    this guards are not cheap to refill: the metadata frames are Redivis table
    downloads, which are charged against the account-wide 30-day export cap
    (issue #21), and the table listings are ~43 paginated requests across the
    six warehouses. A first version of this returned a fresh instant, so an
    unknown tag meant re-downloading four metadata tables on every single
    lookup -- in a long-running MCP server answering an assistant's questions,
    that turns one unresolvable version into sustained export traffic against
    a shared quota.

    So the unknown case is bounded by time instead of by version: refills at
    most once per TTL window, which is the same staleness bound the known case
    already promises. `IRW_VERSION_TTL_SECONDS=0` still means "refresh every
    lookup", because there it is what the caller explicitly asked for.
    """
    if tag is not None:
        return tag
    ttl = _version_ttl_seconds()
    if ttl <= 0:
        return f"unknown:{time.monotonic()!r}"
    return f"unknown-window:{int(time.monotonic() // ttl)}"


def _dataset_table_list(ds: Any) -> List[Any]:
    """`ds.list_tables()`, cached per dataset and keyed on its version tag.

    This was written out four times -- twice in `operations/list_tables.py`,
    once each in `table_metadata.py` and `item_text.py` -- all four sharing the
    same `dataset_tables:{id}` key with no version, so a release could not
    invalidate any of them. One copy, version-keyed, fixes all four call sites.
    """
    label = _dataset_label(ds)
    if not label:
        return list(ds.list_tables())

    # `_cache_version` handles the unknown-tag case: a value that cannot match
    # a stored entry, but stable within a TTL window, so an unresolvable
    # version costs one listing per window rather than one per call.
    version = _cache_version(_dataset_version_tag(ds))

    cache_key = f"dataset_tables:{label}"
    cached = metadata_cache.get(cache_key, version)
    if cached is not None:
        return cached

    tables = list(ds.list_tables())
    metadata_cache.set(cache_key, tables, version)
    return tables

def _order_main_datasets(datasets: List[Any]) -> List[Any]:
    """Return main warehouses in search order: newest first.

    MAIN_REFS is declared oldest-to-newest (mirroring the R package's
    ``.irw_datasource_specs$core``), so reversing makes a table that exists in
    more than one warehouse resolve to its most recent copy -- matching what the
    R package's ``.irw_order_datasources`` does.
    """
    return list(reversed(datasets))


def _init_dataset(user: str, ds_ref: str) -> Any:
    """Create a Redivis dataset handle and ensure metadata is loaded.

    Honours the session's version pin for the dataset (see `pins.py`). Every
    IRW dataset is opened through here, so a pin reaches fetches, listings,
    metadata and item text alike -- R's `.irw_open_dataset()` is the same
    single point.

    A pinned handle is labelled with its tag (``_id`` is
    ``item_response_warehouse:as2e@v46.0``), and `_dataset_label` is what every
    per-dataset cache below keys on -- the version tag, the table list. So a
    pinned handle and a current one never share an entry, even within the
    version-tag TTL.
    """
    version = _pinned_version(ds_ref)
    if version == ABSENT:
        raise IRWVersionUnavailable(_absent_message(ds_ref))

    if version is None:
        ds = redivis.user(user).dataset(ds_ref)
    else:
        ds = redivis.user(user).dataset(ds_ref, version=version)
    ds.get()

    setattr(ds, "_user", user)
    setattr(ds, "_id", ds_ref if version is None else f"{ds_ref}@{version}")
    setattr(ds, "_irw_pin", version)
    return ds


def _verify_version(user: str, ds_ref: str, tag: str) -> Any:
    """Open `ds_ref` at `tag` and confirm Redivis actually serves that version.

    Redivis resolves a version it does not recognise to the current release
    instead of failing, which would make a pin a silent no-op, so the tag it
    resolves to is compared with the tag asked for. Ported from R's
    `.irw_verify_version`, with one difference: R reports every failure as
    "does not exist", and here only a not-found does. A timeout is not evidence
    that a version was never released.
    """
    from .tables import _classify_error, _sanitize_error

    key = _dataset_key(ds_ref)
    try:
        ds = redivis.user(user).dataset(ds_ref, version=tag)
        ds.get()
    except Exception as e:
        kind = _classify_error(e)
        if kind == "not_found":
            raise ValueError(
                f"Version {tag} of {key} does not exist on Redivis. "
                "Use irw.version() to see the tags each IRW version held."
            ) from e
        if kind == "auth":
            raise RuntimeError(
                "Redivis authentication failed while checking a version pin. "
                "Sign in via the browser when prompted, or see the README "
                f"troubleshooting section. Underlying error: {_sanitize_error(str(e))}"
            ) from e
        raise

    resolved = ((getattr(ds, "properties", None) or {}).get("version") or {}).get("tag")
    if resolved != tag:
        raise ValueError(
            f"Version {tag} of {key} could not be resolved on Redivis "
            f"(got {resolved!r} instead)."
        )
    return ds


def _init_main_datasets() -> List[Any]:
    """Initialize all main IRW datasets listed in MAIN_REFS (cached)."""
    cache_key = _main_datasets_cache_key()
    cached = metadata_cache.get(cache_key)
    if cached is not None:
        return cached

    datasets = _order_main_datasets(
        _init_datasets_from_refs(MAIN_REFS, skip_unavailable=True)
    )
    metadata_cache.set(cache_key, datasets)
    return datasets


def _init_sim_dataset() -> Any:
    """Initialize simulation dataset (cached)."""
    cache_key = "sim_dataset" + _pins_fingerprint([SIM_REF])
    cached = metadata_cache.get(cache_key)
    if cached is not None:
        return cached
    
    dataset = _init_dataset(*SIM_REF)
    metadata_cache.set(cache_key, dataset)
    return dataset


def _init_comp_dataset() -> Any:
    """Initialize competition dataset (cached)."""
    cache_key = "comp_dataset" + _pins_fingerprint([COMP_REF])
    cached = metadata_cache.get(cache_key)
    if cached is not None:
        return cached
    
    dataset = _init_dataset(*COMP_REF)
    metadata_cache.set(cache_key, dataset)
    return dataset


def _init_nom_dataset() -> Any:
    """Initialize nominal-response dataset (cached)."""
    cache_key = "nom_dataset" + _pins_fingerprint([NOM_REF])
    cached = metadata_cache.get(cache_key)
    if cached is not None:
        return cached
    
    dataset = _init_dataset(*NOM_REF)
    metadata_cache.set(cache_key, dataset)
    return dataset
