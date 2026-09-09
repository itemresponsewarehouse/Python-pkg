"""Internal Redivis dataset management utilities with lazy loading and caching."""

import logging
import os
import time
from typing import Any, List, Optional, Tuple
from ...config import MAIN_REFS, SIM_REF, COMP_REF, NOM_REF
from .cache import metadata_cache
import redivis

logger = logging.getLogger(__name__)


def _main_datasets_cache_key() -> str:
    """Cache key that changes when MAIN_REFS is updated (e.g. new warehouse added)."""
    return "main_datasets:" + "|".join(f"{user}/{ref}" for user, ref in MAIN_REFS)


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
    for user, ref in refs:
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

    if not datasets:
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
        ds.get()
    except Exception as e:
        logger.debug("Could not refresh version for %s: %s", _dataset_label(ds), e)
        if stamped is not None:
            # Re-stamp so a persistent outage is not one request per lookup.
            metadata_cache.set(key, (now, stamped[1]))
            return stamped[1]
        return None

    properties = getattr(ds, "properties", None) or {}
    tag = (properties.get("version") or {}).get("tag")
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
    """
    return tag if tag is not None else f"unknown:{time.monotonic()!r}"


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

    version = _dataset_version_tag(ds)
    if version is None:
        # No tag means no way to tell a stale list from a current one, so the
        # list is not cached at all. This costs a request per call for a
        # dataset with no released version -- rare, and the alternative is the
        # stale-forever behaviour this whole change exists to remove.
        return list(ds.list_tables())

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
    """Create a Redivis dataset handle and ensure metadata is loaded."""
    ds = redivis.user(user).dataset(ds_ref)
    ds.get()
    
    setattr(ds, "_user", user)
    setattr(ds, "_id", ds_ref)
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
    cached = metadata_cache.get("sim_dataset")
    if cached is not None:
        return cached
    
    dataset = _init_dataset(*SIM_REF)
    metadata_cache.set("sim_dataset", dataset)
    return dataset


def _init_comp_dataset() -> Any:
    """Initialize competition dataset (cached)."""
    cached = metadata_cache.get("comp_dataset")
    if cached is not None:
        return cached
    
    dataset = _init_dataset(*COMP_REF)
    metadata_cache.set("comp_dataset", dataset)
    return dataset


def _init_nom_dataset() -> Any:
    """Initialize nominal-response dataset (cached)."""
    cached = metadata_cache.get("nom_dataset")
    if cached is not None:
        return cached
    
    dataset = _init_dataset(*NOM_REF)
    metadata_cache.set("nom_dataset", dataset)
    return dataset
