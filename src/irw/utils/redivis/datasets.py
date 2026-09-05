"""Internal Redivis dataset management utilities with lazy loading and caching."""

import logging
from typing import List, Any, Tuple
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
