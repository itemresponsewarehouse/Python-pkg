"""Internal Redivis dataset management utilities with lazy loading and caching."""

from typing import List, Any, Tuple
from ...config import MAIN_REFS, SIM_REF, COMP_REF
from .cache import metadata_cache
import redivis


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


def _init_datasets_from_refs(refs: Tuple[Tuple[str, str], ...]) -> List[Any]:
    """Initialize one Redivis dataset handle per (user, dataset_ref) entry."""
    return [_init_dataset(user, ref) for user, ref in refs]


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

    datasets = _init_datasets_from_refs(MAIN_REFS)
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
