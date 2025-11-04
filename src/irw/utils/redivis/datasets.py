"""Internal Redivis dataset management utilities with lazy loading and caching."""

from typing import List, Any
from ...config import MAIN_REFS, SIM_REF, COMP_REF
from .cache import metadata_cache
import redivis


def _init_dataset(user: str, ds_ref: str) -> Any:
    """Create a Redivis dataset handle and ensure metadata is loaded."""
    ds = redivis.user(user).dataset(ds_ref)
    ds.get()
    
    setattr(ds, "_user", user)
    setattr(ds, "_id", ds_ref)
    return ds


def _init_main_datasets() -> List[Any]:
    """Initialize main IRW datasets (cached)."""
    cached = metadata_cache.get("main_datasets")
    if cached is not None:
        return cached
    
    datasets = [_init_dataset(*ref) for ref in MAIN_REFS]
    metadata_cache.set("main_datasets", datasets)
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
