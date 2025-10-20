"""Internal Redivis dataset management utilities."""

from typing import List, Any
from ...config import MAIN_REFS, SIM_REF, COMP_REF
import redivis


def _init_dataset(user: str, ds_ref: str) -> Any:
    """Create a Redivis dataset handle and ensure metadata is loaded."""
    ds = redivis.user(user).dataset(ds_ref)
    ds.get()
    
    setattr(ds, "_user", user)
    setattr(ds, "_id", ds_ref)
    return ds


def _init_main_datasets() -> List[Any]:
    """Initialize only main IRW datasets."""
    return [_init_dataset(*ref) for ref in MAIN_REFS]


def _init_sim_dataset() -> Any:
    """Initialize only simulation dataset."""
    return _init_dataset(*SIM_REF)


def _init_comp_dataset() -> Any:
    """Initialize only competition dataset."""
    return _init_dataset(*COMP_REF)
