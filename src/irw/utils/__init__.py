"""Internal utilities for IRW operations.

This module contains internal utilities that support the IRW API.
Users should not import from this module directly - use module-level functions instead.
"""

# Internal Redivis operations
from .redivis import (
    _init_dataset,
    _init_main_datasets,
    _init_sim_dataset,
    _init_comp_dataset,
    _get_table,
    _classify_error,
    _format_error,
)

__all__ = [
    # Dataset management
    "_init_dataset",
    "_init_main_datasets",
    "_init_sim_dataset",
    "_init_comp_dataset",
    # Table operations
    "_get_table",
    "_classify_error",
    "_format_error",
]
