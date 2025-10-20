"""Internal Redivis utilities."""

# Dataset management
from .datasets import (
    _init_dataset,
    _init_main_datasets,
    _init_sim_dataset,
    _init_comp_dataset,
)

# Table operations
from .tables import (
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
