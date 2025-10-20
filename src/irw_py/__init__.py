"""
irw-py: A Python package for the Item Response Warehouse

This package provides programmatic access to the Item Response Warehouse (IRW),
an open repository of harmonized item response data.
"""

# exports the main IRW class as the main entry point
from .client import IRW


# Suppress known warnings
import warnings
warnings.filterwarnings(
    "ignore",
    message=".*is deprecated as an API*",
    category=DeprecationWarning,
)

warnings.filterwarnings(
    "ignore",
    message=".*No reference id was provided for the table.*",
    category=UserWarning,
)

__all__ = ["IRW"]
__version__ = "0.0.1"