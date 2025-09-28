"""
irw-py: A Python package for the Item Response Warehouse

This package provides programmatic access to the Item Response Warehouse (IRW),
an open repository of harmonized item response data.
"""

import warnings
# Suppress known upstream warnings from dependencies
warnings.filterwarnings(
    "ignore",
    message=".*is deprecated as an API*",
    category=DeprecationWarning,
)

from .client import IRW

warnings.filterwarnings(
    "ignore",
    message=".*No reference id was provided for the table.*",
    category=UserWarning,
)

__all__ = ["IRW"]
__version__ = "0.0.1"