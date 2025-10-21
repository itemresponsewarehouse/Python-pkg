"""
irw-py: A Python package for the Item Response Warehouse

This package provides programmatic access to the Item Response Warehouse (IRW),
an open repository of harmonized item response data.

Classes
-------
IRW : Unified IRW client for all dataset types

Examples
--------
>>> from irw_py import IRW
>>> 
>>> # Main datasets (default)
>>> irw = IRW()
>>> df = irw.fetch("agn_kay_2025")
>>> 
>>> # Simulation datasets
>>> irw_sim = IRW(source="sim")
>>> df_sim = irw_sim.fetch("gilbert_meta_3")
>>> 
>>> # Competition datasets
>>> irw_comp = IRW(source="comp")
>>> df_comp = irw_comp.fetch("collegefb_2021and2022")
"""

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

# Main exports
from .core import IRW

__all__ = ["IRW"]
__version__ = "0.0.1"