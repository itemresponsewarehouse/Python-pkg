"""
irw-py: A Python package for the Item Response Warehouse

This package provides programmatic access to the Item Response Warehouse (IRW),
an open repository of harmonized item response data.

Classes
-------
IRW : Main IRW datasets
IRWSim : Simulation datasets  
IRWComp : Competition datasets

Examples
--------
>>> from irw_py import IRW, IRWSim, IRWComp
>>> 
>>> # Main datasets
>>> irw = IRW()
>>> df = irw.fetch("agn_kay_2025")
>>> 
>>> # Simulation datasets
>>> irw_sim = IRWSim()
>>> df_sim = irw_sim.fetch("gilbert_meta_3")
>>> 
>>> # Competition datasets
>>> irw_comp = IRWComp()
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
from .core import IRW, IRWSim, IRWComp

__all__ = ["IRW", "IRWSim", "IRWComp"]
__version__ = "0.0.1"