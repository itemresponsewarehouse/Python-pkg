"""
irw: A Python package for the Item Response Warehouse

This package provides programmatic access to the Item Response Warehouse (IRW),
an open repository of harmonized item response data.

Usage:
    import irw
    
    # Database operations
    irw.list_tables()
    irw.filter(construct_type="Affective/mental health")
    irw.info()  # Database info
    irw.info("agn_kay_2025")  # Table info
    
    # Table operations
    df = irw.fetch("agn_kay_2025")
    irw.itemtext("agn_kay_2025")
    irw.save_bibtex("agn_kay_2025")
    irw.download("agn_kay_2025")
    resp_matrix = irw.long2resp(df)
"""

# Suppress known warnings
import warnings
warnings.filterwarnings(
    "ignore",
    message=".*No reference id was provided for the table.*",
    category=UserWarning,
)

# Export all API functions
from .api import (
    list_tables,
    filter,
    info,
    fetch,
    itemtext,
    save_bibtex,
    download,
    long2resp,
    get_filters,
    describe_filter,
    list_tables_with_itemtext,
)

__all__ = [
    "list_tables",
    "filter",
    "info",
    "fetch",
    "itemtext",
    "save_bibtex",
    "download",
    "long2resp",
    "get_filters",
    "describe_filter",
    "list_tables_with_itemtext",
]

__version__ = "0.0.1"