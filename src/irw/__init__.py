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
# Redivis suggests qualifying every table as `name:refid`. We deliberately do
# not for the IRW metadata tables: reference ids are reminted on every release,
# so a pinned id stops resolving in the next version. See config.META_TABLES.
warnings.filterwarnings(
    "ignore",
    message=".*No reference id was provided for the table.*",
    category=UserWarning,
)
warnings.filterwarnings(
    "ignore",
    message=".*pkg_resources is deprecated.*",
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
    collections,
    collection,
    collection_members,
    version,
)
from .operations.list_tables import IRWMetadataUnavailable

__all__ = [
    "IRWMetadataUnavailable",
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
    "collections",
    "collection",
    "collection_members",
    "version",
]

# Single source of truth is config.VERSION -- these two literals had drifted
# apart (0.0.1 here, 0.0.2 there), so a user could not tell what they had.
from .config import VERSION as VERSION

__version__ = VERSION