"""Configuration constants for IRW package.

To add another main IRW Redivis warehouse, append a (user, dataset_ref) tuple to
MAIN_REFS below. All package operations (list_tables, fetch, filter, download,
info, etc.) discover tables across every entry in MAIN_REFS automatically — no
other code changes are required. Item text works the same way via ITEMTEXT_REFS.
Both are lists because Redivis caps a dataset at 1000 tables.

See docs/DEVELOPERS.md for the full contributor checklist and test commands.
"""

from typing import Tuple, ClassVar

# Main IRW response-data warehouses on Redivis.
# Declared oldest-to-newest; fetch searches them newest-first, so a table present
# in more than one warehouse resolves to its most recent copy.
MAIN_REFS: ClassVar[Tuple[Tuple[str, str], ...]] = (
    ("datapages", "item_response_warehouse:as2e"),
    ("datapages", "item_response_warehouse_2:epbx"),
    ("datapages", "item_response_warehouse_3:5xaj"),
    ("datapages", "item_response_warehouse_4:980f"),
    ("datapages", "item_response_warehouse_5:3ykx"),
    ("datapages", "item_response_warehouse_6:fpe6"),
)
SIM_REF: ClassVar[Tuple[str, str]] = ("datapages", "irw_simsyn:0btg")
COMP_REF: ClassVar[Tuple[str, str]] = ("datapages", "irw_competitions:cmd7")

# Main IRW metadata dataset references (only for main IRW)
META_REF: ClassVar[Tuple[str, str]] = ("datapages", "irw_meta:bdxt")

# IRW item text shards on Redivis.
# Redivis caps a dataset at 1000 tables, so item text shards the way response
# data does. Declared oldest-to-newest; lookups search newest-first, so a table
# present in more than one shard resolves to its most recent copy.
# See Rpkg/inst/developer/warehouses.md for the checklist to add one.
ITEMTEXT_REFS: ClassVar[Tuple[Tuple[str, str], ...]] = (
    ("datapages", "irw_text:07b6"),
)

# Main IRW metadata table references
META_TABLES: ClassVar[dict[str, str]] = {
    "metadata": "metadata:h5gs",
    "tags": "tags:7nkh", 
    "biblio": "biblio:qahg",
    # Collections (issue #1633). `collections` is the registry, one row per
    # collection; `collection_members` is long, one row per (table, collection).
    "collections": "collections:va83",
    "collection_members": "collection_members:j7rp",
}

# Package metadata
PACKAGE_NAME: str = "irw"
VERSION: str = "0.0.2"
DESCRIPTION: str = "A Python package for the Item Response Warehouse (IRW)"

__all__ = [
    "MAIN_REFS",
    "SIM_REF", 
    "COMP_REF",
    "META_REF",
    "ITEMTEXT_REFS",
    "META_TABLES",
    "PACKAGE_NAME",
    "VERSION",
    "DESCRIPTION",
]
