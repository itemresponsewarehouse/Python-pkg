"""Configuration constants for IRW package.

To add another main IRW Redivis warehouse, append a (user, dataset_ref) tuple to
MAIN_REFS below. All package operations (list_tables, fetch, filter, download,
info, etc.) discover tables across every entry in MAIN_REFS automatically — no
other code changes are required.

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
SIM_REF: ClassVar[Tuple[str, str]] = ("bdomingu", "irw_simsyn:0btg")
COMP_REF: ClassVar[Tuple[str, str]] = ("bdomingu", "irw_competitions:cmd7")

# Main IRW metadata dataset references (only for main IRW)
META_REF: ClassVar[Tuple[str, str]] = ("bdomingu", "irw_meta:bdxt")
ITEMTEXT_REF: ClassVar[Tuple[str, str]] = ("bdomingu", "irw_text:07b6")

# Main IRW metadata table references
META_TABLES: ClassVar[dict[str, str]] = {
    "metadata": "metadata:h5gs",
    "tags": "tags:7nkh", 
    "biblio": "biblio:qahg",
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
    "ITEMTEXT_REF", 
    "META_TABLES",
    "PACKAGE_NAME",
    "VERSION",
    "DESCRIPTION",
]
