"""Configuration constants for IRW package."""

from typing import Tuple, ClassVar

# Redivis dataset references
MAIN_REFS: ClassVar[Tuple[Tuple[str, str], Tuple[str, str]]] = (
    ("datapages", "item_response_warehouse:as2e"),
    ("datapages", "item_response_warehouse_2:epbx"),
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
VERSION: str = "0.0.1"
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
