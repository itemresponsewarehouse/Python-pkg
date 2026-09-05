"""Internal operations for IRW data.

This module contains internal operation functions used by the public API.
These are not part of the public API - users should use module-level functions instead.
"""

# Re-exports for api.py, which is the only thing that should import these.
# The redundant `as` aliases mark them as deliberate re-exports rather than
# dead imports; __all__ stays empty because nothing here is public.
from .fetch import fetch as fetch
from .list_tables import list_tables as list_tables
from .info import info as info
from .filter import filter_tables as filter_tables
from .filter_info import get_filters as get_filters, describe_filter as describe_filter

# No public exports - all functions are internal
__all__ = []
