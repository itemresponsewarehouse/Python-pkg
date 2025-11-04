"""Centralized caching utilities for IRW metadata and datasets.

This module provides a global cache for all IRW resources:
- Datasets: main_datasets, sim_dataset, comp_dataset
- Metadata tables: metadata, tags, biblio, combined_metadata
- Item text: itemtext_tables, itemtext_dataset
- Metadata dataset: meta_dataset

All functions use lazy loading with caching to avoid redundant Redivis API calls.
"""

from typing import Dict, Any, Optional


class MetadataCache:
    """Centralized cache for IRW resources with version checking.
    
    All resources are cached here and reused across all functions.
    This prevents redundant loading from Redivis.
    """
    
    def __init__(self):
        self._cache: Dict[str, Any] = {}
        self._versions: Dict[str, str] = {}
    
    def get(self, key: str, version: Optional[str] = None) -> Optional[Any]:
        """Get cached data if version matches.
        
        If version is None, returns cached data if it exists (no version check).
        If version is provided, only returns data if version matches.
        """
        if version is None:
            return self._cache.get(key)
        if self._versions.get(key) == version:
            return self._cache.get(key)
        return None
    
    def set(self, key: str, data: Any, version: Optional[str] = None) -> None:
        """Cache data with optional version."""
        self._cache[key] = data
        if version:
            self._versions[key] = version
    
    def clear(self) -> None:
        """Clear all cached data."""
        self._cache.clear()
        self._versions.clear()


# Global cache instance - shared across all operations
metadata_cache = MetadataCache()
