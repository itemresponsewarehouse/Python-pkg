"""Centralized caching utilities for IRW metadata."""

from typing import Dict, Any, Optional


class MetadataCache:
    """Centralized cache for metadata tables with version checking."""
    
    def __init__(self):
        self._cache: Dict[str, Any] = {}
        self._versions: Dict[str, str] = {}
    
    def get(self, key: str, version: Optional[str] = None) -> Optional[Any]:
        """Get cached data if version matches."""
        if version and self._versions.get(key) == version:
            return self._cache.get(key)
        return None
    
    def set(self, key: str, data: Any, version: Optional[str] = None) -> None:
        """Cache data with version."""
        self._cache[key] = data
        if version:
            self._versions[key] = version
    
    def clear(self) -> None:
        """Clear all cached data."""
        self._cache.clear()
        self._versions.clear()


# Global metadata cache instance
metadata_cache = MetadataCache()
