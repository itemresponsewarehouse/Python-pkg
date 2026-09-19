"""Public controls for the on-disk table cache (see utils/redivis/disk_cache.py)."""

from __future__ import annotations

from typing import Iterable, Optional, Union

import pandas as pd

from ..utils.redivis import disk_cache


def cache_dir() -> str:
    """Folder where fetched tables are cached.

    ``IRW_CACHE_DIR`` if set, otherwise the platform's user cache folder
    (``~/.cache/irw`` on Linux, ``~/Library/Caches/irw`` on macOS,
    ``%LOCALAPPDATA%\\irw\\Cache`` on Windows). The R package uses the same
    folder, so a table fetched in either language is reused by the other.

    Returns
    -------
    str
    """
    return str(disk_cache.cache_root())


def cache_info() -> pd.DataFrame:
    """List the cached tables.

    `fetch()` and `itemtext()` keep each table they download so that later
    sessions do not export it again -- every export counts against the
    Redivis 30-day export cap. A copy is used for as long as the table is
    unchanged on Redivis, and replaced when it changes.

    Returns
    -------
    pd.DataFrame
        One row per cached file: ``kind`` (``tables`` or ``itemtext``),
        ``table``, ``hash`` (Redivis' content hash, which is what decides
        whether the copy is current), ``version`` (the dataset release it was
        fetched from), ``reference``, ``size_mb``, ``fetched_at`` and ``path``.

    Examples
    --------
    >>> info = irw.cache_info()
    >>> info["size_mb"].sum()
    """
    return disk_cache.info()


def clear_cache(table: Union[str, Iterable[str], None] = None) -> int:
    """Delete cached tables.

    Parameters
    ----------
    table : str or iterable of str, optional
        Tables to remove, matched case-insensitively; a table's item text goes
        with it. By default everything is removed.

    Returns
    -------
    int
        Bytes freed.
    """
    return disk_cache.clear(table)


def set_cache(enabled: Optional[bool]) -> None:
    """Switch the on-disk cache on or off for this session.

    It is on by default. ``IRW_CACHE=0`` switches it off for every session;
    this call overrides that until the interpreter exits, and
    ``set_cache(None)`` goes back to the environment setting. Switching it off
    leaves existing files in place -- use `clear_cache()` to remove them.

    Parameters
    ----------
    enabled : bool or None
    """
    disk_cache.set_enabled(enabled)
