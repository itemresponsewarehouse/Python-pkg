"""Internal Redivis table fetching utilities."""

import time
from typing import Any, Callable, List, Optional, Tuple, TypeVar
import warnings

T = TypeVar("T")

_TRANSIENT_ERROR_MARKERS = (
    "timeout",
    "temporar",
    "connection",
    "server error",
    "502",
    "503",
    "expected to be able to read",
    "message body",
    "incomplete read",
    "connection reset",
    "broken pipe",
    "chunkedencodingerror",
    "read timed out",
    "remotedisconnected",
    "protocolerror",
)


def _get_table(ds: Any, name: str) -> Any:
    """Get a table handle and ensure properties are loaded."""
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", message=".*No reference id was provided for the table.*")
        tbl = ds.table(name)
        try:
            tbl.get()  # populate properties if needed
        except Exception:
            pass
        return tbl


def _classify_error(err: Exception) -> str:
    """Classify Redivis-style errors for control flow."""
    payload = None
    try:
        if isinstance(err.args[0], dict):
            payload = err.args[0]
    except Exception:
        pass

    if payload:
        code = str(payload.get("error", "")).lower()
        if "invalid_request" in code:
            return "invalid_request"
        if "not_found" in code:
            return "not_found"

    msg = str(err).lower()
    if "not_found" in msg or "not found" in msg:
        return "not_found"
    if any(marker in msg for marker in _TRANSIENT_ERROR_MARKERS):
        return "transient"
    return "unknown"


def _retry_transient(
    callback: Callable[[], T],
    *,
    max_attempts: int = 3,
    base_delay: float = 1.0,
) -> T:
    """Retry callback on transient Redivis/network read failures."""
    last_err: Exception | None = None
    for attempt in range(max_attempts):
        try:
            return callback()
        except Exception as e:
            last_err = e
            if _classify_error(e) != "transient" or attempt == max_attempts - 1:
                raise
            time.sleep(base_delay * (2 ** attempt))
    if last_err is not None:
        raise last_err
    raise RuntimeError("retry loop exited without result")


def _format_error(err: Exception | None) -> str:
    """Pretty-print Redivis JSON error payloads when available."""
    if err is None:
        return "not found"
    try:
        payload = err.args[0]
        if isinstance(payload, dict):
            body = {k: payload.get(k) for k in ("status", "error", "error_description") if k in payload}
            if body:
                return str(body)
        return str(payload)
    except Exception:
        return str(err)


def _search_datasets(
    datasets: List[Any],
    callback: Callable[[Any], T],
) -> Tuple[Optional[T], Optional[Exception], Optional[Exception]]:
    """
    Try callback(ds) for each Redivis dataset, in order, until one succeeds.

    Returns (result, last_other_error, invalid_request_error).
    On success, result is set and both errors are None.
    When every dataset reports not-found, all three return values are None.
    """
    last_other: Exception | None = None
    for ds in datasets:
        try:
            return callback(ds), None, None
        except Exception as e:
            kind = _classify_error(e)
            if kind == "invalid_request":
                return None, None, e
            if kind == "not_found":
                continue
            last_other = e
    return None, last_other, None
