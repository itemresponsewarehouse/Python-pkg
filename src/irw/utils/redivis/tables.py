"""Internal Redivis table fetching utilities."""

from typing import Any, Callable, List, Optional, Tuple, TypeVar
import warnings

T = TypeVar("T")


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
    if any(k in msg for k in ("timeout", "temporar", "connection", "server error", "502", "503")):
        return "transient"
    return "unknown"


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
