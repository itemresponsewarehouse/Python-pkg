"""Internal Redivis table fetching utilities."""

from typing import Any, Optional
import warnings


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
