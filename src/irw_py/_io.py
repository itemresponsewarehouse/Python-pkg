from __future__ import annotations
from typing import Any
import warnings
import redivis


def init_dataset(user: str, ds_ref: str):
    """Create a Redivis dataset handle and ensure metadata is loaded."""
    ds = redivis.user(user).dataset(ds_ref)
    ds.get()
    
    setattr(ds, "_user", user)
    setattr(ds, "_id", ds_ref)
    return ds


def get_table(ds: Any, name: str):
    """Get a table handle (suppressing the 'No reference id' warning) and ensure properties."""
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", message=".*No reference id was provided for the table.*")
        tbl = ds.table(name)
        try:
            tbl.get()  # populate properties if needed
        except Exception:
            pass
        return tbl


def classify_error(err: Exception) -> str:
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


def format_error(err: Exception | None) -> str:
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