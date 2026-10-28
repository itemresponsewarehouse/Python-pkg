"""Whether this session can reach Redivis without a browser login.

The Redivis Python SDK sends an ``Authorization: Bearer`` header on every
request, reads included, so unlike the R client there is no anonymous path
here: an account is needed even though IRW itself is public. When no
credential is on hand the SDK falls back to an interactive
device-authorization flow that prints a URL and polls for up to ten minutes.
In a script, a cron job or a CI runner nobody reads that URL, so the symptom
is a process that produces no output and has to be killed -- which is how the
review found it (ben-domingue/irw#3221, R-22).

So the absence of a credential is reported as an error here rather than waited
on, and ``has_credentials()`` is the predicate a caller can branch on first.
It is deliberately the same surface as R's ``irw_has_credentials()`` plus
``.irw_redivis_credentials_available()`` in ``Rpkg/R/redivis-errors.R``, so
code can ask the same question of either client. One thing neither can
promise: this proves a credential is PRESENT, never that Redivis will accept
it. A ``True`` here and a 401 on the next call is a possible pair.

The MCP server has needed this logic since #2157 and had its own copy; that
copy now lives here, and ``mcp.py`` reads the same state so the two cannot
drift.
"""

import json
import os
import sys
import time
from pathlib import Path
from typing import Optional, Tuple

def credentials_path() -> Path:
    """Where the SDK caches an interactive login.

    Resolved per call rather than at import, because ``Path.home()`` is what
    tests patch to point at a temporary directory -- a module-level constant
    would freeze the real home directory at import time and ignore them. The R
    client's cache sits beside this one as ``r_credentials``, which is why
    neither client's login satisfies the other.
    """
    return Path.home() / ".redivis" / "python_credentials"

#: How to get credentials. Worded once, appended to whatever was wrong.
SETUP_HOWTO = (
    "Authenticate once in an interactive session with "
    '`python -c "import irw; irw.list_tables()"` and the login is cached in '
    "~/.redivis for later scripts. For a server or CI job with no browser, set "
    "the REDIVIS_API_TOKEN environment variable to a Redivis API token instead."
)

MISSING_MESSAGE = (
    "No Redivis credentials found: neither REDIVIS_API_TOKEN nor a cached "
    "~/.redivis/python_credentials. " + SETUP_HOWTO
)

EXPIRED_MESSAGE = (
    "Cached Redivis credentials at ~/.redivis/python_credentials have expired. "
    "A refresh needs a token the SDK may not have, and its fallback is the "
    "browser login, which cannot complete here. " + SETUP_HOWTO
)

UNREADABLE_MESSAGE = (
    "Cached Redivis credentials at ~/.redivis/python_credentials could not be "
    "read as JSON. The SDK treats an unreadable file as absent and falls back "
    "to the browser login, so it is reported here instead. Delete the file and "
    "re-authenticate. " + SETUP_HOWTO
)


class IRWCredentialsUnavailable(RuntimeError):
    """Raised when a Redivis call would block on a login that cannot complete.

    Carries ``state``, one of ``"missing"``, ``"expired"`` or ``"unreadable"``,
    so a caller can tell a first-time setup from a stale cache without parsing
    the message.
    """

    def __init__(self, message: str, state: str):
        super().__init__(message)
        self.state = state


def credential_state() -> Tuple[str, Optional[str]]:
    """Classify this session's Redivis credentials without contacting Redivis.

    Returns
    -------
    tuple of (str, str or None)
        ``("ok", None)`` when a token or a usable cached login is on hand,
        otherwise ``("missing" | "expired" | "unreadable", message)``.

    Notes
    -----
    The file is inspected and never repaired: expiry is the SDK's business.
    Checking only that it EXISTS is what #2157 found to be too weak -- a stale
    file passed and the failure resurfaced as an opaque 401 from inside the
    SDK.
    """
    if os.getenv("REDIVIS_API_TOKEN"):
        return "ok", None
    path = credentials_path()
    if not path.is_file():
        return "missing", MISSING_MESSAGE
    try:
        credentials = json.loads(path.read_text())
    except (OSError, ValueError):
        return "unreadable", UNREADABLE_MESSAGE
    if not isinstance(credentials, dict) or "access_token" not in credentials:
        return "unreadable", UNREADABLE_MESSAGE

    expires_at = credentials.get("expires_at")
    if not isinstance(expires_at, (int, float)):
        # No expiry recorded. The SDK's own guard wants `expires_at` before it
        # will trust the cache, so match it rather than guess the token is good.
        return "ok", None
    if expires_at >= time.time():
        return "ok", None
    # Expired. The SDK may still refresh it on the next call, but only where a
    # refresh token is present, and it clears the file and reverts to the
    # browser login when the exchange fails. Report the stale credential rather
    # than start a call that cannot recover. The window is real rather than
    # theoretical: the SDK will not attempt the refresh until five minutes
    # AFTER expiry (`expires_at < time.time() - 5 * 60` in redivis
    # `common/auth.py`).
    return "expired", EXPIRED_MESSAGE


def has_credentials() -> bool:
    """Whether irw can reach Redivis without opening a browser login."""
    return credential_state()[0] == "ok"


def _is_interactive() -> bool:
    """Whether a browser login could plausibly be completed by a human here.

    A TTY on stdin is the test, which is as close as Python gets to R's
    ``interactive()``. It is deliberately conservative in the one direction
    that matters: a session wrongly judged interactive waits for a login that
    someone may be there to finish, whereas a session wrongly judged
    non-interactive raises where waiting would have worked. So the gate only
    fires where nothing could read the SDK's URL anyway.
    """
    try:
        return bool(sys.stdin is not None and sys.stdin.isatty())
    except (AttributeError, ValueError):
        # A closed or replaced stdin (pytest's capture, a daemonised process)
        # cannot carry a login either.
        return False


def require_credentials() -> None:
    """Stop before a Redivis call that would block on an impossible login.

    Does nothing when credentials are on hand, and nothing in an interactive
    session, where the SDK's browser login is the right behaviour and the user
    is there to complete it. Otherwise raises
    :class:`IRWCredentialsUnavailable` naming what is wrong and how to fix it.

    Set ``IRW_ALLOW_INTERACTIVE_LOGIN=1`` to skip the gate -- for a script that
    genuinely wants to sit on the device-authorization prompt, e.g. one being
    run by hand through a pipe.
    """
    if os.getenv("IRW_ALLOW_INTERACTIVE_LOGIN"):
        return
    state, message = credential_state()
    if state == "ok":
        return
    if _is_interactive():
        return
    raise IRWCredentialsUnavailable(
        message + "\n(Set IRW_ALLOW_INTERACTIVE_LOGIN=1 to attempt the browser "
        "login anyway.)",
        state,
    )
