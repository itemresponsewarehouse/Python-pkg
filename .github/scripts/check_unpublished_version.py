"""Assert that the version `main` declares has not already been published.

`check_release_version.py` makes the two literals and the tag agree. It cannot
catch the other half of the same failure: literals that agree with each other
*and* with a version already on PyPI. That is the state `main` sat in between
2026-10-18 and 2026-10-26 (Python-pkg#125) -- declared 0.7.7, published 0.7.7,
several unreleased commits in between -- and it is worse than a mismatch,
because nothing looks wrong. `pip install git+https://...` resolves 0.7.7, finds
0.7.7 installed, skips even under `--upgrade`, and reports success. Anyone told
to install from the repo to get a fix gets the release instead.

So the invariant is: the number `main` declares is a number pip can tell apart
from anything a user may already have. Between releases that means a development
suffix (0.8.0.dev0); during a release PR it means the number being published,
which is not on PyPI yet, so the check passes there too and needs no exception.

Usage:
    python .github/scripts/check_unpublished_version.py

Network: reads PyPI's simple index. A network failure SKIPS rather than fails --
this is a guard against a stale number, not a test of the code, and a CI job
that cannot reach PyPI should not block a merge. A failure here is always real:
it means the version exists.
"""

import json
import pathlib
import re
import sys
import urllib.error
import urllib.request

ROOT = pathlib.Path(__file__).resolve().parents[2]
PYPROJECT = ROOT / "pyproject.toml"
# The JSON API lags a minute or so behind a publish; the simple index does not,
# and it is the one pip itself resolves against.
SIMPLE_INDEX = "https://pypi.org/simple/irw/"
TIMEOUT = 20


def declared_version() -> str:
    text = PYPROJECT.read_text()
    project = text.split("[project]", 1)[1]
    match = re.search(r'^version\s*=\s*"([^"]+)"', project, re.MULTILINE)
    if not match:
        sys.exit(f"no version found under [project] in {PYPROJECT}")
    return match.group(1)


def published_versions() -> set:
    request = urllib.request.Request(
        SIMPLE_INDEX,
        headers={"Accept": "application/vnd.pypi.simple.v1+json"},
    )
    with urllib.request.urlopen(request, timeout=TIMEOUT) as response:
        payload = json.load(response)
    # `versions` is PEP 700, present on pypi.org. Fall back to the filenames,
    # which are what the index has always carried.
    versions = payload.get("versions")
    if versions:
        return set(versions)
    found = set()
    for entry in payload.get("files", []):
        name = entry.get("filename", "")
        match = re.match(r"^irw-([^-]+)(?:-|\.tar\.gz$)", name)
        if match:
            found.add(match.group(1))
    return found


def main() -> None:
    version = declared_version()
    try:
        published = published_versions()
    except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as err:
        print(f"SKIP: could not read {SIMPLE_INDEX} ({err}); not blocking on it.")
        return

    if not published:
        print(f"SKIP: {SIMPLE_INDEX} listed no versions; not blocking on it.")
        return

    if version in published:
        sys.exit(
            f"pyproject.toml declares {version}, which is already on PyPI.\n"
            f"pip cannot tell this tree apart from the release: "
            f"`pip install git+https://...` resolves {version}, finds it "
            f"installed and skips, even under `--upgrade`.\n"
            f"Bump to a development version (e.g. a .devN suffix on the next "
            f"release's number) in pyproject.toml and src/irw/config.py.\n"
            f"If this IS the release PR for {version}, it has been published "
            f"already -- PyPI numbers are burn-once, so pick the next one."
        )

    print(f"OK: {version} is not on PyPI ({len(published)} versions published).")


if __name__ == "__main__":
    main()
