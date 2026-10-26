"""The package version must be one number, and it must move when the code does.

Two separate literals used to disagree -- `irw.__version__` said 0.0.1 while
`irw.config.VERSION` and pyproject said 0.0.2 -- so a user could not tell which
build they had.

That matters more than tidiness, and it did not stop mattering when the package
reached PyPI on 2026-09-07. Two reasons it still bites:

1. The development install is still

       pip install "git+https://github.com/itemresponsewarehouse/Python-pkg.git"

   and pip re-clones, resolves the version, sees that version already installed
   and SKIPS -- even with --upgrade. So a fix that lands without a version bump
   leaves everyone who installed earlier on the broken code. That is exactly what
   happened with the metadata reference-id fix (#14): `filter()` had been
   returning the entire catalogue, and re-running the documented install line
   would not have replaced it.

2. PyPI resolves by the pyproject version too, and a version number can never be
   re-uploaded once used. A release cut against disagreeing literals cannot be
   corrected in place; it costs a whole version number.

`.github/scripts/check_release_version.py` enforces the same agreement at
release time, and adds the tag to it. This test is the half that runs on every
PR.
"""

import pathlib
import re

import irw
from irw.config import VERSION

PYPROJECT = pathlib.Path(__file__).resolve().parents[1] / "pyproject.toml"


def _pyproject_version() -> str:
    """Read [project] version without tomllib, which is 3.11+."""
    text = PYPROJECT.read_text()
    project = text.split("[project]", 1)[1]
    match = re.search(r'^version\s*=\s*"([^"]+)"', project, re.MULTILINE)
    assert match, "no version found under [project] in pyproject.toml"
    return match.group(1)


def test_dunder_version_is_config_version():
    assert irw.__version__ == VERSION


def test_config_version_matches_pyproject():
    assert VERSION == _pyproject_version(), (
        f"irw.config.VERSION is {VERSION} but pyproject.toml says "
        f"{_pyproject_version()}. pip installs by the pyproject version, so a "
        f"mismatch means the number users see is not the number pip resolves."
    )




def test_version_is_a_release_or_development_number():
    """`main` carries a `.devN` suffix between releases; a release PR does not.

    The failure this guards (#125) is not a mismatch between the two literals --
    `check_release_version.py` and the tests above cover that -- but both
    literals agreeing with a number already on PyPI. `pip install git+...` then
    resolves a version it already has, skips even under `--upgrade`, and reports
    success, so someone told to install from the repo to get a fix gets the
    release. Between 2026-10-18 and 2026-10-26 that was the real state of
    `main`: declared 0.7.7, published 0.7.7, several unreleased commits between.

    Whether the number is *published* needs the network, so
    `.github/scripts/check_unpublished_version.py` does that in CI. What can be
    asserted offline is that the version is one of the two shapes that invariant
    allows, which is what catches a hand-edit to something like `0.8.0-dev` that
    PEP 440 would normalise somewhere unexpected.
    """
    assert re.fullmatch(r"\d+\.\d+\.\d+(\.dev\d+)?", VERSION), (
        f"VERSION is {VERSION!r}; expected X.Y.Z for a release or "
        f"X.Y.Z.devN between releases"
    )
