"""The package version must be one number, and it must move when the code does.

Two separate literals used to disagree -- `irw.__version__` said 0.0.1 while
`irw.config.VERSION` and pyproject said 0.0.2 -- so a user could not tell which
build they had.

That matters more than tidiness. The briefing at itemresponsewarehouse.org
installs the package with:

    pip install "git+https://github.com/itemresponsewarehouse/Python-pkg.git"

pip re-clones, resolves the version, sees that version already installed and
SKIPS the install -- even with --upgrade. So when a fix lands without a version
bump, everyone who installed earlier silently keeps the broken code. That is
exactly what happened with the metadata reference-id fix (#14): `filter()` had
been returning the entire catalogue, and re-running the documented install line
would not have replaced it.
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


def test_version_is_a_plain_release_number():
    assert re.fullmatch(r"\d+\.\d+\.\d+", VERSION), VERSION
