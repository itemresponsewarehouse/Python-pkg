"""irw.validate() dispatches to irw_validate, and says so when it is absent.

Issue #27 asks for a way to check a table from Python before depositing it.
The checks themselves are not reimplemented here -- there is one
implementation, in the `irw_validate` package, and a copy would recreate the
fork that package exists to end. So what needs testing is the wrapper: that it
routes frames and paths to the right entry point, that it does not invent
profiles, and above all that a missing optional dependency produces a sentence
naming the install command rather than an ImportError for a module the user has
never heard of.

No network, and no dependency on `irw_validate` actually being installed:
every test installs a stand-in in sys.modules.
"""

import builtins
import sys
import types

import pandas as pd
import pytest

import irw.operations.validate  # noqa: F401  (registers the module)

# `operations/__init__.py` re-exports the FUNCTION under the module's own name,
# following the convention every other operation there uses, so
# `irw.operations.validate` is the function rather than the module. Reach the
# module through sys.modules instead of fighting that.
validate_module = sys.modules["irw.operations.validate"]


class _FakeReport:
    def __init__(self, label, profile, kind):
        self.label = label
        self.profile = profile
        self.kind = kind
        self.ok = True


def _fake_validator(profiles=("core", "triage", "upload", "legacy")):
    """A stand-in for the irw_validate package, recording how it was called."""
    module = types.ModuleType("irw_validate")
    module.PROFILES = profiles
    module.calls = []

    def validate_frame(df, label=None, profile="upload"):
        module.calls.append(("frame", df, label, profile))
        return _FakeReport(label, profile, "frame")

    def validate_file(path, profile="upload", label=None):
        module.calls.append(("file", path, label, profile))
        return _FakeReport(label or str(path), profile, "file")

    module.validate_frame = validate_frame
    module.validate_file = validate_file
    return module


@pytest.fixture
def installed(monkeypatch):
    module = _fake_validator()
    monkeypatch.setitem(sys.modules, "irw_validate", module)
    return module


@pytest.fixture
def not_installed(monkeypatch):
    """Make `import irw_validate` fail the way an uninstalled package does."""
    monkeypatch.delitem(sys.modules, "irw_validate", raising=False)
    real_import = builtins.__import__

    def fake_import(name, *args, **kwargs):
        if name == "irw_validate":
            raise ImportError("No module named 'irw_validate'")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr("builtins.__import__", fake_import)


# --- the missing-dependency path -----------------------------------------

def test_a_missing_validator_names_the_install_command(not_installed):
    with pytest.raises(ImportError) as caught:
        validate_module.validate(pd.DataFrame({"id": [1]}))
    message = str(caught.value)
    assert "pip install irw-validate" in message
    # The reason it is not a hard dependency, so the user can tell this is a
    # deliberate split rather than a broken install.
    assert "read client" in message or "never deposit" in message


def test_the_original_importerror_is_chained(not_installed):
    with pytest.raises(ImportError) as caught:
        validate_module.validate("table.csv")
    assert isinstance(caught.value.__cause__, ImportError)


# --- dispatch -------------------------------------------------------------

def test_a_dataframe_goes_to_validate_frame(installed):
    df = pd.DataFrame({"id": [1, 2], "item": ["a", "b"], "resp": [0, 1]})
    report = validate_module.validate(df)
    kind, passed, label, profile = installed.calls[0]
    assert kind == "frame"
    assert passed is df
    assert label == "<DataFrame>"
    assert profile == "upload"
    assert report.kind == "frame"


def test_a_path_goes_to_validate_file(installed):
    validate_module.validate("out/my_table.csv")
    kind, passed, label, profile = installed.calls[0]
    assert kind == "file"
    assert passed == "out/my_table.csv"
    assert label is None


def test_a_pathlib_path_is_not_treated_as_a_frame(installed):
    import pathlib
    validate_module.validate(pathlib.Path("out/my_table.csv"))
    assert installed.calls[0][0] == "file"


def test_an_explicit_label_is_passed_through(installed):
    validate_module.validate("tmp.csv", label="my_table")
    assert installed.calls[0][2] == "my_table"
    validate_module.validate(pd.DataFrame({"id": [1]}), label="my_table")
    assert installed.calls[1][2] == "my_table"


def test_the_profile_is_passed_through(installed):
    validate_module.validate("tmp.csv", profile="core")
    assert installed.calls[0][3] == "core"


# --- profiles -------------------------------------------------------------

def test_an_unknown_profile_is_refused_before_the_file_is_read(installed):
    with pytest.raises(ValueError, match="Unknown profile"):
        validate_module.validate("tmp.csv", profile="strict")
    assert installed.calls == []


def test_profiles_come_from_the_installed_package_not_a_local_list(monkeypatch):
    """A profile added in irw_validate must not need a release here."""
    module = _fake_validator(profiles=("core", "upload", "brand_new"))
    monkeypatch.setitem(sys.modules, "irw_validate", module)
    validate_module.validate("tmp.csv", profile="brand_new")
    assert module.calls[0][3] == "brand_new"


def test_a_validator_without_PROFILES_still_works(monkeypatch):
    """Do not require an attribute of the other package to exist."""
    module = _fake_validator()
    del module.PROFILES
    monkeypatch.setitem(sys.modules, "irw_validate", module)
    assert validate_module.validate("tmp.csv").kind == "file"


# --- the export -----------------------------------------------------------

def test_validate_is_exported():
    import irw
    assert "validate" in irw.__all__
    assert irw.validate is validate_module.validate
