'''source="main" is an accepted spelling of source="core" (#138).

R calls the response-data family "core" (`.irw_sources`), and so do the site's
`source =` column, the family figures and llms.txt. Python called it "main"
until this change, so a user copying from the site got a ValueError. "core" is
now the canonical spelling here too and "main" is kept working.

The important rule is not that both spellings are accepted but that only one of
them is ever *seen*: `source` reaches a dozen `== "core"` comparisons below the
public API, one of them a metadata cache key, and a second spelling arriving
there would key identical data twice and let the copies drift apart inside a
session. So the alias is resolved once, at the boundary, and
test_every_public_source_argument_is_normalised is the guard that keeps it
that way when a new public function is added.
'''
import ast
import inspect
import pathlib

import pandas as pd
import pytest

import irw
import irw.api as api
import irw.operations.filter_info as filter_info
import irw.utils.redivis.table_metadata as tm
from irw.config import SOURCES, SOURCE_ALIASES, normalize_source


# --- the spellings themselves ---------------------------------------------

def test_core_is_canonical_and_main_is_an_alias():
    assert "core" in SOURCES and "main" not in SOURCES
    assert SOURCE_ALIASES["main"] == "core"
    assert normalize_source("main") == "core"
    assert normalize_source("core") == "core"


def test_the_r_package_spellings_are_all_accepted():
    """Rpkg's .irw_sources, so a call copied from R or from the site works."""
    for source in ("core", "nom", "comp", "conj", "rank", "sim"):
        assert normalize_source(source) in SOURCES


def test_an_unknown_source_is_returned_for_the_caller_to_reject():
    """normalize_source() does not validate -- each caller owns its message."""
    assert normalize_source("nope") == "nope"


# --- the guard: nothing below the boundary sees an alias -------------------

def _public_source_functions():
    """Every public api.py function with a `source` parameter."""
    for name, obj in vars(api).items():
        if name.startswith("_") or not inspect.isfunction(obj):
            continue
        if obj.__module__ != "irw.api":
            continue
        if "source" in inspect.signature(obj).parameters:
            yield name, obj


def test_there_are_public_functions_to_check():
    """A guard on the guard: a broken discovery would vacuously pass below."""
    found = {name for name, _ in _public_source_functions()}
    assert {"list_tables", "metadata", "filter", "info", "fetch", "table_sets",
            "save_bibtex", "get_filters", "describe_filter"} <= found


@pytest.mark.parametrize("name", sorted(n for n, _ in _public_source_functions()))
def test_every_public_source_argument_is_normalised(name):
    """`source = normalize_source(source)` in the body, not `or == "main"` below.

    A public function that takes `source` and skips this passes it on raw, and
    the failure is silent: a second cache namespace, or a branch that quietly
    takes the not-core path. Adding one is the way this regresses, so the test
    discovers the functions rather than listing them.
    """
    func = dict(_public_source_functions())[name]
    tree = ast.parse(inspect.getsource(func).lstrip())
    normalised = [
        node for node in ast.walk(tree)
        if isinstance(node, ast.Assign)
        and any(isinstance(t, ast.Name) and t.id == "source" for t in node.targets)
        and isinstance(node.value, ast.Call)
        and getattr(node.value.func, "id", None) == "normalize_source"
    ]
    assert normalised, (
        f"irw.{name}() takes `source` but never calls normalize_source() on it"
    )


def test_no_module_but_config_holds_a_main_string_literal():
    """The alias is named in one place: SOURCE_ALIASES.

    An AST sweep rather than a grep, so the prose that explains the alias does
    not count and a real comparison cannot hide in a docstring. The cost of
    missing one is a branch that silently takes the not-core path -- the
    `source != "core"` in source_note(), for instance, would drop a
    collection's citation without raising.
    """
    root = pathlib.Path(api.__file__).parent
    offenders = []
    for path in sorted(root.rglob("*.py")):
        if path.name == "config.py":
            continue
        tree = ast.parse(path.read_text())
        docstrings = {
            ast.get_docstring(node, clean=False)
            for node in ast.walk(tree)
            if isinstance(node, (ast.Module, ast.ClassDef,
                                 ast.FunctionDef, ast.AsyncFunctionDef))
        }
        exported = {
            node.lineno
            for node in ast.walk(tree)
            if isinstance(node, ast.Assign)
            and any(isinstance(t, ast.Name) and t.id == "__all__"
                    for t in node.targets)
        }
        for node in ast.walk(tree):
            if not (isinstance(node, ast.Constant) and node.value == "main"):
                continue
            if node.value in docstrings:
                continue
            # mcp.py exports its CLI entry point by name in __all__.
            if any(abs(node.lineno - line) < 40 for line in exported):
                continue
            offenders.append(f"{path.relative_to(root)}:{node.lineno}")
    assert not offenders, (
        "a 'main' string below the boundary: " + ", ".join(offenders))


# --- the public functions, both spellings ---------------------------------

@pytest.fixture
def seen(monkeypatch):
    """Record the source each layer below api.py is handed."""
    calls = []

    def frame(source="core"):
        calls.append(source)
        return pd.DataFrame({"table": ["t"], "name": ["t"]})

    monkeypatch.setattr(api, "_get_datasets", lambda source="core": calls.append(source) or [])
    monkeypatch.setattr(api, "_get_metadata_table", frame)
    monkeypatch.setattr(api, "filter_tables",
                        lambda datasets, source="core", **kw: calls.append(source) or pd.Series(["t"]))
    monkeypatch.setattr(api, "_table_sets",
                        lambda datasets, name, *, source="core", **kw: calls.append(source) or {})
    monkeypatch.setattr(api, "_get_filters_func", lambda source="core": calls.append(source) or [])
    monkeypatch.setattr(api, "_describe_filter",
                        lambda datasets, filter_name, source="core": calls.append(source) or {})
    monkeypatch.setattr(api, "_get_table_info_dict",
                        lambda table_name, source="core": calls.append(source) or {"name": table_name})
    monkeypatch.setattr(api, "_format_table_info", lambda table_name, info_dict: "")
    monkeypatch.setattr(tm, "get_biblio_table", frame)
    monkeypatch.setattr(tm, "_known_table_names", lambda source="core": calls.append(source) or {"t"})
    return calls


@pytest.mark.parametrize("call", [
    lambda: irw.list_tables(source="main"),
    lambda: irw.metadata(source="main"),
    lambda: irw.filter(source="main", n_items=5),
    lambda: irw.info("t", source="main"),
    lambda: irw.table_sets("t", source="main"),
    lambda: irw.save_bibtex("t", source="main"),
    lambda: irw.get_filters(source="main"),
    lambda: irw.describe_filter("n_items", source="main"),
], ids=["list_tables", "metadata", "filter", "info", "table_sets",
        "save_bibtex", "get_filters", "describe_filter"])
def test_main_reaches_the_layers_below_as_core(seen, call):
    call()
    assert seen, "nothing below api.py was reached, so nothing was checked"
    assert set(seen) == {"core"}


def test_an_unknown_source_still_raises_and_names_the_canonical_sources():
    """The message teaches "core", not the alias.

    "main" is kept working so that existing code does not break; a user who
    has mistyped a source should be pointed at the spelling the site, R and
    this package all document, and at nothing else.
    """
    with pytest.raises(ValueError, match="Unknown source") as error:
        irw.list_tables(source="nope")
    message = str(error.value)
    assert "'core'" in message and "'main'" not in message


def test_the_message_for_an_unknown_source_reaches_the_metadata_gate():
    with pytest.raises(ValueError, match="Unknown source") as error:
        tm._check_source("nope")
    assert "'core'" in str(error.value) and "'main'" not in str(error.value)


def test_get_filters_accepts_both_spellings():
    """An end-to-end path with no Redivis call in it, so no mocks.

    Through `irw.get_filters`, not `filter_info.get_filters`: the alias is the
    public API's contract, and the module below it rejects "main" by design.
    """
    assert irw.get_filters("main") == irw.get_filters("core")
    assert "collection" in irw.get_filters("main")
    with pytest.raises(ValueError, match="Unknown source"):
        filter_info.get_filters("main")


# --- the cache ------------------------------------------------------------

def test_the_metadata_cache_is_hit_not_duplicated_by_the_two_spellings(monkeypatch):
    """One fetch for two spellings, and one cache entry, not two.

    _meta_cache_key() gives core the bare key ("metadata") and every other
    source a suffixed one, so an unnormalised "main" would store identical
    rows under "metadata:main" as well -- a second fetch, and two copies able
    to disagree for the rest of the session.
    """
    reads = []

    class FakeTable:
        def __init__(self, name):
            self.name = name

        def to_pandas_dataframe(self, **kwargs):
            reads.append(self.name)
            return pd.DataFrame({"table": ["t"], "n_items": [1]})

    class FakeDataset:
        def table(self, name):
            return FakeTable(name)

    monkeypatch.setattr(tm, "_get_meta_dataset", lambda: FakeDataset())
    monkeypatch.setattr(tm, "_cache_version", lambda *args: "v1")
    monkeypatch.setattr(tm, "_pins_fingerprint", lambda: "")
    tm.metadata_cache.clear()

    first = irw.metadata(source="main")
    second = irw.metadata(source="core")

    assert reads == ["metadata"], f"fetched more than once: {reads}"
    assert list(first["table"]) == list(second["table"])
    keys = list(tm.metadata_cache._cache)
    assert "metadata" in keys and "metadata:main" not in keys, keys


def test_the_two_spellings_do_not_split_the_mcp_filter_cache():
    from irw.mcp import IRWTools

    assert IRWTools._validate_source("main") == "core"
    assert IRWTools._validate_source("core") == "core"
    with pytest.raises(Exception, match="source must be one of"):
        IRWTools._validate_source("nope")


def test_the_mcp_default_source_is_the_canonical_spelling():
    from irw import mcp

    assert mcp.SOURCE == "core"
