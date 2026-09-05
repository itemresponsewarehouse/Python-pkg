"""Offline regression tests for the two failures reported against irw_meta v20.0.

No network and no credentials.

1. META_TABLES must address tables by BARE NAME. Redivis remints a reference id
   for every table on every release, so a pinned `name:refid` resolves only
   inside the version it was copied from. When v20.0 landed, the v19.3 ids in
   config.py stopped resolving and every metadata-dependent call degraded --
   filter() returned the entire catalogue.

2. Filters must fail rather than skip themselves when their column is missing.
   That is what turned (1) from an error into a wrong answer.

3. Nothing in the package may use PEP 604 (`X | Y`) in a runtime-evaluated
   annotation without `from __future__ import annotations`: pyproject declares
   requires-python >=3.9 and PEP 604 is 3.10+.
"""

import ast
import pathlib

import pandas as pd
import pytest

from irw.config import META_TABLES
from irw.operations.filter import (
    IRWMetadataUnavailable,
    _apply_numeric_filter,
    _apply_tag_filter,
    _apply_variable_filter,
)

SRC = pathlib.Path(__file__).resolve().parents[1] / "src" / "irw"


def test_meta_tables_are_bare_names():
    for key, ref in META_TABLES.items():
        assert ":" not in ref, (
            f"META_TABLES['{key}'] = '{ref}' pins a Redivis reference id. "
            f"Reference ids are reminted on every release; use the bare name."
        )


@pytest.mark.parametrize(
    "apply_filter, column, value",
    [
        (_apply_numeric_filter, "n_responses", [0, 1e6]),
        (_apply_tag_filter, "construct_type", "Cognitive"),
        (_apply_variable_filter, "variables", "rt"),
    ],
)
def test_filter_raises_when_its_column_is_missing(apply_filter, column, value):
    """A names-only frame must not quietly satisfy every filter."""
    names_only = pd.DataFrame({"name": ["a", "b", "c"]})
    if apply_filter is _apply_variable_filter:
        args = (names_only, value)
    else:
        args = (names_only, column, value)
    with pytest.raises(IRWMetadataUnavailable, match=column):
        apply_filter(*args)


def test_filter_is_a_noop_only_when_no_criterion_was_given():
    names_only = pd.DataFrame({"name": ["a", "b", "c"]})
    assert len(_apply_numeric_filter(names_only, "n_responses", None)) == 3
    assert len(_apply_tag_filter(names_only, "license", None)) == 3


def _pep604_annotations(path: pathlib.Path) -> list[int]:
    """Line numbers of `X | Y` annotations evaluated at runtime in `path`."""
    tree = ast.parse(path.read_text())
    if any(
        isinstance(node, ast.ImportFrom)
        and node.module == "__future__"
        and any(alias.name == "annotations" for alias in node.names)
        for node in tree.body
    ):
        return []

    lines: list[int] = []
    for node in ast.walk(tree):
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        args = node.args
        annotations = [
            a.annotation
            for a in [*args.posonlyargs, *args.args, *args.kwonlyargs]
            if a.annotation is not None
        ]
        if node.returns is not None:
            annotations.append(node.returns)
        for annotation in annotations:
            for sub in ast.walk(annotation):
                if isinstance(sub, ast.BinOp) and isinstance(sub.op, ast.BitOr):
                    lines.append(sub.lineno)
    return sorted(set(lines))


def test_no_pep604_annotations_without_future_import():
    offenders = {
        str(path.relative_to(SRC)): lines
        for path in sorted(SRC.rglob("*.py"))
        if (lines := _pep604_annotations(path))
    }
    assert not offenders, (
        f"PEP 604 unions in runtime-evaluated annotations break `import irw` on "
        f"Python 3.9 (declared supported): {offenders}. Add "
        f"`from __future__ import annotations` to each file."
    )
