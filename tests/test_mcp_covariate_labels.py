"""describe_table carries covariate value labels (ben-domingue/irw#1775)."""

import asyncio
import json

import pandas as pd
import pytest

from irw import mcp as irw_mcp
from irw.mcp import IRWTools, create_server
from test_mcp import FakeBackend, FakeSource


ROWS = pd.DataFrame({
    "table": ["alpha_depression"] * 3,
    "covariate": ["cov_gender", "cov_gender", "cov_school"],
    "code": ["1", "2", "1"],
    "label": ["female", "male", "[institution name withheld]"],
})


class LabelBackend(FakeBackend):
    def __init__(self, rows):
        super().__init__()
        self.rows = rows

    def covariate_labels(self, table_name):
        if isinstance(self.rows, Exception):
            raise self.rows
        return self.rows


def test_describe_table_maps_covariate_codes_to_labels():
    result = IRWTools(LabelBackend(ROWS), FakeSource()).describe_table("alpha_depression")
    assert result["covariate_labels"] == {
        "cov_gender": {"1": "female", "2": "male"},
        "cov_school": {"1": "[institution name withheld]"},
    }
    assert any("verbatim and per table" in w for w in result["warnings"])
    json.dumps(result, allow_nan=False)


def test_no_rows_is_an_empty_map_without_the_caveat():
    result = IRWTools(LabelBackend(ROWS.iloc[0:0]), FakeSource()).describe_table("alpha_depression")
    assert result["covariate_labels"] == {}
    assert not any("verbatim" in w for w in result["warnings"])


def test_absent_irw_meta_table_is_null_with_a_warning_not_a_failure():
    # PackageBackend.covariate_labels returns None for CovariateLabelsUnavailable.
    result = IRWTools(LabelBackend(None), FakeSource()).describe_table("alpha_depression")
    assert result["covariate_labels"] is None
    assert result["metadata"]["stats"]["n_responses"] == 100
    assert any("no covariate_labels table" in w for w in result["warnings"])


def test_a_failed_label_read_does_not_fail_describe_table():
    backend = LabelBackend(OSError("simulated arrow failure"))
    result = IRWTools(backend, FakeSource()).describe_table("alpha_depression")
    assert result["covariate_labels"] is None
    assert any("could not be loaded" in w for w in result["warnings"])


def test_backend_without_the_method_still_describes():
    result = IRWTools(FakeBackend(), FakeSource()).describe_table("alpha_depression")
    assert result["covariate_labels"] is None


def test_labels_are_bounded(monkeypatch):
    monkeypatch.setattr(irw_mcp, "COVARIATE_LABELS_MAX_CODES", 2)
    result = IRWTools(LabelBackend(ROWS), FakeSource()).describe_table("alpha_depression")
    assert sum(len(v) for v in result["covariate_labels"].values()) == 2
    assert any("truncated" in w for w in result["warnings"])


def test_package_backend_turns_an_absent_table_into_none(monkeypatch):
    from irw.utils.redivis.table_metadata import CovariateLabelsUnavailable

    def absent(table):
        raise CovariateLabelsUnavailable("no table")

    monkeypatch.setattr(irw_mcp.irw, "covariate_labels", absent)
    assert irw_mcp.PackageBackend().covariate_labels("x") is None


def test_wire_schema_accepts_the_labels():
    pytest.importorskip("mcp")
    from mcp import Client

    async def check():
        async with Client(create_server(LabelBackend(ROWS), FakeSource())) as client:
            result = await client.call_tool(
                "describe_table", {"table_name": "alpha_depression"}
            )
            assert not result.is_error
            return result.structured_content["result"]
    payload = asyncio.run(check())
    assert payload["covariate_labels"]["cov_gender"] == {"1": "female", "2": "male"}
