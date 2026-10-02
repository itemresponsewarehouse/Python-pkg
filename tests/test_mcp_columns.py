"""describe_columns: what each column means, and where that is written (irw#2755)."""

from irw.mcp import IRWTools, _parse_data_standard
from test_mcp import FakeBackend, FakeSource

# The shape of datastandard.md's schema table, trimmed.
STANDARD_MD = """# Data standard

| Column | Required | Rules |
|--------|----------|-------|
| `id` | yes | Identifier for the focal unit. |
| `item` | yes | Item identifier. |
| `resp` | yes | Response value. |
| `cov_*` | no | Covariates that are invariant to the focal unit. |
| `treat` | no | Treatment group assignment. |
| `cluster_id` | no | The unit of random assignment. |
| `std_baseline`, `std_baseline_*` | no | A standardised pre-treatment score. |
| `qmatrix1`…`qmatrixN` | no | Q-matrix attributes. |

Prose that mentions `wave` outside the table.
"""

SCRIPT = """# Kim et al. 2021 replication data.
library(dplyr)
df <- read.csv("x.csv") |>
  select(id = s_id, cluster_id = teacher_name, cov_male = s_male_num, item, resp)
"""


class _Backend(FakeBackend):
    def __init__(self):
        super().__init__()
        self.info["alpha_depression"] = {
            "stats": {"variables": "id| item| resp| treat| cluster_id| cov_male| std_baseline_math| qmatrix3| mystery"},
            "biblio": {"url": "https://doi.org/10.1/example"},
        }


class _Source(FakeSource):
    def _fetch_text(self, url):
        if url.endswith("/datastandard.md"):
            self.calls.append(url)
            return STANDARD_MD
        if url.endswith("data/alpha_depression.py"):
            self.calls.append(url)
            return SCRIPT
        return super()._fetch_text(url)


def _columns(result):
    return {c["name"]: c for c in result["columns"]}


def test_parser_reads_exact_names_and_families_from_the_schema_table_only():
    exact, prefix = _parse_data_standard(STANDARD_MD)
    assert {"id", "item", "resp", "treat", "cluster_id", "std_baseline"} <= set(exact)
    assert "wave" not in exact  # prose outside the table is not a definition
    assert set(prefix) == {"cov_", "std_baseline_", "qmatrix"}
    assert "qmatrix1" not in exact


def test_each_column_says_where_its_meaning_is_written():
    result = IRWTools(_Backend(), _Source()).describe_columns("alpha_depression")
    cols = _columns(result)
    assert cols["cluster_id"]["defined_by"] == "standard"
    assert cols["cluster_id"]["definition"] == "The unit of random assignment."
    # The build script's rename is the evidence of which source column it was.
    assert "cluster_id = teacher_name" in cols["cluster_id"]["script_mentions"][0]["text"]
    assert cols["cov_male"]["defined_by"] == "standard_family"
    assert cols["cov_male"]["documented"] is True  # named in the script
    assert cols["std_baseline_math"]["defined_by"] == "standard_family"
    assert cols["qmatrix3"]["defined_by"] == "standard_family"
    assert result["codebook_url"] == "https://doi.org/10.1/example"


def test_an_undocumented_column_is_said_to_be_so_not_guessed():
    result = IRWTools(_Backend(), _Source()).describe_columns("alpha_depression")
    mystery = _columns(result)["mystery"]
    assert mystery == {"name": "mystery", "defined_by": None, "definition": None,
                       "script_mentions": [], "documented": False}
    assert any("documented=false" in w and "https://doi.org/10.1/example" in w
               for w in result["warnings"])


def test_a_family_alone_does_not_count_as_documented():
    backend = _Backend()
    backend.info["alpha_depression"]["stats"]["variables"] = "id| cov_unnamed"
    cols = _columns(IRWTools(backend, _Source()).describe_columns("alpha_depression"))
    assert cols["cov_unnamed"]["defined_by"] == "standard_family"
    assert cols["cov_unnamed"]["documented"] is False


def test_github_down_still_returns_the_columns_with_a_warning():
    result = IRWTools(_Backend(), FakeSource(fail=True)).describe_columns("alpha_depression")
    assert [c["name"] for c in result["columns"]][:3] == ["id", "item", "resp"]
    assert all(c["defined_by"] is None for c in result["columns"])
    assert any("data standard could not be loaded" in w for w in result["warnings"])
    assert any("Build scripts could not be searched" in w for w in result["warnings"])


def test_a_table_without_a_column_list_says_so():
    result = IRWTools(FakeBackend(), _Source()).describe_columns("alpha_depression")
    assert result["columns"] == []
    assert any("lists no columns" in w for w in result["warnings"])
