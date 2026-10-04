"""describe_columns: what each column means, and where that is written (irw#2755)."""

import pandas as pd

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
                       "basis": None, "source_column": None, "script_mentions": [],
                       "value_labels": None, "documented": False}
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


# --- column_docs.csv (irw#2763 step 4): the table pages' own rows ------------

# What stage 14 writes for the fixture table. `cov_male` is renamed, `treat`
# built, `mystery` untraced; `match` is the script's for the whole table.
COLUMN_DOCS_CSV = (
    "table,column,defined_by,basis,source_column,script,script_line,match,documented\n"
    "alpha_depression,id,standard,renamed,s_id,data/alpha_depression.py,4,exact,true\n"
    "alpha_depression,item,standard,,,,,exact,true\n"
    "alpha_depression,resp,standard,,,,,exact,true\n"
    "alpha_depression,treat,standard,built,,data/alpha_depression.py,2,exact,true\n"
    "alpha_depression,cluster_id,standard,renamed,teacher_name,data/alpha_depression.py,4,exact,true\n"
    "alpha_depression,cov_male,standard_family,renamed,s_male_num,data/alpha_depression.py,4,exact,true\n"
    "alpha_depression,std_baseline_math,standard_family,,,,,exact,false\n"
    "alpha_depression,qmatrix3,standard_family,,,,,exact,false\n"
    "alpha_depression,mystery,,,,,,exact,false\n"
)


class _DocsSource(_Source):
    def __init__(self, docs=COLUMN_DOCS_CSV, **kw):
        self.docs = docs
        super().__init__(**kw)

    def _fetch_text(self, url):
        if url.endswith("/metadata/column_docs.csv"):
            self.calls.append(url)
            if isinstance(self.docs, Exception):
                raise self.docs
            return self.docs
        return super()._fetch_text(url)


class _LabelBackend(_Backend):
    def covariate_labels(self, table_name):
        return pd.DataFrame({"table": [table_name] * 2, "covariate": ["cov_male", "cov_male"],
                             "code": ["0", "1"], "label": ["female", "male"]})


def test_column_docs_rows_are_what_the_page_shows():
    result = IRWTools(_LabelBackend(), _DocsSource()).describe_columns("alpha_depression")
    assert result["columns_source"] == "column_docs"
    assert result["match"] == "exact"
    cols = _columns(result)
    assert (cols["cluster_id"]["basis"], cols["cluster_id"]["source_column"]) == ("renamed", "teacher_name")
    # The line behind the rename, with its text, for a link and a quote.
    assert cols["cluster_id"]["script_mentions"] == [{
        "path": "data/alpha_depression.py", "line": 4,
        "text": "select(id = s_id, cluster_id = teacher_name, cov_male = s_male_num, item, resp)"}]
    assert (cols["treat"]["basis"], cols["treat"]["source_column"]) == ("built", None)
    assert cols["cov_male"]["value_labels"] == {"0": "female", "1": "male"}
    assert cols["cov_male"]["documented"] is True
    assert any("same rows the table's web page shows" in w for w in result["warnings"])


def test_untraced_columns_stay_undocumented_and_carry_no_mentions():
    cols = _columns(IRWTools(_Backend(), _DocsSource()).describe_columns("alpha_depression"))
    assert cols["mystery"]["documented"] is False
    assert cols["mystery"]["script_mentions"] == []
    # A family definition is not a documented column (the CSV agrees).
    assert cols["qmatrix3"]["defined_by"] == "standard_family"
    assert cols["qmatrix3"]["documented"] is False


def test_table_not_yet_in_column_docs_falls_back_to_a_live_scan():
    other = COLUMN_DOCS_CSV.replace("alpha_depression", "someone_else")
    result = IRWTools(_Backend(), _DocsSource(docs=other)).describe_columns("alpha_depression")
    assert result["columns_source"] == "live_scan"
    assert any("not in the IRW's column_docs.csv yet" in w for w in result["warnings"])
    # The live scan still finds the rename line, but traces no source column.
    cluster = _columns(result)["cluster_id"]
    assert cluster["basis"] is None and cluster["script_mentions"]


def test_unreadable_column_docs_falls_back_without_claiming_the_table_is_new():
    result = IRWTools(_Backend(), _DocsSource(docs=ConnectionError("down"))).describe_columns(
        "alpha_depression")
    assert result["columns_source"] == "live_scan"
    assert any("column_docs.csv could not be loaded" in w for w in result["warnings"])
    assert not any("not in the IRW's column_docs.csv yet" in w for w in result["warnings"])


# --- codebook_links.csv (irw#2766): the source's own codebook files ----------

CODEBOOK_LINKS_CSV = (
    "table,url,file_name,host,how_found,n_same_kind_in_deposit,deposit_url,evidence,checked_at\n"
    "alpha_depression,https://example.org/f/1,Codebook.pdf,zenodo,name_codebook,1,https://example.org/d,,2026-10-02\n"
    "alpha_depression,https://example.org/f/9,Codebook_v2.pdf,example.org,recorded_at_ingest,1,,,2026-10-02\n"
    "alpha_depression,https://www.cis.es/documents/20117/1/MD2913.zip,codigo2913.pdf (inside MD2913.zip),cis.es,"
    "recorded_by_review,1,,CIS study 2913,2026-10-04\n"
    "alpha_depression,https://ldbase.org/documents/x,Master Codebook,ldbase,typed_codebook,1,,,2026-10-02\n"
    "alpha_depression,https://search.r-project.org/CRAN/refmans/p/html/d.html,p::d,cran,package_doc,1,,,2026-10-02\n"
    "alpha_depression,https://example.org/f/2,README.md,zenodo,readme_names_columns,1,https://example.org/d,"
    "\"names 4/9: treat, male, mystery, math\",2026-10-02\n"
    "alpha_depression,https://journals.plos.org/plosone/article/file?type=supplementary&id=x.s002,S2_File.docx,plos,"
    "doc_names_columns,1,https://journals.plos.org/plosone/article?id=x,\"names 3/9: treat, male, math\",2026-10-02\n"
    "alpha_depression,https://journals.plos.org/plosone/article/file?type=supplementary&id=x.s003,S1_Questionnaire.pdf,plos,"
    "questionnaire,1,https://journals.plos.org/plosone/article?id=x,caption: Questionnaire. (PDF),2026-10-02\n"
    + "".join(f"alpha_depression,https://example.org/ddi/{i},d{i}.tab,dataverse,dataverse_ddi,,https://example.org/d,,2026-10-02\n"
              for i in range(7))
)


class _LinksSource(_DocsSource):
    def __init__(self, links=CODEBOOK_LINKS_CSV, **kw):
        self.links = links
        super().__init__(**kw)

    def _fetch_text(self, url):
        if url.endswith("/metadata/codebook_links.csv"):
            self.calls.append(url)
            if isinstance(self.links, Exception):
                raise self.links
            return self.links
        return super()._fetch_text(url)


def test_source_codebooks_are_listed_with_how_each_was_found():
    result = IRWTools(_Backend(), _LinksSource()).describe_columns("alpha_depression")
    books = result["source_codebooks"]
    assert books[0] == {"file_name": "Codebook.pdf", "url": "https://example.org/f/1",
                        "how_found": "name_codebook", "host": "zenodo",
                        "n_same_kind_in_deposit": 1, "deposit_url": "https://example.org/d",
                        "evidence": None}
    assert books[1]["how_found"] == "recorded_at_ingest"
    assert books[2]["how_found"] == "recorded_by_review"
    assert books[2]["evidence"] == "CIS study 2913"
    assert [b["how_found"] for b in books[3:5]] == ["typed_codebook", "package_doc"]
    assert books[5]["how_found"] == "readme_names_columns"
    assert books[5]["evidence"].startswith("names 4/9")
    assert [b["how_found"] for b in books[6:8]] == ["doc_names_columns", "questionnaire"]
    assert [b["how_found"] for b in books].count("dataverse_ddi") == 5   # capped per kind
    assert any("dataverse_ddi files; the first 5" in w for w in result["warnings"])
    assert any("open a file before relying on it" in w for w in result["warnings"])


def test_no_codebook_found_is_an_empty_list_and_unreadable_is_none():
    other = CODEBOOK_LINKS_CSV.replace("alpha_depression", "someone_else")
    assert IRWTools(_Backend(), _LinksSource(links=other)).describe_columns(
        "alpha_depression")["source_codebooks"] == []
    result = IRWTools(_Backend(), _LinksSource(links=ConnectionError("down"))).describe_columns(
        "alpha_depression")
    assert result["source_codebooks"] is None
    assert any("codebook_links.csv could not be loaded" in w for w in result["warnings"])
    assert result["columns_source"] == "column_docs"   # the rest of the answer stands
