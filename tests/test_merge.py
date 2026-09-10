"""irw.merge() groups by citation, checks size before fetching, and never blocks.

Port of `irw_merge()` (issue #23). What is being pinned, in order of how much
it costs to get wrong:

- **"No citation" is not a citation.** Biblio stores a missing DOI or BibTeX
  as the literal string "NA" (or blank). Grouping on the raw column would put
  every uncited table in one study; these tests hand it several.
- **Nothing is fetched before respondent counts are reported**, and declining
  at that point fetches nothing at all.
- **No prompt unless asked.** The default is R's non-interactive behaviour, so
  a script, notebook or MCP server never sits in `input()`; `confirm=True` is
  R's interactive one.
- R's grouping rules (DOI first, BibTeX only as fallback), its column-mismatch
  skip, and its ID/item notes.

No network: biblio, metadata and fetch are all replaced.
"""

import builtins
import sys
import warnings

import pandas as pd
import pytest

import irw
import irw.operations.merge  # noqa: F401  (registers the module)

# operations/__init__.py re-exports the function under the module's name.
merge_module = sys.modules["irw.operations.merge"]


def _biblio(rows):
    return pd.DataFrame(rows, columns=["table", "DOI__for_paper_", "BibTex"])


def _long(ids, items, resp=1):
    return pd.DataFrame(
        [(i, it, resp) for i in ids for it in items], columns=["id", "item", "resp"]
    )


@pytest.fixture
def world(monkeypatch):
    """Install a fake biblio, metadata and fetch; record every fetch made."""
    state = {
        "biblio": _biblio([]),
        "metadata": pd.DataFrame(columns=["table", "n_participants", "n_responses"]),
        "tables": {},
        "fetched": [],
    }

    def fake_fetch(datasets, name, **kwargs):
        state["fetched"].append(name)
        return state["tables"].get(name)

    def no_input(prompt=""):
        raise AssertionError(f"input() was called without confirm=True: {prompt!r}")

    monkeypatch.setattr(merge_module, "get_biblio_table", lambda: state["biblio"])
    monkeypatch.setattr(merge_module, "get_metadata_table", lambda: state["metadata"])
    monkeypatch.setattr(merge_module, "_init_main_datasets", lambda: [object()])
    monkeypatch.setattr(merge_module, "_fetch", fake_fetch)
    monkeypatch.setattr(builtins, "input", no_input)
    return state


def _study(state, tables, n=(10, 10), doi="10.1/x"):
    """Two or more tables sharing a DOI, with disjoint items and shared ids."""
    state["biblio"] = _biblio([(t, doi, "NA") for t in tables])
    state["metadata"] = pd.DataFrame(
        {"table": list(tables), "n_participants": list(n), "n_responses": [100] * len(tables)}
    )
    for t in tables:
        state["tables"][t] = _long(["a", "b"], [f"{t}_q{j}" for j in range(3)])


# --- the "NA" guard ---------------------------------------------------------

def test_tables_with_no_doi_and_no_bibtex_are_not_one_study(world):
    world["biblio"] = _biblio([
        ("uncited_1", "NA", "NA"),
        ("uncited_2", "NA", "NA"),
        ("uncited_3", "", " "),
        ("uncited_4", None, None),
    ])
    assert merge_module.merge("uncited_1") is None
    assert world["fetched"] == []


def test_na_doi_does_not_group_but_a_shared_bibtex_still_does(world):
    """DOI "NA" is skipped, and the BibTeX fallback finds the real study."""
    world["biblio"] = _biblio([
        ("s_1", "NA", "@article{k1, title={A}}"),
        ("s_2", "NA", "@article{k1, title={A}}"),
        ("other", "NA", "@article{k2, title={B}}"),
    ])
    assert merge_module._find_merge_candidates(world["biblio"], "s_2") == ["s_2", "s_1"]


def test_is_present_biblio_value():
    values = pd.Series(["10.1/x", "NA", "", "  ", None, " NA "], dtype="object")
    assert merge_module._is_present_biblio_value(values).tolist() == [
        True, False, False, False, False, False,
    ]


# --- grouping ---------------------------------------------------------------

def test_doi_wins_over_bibtex(world):
    world["biblio"] = _biblio([
        ("t1", "10.1/a", "@x{shared}"),
        ("t2", "10.1/a", "@x{other}"),
        ("t3", "10.1/b", "@x{shared}"),
    ])
    assert merge_module._find_merge_candidates(world["biblio"], "t1") == ["t1", "t2"]


def test_bibtex_is_the_fallback_when_the_doi_is_unshared(world):
    """R consults BibTeX when the DOI group has one table, not only when DOI is NA."""
    world["biblio"] = _biblio([
        ("t1", "10.1/only_me", "@x{shared}"),
        ("t2", "NA", "@x{shared}"),
    ])
    assert merge_module._find_merge_candidates(world["biblio"], "t1") == ["t1", "t2"]


def test_table_name_is_matched_case_insensitively_and_comes_first(world):
    world["biblio"] = _biblio([
        ("Study_B", "10.1/a", "NA"),
        ("study_a", "10.1/a", "NA"),
    ])
    assert merge_module._find_merge_candidates(world["biblio"], "STUDY_A") == ["STUDY_A", "Study_B"]


def test_surrounding_whitespace_does_not_split_a_group(world):
    world["biblio"] = _biblio([("t1", "10.1/a", "NA"), ("t2", " 10.1/a ", "NA")])
    assert merge_module._find_merge_candidates(world["biblio"], "t1") == ["t1", "t2"]


def test_a_table_not_in_biblio_merges_nothing(world, capsys):
    world["biblio"] = _biblio([("t1", "10.1/a", "NA"), ("t2", "10.1/a", "NA")])
    assert merge_module.merge("missing") is None
    assert "No mergeable tables found for missing" in capsys.readouterr().out
    assert world["fetched"] == []


# --- merging ----------------------------------------------------------------

def test_merges_the_study_with_a_source_column(world):
    _study(world, ["s_1", "s_2"])
    with warnings.catch_warnings():
        warnings.simplefilter("error")  # a clean study raises no notes
        merged = merge_module.merge("s_2")
    assert merged is not None
    assert list(merged.columns) == ["id", "item", "resp", "source_table"]
    assert len(merged) == 12
    # table_name first, as in R
    assert merged["source_table"].tolist()[:6] == ["s_2"] * 6
    assert set(merged["source_table"]) == {"s_1", "s_2"}
    assert merged.index.tolist() == list(range(12))


def test_add_source_column_false(world):
    _study(world, ["s_1", "s_2"])
    merged = merge_module.merge("s_1", add_source_column=False)
    assert list(merged.columns) == ["id", "item", "resp"]


def test_columns_in_a_different_order_still_stack(world):
    _study(world, ["s_1", "s_2"])
    world["tables"]["s_2"] = world["tables"]["s_2"][["resp", "item", "id"]]
    merged = merge_module.merge("s_1")
    assert list(merged.columns) == ["id", "item", "resp", "source_table"]
    assert merged["resp"].notna().all()


def test_a_table_with_different_columns_is_skipped_not_nan_filled(world, capsys):
    """R's rbind() errors on differing names; pd.concat would silently NaN-fill."""
    _study(world, ["s_1", "s_2", "s_3"], n=(10, 10, 10))
    world["tables"]["s_3"] = world["tables"]["s_3"].assign(wave=1)
    merged = merge_module.merge("s_1")
    assert set(merged["source_table"]) == {"s_1", "s_2"}
    assert "wave" not in merged.columns
    out = capsys.readouterr().out
    assert "Skipping table 's_3' due to column mismatch" in out
    assert "=== Skipped Tables ===" in out


def test_an_unfetchable_table_is_skipped(world, capsys):
    _study(world, ["s_1", "s_2", "s_3"], n=(10, 10, 10))
    world["tables"]["s_3"] = None
    merged = merge_module.merge("s_1")
    assert set(merged["source_table"]) == {"s_1", "s_2"}
    assert "s_3 (table could not be fetched)" in capsys.readouterr().out


def test_nothing_is_returned_when_only_one_table_survives(world, capsys):
    """R: `length(skipped) >= length(candidates) - 1` returns NULL."""
    _study(world, ["s_1", "s_2"])
    world["tables"]["s_2"] = pd.DataFrame()
    assert merge_module.merge("s_1") is None
    assert "Merging failed for all tables" in capsys.readouterr().out


def test_empty_table_name_is_refused(world):
    with pytest.raises(ValueError, match="non-empty string"):
        merge_module.merge("  ")


# --- respondent check, before any fetch ------------------------------------

def test_counts_are_reported_before_the_first_fetch(world, capsys, monkeypatch):
    _study(world, ["s_1", "s_2"], n=(27, 27))
    seen_at_first_fetch = {}
    real_fetch = merge_module._fetch

    def fetch_and_snapshot(datasets, name, **kwargs):
        seen_at_first_fetch.setdefault("out", capsys.readouterr().out)
        return real_fetch(datasets, name, **kwargs)

    monkeypatch.setattr(merge_module, "_fetch", fetch_and_snapshot)
    merge_module.merge("s_1")
    out = seen_at_first_fetch["out"]
    assert "=== Found 2 Tables to Merge ===" in out
    assert "s_1 (N = 27, responses = 100)" in out
    assert "same number of respondents (N = 27)" in out


def test_inconsistent_respondent_counts_warn(world):
    _study(world, ["s_1", "s_2"], n=(68, 65))
    with pytest.warns(UserWarning, match="not consistent across tables"):
        merge_module.merge("s_1")


def test_missing_metadata_counts_warn_rather_than_crash(world, capsys):
    _study(world, ["s_1", "s_2"])
    world["metadata"] = pd.DataFrame(
        {"table": ["S_1"], "n_participants": ["NA"], "n_responses": ["NA"]}
    )
    with pytest.warns(UserWarning, match="missing from metadata"):
        merged = merge_module.merge("s_1")
    assert merged is not None
    assert "s_2 (N = NA, responses = NA)" in capsys.readouterr().out


def test_metadata_lookup_is_case_insensitive_and_tolerates_duplicates():
    metadata = pd.DataFrame(
        {"table": ["Zhou_2025", "zhou_2025", "b"], "n_participants": [5, 6, 7]}
    )
    counts = merge_module._metadata_counts(metadata, ["ZHOU_2025", "b"], "n_participants")
    assert counts == {"ZHOU_2025": 5, "b": 7}


# --- confirmation -----------------------------------------------------------

def test_default_never_calls_input_and_says_what_it_assumed(world, capsys):
    """The `world` fixture makes input() raise, so this would fail if it were called."""
    _study(world, ["s_1", "s_2"])
    assert merge_module.merge("s_1") is not None
    assert "yes (assumed; pass confirm=True to be asked)" in capsys.readouterr().out


def _answers(monkeypatch, *replies):
    replies = list(replies)
    asked = []

    def fake_input(prompt=""):
        asked.append(prompt)
        if not replies:
            raise EOFError
        return replies.pop(0)

    monkeypatch.setattr(builtins, "input", fake_input)
    return asked


def test_confirm_no_at_the_respondent_check_fetches_nothing(world, monkeypatch):
    _study(world, ["s_1", "s_2"])
    asked = _answers(monkeypatch, "no")
    assert merge_module.merge("s_1", confirm=True) is None
    assert world["fetched"] == []
    assert len(asked) == 1


def test_confirm_yes_merges_without_a_second_prompt_when_checks_pass(world, monkeypatch):
    _study(world, ["s_1", "s_2"])
    asked = _answers(monkeypatch, "y")
    assert merge_module.merge("s_1", confirm=True) is not None
    assert len(asked) == 1


def test_confirm_no_at_the_id_item_check_cancels(world, monkeypatch):
    _study(world, ["s_1", "s_2"])
    world["tables"]["s_2"] = _long(["c", "d"], ["s_2_q0"])  # ids do not match
    asked = _answers(monkeypatch, "yes", "n")
    with pytest.warns(UserWarning, match="IDs do not match"):
        assert merge_module.merge("s_1", confirm=True) is None
    assert len(asked) == 2


def test_invalid_answers_give_up_and_assume_yes_like_r(world, monkeypatch, capsys):
    _study(world, ["s_1", "s_2"])
    asked = _answers(monkeypatch, *["maybe"] * 5)
    assert merge_module.merge("s_1", confirm=True) is not None
    assert len(asked) == 5
    assert "No valid input received; assuming 'yes'" in capsys.readouterr().out


def test_closed_stdin_assumes_yes_instead_of_raising(world, monkeypatch):
    _study(world, ["s_1", "s_2"])
    _answers(monkeypatch)  # no replies: every input() raises EOFError
    assert merge_module.merge("s_1", confirm=True) is not None


# --- ID and item checks -----------------------------------------------------

def test_id_item_checks_pass_for_shared_string_ids_and_disjoint_items():
    tables = {"a": _long(["x", "y"], ["q1"]), "b": _long(["x", "y"], ["q2"])}
    assert merge_module._check_ids_and_items(tables) == []


def test_mismatched_ids_are_noted():
    tables = {"a": _long(["x", "y"], ["q1"]), "b": _long(["x"], ["q2"])}
    assert merge_module._check_ids_and_items(tables) == ["IDs do not match across tables."]


def test_sequential_integer_ids_are_noted():
    tables = {"a": _long([1, 2, 3], ["q1"]), "b": _long([3, 2, 1], ["q2"])}
    notes = merge_module._check_ids_and_items(tables)
    assert len(notes) == 1 and notes[0].startswith("IDs are sequential")


def test_gapped_integer_ids_are_not_sequential():
    tables = {"a": _long([1, 2, 5], ["q1"]), "b": _long([1, 2, 5], ["q2"])}
    assert merge_module._check_ids_and_items(tables) == []


def test_overlapping_items_are_noted():
    tables = {
        "a": _long(["x"], ["q1", "q2"]),
        "b": _long(["x"], ["q3"]),
        "c": _long(["x"], ["q2"]),
    }
    assert merge_module._check_ids_and_items(tables) == [
        "There are items that overlap across tables."
    ]


def test_merge_is_exported():
    assert irw.merge is merge_module.merge
    assert "merge" in irw.__all__
