"""Reassemble a study deposited as several IRW tables.

Ports `irw_merge()` from `Rpkg/R/merge.R` (issue #23). Tables are grouped by
the paper DOI in the bibliography table or, where a table's DOI is shared with
no other table, by its BibTeX entry; the group containing `table_name` is
fetched and stacked into one frame.

Two behaviours carry over from R because they are not obvious from the
signature:

- **Missing citations are never a shared citation.** Biblio columns record "no
  citation" as the literal string ``"NA"`` (and occasionally ``""``). Grouping
  on the raw column would put every uncited table in one group and merge
  unrelated studies. `_is_present_biblio_value` is the guard.
- **Respondent counts are reported before anything is fetched.** They come
  from the metadata table, so the size of what is about to be downloaded -- and
  whether the tables plausibly describe the same people -- is known while
  stopping still costs nothing.

On confirmation: R asks at the console with `readline()`, twice -- once after
the respondent counts, and again if the ID/item checks raise a note -- and in a
non-interactive session it cannot ask, so it reports the checks and proceeds.
Python cannot tell a notebook from a script from an MCP server reliably, and
`input()` in the wrong one blocks forever or eats the protocol stream. So the
default here is R's non-interactive behaviour everywhere, and `confirm=True`
opts into R's interactive one.
"""

from __future__ import annotations

import warnings
from typing import Dict, List, Mapping, Optional, Sequence

import pandas as pd

from ..utils.redivis import _init_main_datasets
from ..utils.redivis.table_metadata import get_biblio_table, get_metadata_table
from .fetch import fetch as _fetch

DOI_COLUMN = "DOI__for_paper_"
BIBTEX_COLUMN = "BibTex"
SOURCE_COLUMN = "source_table"

_MAX_ATTEMPTS = 5


def _is_present_biblio_value(values: pd.Series) -> pd.Series:
    """True where a biblio value is a usable citation, not IRW's "NA" or blank."""
    text = values.astype("string").str.strip()
    return (text.notna() & (text != "") & (text != "NA")).fillna(False).astype(bool)


def _group_members(biblio: pd.DataFrame, column: str, table_name: str) -> Optional[List[str]]:
    """Tables sharing `table_name`'s value in `column`, or None if it shares with none.

    Matching on the table name is case-insensitive: biblio and the warehouse do
    not always agree on case, and a case-sensitive lookup here would report "no
    mergeable tables" for a study that has them.
    """
    if column not in biblio.columns:
        return None
    present = biblio[_is_present_biblio_value(biblio[column])]
    key = present[column].astype("string").str.strip()
    own = key[present["table"].str.lower() == table_name.lower()]
    if own.empty:
        return None
    members = present.loc[key == own.iloc[0], "table"].tolist()
    if len({m.lower() for m in members}) < 2:
        return None
    return members


def _find_merge_candidates(biblio: pd.DataFrame, table_name: str) -> Optional[List[str]]:
    """The tables to merge, `table_name` first; DOI wins, BibTeX is the fallback.

    As in R, BibTeX is consulted only when the DOI groups `table_name` with no
    other table -- including when its DOI is missing.
    """
    members = _group_members(biblio, DOI_COLUMN, table_name)
    if members is None:
        members = _group_members(biblio, BIBTEX_COLUMN, table_name)
    if members is None:
        return None
    others = [m for m in members if m.lower() != table_name.lower()]
    return [table_name] + others


def _ask_yes_no(prompt: str, confirm: bool, default: bool = True) -> bool:
    """R's `.prompt_yes_no()`: ask when `confirm`, otherwise report the assumed answer.

    With `confirm=True` it re-asks on invalid input and gives up after a
    bounded number of attempts, assuming `default`, exactly as R does. End of
    input (a closed stdin) is treated the way R treats a non-interactive
    session: the default is assumed and said so, rather than raising mid-merge.
    """
    answer_word = "yes" if default else "no"
    if not confirm:
        print(f"{prompt}{answer_word} (assumed; pass confirm=True to be asked)")
        return default

    for _ in range(_MAX_ATTEMPTS):
        try:
            answer = input(prompt).strip().lower()
        except EOFError:
            print(f"\nNo input could be read; assuming '{answer_word}'.")
            return default
        if answer in ("yes", "y"):
            return True
        if answer in ("no", "n"):
            return False
        print("Invalid input. Please enter 'yes' or 'no'.")

    print(f"No valid input received; assuming '{answer_word}'.")
    return default


def _metadata_counts(metadata: pd.DataFrame, table_names: Sequence[str], column: str) -> Dict[str, Optional[int]]:
    """Look up a per-table count, case-insensitively; None where it is unknown."""
    if column not in metadata.columns:
        return {name: None for name in table_names}
    # The metadata table can list one table twice (a copy in two warehouses);
    # take the first rather than letting a duplicate index raise.
    lookup = (
        metadata.assign(_key=metadata["table"].str.lower())
        .drop_duplicates("_key")
        .set_index("_key")[column]
    )
    counts = pd.to_numeric(lookup.astype("string").replace("NA", pd.NA), errors="coerce")
    out: Dict[str, Optional[int]] = {}
    for name in table_names:
        value = counts.get(name.lower())
        out[name] = None if value is None or pd.isna(value) else int(value)
    return out


def _check_n_respondents(table_names: Sequence[str], confirm: bool) -> bool:
    """Report respondent counts from metadata and ask whether to go on.

    Runs before any table is fetched. Returns False only if the user declines.
    """
    metadata = get_metadata_table()
    n_participants = _metadata_counts(metadata, table_names, "n_participants")
    n_responses = _metadata_counts(metadata, table_names, "n_responses")

    print(f"\n=== Found {len(table_names)} Tables to Merge ===")
    for name in table_names:
        n = n_participants[name]
        rows = n_responses[name]
        print(
            f"{name} (N = {'NA' if n is None else n}, "
            f"responses = {'NA' if rows is None else f'{rows:,}'})"
        )

    known = {n for n in n_participants.values() if n is not None}
    if len(known) == 1 and None not in n_participants.values():
        print(f"\nAll tables have the same number of respondents (N = {known.pop()}).")
    else:
        warnings.warn(
            "The number of respondents (N) is not consistent across tables"
            + (" or is missing from metadata for some" if None in n_participants.values() else "")
            + ". Merging tables with inconsistent N respondents may require human judgment.",
            UserWarning,
            stacklevel=3,
        )

    if not _ask_yes_no("Do you want to proceed with merging these tables? (yes/no): ", confirm):
        print("Merge operation canceled.")
        return False
    return True


def _as_integer_ids(ids: set) -> Optional[List[int]]:
    """The ids as sorted integers if every one is integral, else None."""
    numeric = pd.to_numeric(pd.Series(list(ids), dtype="object"), errors="coerce")
    if numeric.isna().any() or not (numeric == numeric.round()).all():
        return None
    return sorted(int(v) for v in numeric)


def _check_ids_and_items(tables: Mapping[str, pd.DataFrame]) -> List[str]:
    """R's `check_ids_and_items()`: the notes that should make a user hesitate.

    1. The tables do not all share the same set of ids.
    2. The shared ids run 1..n, which is what two unrelated samples numbered
       from 1 look like too.
    3. Some item appears in more than one table.

    Returns the notes; an empty list means every check passed.
    """
    id_sets = [set(df["id"].dropna()) if "id" in df.columns else set() for df in tables.values()]
    item_sets = [set(df["item"].dropna()) if "item" in df.columns else set() for df in tables.values()]
    notes: List[str] = []

    all_ids = set().union(*id_sets)
    shared_ids = set.intersection(*id_sets) if id_sets else set()
    if len(all_ids) != len(shared_ids):
        notes.append("IDs do not match across tables.")

    if shared_ids:
        # R calls seq(min, max) on the shared ids, which errors for character
        # ids; string ids simply cannot be "sequential" here.
        as_int = _as_integer_ids(shared_ids)
        if as_int is not None and as_int == list(range(as_int[0], as_int[-1] + 1)):
            notes.append(
                "IDs are sequential (1...n). You may need to manually verify the IDs, "
                "as there could be multiple studies with different subjects, where IDs "
                "are the same in both studies."
            )

    # Some item is in two tables exactly when the tables' item counts add up
    # to more than their union.
    if len(item_sets) > 1 and sum(len(s) for s in item_sets) > len(set().union(*item_sets)):
        notes.append("There are items that overlap across tables.")

    return notes


def merge(
    table_name: str,
    add_source_column: bool = True,
    confirm: bool = False,
) -> Optional[pd.DataFrame]:
    """
    Merge the IRW tables that come from the same study.

    Finds every table whose bibliography entry shares ``table_name``'s paper
    DOI -- or, when no other table shares its DOI, its BibTeX entry -- fetches
    them, and stacks them into one long-format frame. Tables whose columns do
    not match the first table fetched are skipped and listed.

    Parameters
    ----------
    table_name : str
        Any table in the study. It comes first in the result. Matched
        case-insensitively against the bibliography.
    add_source_column : bool, default True
        Add a ``source_table`` column recording which table each row came from.
    confirm : bool, default False
        Ask at the console before fetching and again if the ID/item checks
        raise a note, as R's ``irw_merge()`` does in an interactive session.
        The default is R's non-interactive behaviour: every check is still
        reported, the answer "yes" is assumed, and the merge proceeds. Leave it
        False in scripts and servers, where ``input()`` would block.

    Returns
    -------
    pandas.DataFrame or None
        The merged table. None if no other table shares a citation with
        ``table_name``, if the user declines, or if fewer than two tables could
        be fetched and stacked.

    Warns
    -----
    UserWarning
        When respondent counts differ across the tables (or are missing from
        metadata), and when the ID/item checks raise a note.

    Notes
    -----
    Checks, in the order they run:

    1. **Respondents** -- ``n_participants`` for each table is read from the
       metadata table and reported *before any data is fetched*, alongside
       ``n_responses`` so the size of the download is visible too. Unequal
       counts warn. First confirmation point.
    2. **IDs** -- after fetching: whether every table has the same set of ids,
       and whether those ids run 1..n (which unrelated samples numbered from 1
       also do).
    3. **Items** -- whether any item appears in more than one table.

    Notes from 2 and 3 warn and are the second confirmation point.

    A missing citation is recorded in the bibliography as the literal string
    ``"NA"`` (or left blank). Such values never group tables together, so two
    uncited tables are not treated as one study.

    Examples
    --------
    >>> import irw
    >>> df = irw.merge("ajaykumar_2023_nasa_tlx")        # doctest: +SKIP
    >>> df["source_table"].unique()                      # doctest: +SKIP
    """
    if not isinstance(table_name, str) or not table_name.strip():
        raise ValueError(f"'table_name' must be a non-empty string, got {table_name!r}.")

    candidates = _find_merge_candidates(get_biblio_table(), table_name)
    if candidates is None:
        print(f"No mergeable tables found for {table_name}")
        return None

    if not _check_n_respondents(candidates, confirm):
        return None

    datasets = _init_main_datasets()
    fetched: Dict[str, pd.DataFrame] = {}
    skipped: Dict[str, str] = {}
    columns: Optional[List[str]] = None

    print("\n=== Fetching Tables ===")
    for name in candidates:
        data = _fetch(datasets, name)
        if data is None or len(data) == 0:
            skipped[name] = "table could not be fetched"
            print(f"  - Skipping table '{name}': no data returned.")
            continue

        print(f"- Fetching table: {name} (Rows: {len(data)}, Columns: {data.shape[1]})...")
        if add_source_column:
            data = data.assign(**{SOURCE_COLUMN: name})

        # R's rbind() refuses frames whose column names differ; pd.concat would
        # quietly fill the gaps with NaN instead, so check the names explicitly.
        if columns is None:
            columns = list(data.columns)
        elif set(data.columns) != set(columns):
            skipped[name] = "columns differ from the first table fetched"
            print(f"  - Skipping table '{name}' due to column mismatch.")
            continue
        fetched[name] = data[columns]

    if len(fetched) < 2:
        print("\nMerging failed for all tables. No data merged.")
        return None

    notes = _check_ids_and_items(fetched)
    if notes:
        warnings.warn("\n- ".join(["Merge checks:"] + notes), UserWarning, stacklevel=2)
        if not _ask_yes_no("Do you still want to proceed with merging? (yes/no): ", confirm):
            print("Merge operation canceled.")
            return None

    merged = pd.concat(list(fetched.values()), ignore_index=True)

    print("\n=== Processing Summary ===")
    print(f"- Merged table dimension: (Rows: {len(merged)}, Columns: {merged.shape[1]}).")
    if add_source_column:
        print(f"- The merged table includes a '{SOURCE_COLUMN}' column indicating the source of each row.")
    if skipped:
        print("\n=== Skipped Tables ===")
        print("The following tables were skipped:")
        for name, reason in skipped.items():
            print(f"- {name} ({reason})")

    return merged
