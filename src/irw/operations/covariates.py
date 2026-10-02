"""Person-level covariates from IRW long format, optionally aligned to a matrix.

Ports `irw_covariates()` from `Rpkg/R/covariates.R` (issue #58). `long2resp()`
keeps only `id`, `item` and the response, so person-level columns such as
`cov_group` are dropped, and its row order is whatever the pivot produced.
Matching a covariate back onto that matrix by hand is where people get
silently misassigned to groups -- no error, just a wrong analysis. `align=`
does the match.

The detection rule is the one R uses, and it is deliberately stricter than
`groupby("id").first()`: a column is person-level only if every id has exactly
one distinct value, *counting missing as a value*. A person with `A` on some
rows and `NaN` on others is varying, and the column is rejected -- `first()`
would have quietly kept `A`.

Two deliberate divergences from R, both forced by what pandas has that R does
not:

- R's "matrix with id rownames" becomes a DataFrame whose *index is named*
  ``id`` (``wide.set_index("id")``). A DataFrame always has an index, so
  accepting any index would read a default 0..n-1 RangeIndex as ids -- the
  exact misassignment this function exists to prevent. A bare 2-D numpy array
  has no row labels and is refused, as R refuses a matrix without rownames.
- Integer and boolean columns that gain missing rows under `align` become the
  nullable ``Int64`` / ``boolean`` dtypes rather than float, so an id of 7
  does not come back as 7.0. R's integer NA has no such cost.

Messages go to stdout with `print()`, as `long2resp()` does, where R uses
`message()`.

Value labels (ben-domingue/irw#1775). Many covariates ship as bare codes
(``cov_gender`` in {1, 2}) whose meaning the source file recorded and the IRW
table does not; the meaning varies by table (``cucchi_2018_rfq`` codes
1 = female, ``rowe_2016_cfs_strain`` 1 = male). irw_meta's ``covariate_labels``
table records the source's own labels, and `covariate_labels()` returns them.
``covariates(labels=True)`` turns each decodable covariate into a Categorical.
It is opt-in: `fetch()` never decodes.
"""

from __future__ import annotations

import re
from typing import Any, Dict, List, Optional, Sequence, Tuple, Union

import numpy as np
import pandas as pd

# Response-level columns of the IRW long format. Everything else is a
# candidate person-level column. Same list as `.irw_response_cols` in R.
RESPONSE_COLS = ("item", "resp", "rater", "rt", "text", "date", "source_table")


def _is_constant_within_id(df: pd.DataFrame, col: str, id_col: str = "id") -> bool:
    """True if `col` takes a single value within every id.

    Mirrors R's `!anyDuplicated(unique(df[, c(id, col)])$id)`. `drop_duplicates`
    treats missing values as equal to each other and distinct from any real
    value, which is what makes a partly-missing column count as varying.
    """
    pairs = df[[id_col, col]].drop_duplicates()
    return not pairs[id_col].duplicated().any()


def _align_ids(align: Any) -> pd.Index:
    """Pull the ids out of whatever `align` was supplied as."""
    if isinstance(align, pd.DataFrame):
        if "id" in align.columns:
            return pd.Index(align["id"])
        if align.index.name == "id":
            return pd.Index(align.index)
        raise ValueError(
            "`align` is a DataFrame with neither an `id` column nor an index "
            "named `id`. Supply the result of long2resp(), or pass a sequence "
            "of ids."
        )
    if isinstance(align, np.ndarray) and align.ndim > 1:
        raise ValueError(
            "`align` is a 2-D array without row labels to use as ids. Supply "
            "a sequence of ids instead."
        )
    if isinstance(align, (pd.Series, pd.Index, np.ndarray, list, tuple)):
        return pd.Index(align)
    raise ValueError(
        "`align` must be a DataFrame with an `id` column (or an index named "
        "`id`), or a sequence of ids."
    )


def _code_text(value: Any) -> Any:
    """The shipped value written as `covariate_labels.code` writes it.

    A shipped 1.0 is ``"1"``, as is a shipped 1 or "1". Missing stays missing.
    """
    if value is None or (not isinstance(value, str) and pd.isna(value)):
        return pd.NA
    if isinstance(value, (bool, np.bool_)):
        return str(bool(value))
    if isinstance(value, (int, np.integer)):
        return str(int(value))
    if isinstance(value, (float, np.floating)):
        return str(int(value)) if float(value).is_integer() else repr(float(value))
    text = str(value).strip()
    if re.fullmatch(r"-?\d+\.0+", text):
        return text.split(".")[0]
    return text


def _code_sort_key(code: str) -> Tuple[int, Any]:
    """Numeric codes in numeric order, then any others alphabetically."""
    try:
        return (0, float(code))
    except ValueError:
        return (1, code)


def covariate_labels(table: Union[str, Sequence[str]]) -> pd.DataFrame:
    """
    Source value labels for an IRW table's coded covariates.

    Many covariates ship as bare codes -- ``cov_gender`` in {1, 2} -- whose
    meaning was recorded in the source file (an SPSS or Stata value label) but
    is not in the IRW table, and the meaning differs from table to table. This
    returns the source's own labels, one row per code. It reads irw_meta's
    ``covariate_labels`` table and does not touch the response data.

    Parameters
    ----------
    table : str or sequence of str
        IRW table name(s), case-insensitive.

    Returns
    -------
    pandas.DataFrame
        Columns ``table``, ``covariate``, ``code``, ``label``, all text, one
        row per code, sorted by table, covariate and code. ``code`` is the
        shipped value written as text (a shipped ``1.0`` is ``"1"``).
        ``label`` is the source's wording, verbatim and not harmonised across
        tables, or the literal ``[institution name withheld]`` where the codes
        name institutions. Only codes that occur in the shipped column are
        listed, and a covariate may be partly covered. Empty, with a printed
        message, when IRW has no labels for the table.

    Raises
    ------
    CovariateLabelsUnavailable
        If the irw_meta version in use has no ``covariate_labels`` table (an
        older or pinned version). Absence of the whole table is an error, not
        an empty result, so it cannot be mistaken for "no labelled covariates".

    See Also
    --------
    covariates : ``labels=True`` applies these labels.

    Examples
    --------
    >>> irw.covariate_labels("cucchi_2018_rfq")   # doctest: +SKIP
    """
    from ..utils.redivis.table_metadata import get_covariate_labels_table

    if isinstance(table, str):
        tables = [table]
    else:
        tables = list(table)
    if not tables or not all(isinstance(t, str) for t in tables):
        raise ValueError("`table` must be a table name or a sequence of table names.")

    rows = get_covariate_labels_table()
    want = {t.lower() for t in tables}
    out = rows[rows["table"].str.lower().isin(want)].copy()
    if out.empty:
        print(
            "No covariate value labels in IRW for: " + ", ".join(tables)
            + ". Either its covariates are not coded, or their source labels "
            "could not be recovered (see get_processing_notes / the build script)."
        )
    out["_key"] = out["code"].map(_code_sort_key)
    out = out.sort_values(["table", "covariate", "_key"], kind="stable")
    return out.drop(columns="_key").reset_index(drop=True)


def _resolve_label_rows(
    df: pd.DataFrame, labels: Any, table: Optional[str]
) -> pd.DataFrame:
    """The label rows `covariates(labels=...)` should apply, for one table."""
    if table is None and "source_table" in df.columns:
        names = pd.unique(df["source_table"].dropna())
        if len(names) == 1:
            table = str(names[0])
        elif len(names) > 1:
            raise ValueError(
                "`df` holds rows from several tables (`source_table`), and the "
                "same code can mean different things in each. Decode one table "
                "at a time: filter `df` to one source_table, or pass `table=`."
            )
    if isinstance(labels, pd.DataFrame):
        missing = [c for c in ("covariate", "code", "label") if c not in labels.columns]
        if missing:
            raise ValueError(
                "`labels` as a DataFrame needs the columns covariate, code, "
                f"label (as irw.covariate_labels() returns); missing: {', '.join(missing)}."
            )
        rows = labels
        if "table" in rows.columns:
            if table is not None:
                rows = rows[rows["table"].astype(str).str.lower() == table.lower()]
            elif rows["table"].nunique() > 1:
                raise ValueError(
                    "`labels` covers several tables; pass `table=` to say "
                    "which one `df` is."
                )
        return rows
    if labels is True:
        if table is None:
            raise ValueError(
                "labels=True needs to know which IRW table `df` came from: "
                "pass `table=` (e.g. covariates(df, labels=True, "
                "table=\"cucchi_2018_rfq\"))."
            )
        return covariate_labels(table)
    raise ValueError(
        "`labels` must be True, False, or a DataFrame from irw.covariate_labels()."
    )


def _apply_labels(
    out: pd.DataFrame, cols: List[str], rows: pd.DataFrame
) -> Tuple[pd.DataFrame, List[str]]:
    """Turn each covariate in `cols` that has label rows into a Categorical."""
    decoded: List[str] = []
    kept: List[str] = []
    partial: List[str] = []
    for col in cols:
        these = rows[rows["covariate"] == col]
        if these.empty:
            continue
        labs: Dict[str, str] = {}
        for code, label in zip(these["code"], these["label"]):
            labs[_code_text(code)] = str(label)
        if len(set(labs.values())) < len(labs):
            # Several codes share one label -- the institution-withheld rows,
            # where every school reads "[institution name withheld]". Decoding
            # would merge distinct groups into one, so leave the codes.
            kept.append(col)
            continue
        keys = out[col].map(_code_text)
        present = {k for k in keys if not pd.isna(k)}
        unlabelled = sorted(present - set(labs), key=_code_sort_key)
        if set(unlabelled) & set(labs.values()):
            # An unlabelled code reads the same as another code's label.
            kept.append(col)
            continue
        codes = sorted(set(labs) | set(unlabelled), key=_code_sort_key)
        categories = [labs.get(c, c) for c in codes]
        values = [pd.NA if pd.isna(k) else labs.get(k, k) for k in keys]
        out[col] = pd.Categorical(values, categories=categories)
        decoded.append(col)
        if unlabelled:
            partial.append(f"{col} ({', '.join(unlabelled)})")

    messages: List[str] = []
    if decoded:
        messages.append("Decoded with source labels: " + ", ".join(decoded))
    if partial:
        messages.append(
            "Codes with no source label, kept as their code text: "
            + "; ".join(partial)
        )
    if kept:
        messages.append(
            "Left as codes (several codes share one label, e.g. institution "
            "names withheld): " + ", ".join(kept)
        )
    return out, messages


def covariates(
    df: pd.DataFrame,
    cols: Optional[Union[str, Sequence[str]]] = None,
    align: Optional[Any] = None,
    labels: Union[bool, pd.DataFrame] = False,
    table: Optional[str] = None,
) -> pd.DataFrame:
    """
    Extract person-level covariates from IRW long-format data.

    Reduces the data to one row per ``id``, keeping the columns that describe
    the person rather than the response, and optionally puts those rows in the
    order of a wide response matrix -- which is what most psychometric
    packages need when a grouping variable is passed alongside the matrix.

    With ``cols=None`` the person-level columns are detected: every column
    other than ``id`` and the response-level columns (``item``, ``resp``,
    ``rater``, ``rt``, ``text``, ``date``, ``source_table``) that takes exactly
    one value within every ``id``. A column with a value on some of a person's
    rows and missing on others counts as varying and is not selected, and the
    rejected columns are named in a printed message. ``wave`` is person-level
    only when each person appears in a single wave.

    Parameters
    ----------
    df : pandas.DataFrame
        IRW long-format data with an ``id`` column.
    cols : str or sequence of str, optional
        Person-level columns to extract. Default None: detect them as above.
        A named column that varies within id is an error.
    align : DataFrame, or sequence of ids, optional
        A DataFrame with an ``id`` column (such as the result of
        ``long2resp()``), a DataFrame whose index is named ``id``, or a list,
        array, Series or Index of ids. Rows are returned in this order, one
        per element.
    labels : bool or DataFrame, default False
        Opt in to source value labels (ben-domingue/irw#1775). ``True`` reads
        them with :func:`covariate_labels` for ``table``; a DataFrame from
        :func:`covariate_labels` is used as given (no network). Each returned
        covariate that has labels becomes a pandas Categorical whose
        categories are the labels in code order (unordered: the source does
        not say whether the codes are ordinal). A code with no label keeps its
        code text as a category and is named in a printed message. A
        covariate whose codes share one label -- institution names withheld --
        is left as codes, since decoding would merge distinct groups.
        Covariates without labels are untouched.
    table : str, optional
        The IRW table ``df`` came from, for ``labels=True``. Data fetched with
        ``irw.fetch()`` does not carry its name, so it must be given, unless
        ``df`` has a ``source_table`` column naming a single table. Rows from
        several tables are refused: the same code means different things in
        different tables.

    Returns
    -------
    pandas.DataFrame
        ``id`` followed by the person-level covariates, one row per id, in
        order of first appearance in ``df``. With ``align``, rows follow that
        order instead, and ids not present in ``df`` yield rows of missing
        values rather than being dropped, so the result lines up row for row.

    Examples
    --------
    >>> import pandas as pd
    >>> import irw
    >>> df = pd.DataFrame({
    ...     "id": [1, 1, 2, 2],
    ...     "item": ["i1", "i2", "i1", "i2"],
    ...     "resp": [1, 0, 1, 1],
    ...     "cov_group": ["A", "A", "B", "B"],
    ... })
    >>> irw.covariates(df)["cov_group"].tolist()
    ['A', 'B']
    >>> wide = irw.long2resp(df, id_density_threshold=None)
    >>> irw.covariates(df, align=wide)["id"].tolist() == wide["id"].tolist()
    True

    With source value labels:

    >>> df = irw.fetch("cucchi_2018_rfq")                       # doctest: +SKIP
    >>> irw.covariates(df, labels=True, table="cucchi_2018_rfq")  # doctest: +SKIP
    """
    if not isinstance(df, pd.DataFrame):
        raise ValueError("`df` must be a pandas DataFrame.")
    if "id" not in df.columns:
        raise ValueError("Missing required IRW columns: id")

    messages: List[str] = []

    if cols is None:
        candidates = [c for c in df.columns if c != "id" and c not in RESPONSE_COLS]
        keep = [c for c in candidates if _is_constant_within_id(df, c)]
        dropped = [c for c in candidates if c not in keep]
        cols = keep
        if dropped:
            messages.append(
                "Not person-level (varies within id), so not returned: "
                + ", ".join(dropped)
            )
        if not cols:
            messages.append("No person-level covariates found; returning ids only.")
    else:
        if isinstance(cols, str):
            cols = [cols]
        cols = list(cols)
        if not all(isinstance(c, str) for c in cols):
            raise ValueError("`cols` must be a column name or a sequence of column names.")
        missing_cols = [c for c in cols if c not in df.columns]
        if missing_cols:
            raise ValueError(f"Missing required IRW columns: {', '.join(missing_cols)}")
        # `id` is always the first column; naming it again would duplicate it.
        cols = [c for c in cols if c != "id"]
        varying = [c for c in cols if not _is_constant_within_id(df, c)]
        if varying:
            raise ValueError(
                "These columns are not person-level (they vary within id): "
                f"{', '.join(varying)}. A person-level covariate must take "
                "one value per id."
            )

    out = df.loc[~df["id"].duplicated(), ["id"] + cols].reset_index(drop=True)

    if labels is not False and labels is not None:
        rows = _resolve_label_rows(df, labels, table)
        out, label_messages = _apply_labels(out, cols, rows)
        messages.extend(label_messages)

    if align is not None:
        ids = _align_ids(align)
        # get_indexer is R's match(): -1 where an id is absent.
        idx = pd.Index(out["id"]).get_indexer(ids)
        n_missing = int((idx == -1).sum())
        if n_missing:
            msg = (
                f"{n_missing} id(s) in `align` were not found in `df`; "
                "those rows are NA."
            )
            if n_missing == len(ids) and ids.dtype != out["id"].dtype:
                # R's match() coerces numbers and strings to a common type;
                # pandas does not, so "1" and 1 are different ids here.
                msg += (
                    f" No id matched: `align` ids are {ids.dtype} but "
                    f"df['id'] is {out['id'].dtype}."
                )
            messages.append(msg)
            # Give integer and boolean columns a dtype that can hold missing,
            # so they do not come back as float.
            for c in cols:
                if pd.api.types.is_bool_dtype(out[c]):
                    out[c] = out[c].astype("boolean")
                elif pd.api.types.is_integer_dtype(out[c]):
                    out[c] = out[c].astype("Int64")
        # `out["id"]` is unique by construction, so reindex is safe; the id
        # column is then the `align` ids themselves, as in R.
        out = out.set_index("id").reindex(ids)
        out.index.name = "id"
        out = out.reset_index()

    if messages:
        print("\n".join(messages))

    return out
