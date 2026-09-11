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
"""

from __future__ import annotations

from typing import Any, List, Optional, Sequence, Union

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


def covariates(
    df: pd.DataFrame,
    cols: Optional[Union[str, Sequence[str]]] = None,
    align: Optional[Any] = None,
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
