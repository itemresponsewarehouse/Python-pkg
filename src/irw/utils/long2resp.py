"""Convert between IRW long format and a wide response matrix, and check responses.

`long2resp()` widens; `resp2long()` (issue #60) is its inverse; `check_resp()`
(issue #61) reports the two response patterns that most often break an
estimator without naming the cause. All three port `Rpkg/R/long2resp.R`.

One deliberate divergence from R runs through all of it: R attaches the
diagnostics to the wide frame as an attribute, and `irw_check_resp()` reads
them back off it. `DataFrame.attrs` does not survive most pandas operations, so
here they are *returned* -- `long2resp(check_resp=True)` gives a
`(wide, checks)` tuple -- and `check_resp()` takes long data only.
"""

from __future__ import annotations
from typing import Any, Dict, List, Literal, Optional, Tuple, Union
import pandas as pd
import numpy as np


def _is_numeric_resp(values: pd.Series) -> bool:
    """True unless the responses contain values and none of them is a number.

    R decides on the column's type: numeric is averaged, character is not. That
    does not carry over. An object column of numeric strings is common in
    pandas (a CSV read with `dtype=str`), and Python's `long2resp()` has always
    coerced `resp` to numbers, setting the odd unparseable value to NaN with a
    message. To leave every existing call unchanged, a column is non-numeric
    only when nothing in it parses -- the one case the old coercion turned
    into a matrix of NaN, and the case `resp_col="text"` exists for.
    """
    if pd.api.types.is_numeric_dtype(values):
        return True
    observed = values.dropna()
    if observed.empty:
        return True
    return bool(pd.to_numeric(observed, errors="coerce").notna().any())


def _sorted_counts(values: pd.Series) -> pd.Series:
    """Category counts in sorted category order, as R's `table()` gives them."""
    counts = values.value_counts(sort=False)
    try:
        return counts.sort_index()
    except TypeError:
        # A mixed-type object column (1 and "yes") has no order; keep counts
        # rather than fail a diagnostic over it.
        return counts


def _check_resp(
    df: pd.DataFrame,
    min_count: int = 5,
    min_prop: float = 0.01,
    resp_col: str = "resp",
) -> Dict[str, Any]:
    """Diagnostics on long data whose items are already the names to report."""
    single_category_items: List[str] = []
    sparse_category_items: Dict[str, pd.DataFrame] = {}

    observed = df.loc[df[resp_col].notna(), ["item", resp_col]]
    for item_name, responses in observed.groupby("item", sort=True)[resp_col]:
        counts = _sorted_counts(responses)
        n_categories = len(counts)

        # 1. An item with one observed category carries no information.
        if n_categories <= 1:
            single_category_items.append(item_name)
            continue

        # 2. Sparse categories, polytomous items only (3+ categories), as in
        #    R. A dichotomous item with a rare category is an easy or a hard
        #    item, which is not what this check is for.
        if n_categories < 3:
            continue
        props = counts / counts.sum()
        rare = ((counts < min_count) | (props < min_prop)).to_numpy()
        if rare.any():
            sparse_category_items[item_name] = pd.DataFrame(
                {
                    "resp": counts.index[rare],
                    "count": counts.to_numpy()[rare].astype(int),
                    "prop": props.to_numpy()[rare].astype(float),
                }
            )

    return {
        "single_category_items": single_category_items,
        "sparse_category_items": sparse_category_items,
    }


def check_resp(
    x: pd.DataFrame,
    min_count: int = 5,
    min_prop: float = 0.01,
    resp_col: str = "resp",
) -> Dict[str, Any]:
    """
    Check IRW long-format data for single-category items and sparse categories.

    Reports two problems that make a model fail, or fit badly, with an error
    that does not name the cause: items with only one observed response
    category, and categories of polytomous items (three or more categories)
    observed fewer than ``min_count`` times **or** making up less than
    ``min_prop`` of that item's responses. Missing responses are ignored, and
    duplicate (id, item) rows are counted as they are -- use
    ``long2resp(df, check_resp=True)`` to check the aggregated responses.

    Parameters
    ----------
    x : pandas.DataFrame
        Long-format data with ``id``, ``item`` and the column named by
        ``resp_col``. To check a wide response matrix, convert it first:
        ``check_resp(resp2long(wide))``.
    min_count : int, default 5
        A category with fewer observations than this is sparse.
    min_prop : float, default 0.01
        A category with a smaller within-item proportion than this is sparse.
    resp_col : str, default "resp"
        The response column to inspect.

    Returns
    -------
    dict
        ``single_category_items``: list of item names, as strings.
        ``sparse_category_items``: dict mapping item name to a DataFrame of
        its sparse categories, with columns ``resp``, ``count``, ``prop``.

    Examples
    --------
    >>> import irw
    >>> checks = irw.check_resp(irw.fetch("agn_kay_2025"))
    >>> checks["single_category_items"]
    """
    if not isinstance(x, pd.DataFrame) or not {"id", "item"} <= set(x.columns):
        raise ValueError(
            "check_resp() expects long-format data with 'id' and 'item' columns "
            f"plus the column named in `resp_col` ('{resp_col}'). For a wide "
            "response matrix, use check_resp(resp2long(wide)), or get the "
            "diagnostics while widening with long2resp(df, check_resp=True)."
        )
    if resp_col not in x.columns:
        raise ValueError(f"Column specified by `resp_col` not found in `x`: {resp_col}")

    # Items are reported as the strings long2resp() uses for its columns, so a
    # flagged item can be looked up in the wide frame directly. R reports them
    # with an `item_` prefix; that prefix never reaches a caller here (#31).
    df = pd.DataFrame({"item": x["item"].astype(str), resp_col: x[resp_col]})
    return _check_resp(df, min_count=min_count, min_prop=min_prop, resp_col=resp_col)


def long2resp(
    df: pd.DataFrame,
    wave: Optional[int] = None,
    id_density_threshold: Optional[float] = 0.1,
    agg_method: Optional[Literal["mean", "mode", "median", "first"]] = None,
    check_resp: bool = False,
    resp_col: str = "resp",
) -> Union[pd.DataFrame, Tuple[pd.DataFrame, Dict[str, Any]]]:
    """
    Convert IRW long-format data to wide-format response matrix.
    
    Parameters
    ----------
    df : pandas.DataFrame
        Long-format DataFrame with columns: id, item, the column named by
        ``resp_col`` (and optionally wave).
    wave : int, optional
        Filter by wave. Defaults to most frequent wave if None.
    id_density_threshold : float, optional
        Minimum response density (0.0-1.0). None to disable. Default 0.1.
    agg_method : str, optional
        How to handle multiple id-item pairs: "mean", "mode", "median", "first".
        Defaults to "mean", or to "first" when no response is a number, so a
        text column is not averaged into NaN.
    check_resp : bool, default False
        If True, also run ``check_resp()`` with its default thresholds on the
        responses that make up the returned matrix, and return
        ``(wide, checks)``.
    resp_col : str, default "resp"
        Column holding the response values, e.g. ``"text"`` for nominal data
        whose responses are labels.
        
    Returns
    -------
    pandas.DataFrame or tuple
        Wide-format response matrix where rows are ids and columns are items.
        With ``check_resp=True``, a ``(wide, checks)`` tuple, where ``checks``
        is the dict ``check_resp()`` returns.
    """
    # Ensure required columns exist
    required_cols = ["id", "item"]
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing required IRW columns: {', '.join(missing_cols)}")
    if resp_col not in df.columns:
        raise ValueError(f"Column specified by `resp_col` not found in `df`: {resp_col}")
    
    # Stop execution if 'date' exists
    if "date" in df.columns:
        raise ValueError("This function does not yet support data with 'date'.")
    
    if agg_method is not None and agg_method not in ("mean", "mode", "median", "first"):
        raise ValueError("Invalid `agg_method`. Choose from 'mode', 'mean', 'median', or 'first'.")

    # Store messages to print at the end
    messages = []

    # Averaging a text column would turn every label into NaN, so the default
    # follows the data: numbers are averaged, labels keep the first.
    is_numeric_resp = _is_numeric_resp(df[resp_col])
    agg_method_defaulted = agg_method is None
    if agg_method is None:
        agg_method = "mean" if is_numeric_resp else "first"
        if not is_numeric_resp:
            messages.append(
                f"Response column '{resp_col}' is non-numeric; agg_method defaulted to 'first'."
            )
    elif agg_method in ("mean", "median") and not is_numeric_resp:
        raise ValueError(
            f"agg_method='{agg_method}' requires a numeric response column, but "
            f"`resp_col='{resp_col}'` has no numeric values. Use 'first' or 'mode' instead."
        )
    
    # Check for "rater" column and count unique raters
    if "rater" in df.columns:
        num_raters = df["rater"].nunique()
        messages.append(f"NOTE: This dataset contains 'rater' information with {num_raters} unique raters.")
    
    # Keep id, item and wave (if it exists), plus the response column, which is
    # called `resp` from here on whichever column it came from.
    essential_cols = ["id", "item"]
    if "wave" in df.columns:
        essential_cols.append("wave")
    
    resp_values = df[resp_col]
    df = df[essential_cols].copy()
    df["resp"] = resp_values
    
    # Handle wave filtering
    if "wave" in df.columns:
        if wave is None:
            # Find the most frequent wave
            wave_counts = df["wave"].value_counts()
            wave = wave_counts.index[0]  # Most frequent wave
            messages.append(f"Defaulting to the most frequent wave: {wave}")
        
        if wave in df["wave"].values:
            df = df[df["wave"] == wave].copy()
            messages.append(f"Filtering applied: Keeping only responses from wave {wave}")
        else:
            messages.append(f"Wave {wave} not found in data. No filtering applied.")
    
    # Ensure item names have "item_" prefix, remembering what each one was.
    #
    # The prefix is added so numeric item ids do not become numeric column
    # labels, and is stripped again after the pivot. Stripping it by
    # `col.replace("item_", "")` removed EVERY occurrence, so an item named
    # `myitem_x` was prefixed to `item_myitem_x` and came back as `myx`. Keep
    # the mapping instead of trying to reconstruct the original by string
    # surgery.
    #
    # Prefix UNCONDITIONALLY, including items already called `item_x`. Skipping
    # those made the mapping non-injective: `a` and `item_a` both became
    # `item_a`, so the pivot treated two distinct items as one and averaged
    # their responses together, reporting it as duplicate (id, item) pairs in
    # the user's data rather than as a collision this function created. The
    # doubled name (`item_item_a`) is internal and never reaches the caller.
    original_item_name = {}

    def _prefixed(value):
        name = f"item_{value}"
        original_item_name[name] = str(value)
        return name

    df["item"] = df["item"].apply(_prefixed)
    
    # Convert response values to numeric where the aggregation needs numbers.
    # "first" and "mode" work on labels, so a text column passes through them.
    if agg_method in ("mean", "median"):
        original_resp = df["resp"]
        df["resp"] = pd.to_numeric(df["resp"], errors="coerce")
        # Only a present value that failed to parse counts; a response that was
        # already missing is not a conversion failure.
        if (df["resp"].isna() & original_resp.notna()).any():
            messages.append("Some responses could not be converted to numeric. These have been set to NA.")
    
    # Compute total id count before filtering
    total_ids = df["id"].nunique()
    filtering_occurred = False
    
    # Apply filtering for sparse ids
    if id_density_threshold is not None:
        # Compute response density per id
        total_items = df["item"].nunique()
        id_resp_counts = df.groupby("id")["resp"].apply(
            lambda x: (~x.isna()).sum()
        ).reset_index(name="response_count")
        id_resp_counts["density"] = id_resp_counts["response_count"] / total_items
        
        # Filter ids based on density threshold
        ids_to_keep = id_resp_counts[
            id_resp_counts["density"] >= id_density_threshold
        ]["id"].values
        filtered_ids = set(df["id"].unique()) - set(ids_to_keep)
        
        if len(filtered_ids) > 0:
            percent_removed = round((len(filtered_ids) / total_ids) * 100, 2)
            messages.append(
                f"{len(filtered_ids)} ids removed ({len(filtered_ids)} out of {total_ids}, "
                f"{percent_removed}%) due to response density below threshold ({id_density_threshold})."
            )
            filtering_occurred = True
        
        df = df[df["id"].isin(ids_to_keep)].copy()
    
    # Provide message on how to disable filtering if it occurred
    if filtering_occurred:
        messages.append("To disable filtering, set `id_density_threshold = None`.")
    
    # Count duplicate id-item responses properly
    dup_summary = df.groupby(["id", "item"]).size().reset_index(name="count")
    total_unique_pairs = len(dup_summary)
    
    # Find only pairs with duplicates (more than 1 response)
    affected_pairs = dup_summary[dup_summary["count"] > 1]
    num_affected_pairs = len(affected_pairs)
    num_duplicate_responses = affected_pairs["count"].sum() - num_affected_pairs
    avg_duplicates_per_pair = (
        round(num_duplicate_responses / num_affected_pairs, 2)
        if num_affected_pairs > 0
        else 0
    )
    prop_dup_pairs = round((num_affected_pairs / total_unique_pairs) * 100, 2) if total_unique_pairs > 0 else 0
    
    if num_affected_pairs > 0:
        default_note = (
            f" (defaulted for {'numeric' if is_numeric_resp else 'non-numeric'} responses)"
            if agg_method_defaulted
            else ""
        )
        messages.append(
            f"Found {num_duplicate_responses} responses across {num_affected_pairs} unique id-item pairs "
            f"({prop_dup_pairs}% of total).\n"
            f"Average responses per pair: {avg_duplicates_per_pair}.\n"
            f"Aggregating responses based on agg_method='{agg_method}'{default_note}."
        )
    
    # Aggregation based on user input
    if agg_method == "mode":
        def mode_fn(x):
            x_clean = x.dropna()
            if len(x_clean) == 0:
                return np.nan
            # Calculate mode manually
            value_counts = x_clean.value_counts()
            if len(value_counts) == 0:
                return np.nan
            # Return the most frequent value (first if tie)
            return value_counts.index[0]
        
        df = df.groupby(["id", "item"])["resp"].apply(mode_fn).reset_index()
    elif agg_method == "mean":
        df = df.groupby(["id", "item"])["resp"].mean().reset_index()
    elif agg_method == "median":
        df = df.groupby(["id", "item"])["resp"].median().reset_index()
    elif agg_method == "first":
        df = df.drop_duplicates(subset=["id", "item"], keep="first")
    
    # Optional response checks, on the responses that are about to become the
    # matrix: aggregated, after the density filter, and under the item names
    # the caller will see as columns. R checks before its density filter; here
    # the diagnostics describe exactly the matrix that is returned.
    resp_checks = None
    if check_resp:
        checked = pd.DataFrame(
            {"item": df["item"].map(original_item_name), "resp": df["resp"]}
        )
        resp_checks = _check_resp(checked)
        total_items_chk = checked["item"].nunique()
        n_single = len(resp_checks["single_category_items"])
        n_sparse = len(resp_checks["sparse_category_items"])
        if n_single + n_sparse > 0:
            messages.append(
                "NOTE (check_resp): potential response issues detected.\n"
                f"  - Single-category items: {n_single} out of {total_items_chk}\n"
                f"  - Items with sparse categories: {n_sparse} out of {total_items_chk}\n"
                "For custom thresholds, call check_resp() on the long data."
            )
        else:
            messages.append(
                "NOTE (check_resp): no response issues detected with default thresholds."
            )
    
    # Convert to wide format
    wide_df = df.pivot(index="id", columns="item", values="resp").reset_index()
    
    # Restore the original item names (keep "id" as is)
    wide_df.columns = [
        col if col == "id" else original_item_name[col] for col in wide_df.columns
    ]
    
    # Print messages at the end
    if messages:
        print("\n".join(messages))
    
    if check_resp:
        return wide_df, resp_checks
    return wide_df


def resp2long(x: Union[pd.DataFrame, np.ndarray], id: bool = True) -> pd.DataFrame:
    """
    Convert a wide response matrix back to IRW long format.

    The inverse of ``long2resp()``, for data that was widened and then imputed,
    simulated, subset or handed to a package that returns a matrix, and now
    needs to be back in the format the rest of the package speaks.

    Parameters
    ----------
    x : pandas.DataFrame or numpy.ndarray
        Wide responses, one row per person and one column per item. Every
        column other than ``id`` is taken to be an item, so drop covariates
        first.
    id : bool, default True
        If True, ``x`` must have an ``id`` column, as ``long2resp()`` output
        does. If False and there is none, ids are generated as ``1..n`` by row
        position: they identify rows of this matrix and mean nothing outside
        it, so they cannot be joined back to the table the data came from. If
        False and an ``id`` column exists, it is used anyway.

    Returns
    -------
    pandas.DataFrame
        Columns ``id``, ``item``, ``resp``, item-major (every row of the first
        item, then the next). Items are the column labels unchanged, so the
        names ``long2resp()`` restored come back exactly -- including an item
        genuinely called ``item_something``. A missing cell becomes a row with
        a missing ``resp``; drop them with ``dropna(subset=["resp"])``. An
        array has no column labels, so its items are numbered ``1..k``.

    Examples
    --------
    >>> import irw
    >>> wide = irw.long2resp(irw.fetch("agn_kay_2025"))
    >>> long = irw.resp2long(wide)
    """
    if isinstance(x, np.ndarray):
        if x.ndim != 2:
            raise ValueError(f"`x` must be a 2-dimensional array, got {x.ndim} dimension(s).")
        # 1-based, like the generated ids and like simdata()'s items.
        x = pd.DataFrame(x, columns=range(1, x.shape[1] + 1))
    elif not isinstance(x, pd.DataFrame):
        raise ValueError("`x` must be a pandas DataFrame or a numpy array.")

    has_id_col = "id" in x.columns

    if id and not has_id_col:
        raise ValueError(
            "No `id` column found in `x`.\n"
            "resp2long() defaults to `id=True` (it expects the output of long2resp()).\n"
            "If your wide matrix has no id column, call resp2long(x, id=False); "
            "if the ids are the index, call resp2long(x.reset_index())."
        )
    if not id and has_id_col:
        print("`id=False` ignored because an `id` column already exists; using existing ids.")

    if has_id_col:
        id_values = x["id"].to_numpy()
        item_positions = [j for j, col in enumerate(x.columns) if col != "id"]
    else:
        id_values = np.arange(1, len(x) + 1)
        item_positions = list(range(x.shape[1]))

    if not item_positions:
        raise ValueError("No item columns found to convert.")

    # Deliberately no prefix strip. R's long2resp() returns `item_<name>`
    # columns; this package's returns the original names (#30, #31), so there
    # is nothing to remove -- and stripping a leading `item_` would rename a
    # genuine item called `item_a` to `a`, reintroducing on the way back the
    # merge #31 fixed on the way out.
    n_rows = len(x)
    return pd.DataFrame(
        {
            "id": np.tile(id_values, len(item_positions)),
            "item": np.repeat(np.array([x.columns[j] for j in item_positions], dtype=object), n_rows),
            # pd.concat rather than numpy, so a nullable column (Int64) keeps
            # its dtype instead of degrading to object.
            "resp": pd.concat([x.iloc[:, j] for j in item_positions], ignore_index=True),
        }
    )
