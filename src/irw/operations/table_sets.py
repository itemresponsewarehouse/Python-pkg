"""Server-side value-set summaries of an IRW table.

Ported from Rpkg/R/aggregate.R (`irw_table_sets()`). Every answer here comes
from an aggregate query that Redivis runs server-side, so only the handful of
result rows crosses the wire. The table itself is never exported: the Python
client reads a query result through a read session and never routes a query
through the export API (`should_use_export_api` is False for anything that is
not a table), which is why this does not draw down the export quota the way
`fetch()` on a 68-million-row table does.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional
import warnings

import pandas as pd
import redivis

from ..utils.redivis.tables import (
    _classify_error,
    _format_error,
    _get_table,
    _retry_transient,
    _sanitize_error,
    _search_datasets,
    _terminal_error_message,
)

# `resp` is stored as a string and a missing response is the literal "NA"
# token, sometimes "". Without this filter the distinct-response set gains a
# phantom level that fetch() never shows, because fetch() coerces both to NaN.
_NOT_MISSING = "resp IS NOT NULL AND TRIM(CAST(resp AS STRING)) NOT IN ('NA', '')"


def _run_query(sql: str, table_name: str) -> pd.DataFrame:
    """Run one Redivis SQL query and return its (small) result.

    Errors go through the same classification as fetch() and download(): a
    query can hit the rate limit or an expired login just as a download can,
    and those must not surface as a raw exception. Transient read failures are
    retried.
    """
    def _query() -> pd.DataFrame:
        return redivis.query(sql).to_pandas_dataframe(progress=False)

    try:
        return _retry_transient(_query)
    except Exception as e:
        if _classify_error(e) in ("quota", "auth"):
            raise RuntimeError(_terminal_error_message(e, table_name)) from e
        raise RuntimeError(
            f"\nAn error occurred while querying IRW table '{table_name}': "
            f"{_sanitize_error(_format_error(e))}"
        ) from e


def _coerce_resp_set(values: List[str], source: str = "main") -> List[Any]:
    """Coerce a distinct-response value set to numbers when every value parses.

    Numeric when all values are numeric, strings otherwise -- and always
    strings for the ``nom`` source, because nominal responses are category
    labels, not numbers, even when they are spelled with digits.
    """
    values = [str(v) for v in values]
    if source == "nom" or not values:
        return sorted(set(values))
    num = pd.to_numeric(pd.Series(values, dtype=object), errors="coerce")
    if num.isna().any():
        return sorted(set(values))
    # "1" and "1.0" are distinct strings but the same number.
    return sorted(set(num.tolist()))


def table_sets(
    datasets: List[Any],
    name: str,
    *,
    source: str = "main",
    per_item: bool = False,
) -> Dict[str, Any]:
    """
    Summarize the value sets of one IRW table without downloading it.

    Parameters
    ----------
    datasets : List[Any]
        Redivis dataset objects to search, in order, for the table.
    name : str
        Bare table name. Resolved exactly as fetch() resolves it, so the
        current reference id and shard are looked up rather than assumed.
    source : str, default "main"
        Resolved source name; only ``"nom"`` changes behaviour here.
    per_item : bool, default False
        Also run the per-item summary query.

    Returns
    -------
    dict
        ``table``, ``n_rows``, ``items``, ``resp`` and ``per_item``; see
        irw.table_sets() for their meaning.

    Raises
    ------
    ValueError
        If the table does not exist, or the lookup fails terminally (quota,
        authentication, invalid table).
    RuntimeError
        If a query fails.
    """
    if not isinstance(name, str) or not name:
        raise ValueError("table_name must be a single non-empty table name.")

    tbl, last_other, terminal = _search_datasets(
        datasets, lambda ds: _get_table(ds, name)
    )
    if terminal is not None:
        raise ValueError(_terminal_error_message(terminal, name)) from terminal
    if tbl is None:
        if last_other is not None:
            raise ValueError(
                f"Error looking up table '{name}': {_format_error(last_other)}"
            ) from last_other
        raise ValueError(f"Table '{name}' does not exist in the IRW database.")

    ref = tbl.qualified_reference
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", message=".*No reference id was provided for the table.*")
        variables = {v.name for v in _retry_transient(tbl.list_variables)}

    counts = _run_query(f"SELECT COUNT(*) AS n FROM `{ref}`", name)
    n_rows = int(counts["n"].iloc[0])

    # Sorted here as well as in SQL: a query result is read back through
    # parallel read streams, so ORDER BY is not a promise about row order.
    items: Optional[List[str]] = None
    if "item" in variables:
        items_df = _run_query(
            f"SELECT DISTINCT CAST(item AS STRING) AS item FROM `{ref}` "
            "WHERE item IS NOT NULL ORDER BY item",
            name,
        )
        items = sorted(str(v) for v in items_df["item"].tolist())

    resp: Optional[List[Any]] = None
    if "resp" in variables:
        resp_df = _run_query(
            f"SELECT DISTINCT TRIM(CAST(resp AS STRING)) AS resp FROM `{ref}` "
            f"WHERE {_NOT_MISSING} ORDER BY resp",
            name,
        )
        resp = _coerce_resp_set(resp_df["resp"].tolist(), source=source)

    per_item_df: Optional[pd.DataFrame] = None
    if per_item and {"item", "resp"}.issubset(variables):
        per_item_df = _run_query(
            "SELECT CAST(item AS STRING) AS item, COUNT(*) AS n, "
            "MIN(SAFE_CAST(TRIM(CAST(resp AS STRING)) AS FLOAT64)) AS resp_min, "
            "MAX(SAFE_CAST(TRIM(CAST(resp AS STRING)) AS FLOAT64)) AS resp_max, "
            "COUNT(DISTINCT TRIM(CAST(resp AS STRING))) AS n_resp_levels "
            f"FROM `{ref}` WHERE {_NOT_MISSING} GROUP BY item ORDER BY item",
            name,
        )
        per_item_df = per_item_df.sort_values("item", kind="stable").reset_index(drop=True)

    return {
        "table": ref,
        "n_rows": n_rows,
        "items": items,
        "resp": resp,
        "per_item": per_item_df,
    }
