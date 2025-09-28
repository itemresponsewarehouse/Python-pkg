from __future__ import annotations
from typing import Optional, Any
import warnings
import logging
import numpy as np
import pandas as pd

from ._io import retry, get_table, classify_error, format_error

logger = logging.getLogger(__name__)


def fetch_one(irw: "IRW", name: str, *, sim: bool, dedup: bool) -> Optional[pd.DataFrame]:
    """
    Internal: fetch a single table using the IRW instance's cached datasets.
    """
    if not isinstance(name, str) or name == "":
        logger.warning(f"Table '{name}' cannot be fetched due to an invalid format.")
        return None

    last_err: Exception | None = None
    for ds in irw._datasets(sim):
        try:
            tbl = retry(lambda: get_table(ds, name))
            df = tbl.to_pandas_dataframe()

            # --- inline transforms needed only for fetch() ---

            # Coerce 'resp' to numeric (exact R-style warning text)
            if "resp" in df.columns:
                s = df["resp"]

                def missing_like(x) -> bool:
                    if x is None:
                        return True
                    if isinstance(x, float) and pd.isna(x):
                        return True
                    if isinstance(x, str) and x.strip().upper() in {"", "NA", "NAN", "NULL"}:
                        return True
                    return False

                if pd.api.types.is_object_dtype(s) or pd.api.types.is_string_dtype(s):
                    coerced = pd.to_numeric(s.where(s.map(missing_like), s), errors="coerce")
                    bad = (~s.map(missing_like)) & coerced.isna()
                    if bad.any():
                        warnings.warn(
                            f"In dataset '{name}': 'resp' column contained non-numeric values that could not be coerced. Some NAs were introduced.",
                            RuntimeWarning,
                            stacklevel=2,
                        )
                    df = df.copy()
                    df["resp"] = coerced

            # Dedup without pandas GroupBy.apply warning
            if dedup:
                if "date" in df.columns:
                    logger.info(f"Deduplication skipped for dataset '{name}': 'date' column detected (timestamped responses).")
                elif {"id", "item"}.issubset(df.columns):
                    keys = ["id", "item"] + (["wave"] if "wave" in df.columns else [])
                    msg = (
                        f"Deduplicated dataset '{name}': one response randomly retained per (id, item, wave) group."
                        if "wave" in df.columns
                        else f"Deduplicated dataset '{name}': one response randomly retained per (id, item) pair."
                    )
                    n0 = len(df)
                    rng = np.random.RandomState(42)
                    groups = df.groupby(keys, dropna=False).indices  # dict[group_key -> ndarray of row indices]
                    if groups:
                        chosen_idx = [rng.choice(ix) for ix in groups.values()]
                        df = df.loc[sorted(chosen_idx)].reset_index(drop=True)
                        if len(df) < n0:
                            logger.info(msg)
                        else:
                            logger.info(f"Deduplication not needed for dataset '{name}': no duplicate responses found.")
                    else:
                        logger.info(f"Deduplication not needed for dataset '{name}': no duplicate responses found.")
                # else: if missing id/item, silently skip dedup (matches R spirit)

            return df

        except Exception as e:
            last_err = e
            kind = classify_error(e)
            if kind == "invalid_request":
                logger.warning(f"Table '{name}' cannot be fetched due to an invalid format.")
                return None
            # not_found/transient/unknown → try next dataset
            continue

    # After searching all datasets
    if last_err is None or classify_error(last_err) == "not_found":
        logger.warning(f"Table '{name}' does not exist in the IRW database.")
    else:
        logger.error(f"Error fetching dataset '{name}' : {format_error(last_err)}")
    return None