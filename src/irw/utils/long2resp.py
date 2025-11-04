"""Efficient long2resp conversion function."""

from __future__ import annotations
from typing import Optional, Literal
import pandas as pd
import numpy as np


def long2resp(
    df: pd.DataFrame,
    wave: Optional[int] = None,
    id_density_threshold: Optional[float] = 0.1,
    agg_method: Literal["mean", "mode", "median", "first"] = "mean"
) -> pd.DataFrame:
    """
    Convert IRW long-format data to wide-format response matrix.
    
    Parameters
    ----------
    df : pandas.DataFrame
        Long-format DataFrame with columns: id, item, resp (and optionally wave).
    wave : int, optional
        Filter by wave. Defaults to most frequent wave if None.
    id_density_threshold : float, optional
        Minimum response density (0.0-1.0). None to disable. Default 0.1.
    agg_method : str, default "mean"
        How to handle multiple id-item pairs: "mean", "mode", "median", "first".
        
    Returns
    -------
    pandas.DataFrame
        Wide-format response matrix where rows are ids and columns are items.
    """
    # Ensure required columns exist
    required_cols = ["id", "item", "resp"]
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing required IRW columns: {', '.join(missing_cols)}")
    
    # Stop execution if 'date' exists
    if "date" in df.columns:
        raise ValueError("This function does not yet support data with 'date'.")
    
    # Store messages to print at the end
    messages = []
    
    # Check for "rater" column and count unique raters
    if "rater" in df.columns:
        num_raters = df["rater"].nunique()
        messages.append(f"NOTE: This dataset contains 'rater' information with {num_raters} unique raters.")
    
    # Drop non-essential columns except id, item, resp, and wave (if exists)
    essential_cols = ["id", "item", "resp"]
    if "wave" in df.columns:
        essential_cols.append("wave")
    
    df = df[essential_cols].copy()
    
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
    
    # Ensure item names have "item_" prefix
    df["item"] = df["item"].apply(
        lambda x: x if str(x).startswith("item_") else f"item_{x}"
    )
    
    # Convert response values to numeric
    df["resp"] = pd.to_numeric(df["resp"], errors="coerce")
    
    # Warn if non-numeric responses exist
    if df["resp"].isna().any():
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
        messages.append(
            f"Found {num_duplicate_responses} responses across {num_affected_pairs} unique id-item pairs "
            f"({prop_dup_pairs}% of total).\n"
            f"Average responses per pair: {avg_duplicates_per_pair}.\n"
            f"Aggregating responses based on agg_method='{agg_method}'."
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
    else:
        raise ValueError(f"Invalid `agg_method`. Choose from 'mode', 'mean', 'median', or 'first'.")
    
    # Convert to wide format
    wide_df = df.pivot(index="id", columns="item", values="resp").reset_index()
    
    # Remove "item_" prefix from column names (keep "id" as is)
    wide_df.columns = [col.replace("item_", "") if col != "id" else col for col in wide_df.columns]
    
    # Print messages at the end
    if messages:
        print("\n".join(messages))
    
    return wide_df

