"""Convert a conjoint table to the IRW long format (twin of Rpkg's irw_conj_long())."""
import re
from typing import Iterable, Optional

import pandas as pd

_OUTCOME = re.compile(r"^(choice|rating)(_.+)?$")


def conj_long(df: pd.DataFrame, outcomes: Optional[Iterable[str]] = None) -> pd.DataFrame:
    """Stack a conjoint table's outcomes into id / item / resp.

    Conjoint tables (``source="conj"``) have one row per respondent, task and
    profile: ``id``, ``task``, ``profile``, the outcomes (``choice``, ``rating``
    and any ``choice_<name>`` / ``rating_<name>``), attribute levels in
    ``attr_*`` and respondent covariates in ``cov_*``. There is no ``item`` or
    ``resp``, because every profile is a new random bundle of attributes.

    This puts the outcomes into the core layout so tools built for
    id/item/resp can be used: ``item`` is the outcome's name, ``resp`` its
    value, and task, profile and the attributes become ``trial_`` columns.
    Rows with a missing outcome are dropped.

    Parameters
    ----------
    df : pandas.DataFrame
        A conjoint table, as returned by ``fetch(name, source="conj")``.
    outcomes : iterable of str, optional
        Outcome columns to keep. Default: all of them.
    """
    need = ["id", "task", "profile"]
    missing = [c for c in need if c not in df.columns]
    if missing:
        raise ValueError(f"Not a conjoint table: missing {', '.join(missing)}.")
    all_out = [c for c in df.columns if _OUTCOME.match(c)]
    outcomes = list(all_out if outcomes is None else outcomes)
    bad = [o for o in outcomes if o not in all_out]
    if bad:
        raise ValueError(f"Not outcome columns: {', '.join(bad)}")
    if not outcomes:
        raise ValueError("No outcome columns (choice, rating, ...) in this table.")

    rest = [c for c in df.columns if c not in all_out and c not in ("task", "profile")]
    attrs = [c for c in rest if c.startswith("attr_")]
    others = [c for c in rest if c != "id" and c not in attrs]
    pieces = []
    for o in outcomes:
        x = df[df[o].notna()]
        out = pd.DataFrame({"id": x["id"].values, "item": o,
                            "resp": pd.to_numeric(x[o]).values,
                            "trial_task": x["task"].values, "trial_profile": x["profile"].values})
        for a in attrs:
            out["trial_" + a] = x[a].values
        for c in others:
            out[c] = x[c].values
        pieces.append(out)
    long = pd.concat(pieces, ignore_index=True)
    return long.sort_values(["id", "trial_task", "trial_profile", "item"], kind="stable").reset_index(drop=True)
