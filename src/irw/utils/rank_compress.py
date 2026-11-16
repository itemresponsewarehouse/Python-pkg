"""Compress the positions of a rankings table (twin of Rpkg's irw_rank_compress())."""
from typing import FrozenSet

import pandas as pd

#: The reserved item values of a rankings table (data/rankings/README.md, "Layout").
RESERVED: FrozenSet[str] = frozenset({"(write-in)", "(overvote)"})


def rank_compress(df: pd.DataFrame, drop_repeats: bool = True, ties: str = "error") -> pd.DataFrame:
    """Renumber a rankings table's positions ``1..k`` within each ranking.

    Rankings tables (``source="rank"``) keep the core column names, but
    ``resp`` is a *position*: one row per respondent (``id``), ranking
    (``task``) and marked position, ``resp = 1`` for the first choice, **as
    recorded**. Three things in the stored table need care before a
    person-by-item pivot or a Plackett-Luce fit: a skipped position stays a
    gap (a ballot marked at positions 1 and 3 keeps ``resp`` 1 and 3), a
    candidate marked twice keeps both rows, and two items at the same
    position are a tie (Debian ballots allow equal ranks). This function
    closes the gaps and, by default, drops repeated marks.

    It is **not** a jurisdiction's counting rule. It does not stop a ballot
    at an overvote, does not treat a second write-in mark as a duplicate, and
    does not apply a limit on consecutive skips; where those rules matter,
    the rankings family's ``counting_rules.csv`` records them per
    jurisdiction and the count has to apply them itself.

    Within each ``id`` x ``task``, rows are taken in ``resp`` order. With
    ``drop_repeats=True`` an item already marked at an earlier position is
    dropped at every later one (most counting rules treat the later mark as a
    skip). The reserved items ``(write-in)`` and ``(overvote)`` are never
    dropped: two write-in rows are usually two different people, and two
    overvote rows are two separate events. Then ``resp`` is renumbered
    ``1..k`` in rank order, so the gaps close. Every other column is kept as
    it was. The result is sorted by ``id``, ``task``, ``resp`` and then
    ``item``.

    A tie (two or more items at one position, after repeats are dropped) has
    no order to renumber by. With ``ties="error"``, the default, the function
    raises and says how many rankings have one. With ``ties="keep"`` the
    tied items share one position and the positions are renumbered densely,
    so a ballot recorded as 1, 1, 3 or as 1, 1, 2 comes back as 1, 1, 2.
    Version 0.8.0 and earlier renumbered tied items in input row order, which
    put one of them ahead of the other without the voter having said so.

    The R package's ``irw_rank_compress()`` does exactly the same; the two are
    tested on one shared fixture.

    Parameters
    ----------
    df : pandas.DataFrame
        A rankings table, as returned by ``fetch(name, source="rank")``:
        columns ``id``, ``item``, ``resp``, and ``task`` (taken as a single
        task when absent).
    drop_repeats : bool, default True
        Drop an item's later marks within a ranking?
    ties : {"error", "keep"}, default "error"
        ``"error"`` raises when any ranking has two items at one position;
        ``"keep"`` gives tied items one shared position.

    Returns
    -------
    pandas.DataFrame
        The same columns as ``df``, ``resp`` an integer running ``1..k``
        within each ``id`` x ``task`` (with repeats where items are tied,
        under ``ties="keep"``).

    Examples
    --------
    >>> d = pd.DataFrame({"id": [1, 1, 1, 2], "task": 1,
    ...                   "item": ["c01", "c03", "c01", "c02"], "resp": [1, 3, 4, 2]})
    >>> rank_compress(d)
    >>> rank_compress(d, drop_repeats=False)
    >>> tied = pd.DataFrame({"id": 1, "task": 1, "item": ["A", "B", "C"], "resp": [1, 1, 3]})
    >>> rank_compress(tied, ties="keep")
    """
    missing = [c for c in ("id", "item", "resp") if c not in df.columns]
    if missing:
        raise ValueError(f"Not a rankings table: missing {', '.join(missing)}.")
    if drop_repeats not in (True, False):
        raise ValueError("`drop_repeats` must be True or False.")
    if ties not in ("error", "keep"):
        raise ValueError('`ties` must be "error" or "keep".')
    resp = pd.to_numeric(df["resp"], errors="coerce")
    if resp.isna().any() or (resp < 1).any() or (resp != resp.round()).any():
        raise ValueError("`resp` must be positive integer positions in a rankings table.")

    out = df.copy()
    if out.empty:
        out["resp"] = out["resp"].astype(int)
        return out.reset_index(drop=True)

    task = out["task"] if "task" in out.columns else pd.Series(1, index=out.index)
    out["_resp"] = resp
    out["_task"] = task
    out["_item"] = out["item"].astype(str)
    out = out.sort_values(["id", "_task", "_resp", "_item"], kind="stable")

    if drop_repeats:
        repeatable = out["item"].isin(RESERVED)
        keep = repeatable | ~out.duplicated(["id", "_task", "item"], keep="first")
        out = out[keep]

    tied = out.duplicated(["id", "_task", "_resp"], keep="first")
    if tied.any():
        if ties == "error":
            n_tied = out.loc[tied, ["id", "_task"]].drop_duplicates().shape[0]
            raise ValueError(
                f"{n_tied} ranking{' has' if n_tied == 1 else 's have'} a tie (two or more items at "
                "one position), so there is no order to renumber by. "
                'Pass `ties="keep"` to give tied items one shared position.'
            )
        # dense positions: rows are in resp order, so a new position starts at every
        # row that does not repeat the previous row's resp
        out["resp"] = (~tied).astype(int).groupby([out["id"], out["_task"]], sort=False).cumsum().astype(int)
    else:
        out["resp"] = out.groupby(["id", "_task"], sort=False).cumcount().add(1).astype(int)
    out = out.drop(columns=["_resp", "_task", "_item"]).reset_index(drop=True)
    return out
