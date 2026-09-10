"""InterModel Vigorish: compare two sets of predictions for binary outcomes.

Ports `irw_imv()` from `Rpkg/R/imv.R` (issue #56). Domingue et al. (2021),
https://doi.org/10.31235/osf.io/8sgz5.

The one piece that is not a line-for-line port is the coin solve. R minimises
``|p log p + (1-p) log(1-p) - log(a)|`` over ``[1e-3, 1 - 1e-3]`` with
``stats::nlminb`` started at 0.5. The entropy term is symmetric about 0.5, so
that problem has two roots, and R always lands on the upper one (p >= 0.5).
On that branch the term is strictly increasing, so a bisection finds the same
root without adding scipy as a dependency. The behaviour at the edges matches
R's as well:

- a geometric mean likelihood at or below 0.5 (a model no better than a fair
  coin) has no root; the minimiser is 0.5, which R returns as its start value.
- one above the entropy at ``1 - 1e-3`` has no root inside the bounds; R stops
  at the bound, and so does this.

Where the two differ is precision. The bisection runs to machine precision.
``nlminb`` agrees with it to about 1e-8 for almost every input, but on a small
fraction of likelihoods between 0.5 and 0.7 it stops early, by up to ~2e-4 in
the IMV (`tests/test_imv.py` pins one such case). The value here is the exact
root; R's is the approximation.
"""

from __future__ import annotations

import math
from typing import Any, Sequence, Union

import numpy as np
import pandas as pd

# R's upper bound on the coin weight, from `.irw_imv_coin()`. Its lower bound,
# 1e-3, is never reached: R's search starts at 0.5 and finds the upper root.
_COIN_UPPER = 1 - 1e-3


def _neg_entropy(p: float) -> float:
    """``p log p + (1-p) log(1-p)``: log(0.5) at p = 0.5, rising towards 0."""
    return p * math.log(p) + (1 - p) * math.log(1 - p)


def _coin(a: float) -> float:
    """Weight of the coin (>= 0.5) whose entropy matches likelihood ``a``."""
    # `a <= 0.5` before the log also covers a = 0, which a tiny `eps` reaches.
    if a <= 0.5:
        return 0.5
    target = math.log(a)
    if target >= _neg_entropy(_COIN_UPPER):
        return _COIN_UPPER
    lo, hi = 0.5, _COIN_UPPER
    # Each step halves an interval that starts under 0.5 wide, so 100 steps
    # is past machine precision; the loop normally exits on the width check.
    for _ in range(100):
        mid = (lo + hi) / 2
        if _neg_entropy(mid) < target:
            lo = mid
        else:
            hi = mid
        if hi - lo <= 1e-15:
            break
    return (lo + hi) / 2


def _gml(resp: np.ndarray, p: np.ndarray) -> float:
    """Geometric mean likelihood of predictions ``p`` for outcomes ``resp``."""
    return float(np.exp(np.mean(np.log(p) * resp + np.log(1 - p) * (1 - resp))))


def _as_numeric(values: Any) -> np.ndarray:
    """Coerce outcomes or predictions to a float vector, refusing non-numbers.

    Missing values survive as NaN so the NA check can name them; booleans and
    strings are refused, as R's ``is.numeric()`` refuses logicals and
    characters.
    """
    series = values if isinstance(values, pd.Series) else pd.Series(
        np.asarray(values, dtype=object).ravel()
    )
    if pd.api.types.is_bool_dtype(series) or not (
        pd.api.types.is_numeric_dtype(series)
        or pd.api.types.infer_dtype(series, skipna=True)
        in ("integer", "floating", "mixed-integer-float", "empty")
    ):
        raise ValueError("Outcomes and predictions must be numeric.")
    return pd.to_numeric(series).to_numpy(dtype=float, na_value=np.nan)


def imv(
    data: Union[pd.DataFrame, Sequence[float]],
    p1: Union[str, Sequence[float]],
    p2: Union[str, Sequence[float]],
    resp: str = "resp",
    eps: float = 1e-6,
) -> float:
    """
    InterModel Vigorish (IMV) of two sets of predicted probabilities.

    Compares two sets of predictions for the same binary outcomes. Each
    model's geometric mean likelihood is converted to the weight of a coin
    with the same entropy, and the IMV is the proportional gain in that
    weight. 0 means the two predict equally well; 0.05 means model 2 is
    equivalent to a coin 5% more predictable than model 1's.

    The statistic is not symmetric: ``imv(data, "p1", "p2")`` is the gain from
    moving from ``p1`` to ``p2``, and swapping the arguments does not simply
    flip the sign.

    Predictions are clamped to ``[eps, 1 - eps]`` before the likelihoods are
    computed, so a prediction of exactly 0 or 1 does not produce an infinite
    log-likelihood.

    Out-of-sample predictions are the intended use: comparing in-sample
    predictions favours the more flexible model by construction.

    Parameters
    ----------
    data : pandas.DataFrame or sequence of float
        A frame holding the outcomes and both sets of predictions, or the
        binary outcomes themselves.
    p1 : str or sequence of float
        Predictions from the baseline model: a column name in ``data``, or a
        vector of probabilities when ``data`` is a vector of outcomes.
    p2 : str or sequence of float
        Predictions from the comparison model, in the same form as ``p1``.
    resp : str, default 'resp'
        Column holding the 0/1 outcome. Ignored when ``data`` is a vector.
    eps : float, default 1e-6
        Clamping tolerance for the predictions, in (0, 0.5).

    Returns
    -------
    float
        The proportional gain in coin weight from ``p1`` to ``p2``.

    Raises
    ------
    ValueError
        If a column is missing, the inputs are not numeric, their lengths
        differ, there are no observations, any value is missing, an outcome is
        not 0/1, a prediction is outside [0, 1], or ``eps`` is outside
        (0, 0.5).

    References
    ----------
    Domingue, B. W., Rahal, C., Faul, J., Freese, J., Kanopka, K., Rigos, A.,
    Stenhaug, B., & Tripathi, A. (2021). InterModel Vigorish (IMV): A novel
    approach for quantifying predictive accuracy with binary outcomes.
    https://doi.org/10.31235/osf.io/8sgz5

    Examples
    --------
    >>> import irw
    >>> truth = [1] * 7 + [0] * 3
    >>> # A model that knows the base rate beats one that guesses at random.
    >>> round(irw.imv(truth, p1=[0.5] * 10, p2=[0.7] * 10), 6)
    0.4
    """
    if isinstance(data, pd.DataFrame):
        for name in (resp, p1, p2):
            if not isinstance(name, str):
                raise ValueError(
                    "When 'data' is a DataFrame, 'resp', 'p1' and 'p2' must be "
                    "column names."
                )
            if name not in data.columns:
                raise ValueError(f"Column not found in 'data': {name}")
        y_raw, v1_raw, v2_raw = data[resp], data[p1], data[p2]
    else:
        y_raw, v1_raw, v2_raw = data, p1, p2

    if (
        isinstance(eps, bool)
        or not isinstance(eps, (int, float, np.integer, np.floating))
        or not (0 < eps < 0.5)  # also false for NaN
    ):
        raise ValueError("'eps' must be a single number in (0, 0.5).")

    y, v1, v2 = (_as_numeric(v) for v in (y_raw, v1_raw, v2_raw))
    if not (y.size == v1.size == v2.size):
        raise ValueError(
            "Outcomes and predictions must have the same length: got "
            f"{y.size}, {v1.size}, {v2.size}."
        )
    if y.size == 0:
        raise ValueError("No observations to compare.")
    if np.isnan(y).any() or np.isnan(v1).any() or np.isnan(v2).any():
        raise ValueError(
            "Outcomes and predictions must not contain missing values. "
            "Drop incomplete rows first."
        )
    if not np.isin(y, (0, 1)).all():
        raise ValueError("The IMV is defined for binary outcomes; 'resp' must be 0/1.")
    if ((v1 < 0) | (v1 > 1)).any() or ((v2 < 0) | (v2 > 1)).any():
        raise ValueError("Predictions must be probabilities in [0, 1].")

    v1 = np.clip(v1, eps, 1 - eps)
    v2 = np.clip(v2, eps, 1 - eps)

    c1 = _coin(_gml(y, v1))
    c2 = _coin(_gml(y, v2))
    return (c2 - c1) / c1
