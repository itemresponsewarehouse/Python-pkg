"""Item correlation and covariance matrices (twin of Rpkg's irw_cov_compute() and
irw_cov2matrix()).

The covariance family (``source="cov"``) holds published matrices, one row per
cell of the lower triangle (ben-domingue/irw ``data/covariance/README.md``).
Matrices derived from IRW tables are never stored (Ben, 2026-11-17); this module
computes one from a core table, and turns either kind back into a square matrix.

Polychoric and tetrachoric correlations are computed here with numpy alone,
because scipy is not a dependency of this package. The method is psych's
two-step estimator: each item's thresholds from its own marginal proportions,
then the correlation that maximises the bivariate-normal likelihood of each
pair's contingency table, with 0.3 added to empty cells as psych does by
default. Results agree with psych::polychoric to about two decimals, not
exactly.
"""
from __future__ import annotations

import math
from typing import Callable, Optional, Union

import numpy as np
import pandas as pd

COV_COLUMNS = ("stat", "var1", "var2", "value", "n")
METHODS = ("pearson", "spearman", "kendall", "polychoric", "tetrachoric")

# --- normal distribution helpers (no scipy) ---------------------------------

_SQRT2 = math.sqrt(2.0)


def _pnorm(x):
    x = np.asarray(x, dtype=float)
    return 0.5 * (1.0 + np.vectorize(math.erf)(x / _SQRT2))


def _qnorm(p: float) -> float:
    """Acklam's inverse normal CDF (relative error < 1.2e-9)."""
    a = (-3.969683028665376e01, 2.209460984245205e02, -2.759285104469687e02,
         1.383577518672690e02, -3.066479806614716e01, 2.506628277459239e00)
    b = (-5.447609879822406e01, 1.615858368580409e02, -1.556989798598866e02,
         6.680131188771972e01, -1.328068155288572e01)
    c = (-7.784894002430293e-03, -3.223964580411365e-01, -2.400758277161838e00,
         -2.549732539343734e00, 4.374664141464968e00, 2.938163982698783e00)
    d = (7.784695709041462e-03, 3.224671290700398e-01, 2.445134137142996e00,
         3.754408661907416e00)
    if p <= 0:
        return -8.0
    if p >= 1:
        return 8.0
    lo = 0.02425
    if p < lo:
        q = math.sqrt(-2 * math.log(p))
        return (((((c[0] * q + c[1]) * q + c[2]) * q + c[3]) * q + c[4]) * q + c[5]) / \
               ((((d[0] * q + d[1]) * q + d[2]) * q + d[3]) * q + 1)
    if p > 1 - lo:
        return -_qnorm(1 - p)
    q = p - 0.5
    r = q * q
    return (((((a[0] * r + a[1]) * r + a[2]) * r + a[3]) * r + a[4]) * r + a[5]) * q / \
           (((((b[0] * r + b[1]) * r + b[2]) * r + b[3]) * r + b[4]) * r + 1)


_GL_X, _GL_W = np.polynomial.legendre.leggauss(40)


def _pbvn_grid(h: np.ndarray, k: np.ndarray, rho: float) -> np.ndarray:
    """P(X <= h_i, Y <= k_j) for a standard bivariate normal with correlation rho.

    Plackett's identity: Phi2 = Phi(h)Phi(k) + integral_0^rho phi2(h, k; r) dr,
    the integral by 40-point Gauss-Legendre. Thresholds are clipped to +-8 so an
    infinite bound gives the right limit rather than inf * 0.
    """
    h = np.clip(np.asarray(h, float), -8, 8)[:, None, None]
    k = np.clip(np.asarray(k, float), -8, 8)[None, :, None]
    r = (rho / 2.0) * (_GL_X + 1.0)          # nodes on [0, rho]
    w = (rho / 2.0) * _GL_W
    one = 1.0 - r * r
    dens = np.exp(-(h * h - 2 * r * h * k + k * k) / (2 * one)) / (2 * math.pi * np.sqrt(one))
    return _pnorm(h[:, :, 0]) * _pnorm(k[:, :, 0]) + (dens * w).sum(axis=2)


def _thresholds(x: np.ndarray, levels: np.ndarray) -> np.ndarray:
    x = x[~np.isnan(x)]
    cum = np.cumsum([np.mean(x == v) for v in levels])[:-1]
    return np.array([-8.0] + [_qnorm(p) for p in cum] + [8.0])


def _polychoric_pair(x, y, lx, ly, tx, ty, correct: float = 0.3) -> float:
    ok = ~np.isnan(x) & ~np.isnan(y)
    if ok.sum() < 3:
        return float("nan")
    xi = np.searchsorted(lx, x[ok])
    yi = np.searchsorted(ly, y[ok])
    counts = np.zeros((len(lx), len(ly)))
    np.add.at(counts, (xi, yi), 1)
    counts[counts == 0] = correct

    def loglik(rho):
        F = _pbvn_grid(tx, ty, rho)
        p = F[1:, 1:] - F[:-1, 1:] - F[1:, :-1] + F[:-1, :-1]
        return float((counts * np.log(np.clip(p, 1e-300, None))).sum())

    lo, hi = -0.995, 0.995                    # golden-section search for the maximum
    g = (math.sqrt(5) - 1) / 2
    a, b = lo + (1 - g) * (hi - lo), lo + g * (hi - lo)
    fa, fb = loglik(a), loglik(b)
    for _ in range(60):
        if fa < fb:
            lo, a, fa = a, b, fb
            b = lo + g * (hi - lo)
            fb = loglik(b)
        else:
            hi, b, fb = b, a, fa
            a = lo + (1 - g) * (hi - lo)
            fa = loglik(a)
    return (lo + hi) / 2


def _polychoric(wide: np.ndarray) -> np.ndarray:
    k = wide.shape[1]
    levels = [np.unique(wide[~np.isnan(wide[:, j]), j]) for j in range(k)]
    thr = [_thresholds(wide[:, j], levels[j]) for j in range(k)]
    m = np.eye(k)
    for i in range(k):
        for j in range(i):
            m[i, j] = m[j, i] = _polychoric_pair(wide[:, i], wide[:, j], levels[i], levels[j], thr[i], thr[j])
    return m


def _kendall_cov(x, y) -> float:
    """R's cov(method = "kendall"): the sum of sign(x_i - x_j) * sign(y_i - y_j)."""
    sx = np.sign(x[:, None] - x[None, :])
    sy = np.sign(y[:, None] - y[None, :])
    return float((sx * sy).sum())


def _rankdata(x: np.ndarray) -> np.ndarray:
    return pd.Series(x).rank(method="average").to_numpy()


# --- public functions -------------------------------------------------------

def cov_compute(
    df: pd.DataFrame,
    method: Union[str, Callable[[np.ndarray], np.ndarray]] = "pearson",
    type: str = "cor",
    use: str = "pairwise",
    max_items: int = 500,
    table: Optional[str] = None,
) -> pd.DataFrame:
    """Compute an item correlation or covariance matrix from an IRW response table.

    Turns a core table (long ``id``, ``item``, ``resp``) into an item-by-item
    matrix in the covariance-family layout: one row per cell of the lower
    triangle, diagonal included, with columns ``stat`` (``"cor"`` or
    ``"cov"``), ``var1``, ``var2``, ``value`` and ``n`` (the number of people
    who answered both items). ``irw.fetch(name, source="cov")`` calls this.

    Parameters
    ----------
    df : pandas.DataFrame
        A core table with ``id``, ``item`` and a numeric ``resp``.
    method : str or callable, default "pearson"
        ``"pearson"``, ``"spearman"``, ``"kendall"``, ``"polychoric"`` or
        ``"tetrachoric"`` (correlations only; tetrachoric needs every item
        dichotomous). Or a function of your own: it receives the wide
        person-by-item array (``NaN`` where an item was not answered, after
        ``use`` is applied) and must return a square symmetric array with one
        row and column per item, in the same order. Say what it returns with
        ``type``.
    type : {"cor", "cov"}, default "cor"
        Correlation or covariance. ``"cov"`` is not defined for polychoric or
        tetrachoric correlations.
    use : {"pairwise", "complete"}, default "pairwise"
        ``"pairwise"`` uses everyone who answered both items of a pair and
        records that count in ``n``; ``"complete"`` first drops anyone with a
        missing response.
    max_items : int, default 500
        Refuse tables with more items than this.
    table : str, optional
        Table name, recorded in ``result.attrs["irw_table"]``.

    Returns
    -------
    pandas.DataFrame
        Columns ``stat``, ``var1``, ``var2``, ``value``, ``n``. A pair answered
        by fewer than three people in common has ``value`` NaN. ``attrs``
        records ``irw_computed``, ``irw_method``, ``irw_type``, ``irw_use`` and
        ``irw_table``: the matrix is computed, not published.

    Notes
    -----
    Tables with repeated measures (more than one response per ``id`` and
    ``item``) are refused: filter to one occasion first. Items with no
    variation are dropped with a warning.
    """
    import warnings

    user_fun = method if callable(method) else None
    if user_fun is None and method not in METHODS:
        raise ValueError(f"method must be one of {METHODS} or a function; got {method!r}")
    if type not in ("cor", "cov"):
        raise ValueError("type must be 'cor' or 'cov'")
    if use not in ("pairwise", "complete"):
        raise ValueError("use must be 'pairwise' or 'complete'")
    if type == "cov" and method in ("polychoric", "tetrachoric"):
        raise ValueError(f'type="cov" is not defined for {method} correlations; use type="cor".')
    missing = [c for c in ("id", "item", "resp") if c not in df.columns]
    if missing:
        raise ValueError("cov_compute() needs a core table with id, item and resp; missing: "
                         + ", ".join(missing) + ". Other families have no item-by-item matrix.")
    d = df.loc[:, ["id", "item", "resp"]].copy()
    d["resp"] = pd.to_numeric(d["resp"], errors="coerce")
    d["item"] = d["item"].astype(str)
    if d.duplicated(["id", "item"]).any():
        raise ValueError("Some people answered an item more than once (repeated measures, or duplicate "
                         "rows). Filter to one occasion first, e.g. df[df['wave'] == 1], then call cov_compute().")
    items = list(dict.fromkeys(d["item"]))
    if len(items) > max_items:
        raise ValueError(f"This table has {len(items)} items; max_items is {max_items}. A matrix this size is "
                         "mostly pairs nobody answered together. Raise max_items if you mean it.")
    w = d.pivot(index="id", columns="item", values="resp").reindex(columns=items)
    if use == "complete":
        w = w.dropna()
    sd = w.std()
    flat = sd.isna() | (sd == 0)
    if flat.any():
        warnings.warn(f"Dropped {int(flat.sum())} item(s) with no variation: "
                      + ", ".join(list(w.columns[flat])[:5]), UserWarning, stacklevel=2)
        w = w.loc[:, ~flat]
    if w.shape[1] < 2:
        raise ValueError("Fewer than two items with variation; no matrix to compute.")
    arr = w.to_numpy(dtype=float)
    obs = ~np.isnan(arr)
    npair = obs.T.astype(int) @ obs.astype(int)
    k = arr.shape[1]
    label = "user-defined" if user_fun is not None else method

    if user_fun is not None:
        m = np.asarray(user_fun(arr), dtype=float)
        if m.shape != (k, k):
            raise ValueError(f"The method function must return a {k} x {k} array (one row and column per item).")
        if not np.allclose(m, m.T, equal_nan=True):
            raise ValueError("The method function returned a matrix that is not symmetric.")
    elif method in ("polychoric", "tetrachoric"):
        if method == "tetrachoric":
            ncat = [len(np.unique(arr[obs[:, j], j])) for j in range(k)]
            if any(c != 2 for c in ncat):
                raise ValueError('method="tetrachoric" needs every item dichotomous; '
                                 f'{sum(c != 2 for c in ncat)} item(s) are not. Use "polychoric".')
        m = _polychoric(arr)
    elif type == "cor":
        m = w.corr(method=method, min_periods=3).to_numpy()
    elif method == "pearson":
        m = w.cov(min_periods=3).to_numpy()
    else:
        # pandas' cov is Pearson only; rank methods are computed pair by pair
        m = np.full((k, k), np.nan)
        for i in range(k):
            for j in range(i + 1):
                ok = obs[:, i] & obs[:, j]
                if ok.sum() < 3:
                    continue
                x, y = arr[ok, i], arr[ok, j]
                if method == "spearman":
                    v = float(np.cov(_rankdata(x), _rankdata(y))[0, 1])
                else:
                    v = _kendall_cov(x, y)
                m[i, j] = m[j, i] = v
    m = np.where(npair < 3, np.nan, m)
    names = list(w.columns)
    rows, cols = np.tril_indices(k)
    order = np.lexsort((cols, rows))
    rows, cols = rows[order], cols[order]
    out = pd.DataFrame({"stat": type, "var1": [names[i] for i in rows], "var2": [names[j] for j in cols],
                        "value": m[rows, cols], "n": npair[rows, cols]})
    out.attrs.update({"irw_computed": True, "irw_method": label, "irw_type": type,
                      "irw_use": use, "irw_table": table})
    return out


def cov2matrix(df: pd.DataFrame, group: Optional[str] = None) -> dict:
    """Turn a covariance-family table into a square matrix.

    Works on tables from ``fetch(..., source="cov")`` and ``cov_compute()``,
    and on published tables with a ``group`` column.

    Returns
    -------
    dict
        ``matrix`` (square DataFrame), ``type`` (``"cor"``/``"cov"``), ``n``
        (the largest cell n), ``n_pairwise`` (DataFrame), ``sd`` and ``mean``
        (Series, empty when the table has none), ``computed`` and, when
        computed, ``method``, ``use`` and ``table``.
    """
    missing = [c for c in COV_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError("Not a covariance-family table; missing: " + ", ".join(missing))
    attrs = dict(df.attrs)
    if "group" in df.columns:
        groups = [g for g in pd.unique(df["group"]) if isinstance(g, str) and g != ""]
        if group is None:
            if len(groups) > 1:
                raise ValueError(f"This table holds {len(groups)} samples ({', '.join(groups)}); "
                                 "choose one with group=.")
            group = groups[0] if groups else None
        if group is not None:
            if group not in groups:
                raise ValueError(f"No group {group!r} in this table.")
            df = df[df["group"] == group]
    cells = df[df["stat"].isin(["cor", "cov"])]
    kinds = cells["stat"].unique()
    if len(kinds) != 1:
        raise ValueError("A matrix is all cor or all cov rows.")
    v = list(dict.fromkeys(list(cells["var1"]) + list(cells["var2"])))
    m = pd.DataFrame(np.nan, index=v, columns=v)
    npw = pd.DataFrame(np.nan, index=v, columns=v)
    for a, b, val, n in zip(cells["var1"], cells["var2"], cells["value"], cells["n"]):
        m.loc[a, b] = m.loc[b, a] = val
        npw.loc[a, b] = npw.loc[b, a] = n

    def pick(s):
        r = df[df["stat"] == s]
        return pd.Series(r["value"].to_numpy(dtype=float), index=r["var1"].to_numpy())

    return {"matrix": m, "type": kinds[0], "n": int(np.nanmax(npw.to_numpy())), "n_pairwise": npw,
            "sd": pick("sd"), "mean": pick("mean"), "computed": bool(attrs.get("irw_computed", False)),
            "method": attrs.get("irw_method"), "use": attrs.get("irw_use"), "table": attrs.get("irw_table")}
