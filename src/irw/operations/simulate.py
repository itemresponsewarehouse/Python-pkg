"""Simulate IRW-shaped data: dichotomous item responses, and pairwise comparisons.

Ports `irw_simdata()` and `irw_simdata_comp()` from `Rpkg/R/simulate.R`
(issue #24). The point of both is that what comes out has the same shape as
what `fetch()` returns -- `id`, `item`, `resp` for responses -- so simulated
and real data go through the same downstream code without a special case.

On `seed`: the promise is the same *distribution* as R, not the same *numbers*.
R's Mersenne-Twister stream and numpy's PCG64 are different generators, and
even seeded identically they would not agree draw for draw; matching them would
mean reimplementing R's RNG, which buys reproducibility across languages that
nobody has asked for at the cost of code nobody can check. Within Python, a
given seed reproduces exactly.

One deliberate divergence from R: seeding uses `np.random.default_rng(seed)`
rather than the global stream, so calling these functions does not reach out
and change the random state of unrelated code in the caller's session.
"""

from __future__ import annotations

from typing import Any, Dict, Optional, Sequence, Union

import numpy as np
import pandas as pd

MODELS = ("1PL", "2PL", "3PL")


def _as_vector(values: Sequence[float], name: str, expected: int) -> np.ndarray:
    """Coerce a user-supplied parameter vector and check its length."""
    array = np.asarray(values, dtype=float).ravel()
    if array.size != expected:
        raise ValueError(
            f"'{name}' has {array.size} values but n_item is {expected}; "
            f"supply one value per item."
        )
    if not np.all(np.isfinite(array)):
        raise ValueError(f"'{name}' contains non-finite values.")
    return array


def _sigmoid(x: np.ndarray) -> np.ndarray:
    """Logistic function, computed without overflowing at the tails.

    `1 / (1 + exp(-x))` overflows for very negative x, which is reachable here:
    a discrimination of 2 and a difficulty four SDs from theta already gets
    there, and numpy warns rather than returning the correct 0.
    """
    out = np.empty_like(x, dtype=float)
    positive = x >= 0
    out[positive] = 1.0 / (1.0 + np.exp(-x[positive]))
    exp_x = np.exp(x[~positive])
    out[~positive] = exp_x / (1.0 + exp_x)
    return out


def _resolve_theta(
    theta: Optional[Sequence[float]],
    n: int,
    theta_mean: float,
    theta_sd: float,
    rng: np.random.Generator,
    n_name: str,
) -> np.ndarray:
    """Supplied thetas win and set n; otherwise draw n of them."""
    if theta is not None:
        resolved = np.asarray(theta, dtype=float).ravel()
        if resolved.size == 0:
            raise ValueError("'theta' is empty.")
        if not np.all(np.isfinite(resolved)):
            raise ValueError("'theta' contains non-finite values.")
        return resolved

    if not isinstance(n, (int, np.integer)) or n < 1:
        raise ValueError(f"'{n_name}' must be a positive integer, got {n!r}.")
    if theta_sd < 0:
        raise ValueError(f"'theta_sd' must be non-negative, got {theta_sd!r}.")
    return rng.normal(loc=theta_mean, scale=theta_sd, size=int(n))


def simdata(
    n_id: int = 1000,
    n_item: int = 20,
    model: str = "1PL",
    a: Optional[Sequence[float]] = None,
    b: Optional[Sequence[float]] = None,
    g: Optional[Sequence[float]] = None,
    theta: Optional[Sequence[float]] = None,
    theta_mean: float = 0.0,
    theta_sd: float = 1.0,
    seed: Optional[int] = None,
    return_params: bool = False,
) -> Union[pd.DataFrame, Dict[str, Any]]:
    """
    Simulate IRW-compliant dichotomous item response data.

    Draws from a 1PL, 2PL or 3PL logistic IRT model and returns the result in
    IRW long format, the same shape ``fetch()`` returns.

    Parameters
    ----------
    n_id : int, default 1000
        Number of respondents. Ignored if ``theta`` is given.
    n_item : int, default 20
        Number of items.
    model : {'1PL', '2PL', '3PL'}, default '1PL'
    a : sequence of float, optional
        Item discriminations. Default lognormal(0, 0.5) -- a median of 1, with
        roughly the middle half of items between 0.7 and 1.4. Fixed at 1 for
        1PL, where a supplied value is an error rather than a silent no-op.
    b : sequence of float, optional
        Item difficulties. Default N(0, 1).
    g : sequence of float, optional
        Guessing parameters, 3PL only. Default Beta(5, 17).
    theta : sequence of float, optional
        Person abilities. Overrides ``n_id``, ``theta_mean`` and ``theta_sd``.
    theta_mean, theta_sd : float, default 0.0, 1.0
        Latent trait distribution, used only when ``theta`` is not given.
    seed : int, optional
        Reproducible within Python. See the module docstring on cross-language
        parity: same distribution as R, not the same numbers.
    return_params : bool, default False
        If True, return a dict with the data and the generating parameters.

    Returns
    -------
    pandas.DataFrame or dict
        Columns ``id``, ``item``, ``resp``. With ``return_params=True``, a dict
        with keys ``data``, ``theta``, ``a``, ``b`` and (3PL only) ``g``.

    Examples
    --------
    >>> import irw
    >>> df = irw.simdata(n_id=200, n_item=5, seed=1)
    >>> sorted(df.columns.tolist())
    ['id', 'item', 'resp']
    >>> sim = irw.simdata(n_item=5, model="3PL", theta_mean=-0.5,
    ...                   return_params=True, seed=1)
    >>> sim["g"].shape
    (5,)
    """
    if model not in MODELS:
        raise ValueError(
            f"'model' must be one of {', '.join(MODELS)}, got {model!r}."
        )
    if not isinstance(n_item, (int, np.integer)) or n_item < 1:
        raise ValueError(f"'n_item' must be a positive integer, got {n_item!r}.")
    n_item = int(n_item)

    rng = np.random.default_rng(seed)
    theta_vec = _resolve_theta(theta, n_id, theta_mean, theta_sd, rng, "n_id")
    n_id = theta_vec.size

    if a is None:
        # 1PL fixes discrimination at 1 by definition; the other two draw it.
        a_vec = (
            np.ones(n_item)
            if model == "1PL"
            else rng.lognormal(mean=0.0, sigma=0.5, size=n_item)
        )
    else:
        if model == "1PL":
            # R accepts this and then uses it, which makes the "1PL" label a
            # lie. Refusing is the honest reading of what the caller asked for.
            raise ValueError(
                "'a' cannot be supplied for a 1PL model, which fixes "
                "discrimination at 1. Use model='2PL' to vary it."
            )
        a_vec = _as_vector(a, "a", n_item)

    b_vec = rng.normal(0.0, 1.0, size=n_item) if b is None else _as_vector(b, "b", n_item)

    if model == "3PL":
        g_vec = rng.beta(5.0, 17.0, size=n_item) if g is None else _as_vector(g, "g", n_item)
        if np.any(g_vec < 0) or np.any(g_vec >= 1):
            raise ValueError("'g' values must be in [0, 1).")
    else:
        if g is not None:
            raise ValueError(
                f"'g' is a 3PL parameter and has no effect under {model}."
            )
        g_vec = np.zeros(n_item)

    # persons x items, in one pass rather than a loop over items
    logits = a_vec[np.newaxis, :] * (theta_vec[:, np.newaxis] - b_vec[np.newaxis, :])
    p = g_vec[np.newaxis, :] + (1.0 - g_vec[np.newaxis, :]) * _sigmoid(logits)
    responses = rng.binomial(1, p)

    # Column-major flattening, so the frame is item-major: all of item 1, then
    # all of item 2. Matches the R version's `as.vector(response_matrix)`.
    data = pd.DataFrame(
        {
            "id": np.tile(np.arange(1, n_id + 1), n_item),
            "item": np.repeat(np.arange(1, n_item + 1), n_id),
            "resp": responses.ravel(order="F").astype(int),
        }
    )

    if not return_params:
        return data

    params: Dict[str, Any] = {
        "data": data,
        "theta": theta_vec,
        "a": a_vec,
        "b": b_vec,
    }
    if model == "3PL":
        params["g"] = g_vec
    return params


def simdata_comp(
    n_agent: int = 100,
    n_pairs: int = 10000,
    nu: float = 0.0,
    theta: Optional[Sequence[float]] = None,
    theta_mean: float = 0.0,
    theta_sd: float = 1.0,
    seed: Optional[int] = None,
    return_params: bool = False,
) -> Union[pd.DataFrame, Dict[str, Any]]:
    """
    Simulate IRW-compliant pairwise-comparison data.

    Outcomes come from the Davidson model with ties
    (https://link.springer.com/article/10.3758/s13428-021-01714-2): for agents
    with abilities t1 and t2, the three probabilities are proportional to
    ``exp(t1)``, ``exp(t2)`` and ``exp(nu + (t1 + t2) / 2)``.

    Parameters
    ----------
    n_agent : int, default 100
        Number of agents. Ignored if ``theta`` is given. Must be at least 2:
        self-matches are excluded, so one agent has nothing to compare against.
    n_pairs : int, default 10000
        Number of comparisons to sample, with replacement.
    nu : float, default 0.0
        Tie propensity. Larger values yield more draws.
    theta : sequence of float, optional
        Agent abilities. Overrides ``n_agent``.
    theta_mean, theta_sd : float, default 0.0, 1.0
        Used only when ``theta`` is not given.
    seed : int, optional
        Reproducible within Python; see the module docstring.
    return_params : bool, default False
        If True, return a dict with the data, ``theta`` and ``nu``.

    Returns
    -------
    pandas.DataFrame or dict
        Columns ``agent_a``, ``agent_b``, ``winner``, where ``winner`` is one
        of ``'agent_a'``, ``'agent_b'`` or ``'draw'``.

    Examples
    --------
    >>> import irw
    >>> d = irw.simdata_comp(n_agent=20, n_pairs=100, nu=0.1, seed=1)
    >>> set(d["winner"]) <= {"agent_a", "agent_b", "draw"}
    True
    """
    if not isinstance(n_pairs, (int, np.integer)) or n_pairs < 1:
        raise ValueError(f"'n_pairs' must be a positive integer, got {n_pairs!r}.")
    n_pairs = int(n_pairs)
    if not np.isfinite(nu):
        raise ValueError(f"'nu' must be finite, got {nu!r}.")

    rng = np.random.default_rng(seed)
    theta_vec = _resolve_theta(theta, n_agent, theta_mean, theta_sd, rng, "n_agent")
    n_agent = theta_vec.size
    if n_agent < 2:
        # R's rejection loop resamples until agent_b differs from agent_a,
        # which with one agent never terminates. Say so instead of hanging.
        raise ValueError(
            "At least 2 agents are needed to form a pair; "
            f"got {n_agent}."
        )

    # Draw both sides, then resample only the self-matches -- the same
    # rejection scheme as R, but on the whole vector at once.
    agent_a = rng.integers(0, n_agent, size=n_pairs)
    agent_b = rng.integers(0, n_agent, size=n_pairs)
    same = agent_a == agent_b
    while same.any():
        agent_b[same] = rng.integers(0, n_agent, size=int(same.sum()))
        same = agent_a == agent_b

    t1 = theta_vec[agent_a]
    t2 = theta_vec[agent_b]
    # Subtracting the row maximum leaves the ratios unchanged and keeps exp()
    # from overflowing on abilities a few units from zero.
    terms = np.column_stack(
        [nu + (t1 + t2) / 2.0, t1, t2]  # draw, agent_a, agent_b
    )
    terms = np.exp(terms - terms.max(axis=1, keepdims=True))
    probs = terms / terms.sum(axis=1, keepdims=True)

    # One uniform per comparison against the cumulative probabilities: the
    # vectorised equivalent of a multinomial draw per row.
    draws = rng.random(n_pairs)[:, np.newaxis]
    outcome = (draws > probs.cumsum(axis=1)[:, :-1]).sum(axis=1)
    winner = np.array(["draw", "agent_a", "agent_b"])[outcome]

    data = pd.DataFrame(
        {
            # 1-based agent labels, matching the R version.
            "agent_a": agent_a.astype(int) + 1,
            "agent_b": agent_b.astype(int) + 1,
            "winner": winner,
        }
    )

    if not return_params:
        return data
    return {"data": data, "theta": theta_vec, "nu": float(nu)}
