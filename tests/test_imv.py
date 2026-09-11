"""The IMV must give R's numbers and refuse what R refuses.

Port of `irw_imv()` (issue #56). The expected values below are hardcoded from
the R package (irw 1.1.3, whose `irw_imv` is identical to `Rpkg` origin/main
`R/imv.R`), generated with, for example:

    Rscript -e 'library(irw); y <- c(1,1,1,0,0,1,1,0,1,1);
                sprintf("%.15g", irw_imv(y, rep(0.5, 10), rep(0.7, 10)))'

Each case was also checked against an exact root (`uniroot`, tol 1e-15) to
confirm R's `nlminb` had converged there, so a match is a match to the IMV and
not to R's solver noise. The one case where it had not is pinned separately.

No network: nothing here touches Redivis.
"""

import math

import numpy as np
import pandas as pd
import pytest

import irw
from irw.operations.imv import _coin, _neg_entropy, imv

Y10 = [1, 1, 1, 0, 0, 1, 1, 0, 1, 1]
P1_GRADED = [0.6, 0.7, 0.55, 0.4, 0.5, 0.65, 0.8, 0.45, 0.6, 0.7]
P2_GRADED = [0.8, 0.9, 0.75, 0.2, 0.3, 0.85, 0.9, 0.25, 0.8, 0.85]

# n = 200: outcomes 60% ones; p2 informative, p1 the base rate.
_I = np.arange(1, 201)
Y200 = ((_I * 7) % 10 < 6).astype(float)
P1_BASE = np.full(200, 0.6)
P2_INFORMATIVE = 1 / (1 + np.exp(-(1.5 * (2 * Y200 - 1) * np.abs(np.sin(_I)) + 0.2 * np.cos(_I))))


# --- parity with R ----------------------------------------------------------

@pytest.mark.parametrize(
    "y, p1, p2, eps, expected",
    [
        # plogis/sin/cos inputs are reproduced in numpy above.
        (Y10, [0.5] * 10, [0.7] * 10, 1e-6, 0.39999999995927),
        (Y10, [0.7] * 10, [0.5] * 10, 1e-6, -0.285714285693505),
        (Y10, P1_GRADED, P2_GRADED, 1e-6, 0.15915703625966),
        (Y10, P2_GRADED, P1_GRADED, 1e-6, -0.137304119529158),
        (Y200, P1_BASE, P2_INFORMATIVE, 1e-6, 0.481795921402321),
        # p2 worse than a fair coin: its likelihood is below 0.5, coin = 0.5.
        (Y200, P1_BASE, 1 - P2_INFORMATIVE, 1e-6, -0.166666665966491),
        # Exact 0/1 predictions, all right: clamped, and the coin hits R's bound.
        ([1, 0, 1, 1, 0], [0.5] * 5, [1, 0, 1, 1, 0], 1e-6, 0.998),
        ([1, 0, 1, 1, 0], [0.5] * 5, [1, 0, 1, 1, 0], 0.01, 0.997366182828967),
        # Exact 0/1 predictions, some wrong: finite only because of the clamp.
        ([1, 0, 1, 1, 0], [0.6] * 5, [0, 1, 1, 1, 0], 1e-6, -0.166666665966491),
    ],
)
def test_matches_r(y, p1, p2, eps, expected):
    assert imv(y, p1, p2, eps=eps) == pytest.approx(expected, abs=1e-6)


def test_data_frame_form_matches_r():
    df = pd.DataFrame({"resp": Y10, "m1": P1_GRADED, "m2": P2_GRADED})
    assert imv(df, "m1", "m2") == pytest.approx(0.15915703625966, abs=1e-6)


def test_custom_resp_column():
    df = pd.DataFrame({"y": Y10, "m1": P1_GRADED, "m2": P2_GRADED})
    assert imv(df, "m1", "m2", resp="y") == pytest.approx(0.15915703625966, abs=1e-6)


def test_exported_at_package_level():
    assert irw.imv is imv


def test_exact_root_where_r_solver_stops_early():
    # Outcomes with base rate 0.844, predicted at exactly that rate, have a coin
    # of exactly 0.844, so the IMV against a fair coin is exactly 0.688. R's
    # nlminb stops early here and returns 0.688212733997571; this port solves
    # the root exactly. See the module docstring.
    y = [1] * 844 + [0] * 156
    assert imv(y, [0.5] * 1000, [0.844] * 1000) == pytest.approx(0.688, abs=1e-12)


# --- the coin solve ---------------------------------------------------------

@pytest.mark.parametrize("q", [0.55, 0.6, 0.7, 0.844, 0.9, 0.99])
def test_coin_inverts_the_entropy_on_the_upper_branch(q):
    # R's start value of 0.5 sends nlminb to the root above 0.5; so does this.
    assert _coin(math.exp(_neg_entropy(q))) == pytest.approx(q, abs=1e-12)


def test_coin_edges_match_r():
    # No root at or below a fair coin: R returns its start value.
    assert _coin(0.5) == 0.5
    assert _coin(0.1) == 0.5
    assert _coin(0.0) == 0.5
    # No root inside R's upper bound: R stops at the bound.
    assert _coin(0.9999) == 1 - 1e-3


def test_equal_predictions_give_zero():
    assert imv(Y10, P1_GRADED, P1_GRADED) == 0.0


def test_swapping_arguments_does_not_flip_the_sign():
    forward = imv(Y10, P1_GRADED, P2_GRADED)
    backward = imv(Y10, P2_GRADED, P1_GRADED)
    assert forward > 0 > backward
    assert forward != pytest.approx(-backward, abs=1e-3)


def test_accepts_numpy_arrays_and_nullable_integers():
    y = pd.Series(Y10, dtype="Int64")
    assert imv(y, np.array(P1_GRADED), pd.Series(P2_GRADED)) == pytest.approx(
        0.15915703625966, abs=1e-6
    )


# --- refusals ---------------------------------------------------------------

def test_refuses_missing_column():
    df = pd.DataFrame({"resp": Y10, "m1": P1_GRADED})
    with pytest.raises(ValueError, match="Column not found in 'data': m2"):
        imv(df, "m1", "m2")


def test_refuses_vectors_alongside_a_data_frame():
    df = pd.DataFrame({"resp": Y10, "m1": P1_GRADED})
    with pytest.raises(ValueError, match="must be column names"):
        imv(df, "m1", P2_GRADED)


@pytest.mark.parametrize(
    "y, p1, p2",
    [
        (["1", "0"], [0.5, 0.5], [0.6, 0.6]),
        ([True, False], [0.5, 0.5], [0.6, 0.6]),
        ([1, 0], ["a", "b"], [0.6, 0.6]),
    ],
)
def test_refuses_non_numeric(y, p1, p2):
    with pytest.raises(ValueError, match="must be numeric"):
        imv(y, p1, p2)


def test_refuses_mismatched_lengths():
    with pytest.raises(ValueError, match="same length: got 3, 2, 3"):
        imv([1, 0, 1], [0.5, 0.5], [0.6, 0.6, 0.6])


def test_refuses_empty_input():
    with pytest.raises(ValueError, match="No observations"):
        imv([], [], [])


@pytest.mark.parametrize(
    "y, p1",
    [
        ([1, np.nan, 1], [0.5, 0.5, 0.5]),
        ([1, None, 1], [0.5, 0.5, 0.5]),
        ([1, 0, 1], [0.5, pd.NA, 0.5]),
    ],
)
def test_refuses_missing_values(y, p1):
    with pytest.raises(ValueError, match="must not contain missing values"):
        imv(y, p1, [0.6, 0.6, 0.6])


def test_refuses_non_binary_outcomes():
    with pytest.raises(ValueError, match="binary outcomes"):
        imv([1, 2, 0], [0.5] * 3, [0.6] * 3)


@pytest.mark.parametrize("bad", [-0.1, 1.1, np.inf])
def test_refuses_predictions_outside_unit_interval(bad):
    with pytest.raises(ValueError, match=r"probabilities in \[0, 1\]"):
        imv([1, 0, 1], [0.5, 0.5, 0.5], [0.6, bad, 0.6])


@pytest.mark.parametrize("eps", [0, 0.5, -1e-6, 1.0, np.nan, "0.01", True])
def test_refuses_eps_outside_open_interval(eps):
    with pytest.raises(ValueError, match="'eps' must be"):
        imv([1, 0, 1], [0.5] * 3, [0.6] * 3, eps=eps)
