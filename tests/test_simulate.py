"""Simulated data must have the shape fetch() returns and the distribution R gives.

Ports of `irw_simdata()` and `irw_simdata_comp()` (issue #24). Two things are
being checked, and they are different in kind:

- **Shape.** The whole point of these functions is that what comes out looks
  like what `fetch()` returns, so simulated and real data go through the same
  downstream code. Columns, dtypes and 1-based labels are exact assertions.
- **Distribution.** Parity with R is a distributional claim, not a
  draw-for-draw one -- R's Mersenne-Twister and numpy's PCG64 are different
  generators. So the statistical assertions are against the *analytic*
  probabilities the model defines, which both implementations must match, with
  tolerances set from the Monte Carlo standard error at the sample sizes used.

No network: nothing here touches Redivis.
"""

import numpy as np
import pandas as pd
import pytest

from irw.operations.simulate import simdata, simdata_comp


# --- shape ----------------------------------------------------------------

def test_simdata_returns_irw_long_format():
    df = simdata(n_id=50, n_item=4, seed=1)
    assert list(df.columns) == ["id", "item", "resp"]
    assert len(df) == 50 * 4
    assert set(df["resp"].unique()) <= {0, 1}
    assert pd.api.types.is_integer_dtype(df["resp"])


def test_ids_and_items_are_one_based_like_the_r_version():
    df = simdata(n_id=7, n_item=3, seed=1)
    assert df["id"].min() == 1 and df["id"].max() == 7
    assert df["item"].min() == 1 and df["item"].max() == 3
    # Item-major ordering, matching R's as.vector() over a persons-by-items
    # matrix: all of item 1, then all of item 2.
    assert list(df["item"].head(7)) == [1] * 7
    assert list(df["id"].head(7)) == list(range(1, 8))


def test_every_person_answers_every_item():
    df = simdata(n_id=20, n_item=5, seed=1)
    counts = df.groupby("id").size()
    assert set(counts) == {5}
    assert not df.duplicated(subset=["id", "item"]).any()


def test_supplied_theta_overrides_n_id():
    df = simdata(n_id=999, n_item=2, theta=[0.0, 1.0, 2.0], seed=1)
    assert df["id"].max() == 3
    assert len(df) == 6


def test_return_params_carries_the_generating_parameters():
    out = simdata(n_id=30, n_item=6, model="3PL", seed=1, return_params=True)
    assert set(out) == {"data", "theta", "a", "b", "g"}
    assert isinstance(out["data"], pd.DataFrame)
    assert out["theta"].shape == (30,)
    for key in ("a", "b", "g"):
        assert out[key].shape == (6,)


@pytest.mark.parametrize("model", ["1PL", "2PL"])
def test_g_is_absent_for_models_that_have_no_guessing(model):
    out = simdata(n_id=10, n_item=3, model=model, seed=1, return_params=True)
    assert "g" not in out


def test_simdata_comp_returns_the_comparison_shape():
    df = simdata_comp(n_agent=10, n_pairs=200, seed=1)
    assert list(df.columns) == ["agent_a", "agent_b", "winner"]
    assert len(df) == 200
    assert set(df["winner"]) <= {"agent_a", "agent_b", "draw"}
    assert df[["agent_a", "agent_b"]].min().min() == 1
    assert df[["agent_a", "agent_b"]].max().max() <= 10


def test_no_agent_is_compared_with_itself():
    df = simdata_comp(n_agent=3, n_pairs=5000, seed=1)
    assert not (df["agent_a"] == df["agent_b"]).any()


# --- reproducibility ------------------------------------------------------

def test_a_seed_reproduces_within_python():
    first = simdata(n_id=40, n_item=4, model="2PL", seed=123)
    second = simdata(n_id=40, n_item=4, model="2PL", seed=123)
    assert first.equals(second)

    third = simdata_comp(n_agent=8, n_pairs=100, seed=123)
    assert third.equals(simdata_comp(n_agent=8, n_pairs=100, seed=123))


def test_different_seeds_give_different_draws():
    assert not simdata(n_id=40, n_item=4, seed=1).equals(
        simdata(n_id=40, n_item=4, seed=2)
    )


def test_seeding_does_not_disturb_the_callers_random_state():
    """Deliberate divergence from R, which calls set.seed() on the global
    stream and so silently reseeds whatever else the session is doing."""
    np.random.seed(5)
    before = np.random.random()
    np.random.seed(5)
    simdata(n_id=10, n_item=2, seed=999)
    assert np.random.random() == before


# --- distribution ---------------------------------------------------------

def test_response_probabilities_match_the_2pl_curve():
    """P(correct) = sigmoid(a * (theta - b)), which is what both clients claim."""
    theta = np.zeros(40000)
    a = np.array([0.5, 1.0, 2.0])
    b = np.array([-1.0, 0.0, 1.0])
    df = simdata(theta=theta, n_item=3, a=a, b=b, model="2PL", seed=7)

    expected = 1.0 / (1.0 + np.exp(-a * (0.0 - b)))
    observed = df.groupby("item")["resp"].mean().to_numpy()
    # SE at n=40,000 is under 0.0025; 0.01 is four of them.
    assert np.allclose(observed, expected, atol=0.01)


def test_guessing_lifts_the_floor_for_3pl():
    """A 3PL item far above everyone's ability floors at g, not at 0."""
    theta = np.full(40000, -6.0)
    df = simdata(theta=theta, n_item=2, a=np.array([1.0, 1.0]),
                 b=np.array([3.0, 3.0]), g=np.array([0.25, 0.0]),
                 model="3PL", seed=7)
    observed = df.groupby("item")["resp"].mean().to_numpy()
    assert abs(observed[0] - 0.25) < 0.01
    assert observed[1] < 0.001


def test_the_davidson_probabilities_are_what_the_model_says():
    t1, t2, nu = 1.0, 2.0, 0.3
    k = np.exp(t1) + np.exp(t2) + np.exp(nu + (t1 + t2) / 2)
    expected = {
        "draw": np.exp(nu + (t1 + t2) / 2) / k,
        "t1": np.exp(t1) / k,
        "t2": np.exp(t2) / k,
    }

    df = simdata_comp(theta=[t1, t2], n_pairs=200000, nu=nu, seed=7)
    a_is_first = df["agent_a"] == 1
    observed = {
        "draw": (df["winner"] == "draw").mean(),
        "t1": (
            (a_is_first & (df["winner"] == "agent_a"))
            | (~a_is_first & (df["winner"] == "agent_b"))
        ).mean(),
        "t2": (
            (a_is_first & (df["winner"] == "agent_b"))
            | (~a_is_first & (df["winner"] == "agent_a"))
        ).mean(),
    }
    # SE at n=200,000 is about 0.001; 0.005 is five of them.
    for key, value in expected.items():
        assert abs(observed[key] - value) < 0.005, key


def test_a_larger_nu_yields_more_draws():
    thetas = [0.0, 0.5, 1.0]
    low = simdata_comp(theta=thetas, n_pairs=20000, nu=-2.0, seed=7)
    high = simdata_comp(theta=thetas, n_pairs=20000, nu=2.0, seed=7)
    low_rate = (low["winner"] == "draw").mean()
    high_rate = (high["winner"] == "draw").mean()
    # Analytic draw rates for these abilities are about 0.06 and 0.78.
    assert low_rate < 0.10
    assert high_rate > 0.70
    assert low_rate < high_rate


def test_extreme_abilities_do_not_overflow():
    """exp() of a raw ability overflows well inside the range someone might
    plausibly pass in; both samplers work on shifted logits."""
    with np.errstate(over="raise"):
        simdata(theta=[-800.0, 800.0], n_item=2, a=[3.0, 3.0], b=[0.0, 0.0],
                model="2PL", seed=1)
        simdata_comp(theta=[-800.0, 800.0], n_pairs=100, nu=0.0, seed=1)


# --- refusals -------------------------------------------------------------

def test_an_unknown_model_is_refused():
    with pytest.raises(ValueError, match="model"):
        simdata(model="4PL")


def test_discrimination_cannot_be_supplied_for_1pl():
    """R accepts it and then uses it, which makes the '1PL' label a lie."""
    with pytest.raises(ValueError, match="1PL"):
        simdata(n_item=2, model="1PL", a=[1.5, 2.0])


def test_guessing_cannot_be_supplied_for_a_model_without_it():
    with pytest.raises(ValueError, match="3PL"):
        simdata(n_item=2, model="2PL", g=[0.2, 0.2])


@pytest.mark.parametrize("kwargs", [
    {"n_item": 3, "b": [0.0, 1.0]},
    {"n_item": 3, "model": "2PL", "a": [1.0]},
    {"n_item": 2, "model": "3PL", "g": [0.1, 0.2, 0.3]},
])
def test_a_parameter_vector_of_the_wrong_length_is_refused(kwargs):
    with pytest.raises(ValueError, match="n_item"):
        simdata(n_id=5, **kwargs)


def test_a_single_agent_is_refused_rather_than_hanging():
    """R's rejection loop resamples until the two agents differ, which with one
    agent never terminates."""
    with pytest.raises(ValueError, match="2 agents"):
        simdata_comp(n_agent=1, n_pairs=10)


@pytest.mark.parametrize("kwargs", [
    {"n_id": 0},
    {"n_item": 0},
    {"n_id": -5},
])
def test_non_positive_sizes_are_refused(kwargs):
    with pytest.raises(ValueError):
        simdata(**kwargs)


def test_a_guessing_parameter_outside_the_unit_interval_is_refused():
    with pytest.raises(ValueError, match=r"\[0, 1\)"):
        simdata(n_id=5, n_item=2, model="3PL", g=[0.2, 1.4])
