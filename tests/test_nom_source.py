"""Offline coverage for the `nom` (nominal response) source.

Both R configs define irw_nominal -- Rpkg/R/redivis-config.R and the pipeline's
metadata/redivis_config.R -- and the Python package had no reference to it
anywhere, so `source="nom"` was a capability gap between the two clients rather
than a documentation problem. See issue #9.

No network: _init_dataset is patched.
"""

from unittest.mock import MagicMock, patch

import pytest

import irw
from irw.config import NOM_REF
from irw.utils.redivis.cache import metadata_cache
from irw.utils.redivis.datasets import _init_nom_dataset


@pytest.fixture(autouse=True)
def clear_cache():
    metadata_cache.clear()
    yield
    metadata_cache.clear()


def test_nom_ref_matches_the_r_package_spec():
    """Rpkg/R/redivis-config.R: nom = datapages / irw_nominal:614n."""
    assert NOM_REF == ("datapages", "irw_nominal:614n")


@patch("irw.utils.redivis.datasets._init_dataset")
def test_init_nom_dataset_opens_nom_ref(mock_init_dataset):
    mock_init_dataset.return_value = MagicMock()
    _init_nom_dataset()
    mock_init_dataset.assert_called_once_with(*NOM_REF)


@patch("irw.utils.redivis.datasets._init_dataset")
def test_init_nom_dataset_is_cached(mock_init_dataset):
    mock_init_dataset.return_value = MagicMock()
    first = _init_nom_dataset()
    second = _init_nom_dataset()
    assert first is second
    assert mock_init_dataset.call_count == 1


@patch("irw.utils.redivis.datasets._init_dataset")
def test_a_failed_nom_dataset_is_fatal_not_skipped(mock_init_dataset):
    """nom is a single dataset, so an unavailable one has nothing to fall back
    to -- unlike MAIN_REFS, where skip_unavailable drops one shard of six."""
    mock_init_dataset.side_effect = RuntimeError("dataset unavailable")
    with pytest.raises(RuntimeError):
        _init_nom_dataset()


@patch("irw.api._init_nom_dataset")
def test_get_datasets_dispatches_nom(mock_init_nom):
    ds = MagicMock()
    mock_init_nom.return_value = ds
    assert irw.api._get_datasets("nom") == [ds]


def test_unknown_source_error_names_nom():
    with pytest.raises(ValueError, match="nom"):
        irw.api._get_datasets("not_a_source")
