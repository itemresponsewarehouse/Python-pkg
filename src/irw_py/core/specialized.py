"""Specialized IRW classes for different dataset types."""

from __future__ import annotations
from dataclasses import dataclass
from typing import Any, List, Optional, Union, Dict
import pandas as pd

from ..utils.redivis import _init_main_datasets, _init_sim_dataset, _init_comp_dataset
from ..operations.fetch import fetch
from ..operations.list_tables import list_tables, list_tables_basic
from ..operations.info import info_for


@dataclass
class _IRWBase:
    """Base class for IRW clients with common functionality."""
    
    def fetch(self, name: Union[str, List[str]], *, dedup: bool = False) -> Union[pd.DataFrame, Dict[str, Optional[pd.DataFrame]], None]:
        """
        Fetch one or more IRW tables into a pandas DataFrame.
        
        Parameters
        ----------
        name : str | List[str]
            Single table name or list of table names.
        dedup : bool, default False
            Apply deduplication to responses.
            
        Returns
        -------
        pandas.DataFrame | dict[str, pandas.DataFrame | None] | None
            Single table returns DataFrame, multiple return dict.
        """
        return fetch(self._datasets, name, dedup=dedup)
    
    def list_tables(self, include_metadata: bool = False) -> pd.DataFrame:
        """
        List available tables.

        Parameters
        ----------
        include_metadata : bool, default False
            Hint for subclasses on whether to return enriched metadata.

        Returns
        -------
        pandas.DataFrame
        """
        return list_tables(self._datasets)

    def info(self) -> None:
        """Print information summary for this IRW client."""
        return info_for(self._datasets, title=self._info_title)


@dataclass
class IRW(_IRWBase):
    """IRW client for accessing different types of datasets.
    
    Parameters
    ----------
    source : str, default "main"
        Dataset source to use. Options:
        - "main": Main IRW datasets (default)
        - "sim": Simulation datasets
        - "comp": Competition datasets
    """
    source: str = "main"
    _info_title: str = "IRW Database Information"

    def __post_init__(self) -> None:
        """Initialize with datasets based on source."""
        if self.source == "main":
            self._datasets = _init_main_datasets()
            self._info_title = "IRW Database Information"
        elif self.source == "sim":
            self._datasets = [_init_sim_dataset()]
            self._info_title = "IRW Simulation Database Information"
        elif self.source == "comp":
            self._datasets = [_init_comp_dataset()]
            self._info_title = "IRW Competition Database Information"
        else:
            raise ValueError(f"Unknown source '{self.source}'. Must be one of: 'main', 'sim', 'comp'")
    
    def list_tables(self, include_metadata: bool = False) -> pd.DataFrame:
        """
        List IRW tables.

        Parameters
        ----------
        include_metadata : bool, default False
            If True and source="main", return enriched IRW metadata (stats, tags, biblio, item-text).
            If False, return basic Redivis properties (name, numRows, variableCount).
            Note: Only main datasets support enriched metadata.

        Returns
        -------
        pandas.DataFrame
        """
        if self.source == "main":
            return list_tables(self._datasets) if include_metadata else list_tables_basic(self._datasets)
        else:
            # For sim and comp, always use basic listing
            return list_tables_basic(self._datasets)
    
    def info(self) -> None:
        """Print information summary for this IRW client."""
        return info_for(self._datasets, title=self._info_title)

