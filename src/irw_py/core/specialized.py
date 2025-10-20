"""Specialized IRW classes for different dataset types."""

from __future__ import annotations
from dataclasses import dataclass
from typing import Any, List, Optional, Union, Dict
import pandas as pd

from ..utils.redivis import _init_main_datasets, _init_sim_dataset, _init_comp_dataset
from ..operations.fetch import fetch
from ..operations.list_tables import list_tables, list_tables_basic
from ..operations.info import info, info_for


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
            
        Examples
        --------
        >>> from irw_py import IRW
        >>> irw = IRW()
        >>> df = irw.fetch("agn_kay_2025")
        >>> dfs = irw.fetch(["agn_kay_2025", "pks_probability"])
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


@dataclass
class IRW(_IRWBase):
    """IRW client for main Redivis-hosted item response datasets."""

    def __post_init__(self) -> None:
        """Initialize with main IRW datasets."""
        self._datasets = _init_main_datasets()
    
    def list_tables(self, include_metadata: bool = False) -> pd.DataFrame:
        """
        List IRW tables.

        Parameters
        ----------
        include_metadata : bool, default False
            If True, return enriched IRW metadata (stats, tags, biblio, item-text).
            If False, return basic Redivis properties (name, numRows, variableCount).

        Returns
        -------
        pandas.DataFrame
        """
        return list_tables(self._datasets) if include_metadata else list_tables_basic(self._datasets)
    
    def info(self) -> None:
        """
        Print comprehensive information about the IRW database.
        
        Displays database-level statistics including total tables, responses,
        participants, items, and basic metadata about the IRW database.
        
        Examples
        --------
        >>> from irw_py import IRW
        >>> irw = IRW()
        >>> irw.info()
        """
        return info()


@dataclass  
class IRWSim(_IRWBase):
    """IRW client for simulation datasets."""

    def __post_init__(self) -> None:
        """Initialize with simulation datasets."""
        self._datasets = [_init_sim_dataset()]

    def list_tables(self) -> pd.DataFrame:
        """List Redivis tables with basic properties for simulation datasets."""
        return list_tables_basic(self._datasets)

    def info(self) -> None:
        """Print information summary for IRW simulation dataset."""
        return info_for(self._datasets, title="IRW Simulation Dataset Information")


@dataclass
class IRWComp(_IRWBase):
    """IRW client for competition datasets."""

    def __post_init__(self) -> None:
        """Initialize with competition datasets."""
        self._datasets = [_init_comp_dataset()]

    def list_tables(self) -> pd.DataFrame:
        """List Redivis tables with basic properties for competition datasets."""
        return list_tables_basic(self._datasets)

    def info(self) -> None:
        """Print information summary for IRW competition dataset."""
        return info_for(self._datasets, title="IRW Competition Dataset Information")