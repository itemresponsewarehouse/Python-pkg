"""Specialized IRW classes for different dataset types."""

from __future__ import annotations
from dataclasses import dataclass
from typing import Any, List, Optional, Union, Dict
import pandas as pd

from ..utils.redivis import _init_main_datasets, _init_sim_dataset, _init_comp_dataset
from ..operations.fetch import fetch
from ..operations.list_tables import list_tables, list_tables_basic
from ..operations.info import info_for
from ..operations.filter import filter_tables
from ..operations.filter_info import get_filters, describe_filter


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
    
    def filter(
        self,
        n_responses: Optional[Union[float, int, List[Optional[Union[float, int]]]]] = None,
        n_categories: Optional[Union[float, int, List[Optional[Union[float, int]]]]] = None,
        n_participants: Optional[Union[float, int, List[Optional[Union[float, int]]]]] = None,
        n_items: Optional[Union[float, int, List[Optional[Union[float, int]]]]] = None,
        responses_per_participant: Optional[Union[float, int, List[Optional[Union[float, int]]]]] = None,
        responses_per_item: Optional[Union[float, int, List[Optional[Union[float, int]]]]] = None,
        density: Optional[Union[float, int, List[Optional[Union[float, int]]]]] = [0.5, 1],
        var: Optional[Union[str, List[str]]] = None,
        age_range: Optional[Union[str, List[str]]] = None,
        child_age: Optional[Union[str, List[str]]] = None,
        construct_type: Optional[Union[str, List[str]]] = None,
        construct_name: Optional[Union[str, List[str]]] = None,
        sample: Optional[Union[str, List[str]]] = None,
        measurement_tool: Optional[Union[str, List[str]]] = None,
        item_format: Optional[Union[str, List[str]]] = None,
        language: Optional[Union[str, List[str]]] = None,
        longitudinal: Optional[bool] = None,
        license: Optional[Union[str, List[str]]] = None,
    ) -> pd.Series:
        """
        Filter IRW tables based on metadata criteria.
        
        Returns the names of datasets in the Item Response Warehouse (IRW) that match
        user-specified metadata, tag values, variable presence, and license criteria.
        
        **Note:** This method only works for the main IRW database (source="main").
        It is not available for simulation or competition datasets.
        
        Parameters
        ----------
        n_responses : float, int, or list of length 1 or 2, optional
            Filter by total number of responses.
            - Single value: exact match (e.g., n_responses=1000)
            - List of length 2: range [min, max], use None for infinity (e.g., n_responses=[1000, None] for >= 1000)
        
        n_categories : float, int, or list of length 1 or 2, optional
            Filter by number of unique response categories.
            - List of length 2: range [min, max], use None for infinity (e.g., [3, None] for >= 3)
        
        n_participants : float, int, or list of length 1 or 2, optional
            Filter by number of unique participants.
            - List of length 2: range [min, max], use None for infinity (e.g., [500, None] for >= 500)
        
        n_items : float, int, or list of length 1 or 2, optional
            Filter by number of unique items.
            - List of length 2: range [min, max], use None for infinity (e.g., [10, 50] for between 10-50)
        
        responses_per_participant : float, int, or list of length 1 or 2, optional
            Filter by average responses per participant.
            - List of length 2: range [min, max], use None for infinity
        
        responses_per_item : float, int, or list of length 1 or 2, optional
            Filter by average responses per item.
            - List of length 2: range [min, max], use None for infinity
        
        density : float, int, list of length 1 or 2, or None, optional
            Filter by matrix density (proportion of cells with valid responses).
            A density of 1 means every person responded to every item (100% of cells have valid responses).
            Lower density indicates that some individuals did not respond to all items.
            Default is [0.5, 1] to exclude sparse matrices.
            - List of length 2: range [min, max], use None for infinity (e.g., density=[0.5, None] for >= 0.5)
            - Set to None to disable density filtering
        
        var : str or list of str, optional
            Filter datasets by presence of variables.
            - Exact names: "rt", "wave"
            - Prefix matching: "cov_" (matches any variable starting with "cov_")
        
        age_range : str or list of str, optional
            Filter by participant age group (e.g., "Adult (18+)").
        
        child_age : str or list of str, optional
            Filter by child age subgroup.
        
        construct_type : str or list of str, optional
            Filter by high-level construct category (e.g., "Affective/mental health").
        
        construct_name : str or list of str, optional
            Filter by specific construct (e.g., "Big Five").
        
        sample : str or list of str, optional
            Filter by sample type or recruitment method (e.g., "Educational", "Clinical").
        
        measurement_tool : str or list of str, optional
            Filter by instrument type (e.g., "Survey/questionnaire").
        
        item_format : str or list of str, optional
            Filter by item format (e.g., "Likert Scale/selected response").
        
        language : str or list of str, optional
            Filter by language used (e.g., "eng").
        
        longitudinal : bool or None, optional
            Filter longitudinal datasets.
            - True: include only datasets flagged as longitudinal
            - False: exclude datasets flagged as longitudinal
            - None: no filter (default)
        
        license : str or list of str, optional
            Filter datasets by license (e.g., "CC BY 4.0").
        
        Returns
        -------
        pandas.Series
            A sorted Series of dataset names (table names) that match all specified filters.
            Returns empty Series if no matches are found.
        
        Raises
        ------
        ValueError
            If called on a non-main IRW client (source != "main").
        
        Examples
        --------
        >>> from irw_py import IRW
        >>> irw = IRW()  # source="main" by default
        >>> 
        >>> # Numeric filters
        >>> filtered = irw.filter(n_responses=[1000, None], n_items=[10, 50])  # >= 1000 responses, 10-50 items
        >>> 
        >>> # Variable presence
        >>> filtered = irw.filter(var="rt")
        >>> filtered = irw.filter(var=["wave", "cov_"])
        >>> 
        >>> # Tag metadata filtering
        >>> filtered = irw.filter(construct_type="Affective/mental health", sample="Educational")
        >>> 
        >>> # License filtering
        >>> filtered = irw.filter(license="CC BY 4.0")
        >>> 
        >>> # Filter by response category complexity
        >>> filtered = irw.filter(n_categories=2)  # binary
        >>> filtered = irw.filter(n_categories=[3, 5])  # small multi-category
        """
        if self.source != "main":
            raise ValueError(
                f"filter() is only available for main IRW datasets (source='main'). "
                f"Current source is '{self.source}'."
            )
        
        return filter_tables(
            self._datasets,
            n_responses=n_responses,
            n_categories=n_categories,
            n_participants=n_participants,
            n_items=n_items,
            responses_per_participant=responses_per_participant,
            responses_per_item=responses_per_item,
            density=density,
            var=var,
            age_range=age_range,
            child_age=child_age,
            construct_type=construct_type,
            construct_name=construct_name,
            sample=sample,
            measurement_tool=measurement_tool,
            item_format=item_format,
            language=language,
            longitudinal=longitudinal,
            license=license,
        )
    
    def get_filters(self) -> List[str]:
        """
        Get the list of available filter argument names for the filter() method.
        
        Returns
        -------
        list of str
            List of all available filter parameter names that can be used with filter().
        
        Examples
        --------
        >>> from irw_py import IRW
        >>> irw = IRW()
        >>> 
        >>> # See what filters are available
        >>> filters = irw.get_filters()
        >>> print(filters)
        """
        return get_filters()
    
    def describe_filter(self, filter_name: str) -> Optional[Dict[str, Any]]:
        """
        Describe a specific filter and show its available values/statistics.
        
        Returns a dictionary with:
        - 'description': What the filter does
        - 'info': Additional information about available values:
                  - For numeric filters: dict with summary statistics (min, max, mean, median, etc.)
                  - For categorical filters: pandas.Series with value_counts
                  - For special filters: dict with descriptive information
        
        **Note:** This method only works for the main IRW database (source="main").
        It is not available for simulation or competition datasets.
        
        **Performance Note:** This method only loads the necessary metadata (stats, tags, or
        bibliography) based on the filter type. For example, requesting 'n_responses' only loads
        the stats table, not tags or bibliography. All loaded data is cached for subsequent calls.
        If you plan to explore multiple filters across different types, you can also call
        list_tables(include_metadata=True) once and then access the DataFrame columns directly.
        
        Parameters
        ----------
        filter_name : str
            Name of the filter to describe (e.g., 'n_responses', 'construct_type', 'var').
        
        Returns
        -------
        dict or None
            Dictionary with description and stats/values/info, or None if filter not found.
        
        Raises
        ------
        ValueError
            If called on a non-main IRW client (source != "main").
        
        Examples
        --------
        >>> from irw_py import IRW
        >>> irw = IRW()  # source="main" by default
        >>> 
        >>> # Describe numeric filter
        >>> info = irw.describe_filter('n_responses')
        >>> print(info['description'])  # What the filter does
        >>> print(info['info'])  # Summary statistics (min, max, mean, etc.)
        >>> 
        >>> # Describe categorical filter
        >>> info = irw.describe_filter('construct_type')
        >>> print(info['description'])  # What the filter does
        >>> print(info['info'])  # value_counts Series
        >>> 
        >>> # Describe variable filter
        >>> info = irw.describe_filter('var')
        >>> print(info['description'])
        >>> print(info['info'])  # Descriptive information
        """
        if self.source != "main":
            raise ValueError(
                f"describe_filter() is only available for main IRW datasets (source='main'). "
                f"Current source is '{self.source}'."
            )
        
        return describe_filter(self._datasets, filter_name)

