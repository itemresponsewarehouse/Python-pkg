from __future__ import annotations
from dataclasses import dataclass, field
from typing import Iterable, Union, Dict, Optional, List, Tuple, Any, ClassVar
import pandas as pd

from ._io import init_dataset
from .mixin_fetch import FetchMixin
from .mixin_explore import ExploreMixin


@dataclass
class IRW(FetchMixin, ExploreMixin):
    """
    IRW client for Redivis-hosted item response data.

    Provides programmatic access to harmonized item response datasets.

    Examples
    --------
    >>> from irw_py import IRW
    >>> irw = IRW()
    >>> df = irw.fetch("agn_kay_2025")
    >>> dfs = irw.fetch(["agn_kay_2025", "pks_probability"], dedup=True)
    >>> tbls = irw.list_tables()
    """

    # Redivis dataset references
    _MAIN_REFS: ClassVar[Tuple[Tuple[str, str], Tuple[str, str]]] = (
        ("datapages", "item_response_warehouse:as2e"),
        ("datapages", "item_response_warehouse_2:epbx"),
    )
    _SIM_REF: ClassVar[Tuple[str, str]] = ("bdomingu", "irw_simsyn:0btg")
    _COMP_REF: ClassVar[Tuple[str, str]] = ("bdomingu", "irw_competitions:cmd7")

    # Cached dataset handles
    _main: List[Any] = field(default_factory=list)
    _sim: Any = None
    _comp: Any = None

    def __post_init__(self):
        """Initialize and cache dataset handles."""
        try:
            self._main = [init_dataset(*ref) for ref in self._MAIN_REFS]
            self._sim = init_dataset(*self._SIM_REF)
            self._comp = init_dataset(*self._COMP_REF)
        except Exception as e:
            raise RuntimeError(f"Failed to initialize IRW datasets: {e}") from e

    def __dir__(self) -> list[str]:
        """
        Limit tab-completion and dir() output to public-facing methods only.
        Automatically includes all methods that don't start with '_'.
        """
        # Get all methods from the class and all its mixins
        all_methods = []
        for cls in self.__class__.__mro__:
            all_methods.extend(cls.__dict__.keys())
        
        return sorted(
            [
                attr
                for attr in set(all_methods)  # Remove duplicates
                if callable(getattr(self, attr, None)) and not attr.startswith("_")
            ]
        )

    def _datasets(self, sim: bool = False, comp: bool = False):
        """
        Yield cached datasets in the correct search order.
        
        Parameters
        ----------
        sim : bool, default False
            If True, yield simulation dataset.
        comp : bool, default False
            If True, yield competitions dataset.
            
        Yields
        ------
        Any
            Dataset handle from Redivis.
            
        Raises
        ------
        ValueError
            If both sim and comp are True.
        """
        if sim and comp:
            raise ValueError("Cannot set both 'sim = True' and 'comp = True'. Please choose one source.")
        elif sim:
            yield self._sim
        elif comp:
            yield self._comp
        else:
            yield from self._main

    def get_dataset_info(self) -> Dict[str, Any]:
        """
        Get information about available datasets.
        
        Returns
        -------
        Dict[str, Any]
            Dictionary containing dataset information including names and references.

        Examples
        --------
        >>> from irw_py import IRW
        >>> irw = IRW()
        >>> 
        >>> # Get dataset information
        >>> info = irw.get_dataset_info()
        >>> print(info)
        >>> 
        >>> # Access specific dataset references
        >>> main_datasets = info["main_datasets"]
        >>> sim_dataset = info["simulation_dataset"]
        >>> comp_dataset = info["competitions_dataset"]
        """
        return {
            "main_datasets": [
                {"user": ref[0], "dataset": ref[1]} for ref in self._MAIN_REFS
            ],
            "simulation_dataset": {"user": self._SIM_REF[0], "dataset": self._SIM_REF[1]},
            "competitions_dataset": {"user": self._COMP_REF[0], "dataset": self._COMP_REF[1]},
        }