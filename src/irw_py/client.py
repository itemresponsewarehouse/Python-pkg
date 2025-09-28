from __future__ import annotations
from dataclasses import dataclass, field
from typing import Iterable, Union, Dict, Optional, List, Tuple, Any, ClassVar
import pandas as pd

from ._io import init_dataset
from ._fetch_helpers import fetch_one


@dataclass
class IRW:
    """
    IRW client for Redivis-hosted item response data.

    Provides programmatic access to harmonized item response datasets.

    Examples
    --------
    >>> from irw_py import IRW
    >>> irw = IRW()
    >>> df = irw.fetch("abortion")
    >>> dfs = irw.fetch(["abortion", "pks_probability"], dedup=True)
    >>> tbls = irw.list_tables()
    """

    # Redivis dataset references
    _MAIN_REFS: ClassVar[Tuple[Tuple[str, str], Tuple[str, str]]] = (
        ("datapages", "item_response_warehouse:as2e"),
        ("datapages", "item_response_warehouse_2:epbx"),
    )
    _SIM_REF: ClassVar[Tuple[str, str]] = ("bdomingu", "irw_simsyn:0btg")

    # Cached dataset handles
    _main: List[Any] = field(default_factory=list)
    _sim: Any = None

    def __post_init__(self):
        # Initialize and cache datasets once
        self._main = [init_dataset(*ref) for ref in self._MAIN_REFS]
        self._sim = init_dataset(*self._SIM_REF)

    # ---------------- Public API ----------------

    def fetch(
        self,
        name: Union[str, Iterable[str]],
        *,
        sim: bool = False,
        dedup: bool = False,
    ) -> Union[pd.DataFrame, Dict[str, Optional[pd.DataFrame]], None]:
        """
        Fetch one or more IRW tables into a pandas DataFrame.

        Parameters
        ----------
        name : str | Iterable[str]
            Single table id → returns DataFrame or None.
            Multiple        → returns dict[name -> DataFrame | None].
        sim : bool, default False
            If True, fetch from the simulated dataset collection.
        dedup : bool, default False
            After fetch, keep one row per (id,item[,wave]).

        Returns
        -------
        pandas.DataFrame | dict[str, pandas.DataFrame | None] | None
        """
        if not name:
            raise ValueError("name cannot be empty")

        if isinstance(name, str):
            return fetch_one(self, name, sim=sim, dedup=dedup)

        # Validate iterable
        try:
            name_list = list(name)
            if not name_list:
                raise ValueError("name iterable cannot be empty")
        except TypeError:
            raise TypeError("name must be a string or iterable of strings")

        out: Dict[str, Optional[pd.DataFrame]] = {}
        for nm in name_list:
            if not isinstance(nm, str):
                raise TypeError(f"All names must be strings, got {type(nm)}")
            key = str(nm)
            out[key] = fetch_one(self, key, sim=sim, dedup=dedup)
        return out

    def list_tables(self, *, sim: bool = False) -> pd.DataFrame:
        """
        List available tables in IRW.

        Parameters
        ----------
        sim : bool, default False
            If True, list from the simulation dataset; else from the main datasets.

        Returns
        -------
        pandas.DataFrame
            Columns:
              - name (str): table name (sorted alphabetically)
              - numRows (int | None): number of rows
              - variableCount (int | None): number of variables
        """
        rows: List[Dict[str, Any]] = []
        for ds in self._datasets(sim):
            try:
                tables = ds.list_tables()
            except Exception:
                # Skip dataset if listing fails
                continue

            for t in tables:
                props = getattr(t, "properties", {}) or {}
                rows.append(
                    {
                        "name": getattr(t, "name", None),
                        "numRows": props.get("numRows"),
                        "variableCount": props.get("variableCount"),
                    }
                )

        if not rows:
            return pd.DataFrame(columns=["name", "numRows", "variableCount"])

        out = pd.DataFrame(rows)
        out = out.sort_values("name", kind="stable").reset_index(drop=True)
        return out

    # ---------------- Discoverability ----------------

    def __dir__(self) -> list[str]:
        """
        Limit tab-completion and dir() output to public-facing methods only.
        Automatically includes all methods that don't start with '_'.
        """
        return sorted(
            [
                attr
                for attr in self.__class__.__dict__
                if callable(getattr(self, attr, None)) and not attr.startswith("_")
            ]
        )

    # ---------------- Internal ----------------

    def _datasets(self, sim: bool):
        """Yield cached datasets in the correct search order."""
        if sim:
            yield self._sim
        else:
            yield from self._main