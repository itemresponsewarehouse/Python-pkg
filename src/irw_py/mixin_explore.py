"""
Exploration functionality for IRW package.

This module contains methods for exploring and discovering available datasets
in the IRW repository.
"""

from typing import List, Dict, Any
import pandas as pd


class ExploreMixin:
    """
    Mixin class providing exploration functionality for IRW.
    
    This mixin contains methods for discovering and exploring available
    datasets, tables, and metadata.
    """
    
    def list_tables(self, *, sim: bool = False, comp: bool = False) -> pd.DataFrame:
        """
        List available tables in IRW.

        Parameters
        ----------
        sim : bool, default False
            If True, list from the simulation dataset; else from the main datasets.
        comp : bool, default False
            If True, list from the competitions dataset; else from the main datasets.

        Returns
        -------
        pandas.DataFrame
            Columns:
              - name (str): table name (sorted alphabetically)
              - numRows (int | None): number of rows
              - variableCount (int | None): number of variables
              
        Raises
        ------
        ValueError
            If both sim and comp are True.

        Examples
        --------
        >>> from irw_py import IRW
        >>> irw = IRW()
        >>> 
        >>> # List all available tables
        >>> tables = irw.list_tables()
        >>> 
        >>> # List tables from simulation dataset
        >>> sim_tables = irw.list_tables(sim=True)
        
        >>> # List tables from competitions dataset
        >>> comp_tables = irw.list_tables(comp=True)
        >>> 
        >>> # Filter tables by size
        >>> large_tables = tables[tables["numRows"] > 10000]
        """
        if sim and comp:
            raise ValueError("Cannot set both 'sim = True' and 'comp = True'. Please choose one source.")
        rows: List[Dict[str, Any]] = []
        for ds in self._datasets(sim, comp):
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
