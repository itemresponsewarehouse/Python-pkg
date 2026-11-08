"""Database information operations for IRW data.

This module contains functionality for getting database-level information
and statistics about IRW datasets.
"""

import warnings
from datetime import datetime
from typing import Any, Callable, Dict, List
from ..config import SOURCES
from ..utils.redivis import _init_main_datasets

_FAMILY_LABELS = {
    "core": "Core", "nom": "Nominal", "sim": "Simsyn",
    "comp": "Competitions", "conj": "Conjoint", "rank": "Rankings",
}


def info() -> None:
    """
    Print information about the IRW database (core datasets).

    Displays database-level statistics including total tables, data size,
    and timestamps from the Redivis datasets.

    Examples
    --------
    >>> from irw.operations.info import info
    >>> info()
    """
    # Get the main IRW datasets
    ds_list = _init_main_datasets()
    _print_dataset_info(ds_list, title="IRW Database Information")


def info_for(datasets: List[Any], *, title: str = "IRW Dataset Information") -> None:
    """
    Print information for the provided IRW dataset list (e.g., sim or comp).

    Parameters
    ----------
    datasets : List[Any]
        Redivis dataset objects to summarize.
    """
    _print_dataset_info(datasets, title=title)


def info_all_families(open_family: Callable[[str], List[Any]]) -> None:
    """
    Print table count and size for every data family, with totals across them.

    A family that cannot be opened is shown as unavailable and named in a
    warning, so one family's outage neither stops the report nor passes as a
    smaller total.

    Parameters
    ----------
    open_family : callable
        Takes a source name and returns its opened Redivis datasets.
    """
    opened = {}
    failures = {}
    for source in SOURCES:
        try:
            opened[source] = open_family(source)
        except Exception as e:  # any one family's failure, whatever its type
            failures[source] = e
            opened[source] = []

    totals = {source: _totals(ds_list) for source, ds_list in opened.items() if ds_list}
    if not totals:
        # Nothing opened: usually a login problem, and that error says what to do
        if failures:
            raise next(iter(failures.values()))
        raise RuntimeError("Could not open any IRW data family.")
    for source, e in failures.items():
        warnings.warn(f"Could not open the '{source}' family: {e}", stacklevel=2)

    print("-" * 50)
    print("IRW Database Information (All Families)")
    print("-" * 50)
    print(f"{'Family':<14} {'source':<8} {'Tables':>8} {'Size':>14}")
    for source in SOURCES:
        label = _FAMILY_LABELS.get(source, source)
        if source in totals:
            t = totals[source]
            print(f"{label:<14} {source:<8} {t['tables']:>8} {t['size_gb']:>11.2f} GB")
        else:
            print(f"{label:<14} {source:<8} {'unavailable':>8}")
    print("-" * 50)
    total_label = "Total Table Count:" if len(totals) == len(SOURCES) else "Total (available only):"
    print(f"{total_label:<25} {sum(t['tables'] for t in totals.values())}")
    print(f"{'Total Data Size:':<25} {sum(t['size_gb'] for t in totals.values()):.2f} GB")
    _print_dates(
        min(t['created'] for t in totals.values()),
        max(t['updated'] for t in totals.values()),
    )
    print("One family only: irw.info(source=\"<source>\")")
    _print_footer()


def _totals(ds_list: List[Any]) -> Dict[str, float]:
    """Combined table count, size and date range of opened datasets."""
    return {
        'tables': sum(ds.properties.get('tableCount', 0) for ds in ds_list),
        'size_gb': sum(ds.properties.get('totalNumBytes', 0) for ds in ds_list) / (1024**3),
        'created': min(ds.properties.get('createdAt', 0) for ds in ds_list) / 1000,
        'updated': max(ds.properties.get('updatedAt', 0) for ds in ds_list) / 1000,
    }


def _print_dates(earliest_created: float, latest_updated: float) -> None:
    print(f"{'Earliest Created At:':<25} {datetime.fromtimestamp(earliest_created, tz=None).strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'Latest Updated At:':<25} {datetime.fromtimestamp(latest_updated, tz=None).strftime('%Y-%m-%d %H:%M:%S')}")


def _print_footer() -> None:
    print("-" * 50)
    print(f"{'Data Website:':<25} https://datapages.github.io/irw/")
    print(f"{'Methodology:':<25} Tables harmonized as per https://datapages.github.io/irw/standard.html")
    print(f"{'Usage Information:':<25} License & citation info: https://datapages.github.io/irw/docs.html")
    print("-" * 50)


def _print_dataset_info(ds_list: List[Any], *, title: str) -> None:
    """Internal helper to compute and print dataset information summary."""
    print("-" * 50)
    print(title)
    print("-" * 50)
    if ds_list:
        t = _totals(ds_list)
        print(f"{'Total Table Count:':<25} {t['tables']}")
        print(f"{'Total Data Size:':<25} {t['size_gb']:.2f} GB")
        _print_dates(t['created'], t['updated'])
    else:
        print(f"{'Total Table Count:':<25} 0")
        print(f"{'Total Data Size:':<25} 0.00 GB")
    _print_footer()
