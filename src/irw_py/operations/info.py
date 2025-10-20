"""Database information operations for IRW data.

This module contains functionality for getting database-level information
and statistics about IRW datasets.
"""

from datetime import datetime
from typing import List, Any
from ..utils.redivis import _init_main_datasets


def info() -> None:
    """
    Print information about the IRW database (main datasets).

    Displays database-level statistics including total tables, data size,
    and timestamps from the Redivis datasets.

    Examples
    --------
    >>> from irw_py.operations.info import info
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


def _print_dataset_info(ds_list: List[Any], *, title: str) -> None:
    """Internal helper to compute and print dataset information summary."""
    
    # Calculate combined totals from dataset properties
    total_table_count = 0
    total_size_gb = 0
    created_at_all = []
    updated_at_all = []
    
    for ds in ds_list:
        total_table_count += ds.properties.get('tableCount', 0)
        total_size_gb += ds.properties.get('totalNumBytes', 0) / (1024**3)
        created_at_all.append(ds.properties.get('createdAt', 0) / 1000)
        updated_at_all.append(ds.properties.get('updatedAt', 0) / 1000)
    
    # Print formatted output
    print("-" * 50)
    print(title)
    print("-" * 50)
    print(f"{'Total Table Count:':<25} {total_table_count}")
    print(f"{'Total Data Size:':<25} {total_size_gb:.2f} GB")
    
    if created_at_all:
        earliest_created = min(created_at_all)
        latest_updated = max(updated_at_all)
        print(f"{'Earliest Created At:':<25} {datetime.fromtimestamp(earliest_created, tz=None).strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"{'Latest Updated At:':<25} {datetime.fromtimestamp(latest_updated, tz=None).strftime('%Y-%m-%d %H:%M:%S')}")
    
    print("-" * 50)
    print(f"{'Data Website:':<25} https://datapages.github.io/irw/")
    print(f"{'Methodology:':<25} Tables harmonized as per https://datapages.github.io/irw/standard.html")
    print(f"{'Usage Information:':<25} License & citation info: https://datapages.github.io/irw/docs.html")
    print("-" * 50)