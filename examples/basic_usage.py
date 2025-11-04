"""
Basic usage example for the irw_py package.

This script demonstrates the basic workflow for using irw_py.
"""

from irw_py import IRW

# Initialize main IRW client (default)
irw = IRW()  # or IRW(source="main")

# Get database information
irw.info()

# View available tables
tables = irw.list_tables()
tables_with_metadata = irw.list_tables(include_metadata=True)

# Fetch tables
df = irw.fetch("agn_kay_2025")
dfs = irw.fetch(["agn_kay_2025", "pks_probability"])  # Fetch multiple tables


