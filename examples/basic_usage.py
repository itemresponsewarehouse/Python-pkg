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

# Fetch simulation data
irw_sim = IRW(source="sim")  # initialize IRW simulation client
sim_tables = irw_sim.list_tables()
df_sim = irw_sim.fetch("gilbert_meta_3")

# Fetch competition data
irw_comp = IRW(source="comp")
comp_tables = irw_comp.list_tables()
df_comp = irw_comp.fetch("collegefb_2021and2022")

