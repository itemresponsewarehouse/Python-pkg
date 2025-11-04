"""
Other IRW databases example for the irw_py package.

This script demonstrates the workflow for using irw_py to fetch data from the simulation and competition databases.
"""

from irw_py import IRW

# Fetch simulation data
irw_sim = IRW(source="sim")  # initialize IRW simulation client
sim_tables = irw_sim.list_tables()
df_sim = irw_sim.fetch("gilbert_meta_3")

# Fetch competition data
irw_comp = IRW(source="comp")
comp_tables = irw_comp.list_tables()
df_comp = irw_comp.fetch("collegefb_2021and2022")