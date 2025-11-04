"""
Basic usage example for the irw_py package.

This script demonstrates the basic workflow for using irw_py.
"""

from irw_py import IRW

# Initialize IRW client
irw = IRW()

# Get database information
irw.info()

# View available tables
tables = irw.list_tables()
tables_with_metadata = irw.list_tables(include_metadata=True)

# Fetch tables
df = irw.fetch("agn_kay_2025")
dfs = irw.fetch(["agn_kay_2025", "pks_probability"])  # Fetch multiple tables

# Explore available filters
irw.get_filters()
irw.describe_filter('n_responses') # quantitative filter examplele
irw.describe_filter('construct_type') # qualitative filter example

# Filter and fetch tables
filtered = irw.filter(n_responses=[1000, None], construct_type="Affective/mental health")
dfs = irw.fetch(filtered)

