"""
Workflow example for the irw package.

This demonstrates a typical workflow for working with IRW data:
1. Explore available tables and metadata
2. Filter tables by criteria
3. Fetch and analyze data
4. Access additional resources (item text, citations)
5. Work with other IRW databases
"""

import irw

# ============================================================================
# 1. EXPLORE AVAILABLE DATA
# ============================================================================

# Get overview of the IRW database
irw.info()

# List available tables (basic listing)
tables = irw.list_tables()

# List tables with full metadata (stats, tags, bibliography)
tables_with_metadata = irw.list_tables(include_metadata=True)

# ============================================================================
# 2. FILTER TABLES BY CRITERIA
# ============================================================================

# Get list of available filters
available_filters = irw.get_filters()

# Explore filter details and available values
# Categorical filter (returns value counts)
filter_info = irw.describe_filter('construct_type')
filter_info["description"] # What the filter is and how to use it
filter_info["values"] # Available values with counts

filter_info = irw.describe_filter('var')
filter_info["description"]
filter_info["values"] 

# Numeric filter (returns summary statistics)
filter_info = irw.describe_filter('n_responses')
filter_info["description"]
filter_info["values"] 

# Filter tables by multiple criteria
# Example: Find tables with ≥10000 responses, CC BY 4.0 license, 
#          containing covariates, in the "Affective/mental health" category
filtered = irw.filter(
    construct_type="Affective/mental health",
    license="CC BY 4.0",
    var="cov_",  # Tables with variables starting with "cov_" (covariates)
    n_responses=[10000, None]  # [min, max] where None means no upper limit
)

# ============================================================================
# 3. FETCH AND WORK WITH DATA
# ============================================================================

# Fetch a single table
table_name = "agn_kay_2025"
df = irw.fetch(table_name)

# Convert to response matrix
resp_matrix = irw.long2resp(df)
# Alternatively, fetch directly in wide format (response matrix)
resp_matrix = irw.fetch(table_name, wide=True)

# Fetch multiple tables (from filtered results or a list)
filtered_tables = irw.fetch(filtered[:3]) # Fetch first 3 tables from filtered results

# ============================================================================
# 4. ACCESS ADDITIONAL RESOURCES
# ============================================================================

# Get metadata information for a table
irw.info(table_name)

# Get item-level text (if available)
irw.list_tables_with_itemtext() # Find tables with item-level text available
item_text = irw.itemtext("360emergencymed_azami_2024")

# Get BibTeX citation
irw.save_bibtex(table_name)  # Returns BibTeX entry
# Or save to file
irw.save_bibtex(table_name, "refs.bib")  # Save single table to file
# Or save multiple tables at once
irw.save_bibtex(["agn_kay_2025", "pks_probability"], "refs.bib")

# Download table to file
irw.download(table_name, path="data.csv")

# ============================================================================
# 5. OTHER IRW DATABASES
# ============================================================================

# Simulation datasets
sim_tables = irw.list_tables(source="sim")
df_sim = irw.fetch("gilbert_meta_3", source="sim")

# Competition datasets
comp_tables = irw.list_tables(source="comp")
df_comp = irw.fetch("collegefb_2021and2022", source="comp")
