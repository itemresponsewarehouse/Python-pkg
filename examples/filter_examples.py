"""
Filter examples for the irw_py package.

This script demonstrates how to filter IRW tables based on metadata criteria.
Filtering is only available for the main IRW database (source="main").

The typical workflow:
1. Explore available filters using get_filters() and describe_filter()
2. Filter based on those values
3. Fetch the filtered datasets
"""

from irw_py import IRW

# Initialize main IRW client (required for filtering)
irw = IRW()  # or IRW(source="main")

# Step 1: Explore available filter options
# Get list of available filter arguments
filters = irw.get_filters()

# Describe specific filters to see their values/stats
# Returns dict with 'description' and 'info'
n_responses_info = irw.describe_filter('n_responses')
print(n_responses_info['description'])  # What the filter does
print(n_responses_info['info'])  # Summary statistics (min, max, mean, etc.)

construct_type_info = irw.describe_filter('construct_type')
print(construct_type_info['description'])  # What the filter does
print(construct_type_info['info'])  # value_counts Series

# For exploring multiple filters, use list_tables(include_metadata=True) once
metadata = irw.list_tables(include_metadata=True)
# Then access columns directly for custom exploration
metadata['n_responses'].describe()
metadata['construct_type'].value_counts()

# Step 3: Filter based on explored values
# Tag metadata filtering
filtered = irw.filter(construct_type="Affective/mental health", sample="Educational")

# Filter by language and age range
filtered = irw.filter(language="eng", age_range="Adult (18+)")

# Numeric filters (use None for infinity)
filtered = irw.filter(n_responses=[1000, None], n_items=[10, 50])  # >= 1000 responses, 10-50 items

# Filter by participants (density defaults to [0.5, 1] to exclude sparse matrices)
filtered = irw.filter(n_participants=[500, None])

# Disable default density filter to include sparse matrices
filtered = irw.filter(n_participants=[500, None], density=None)

# Variable presence filtering
filtered = irw.filter(var="rt")  # datasets with response time variable
filtered = irw.filter(var=["wave", "cov_"])  # datasets with BOTH wave and cov_ prefix variables

# Longitudinal filtering
filtered = irw.filter(longitudinal=True)  # only longitudinal datasets
filtered = irw.filter(longitudinal=False)  # exclude longitudinal datasets

# License filtering
filtered = irw.filter(license="CC BY 4.0")

# Combined filters - multiple criteria together
filtered = irw.filter(
    n_responses=[500, None],  # >= 500 responses
    construct_type="Affective/mental health",
    longitudinal=True,
    language="eng"
)

# Complex filter with numeric ranges and tag filters
filtered = irw.filter(
    n_responses=[1000, 10000],  # between 1000 and 10000
    n_items=[10, 50],  # between 10 and 50 items
    construct_type="Cognitive",
    sample="Educational",
    var="rt",
    density=[0.5, 1.0]  # density between 0.5 and 1.0
)

# Step 4: Fetch the filtered datasets
# Filter returns a pandas Series, which can be passed directly to fetch()
dfs = irw.fetch(filtered)  # Returns dict mapping table names to DataFrames
