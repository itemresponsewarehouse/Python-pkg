# `irw`: A Python Package for the Item Response Warehouse

This repository hosts the Python package `irw`, which provides programmatic access to the [Item Response Warehouse (IRW)](https://datapages.github.io/irw/), an open repository of harmonized item response data hosted on Redivis.

## Installation

You can install the latest development version directly from GitHub:
```bash
pip install "git+https://github.com/itemresponsewarehouse/Python-pkg.git"
```

## IMPORTANT: Redivis Authentication

The IRW tables are hosted on [Redivis](https://redivis.com), a data management platform. To access these datasets, you'll need to:

1. Have a Redivis account (create one at <https://redivis.com/?createAccount> if you don't have one).

2. Authenticate using the Redivis Python Client:
   1. When you first use a function in `irw` that connects to Redivis (e.g. `list_tables()`), a browser window will open, prompting you to sign in to your Redivis account.
   2. After signing in, click **Allow** to grant access for the Redivis Python Client.
   3. Once authentication is successful, close the browser window. You will see the message "Authentication was successful" in console.

**Note:** You only need to authenticate once per session. For detailed instructions, refer to the [Redivis Python Client documentation](https://apidocs.redivis.com/client-libraries/redivis-python).

## Quick Start

```python
import irw

# Get database information
irw.info()

# View available tables
tables = irw.list_tables()
tables_with_metadata = irw.list_tables(include_metadata=True)

# Fetch a table
df = irw.fetch("agn_kay_2025")

# Fetch multiple tables
dfs = irw.fetch(["agn_kay_2025", "pks_probability"])

# Fetch directly in wide format (response matrix)
df_wide = irw.fetch("agn_kay_2025", wide=True)

# Explore available filters
filters = irw.get_filters()  # Returns list of filter names
irw.describe_filter('construct_type')  # Get values for a specific filter

# Filter and fetch tables
filtered = irw.filter(n_responses=[1000, None], construct_type="Affective/mental health")
dfs = irw.fetch(filtered)

# Get table info
irw.info("agn_kay_2025")  # Table metadata

# Get item text
item_text = irw.itemtext("agn_kay_2025")

# Get BibTeX citation
irw.save_bibtex("agn_kay_2025")  # Returns BibTeX entry
irw.save_bibtex("agn_kay_2025", "refs.bib")  # Save to file
irw.save_bibtex(["table1", "table2"], "refs.bib")  # Save multiple tables

# Download table
irw.download("agn_kay_2025", path="data.csv")

# Convert to response matrix
df = irw.fetch("agn_kay_2025")
resp_matrix = irw.long2resp(df)
# or fetch directly in wide format
resp_matrix = irw.fetch("agn_kay_2025", wide=True)
```

## Usage Examples

See the `examples/` directory:
- `example.py` - Complete workflow example
- `available_methods.md` - Reference guide for all available methods


## Development

### Setting up Development Environment

1. **Clone the repository**:
   ```bash
   git clone https://github.com/itemresponsewarehouse/Python-pkg.git
   cd Python-pkg
   ```

2. **Create a virtual environment**:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install development dependencies**:
   ```bash
   pip install -e .
   ```
