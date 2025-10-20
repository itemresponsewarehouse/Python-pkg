# `irw_py`: A Python Package for the Item Response Warehouse

This repository hosts the Python package `irw_py`, which provides programmatic access to the [Item Response Warehouse (IRW)](https://datapages.github.io/irw/), an open repository of harmonized item response data hosted on Redivis.

## Installation
You can install the latest development version directly from GitHub:
```bash
pip install "git+https://github.com/itemresponsewarehouse/Python-pkg.git"
```

## IMPORTANT: Redivis Authentication
The IRW tables are hosted on [Redivis](https://redivis.com), a data management platform. To access these datasets, you'll need to:

1.  Have a Redivis account (create one at <https://redivis.com/?createAccount> if you don't have one).

2.  Authenticate using the Redivis Python Client:

    1.  When you first use a function in `irw_py` that connects to Redivis (e.g. `list_tables()`), a browser window will open, prompting you to sign in to your Redivis account.

    2.  After signing in, click **Allow** to grant access for the Redivis Python Client.

    3.  Once authentication is successful, close the browser window. You will see the message "Authentication was successful" in console.

**Note:** You only need to authenticate once per session. For detailed instructions, refer to the [Redivis Python Client documentation](https://apidocs.redivis.com/client-libraries/redivis-python).


## Usage Examples

```python
from irw_py import IRW, IRWSim, IRWComp

# Initialize main IRW client
irw = IRW()
# Get database information
irw.info()  

# View available tables 
tables = irw.list_tables()
tables_with_metadata = irw.list_tables(include_metadata = True) 

# Fetch tables
df = irw.fetch("agn_kay_2025") 
dfs = irw.fetch(["agn_kay_2025", "pks_probability"]) # Fetch multiple tables

# Fetch simulation data
irw_sim = IRWSim() # initialize IRW simulation client
sim_tables = irw_sim.list_tables()
df_sim = irw_sim.fetch("gilbert_meta_3")

# Fetch competition data
irw_comp = IRWComp()
comp_tables = irw_comp.list_tables()
df_comp = irw_comp.fetch("collegefb_2021and2022")
```

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
