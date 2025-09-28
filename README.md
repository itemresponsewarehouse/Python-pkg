# `irw_py`: A Python Package for the Item Response Warehouse

This repository hosts the Python package `irw_py`, which provides programmatic access to the [Item Response Warehouse (IRW)](https://datapages.github.io/irw/), an open repository of harmonized item response data.

## Installation
You can install the latest development version directly from GitHub:
```bash
pip install "git+https://github.com/itemresponsewarehouse/Python-pkg.git"
```

Or install from PyPI (when available):
```bash
pip install irw_py
```

## IMPORTANT: Redivis Authentication

## Usage Examples
```python
from irw_py import IRW

# Initiliaze
irw = IRW()

# View available tables in IRW
irw.list_tables()

# Fetch a single dataset
df = irw.fetch("swmd_mokken")

# Fetch multiple datasets at once
dfs = irw.fetch(["swmd_mokken", "pks_probability"])
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
