# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What This Package Does

`irw` is a Python client for the [Item Response Warehouse (IRW)](https://itemresponsewarehouse.org/), a harmonized repository of item response data (survey responses, test scores) hosted on Redivis. It lets researchers discover, filter, download, and reformat psychometric datasets.

## Installation & Setup

```bash
# Development install
pip install -e .

# Dependencies: redivis, pandas, numpy (see pyproject.toml)
```

No build step required. Uses modern `pyproject.toml` with setuptools. Python 3.9–3.13 supported.

Redivis authentication is handled by the `redivis` client library itself (interactive browser login on first use).

## Architecture

All public functions are exposed through `src/irw/api.py`, which acts as a facade over two internal layers:

```
api.py  (public surface — all user-facing functions)
  ├── operations/   (business logic: fetch, filter, list_tables, info, filter_info)
  └── utils/
        ├── long2resp.py        (long→wide format conversion for response matrices)
        ├── table_helpers.py    (metadata lookups, BibTeX extraction)
        └── redivis/            (Redivis API integration: datasets, tables, metadata, item_text, cache)
```

`config.py` holds dataset references for the three data sources: `"main"`, `"sim"` (simulations), and `"comp"` (competitions). The same public API works across all three.

**Adding a main warehouse:** append a `(user, dataset_ref)` tuple to `MAIN_REFS` in `config.py` only. See `docs/DEVELOPERS.md` for details and test commands.

### Key design decisions

- **Caching** (`utils/redivis/cache.py`): Redivis metadata tables are cached in-process to avoid redundant network calls. Cache is per-session only.
- **`fetch()` return type varies**: returns a single DataFrame for a single table name, or a `dict[name → DataFrame]` for a list or a filtered Series.
- **Warning suppression** in `__init__.py`: silences a known noisy Redivis warning about missing reference IDs.
- **Internal naming**: functions in `operations/` use a leading underscore convention — they are not part of the public API.

### Typical user workflow

1. `irw.list_tables()` — discover available datasets
2. `irw.filter(**kwargs)` — narrow by metadata (construct, sample size, license, etc.)
3. `irw.fetch(name)` — download table(s) as pandas DataFrames in long format (`id`, `item`, `resp`)
4. `irw.long2resp(df)` — pivot to wide-format response matrix for analysis
5. `irw.itemtext(name)` / `irw.save_bibtex(name)` — retrieve item text and citations

## Testing

There is currently no test suite. When adding tests, use `pytest` and place them in a `tests/` directory at the repo root.

## Code Style

No linter or formatter is configured. Follow existing style: no type annotations, plain docstrings, internal helpers prefixed with `_`.
