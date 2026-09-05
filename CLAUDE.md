# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What This Package Does

`irw` is a Python client for the [Item Response Warehouse (IRW)](https://itemresponsewarehouse.org/), a harmonized repository of item response data (survey responses, test scores) hosted on Redivis. It lets researchers discover, filter, download, and reformat psychometric datasets.

Project map: [`ARCHITECTURE.md`](https://github.com/ben-domingue/irw/blob/main/ARCHITECTURE.md) in `ben-domingue/irw` — which repo owns
what, where the data lives, and which document is authoritative when two disagree.

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

`config.py` holds every Redivis reference the package has: `MAIN_REFS` (the
response-data warehouses), `SIM_REF` (simulations), `COMP_REF` (competitions),
`META_REF` plus `META_TABLES` (metadata, tags, bibliography, collections), and
`ITEMTEXT_REFS` (item text). `MAIN_REFS` and `ITEMTEXT_REFS` are lists because
Redivis caps a dataset at 1000 tables. The same public API works across the
data sources.

The `nom` source (`irw_nominal`) that both R configs define is **missing here** —
see issue #9. Do not read the list above as complete.

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

Tests live in `tests/` at the repo root and run with `pytest`:

```bash
pip install pytest
python -m pytest tests/ -q
```

They are offline — Redivis is mocked — so no credentials are needed.
`tests/test_main_refs.py` and `tests/test_itemtext_refs.py` are the templates to
follow when adding a warehouse or an item-text shard.

## Code Style

No linter or formatter is configured. Follow existing style: numpydoc-style
docstrings, and internal helpers prefixed with `_`.

Type annotations are used — `config.py` and `operations/version.py` are fully
annotated and every public signature in `api.py` is. One constraint on them:
the package supports Python 3.9, so PEP 604 unions (`Exception | None`) need
`from __future__ import annotations` at the top of the file. A missing one
broke `import irw` on 3.9; `tests/test_metadata_refs.py` now checks for it.
