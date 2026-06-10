# AGENTS.md

Guidance for AI/coding agents working in this repository.

## Project Overview

`moodys_datahub` is a Python package for selecting, downloading, and processing
Moody's DataHub exports over SFTP. The public API is centered on
`moodys_datahub.Sftp`.

Main source files:

- `src/moodys_datahub/tools.py`: public `Sftp` class and helper workflows.
- `src/moodys_datahub/process.py`: filtering, pandas/Polars processing, downloads.
- `src/moodys_datahub/preflight.py`: dry-run/preflight report helpers.
- `src/moodys_datahub/selection.py`: data product/table/path selection.
- `src/moodys_datahub/connection.py`: SFTP connection and export discovery.
- `src/moodys_datahub/utils.py`: file readers, writers, filters, parallel helpers.
- `src/moodys_datahub/load_data.py`: packaged metadata loaders.

## Working Rules

- Preserve the existing public API unless the user explicitly asks for a
  breaking change.
- Prefer small, focused commits. Avoid mixing data workbook updates, release
  builds, docs, and code changes unless the user asks for a release commit.
- Do not revert user changes in a dirty worktree.
- Avoid destructive git commands such as `git reset --hard` or checkout-based
  reverts unless explicitly requested.
- Use ASCII for new text unless the edited file already requires Unicode.

## Environment and Tests

Use the project conda environment when available:

```powershell
conda activate moody_datahub
$env:PYTHONPATH = "src"
python -m pytest -q
```

Useful focused checks:

```powershell
python -m py_compile src/moodys_datahub/process.py src/moodys_datahub/tools.py
python -m pytest tests/test_process_and_tools.py -q
python -m pytest tests/test_metadata_selection_connection.py -q
```

Build and package checks:

```powershell
python -m build
python -m twine check dist/*
```

## Release Procedure

For a stable release:

1. Update `pyproject.toml` version.
2. Update wheel links in `README.md` and MkDocs markdown pages.
3. Rebuild `dist/` from a clean working tree.
4. Run tests and `twine check`.
5. Commit release changes.
6. Push `main`.
7. Create and push the release tag, e.g. `v1.2.0`.
8. Create the GitHub release and attach both wheel and sdist.

Do not move a published tag unless the user explicitly approves it. If a tag is
moved, verify the release source archive and attached assets still match the
intended release.

## Documentation

Docs exist in both:

- `README.md`: concise project overview, install, main workflows.
- `mkdocs/`: documentation source pages used for the GitHub Pages docs.

When changing user-facing behavior, update both README and MkDocs markdown
pages where relevant. The notebooks in `mkdocs/*.ipynb` may contain older
examples; update them only when the user asks or when they are the source of a
published page.

Important MkDocs pages:

- `mkdocs/moody-s datahub.md`: overview.
- `mkdocs/how_to_get_started.md`: getting-started walkthrough.
- `mkdocs/api_reference.md`: public API reference.
- `mkdocs/reference.md`: compatibility reference page.

## Packaged Data Files

Packaged resources live in `src/moodys_datahub/data`.

Files included in the wheel are configured in `pyproject.toml` under
`[tool.setuptools.package-data]`:

- `bvd_numbers.txt`: example/template BvD list for batch workflows.
- `country_codes.xlsx`: country-code metadata for BvD prefix/country lookup.
- `data_dict.xlsx`: broad packaged DataHub data dictionary used by
  `search_dictionary()`. It may include products that are not licensed or
  available in the current SFTP export.
- `data_products.xlsx`: known DataHub product metadata.
- `date_cols.xlsx`: known date columns used by date helpers.
- `products.xlsx`: batch-search template used by `batch_bvd_search()`.

`data_dict_available.xlsx` provides a repository overview of the data products
available to the current setup/license. It is intentionally excluded by
`MANIFEST.in`. Do not assume it ships in the wheel unless packaging rules are
changed deliberately.

## Behavior Notes

- `paramiko==3.5.1` is pinned because `pysftp` is incompatible with newer
  Paramiko releases. Long-term, replacing `pysftp` with native Paramiko is the
  preferred fix.
- `Sftp(interactive=False)` should not launch widget prompts. Missing choices
  should fail explicitly or be reported through dry-run preflight.
- `Sftp(offline=True)` must not connect to SFTP during construction. Packaged
  metadata helpers such as `search_dictionary()`, `table_dates()`, and
  `search_country_codes()` should work without credentials.
- `Sftp(download_root=...)` controls the root for downloaded remote files. If it
  is not set, the default remains `Data Products/<data_product>/<table>`.
- `Sftp(output_root=...)` controls the root for auto-generated processed
  outputs. It must only affect calls where `destination=None`; explicit
  `destination` values take precedence.
- `process_all(dry_run=True)` and `download_all(dry_run=True)` must not
  download, process, save, delete, spawn worker pools, or open widgets.
- `process_all()` always returns pandas output. `polars_all()` is the explicit
  native Polars path.
- String queries and pandas callables belong on the pandas path.

## Common Pitfalls

- Do not include `mkdocs/`, `tests/`, or `data_dict_available.xlsx` in release
  distributions unless the packaging policy changes.
- Rebuild `dist/` after any code, README, package-data, or version change that
  affects a release.
- If changing BvD filtering, test both pandas and Polars paths, including
  multi-column and layered `AND_bvd_list` / `OR_bvd_list` cases.
- If changing `bvd_list` validation, test both strict non-interactive mode and
  `allow_invalid_bvd_ids=True`.
- If changing download logic, test default `Data Products/...` behavior and
  custom `download_root`.
- If changing output path logic, test generated destinations with `output_root`
  and explicit `destination` precedence.
- If changing non-interactive behavior, verify both `interactive=True` notebook
  flow and `interactive=False` script flow.
