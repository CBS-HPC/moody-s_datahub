# Changelog

All notable changes to this project should be documented in this file.

## [1.0.0rc1] - 2026-02-17

### Added
- CI coverage for linting, tests, and package build validation.
- PyPI publishing workflow for release-driven publishing.
- Initial unit tests for utility and metadata loaders.

### Changed
- `save_to` API contract is now typed as `Literal["csv", "xlsx"] | None` across public methods.
- `process_all` and `polars_all` now use exception-based failure flow (`ValueError`, `TimeoutError`) instead of returning `None` on operational failures.
- Package metadata was hardened for distribution (classifiers, URLs, dependency cleanup, packaging manifest).

### Fixed
- `copy_obj` call bug in `extra.national_identifer`.
- `pool_method` setter validation (`threading` typo fixed, invalid values now raise `ValueError`).
- Missing-column validation logic in column selection.
- `_check_list_format` now handles `values=None` safely.
- `search_company_names` index reset now persists.
- Deprecated resource access updated from `open_binary()` to `files(...).open("rb")`.

### Breaking Changes
- Callers relying on `process_all`/`polars_all` returning `None` on failure must now handle raised exceptions.
- `save_to=False` is no longer the preferred contract; use `save_to=None`.
  - Compatibility is still tolerated in `_save_to`, but consumers should migrate to `None`.

### Migration Notes
- Replace checks like `if df is None:` after `process_all()` with exception handling:
  - `try/except ValueError` for invalid inputs/state.
  - `try/except TimeoutError` for download-timeout conditions.
- Replace `save_to=False` with `save_to=None` in client code.

### Release Readiness Smoke Results
- Local-repo data flow smoke test passed for:
  - `Sftp(local_repo=...)`
  - `tables_available()`
  - `set_data_product` / `set_table`
  - `process_all(files=[absolute_csv_path], num_workers=1)`
- Observed issue to track before stable:
  - `process_one(..., n_rows=2)` returned all rows and printed `No rows were retained` in the same run, indicating inconsistent branching/logging behavior in current `process_one` flow.
