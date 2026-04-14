# Changelog

All notable changes to this project should be documented in this file.

## [1.1.0] - 2026-04-14

### Added
- Layered BvD filtering via `AND_bvd_list` and `OR_bvd_list` with pandas and
  Polars support.
- Auto backend selection that keeps `process_all()` pandas-compatible while
  using Polars for supported workloads.

### Changed
- Public docs now describe the layered BvD model and the explicit pandas /
  Polars backends.
- `README.md` and API reference pages now point to the current release wheel
  workflow.

## [1.0.0] - 2026-03-20

### Added
- Stable release packaging and release-facing installation guidance.
- GitHub release asset upload in the publish workflow so wheels and source
  archives are attached to tagged releases.

### Changed
- Package version promoted from `1.0.0rc1` to `1.0.0`.
- Development status classifier promoted to `Production/Stable`.
- README installation instructions now cover PyPI, GitHub release wheels, and
  local wheel installation.

### Fixed
- Polars exact BvD filtering now supports multiple candidate columns in a single
  pass without duplicate rows.
- `process_one(..., n_rows=...)` sampling behavior is now consistent with its
  returned result.

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
