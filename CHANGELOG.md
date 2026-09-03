# Changelog

All notable changes to this project should be documented in this file.

## [1.4.11] - 2026-09-03

### Fixed
- Column selection, BvD column resolution, and interactive column selection
  now fall back to the first source-file schema when bundled dictionary
  metadata is unavailable.
- Source schema discovery supports CSV, Parquet, ORC, Avro, and Excel files and
  now reports discovery or download failures instead of hiding them.
- `time_period` accepts an explicitly named source date column when packaged
  date metadata is unavailable.
- Pandas and Polars date filtering now raises when requested date columns are
  absent rather than silently returning unfiltered data.
- `process_all(dry_run=True)` validates required columns for local source files
  and warns without downloading when remote schema validation is unavailable.
- `search_dictionary()` and `table_dates()` safely handle quoted search terms
  and comma-separated table metadata.

### Tests
- Added regression coverage for source-schema fallbacks, strict date filtering,
  local preflight schema validation, Avro/CSV schema discovery, and metadata
  table matching.

## [1.4.10] - 2026-09-03

### Fixed
- Packaged metadata now includes complete Ownership History links_2022,
  links_2023, and links_2024 CSV/parquet entries in data_products.xlsx.
- Packaged dictionary metadata now includes links_2023 and links_2024
  in the Ownership History link-table definitions.
- Packaged date-column and batch-search template metadata now include recent
  Ownership History link tables.

### Tests
- Added package-data consistency checks for complete Ownership History link
  metadata across data_products.xlsx, date_cols.xlsx, and products.xlsx.

## [1.4.9] - 2026-09-03

### Fixed
- Cached remote files are now validated against remote sizes when available,
  so non-zero truncated files are re-downloaded before processing.
- Sequential pandas processing now raises aggregated file failures instead of
  printing errors and returning partial output.
- Polars processing now raises when any requested local input is missing or
  incomplete after download instead of silently dropping that file.

## [1.4.8] - 2026-08-27

### Fixed
- Parallel download failures now raise instead of being printed and ignored.
- Per-file downloads now retry transient failures, remove partial/zero-byte
  files, and verify non-empty/local-vs-remote size before processing.
- Async download readiness now requires local files to exist with non-zero size.

## [1.4.7] - 2026-08-25

### Fixed
- `num_workers=1` is now preserved as an explicit single-worker setting instead
  of being treated as automatic worker selection.

## [1.4.6] - 2026-08-18

### Fixed
- `profile_table(file_scope="all_files")` now removes a newly staged first file
  when validation fails before the full scan starts.
- Profile report privacy wording now reflects that all-files summaries may
  include aggregate date min/max values.

## [1.4.5] - 2026-08-18

### Added
- `file_scope="all_files"` for `profile_table()` and `profile_tables()`.
  It scans CSV, Parquet, and Avro source files sequentially, removes newly
  staged files, and exposes privacy-safe complete table aggregates through
  DataFrame attributes and Excel report summaries.

## [1.4.4] - 2026-08-18

### Fixed
- Bounded pandas CSV fallback parallelism so multi-file processing uses the
  outer file worker pool without spawning unrestricted inner CSV worker pools.
- Single-worker CSV processing now reads in-process instead of creating a
  one-worker child pool.

## [1.4.3] - 2026-08-18

### Fixed
- `polars_all(num_workers=...)` now applies the resolved worker budget to the
  Polars download phase as well as output saving.

## [1.4.0] - 2026-06-10

### Added
- Optimized company-name fuzzy matching with indexed RapidFuzz blocking,
  batched scoring, and scorer selection.
- Privacy-safe table profiling via `profile_table()` and `profile_tables()`,
  including dtype, missingness, uniqueness, date-format, BvD-ID-like, and
  operation-readiness metadata.
- Reusable profile report exports for Excel, CSV, and Parquet.
- Offline metadata mode with `Sftp(offline=True)` and
  `offline_capabilities()` for no-login dictionary/date/country-code workflows.
- Non-interactive override `allow_invalid_bvd_ids=True` for keeping
  invalid-looking `bvd_list` values as exact IDs.

### Changed
- Documentation now covers offline metadata mode, profile reports, BvD format
  overrides, and optimized fuzzy matching behavior.
- `search_dictionary()` and `table_dates()` can operate from packaged metadata
  without requiring remote table discovery.

### Notes
- Profile reports intentionally exclude source values, example values, top
  values, and actual min/max values.

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
