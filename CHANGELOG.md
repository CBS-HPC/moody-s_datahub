# Changelog

All notable changes to this project should be documented in this file.

## [1.4.13] - 2026-10-08

### Added
- `resolve_cache_file(file)` exposes read-only source/cache path resolution
  before downloads without creating directories or changing selection.

### Fixed
- Early cache lookup now derives the same versioned directory as dry-run and
  runtime downloads when `local_path` is unset. Incomplete remote selection
  raises an actionable `ValueError` instead of a `Path(None)` `TypeError`.
- Changing `remote_path` clears the previous export timestamp and resolves
  matching export metadata from the full inventory, keeping cache versions
  separate even when the active inventory was narrowed to an older export.
- Parquet/Arrow read errors preserve sources and sibling directories. Explicit
  local inputs are no longer deleted or replaced by unrelated remote size checks.
- All-files profiling preserves preexisting managed caches, cleans only owned
  staging on success/failure, and checks schema drift in empty Parquet shards.
- Discovery uses an owned temporary marker file and no longer deletes remote
  marker files before cleanup authorization. Local discovery accepts empty
  repositories and ignores ordinary root files.
- Every requested local/cache/remote input is resolved independently. Unknown
  inputs raise instead of silently producing partial extracts; parallel pandas
  filenames are returned intact.
- Pandas CSV parsing retains headers, quoted multiline records, remainders,
  and final rows using bounded logical-record batches without nested pools.
  Projection and date/BvD filters are applied before retained chunks are
  concatenated; custom queries run on the retained whole-file table.
- Batch AND/OR filters survive product/table resets. Missing templates are
  created at their requested paths without overwriting unrelated defaults.
- Non-interactive invalid/ambiguous selections fail explicitly, failed BvD/date
  assignments retain valid filters, and cancelled BvD prompts retain prior state.
- Cache/output planning is shared by execution and dry-run. Local-only default
  outputs use a timestamped `output` suffix; explicit destinations still win.
- Preflight blocks invalid engines, unresolved inputs, incomplete cache settings,
  empty explicit inputs, and overlong paths; it checks required columns in every
  local shard. Explicit backend reports match execution. Profiling dry-run
  resolves cached inventory without SFTP and reports unknown remote listings.
- `process_one()` handles string/Path inputs and plans only the first default
  shard. National identifier helpers no longer launch selection UI internally.
- Windows downloads use bounded threads. Auto worker defaults never resolve to
  zero on small-memory hosts. Avro projection and Excel/IPC chunk sizing work.
- Python 3.9 imports no longer evaluate unsupported union annotations.

### Maintenance
- Added regression coverage for source ownership, row/filter parity, export
  isolation, failure cleanup, interactive cancellation, and read-only preflight.
- Fixed the repository-wide formatter gate, pinned Ruff for reproducibility,
  and added Windows/Python 3.13 to the Linux/Python 3.9-3.13 CI matrix.
- Release distributions are checked with Twine before upload. PyPI publication
  is opt-in via `PYPI_PUBLISH_ENABLED` after trusted publisher configuration.

### Migration
- Scripts relying on skipped missing files or silently retained invalid
  selections must now handle `ValueError` or fix their inputs before extraction.
- Explicit existing local files are inputs, not replaceable managed downloads.
  Request remote basenames when cache size validation/re-download is intended.
- Select a different export via `remote_path` before selecting its tables.
- `num_workers` caps download/file workers; pandas CSV parsing no longer creates
  nested parser pools. Configure Polars' native CPU cap before import with
  `POLARS_MAX_THREADS`. Shared-cache concurrent extractions remain unsupported.

## [1.4.12] - 2026-09-30

### Added
- `search_company_names()` now accepts `country` for all names or
  `countries_by_name` aligned with the input list. It excludes IDs beginning
  with other recognised country prefixes before matching while retaining
  unrecognised prefixes as unverified candidates.
- Country-filtered results include input positions, requested country codes,
  and prefix-status labels so repeated names remain distinguishable.

### Fixed
- Company-name matching now retains distinct BvD IDs for the same normalized
  name in both the indexed matcher and pandas fallback. Repeated occurrences
  of the same normalized name and BvD ID are returned once.
- Country-code metadata now preserves Namibia's `NA` code, which pandas had
  previously interpreted as a missing value.

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
