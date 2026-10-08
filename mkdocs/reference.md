title: Reference

# Reference

This page is kept for compatibility with existing links.

For the maintained API documentation, see `api_reference.md`.

## Public API overview

`moodys_datahub` exposes one stable public entry point: `Sftp`.

- session setup: `Sftp(...)`, `download_root`, `output_root`, `interactive`,
  `offline`, `allow_invalid_bvd_ids`, `tables_available()`,
  `offline_capabilities()`, `set_data_product`, `set_table`, `select_data()`
- filtering: `select_cols`, `select_columns()`, `bvd_list`, `AND_bvd_list`,
  `OR_bvd_list`, `time_period`
- processing: `process_one()`, `process_all(dry_run=True)`, `pandas_all()`,
  `polars_all()`, `download_all(dry_run=True)`, `resolve_cache_file()`,
  `profile_table()`, `profile_tables()`
- diagnostics: `download_finished`, `last_process_engine`,
  `last_process_reason`
- helper workflows: `search_company_names()`, `search_bvd_changes()`,
  `batch_bvd_search()`, `orbis_to_moodys()`

`search_company_names()` uses indexed RapidFuzz matching with exact-match
short-circuiting, prefix/token/length candidate blocking, and optional scorer
selection through the `scorer` argument. Distinct BvD IDs with the same best
normalized name are retained as separate result rows. Use `country="DK"` for a
single country or `countries_by_name=["DK", "SE"]` to pair each input name with
its country. Unknown-prefix IDs remain candidates and are labelled as
unverified by `BvD_prefix_status`.

`profile_table()` and `profile_tables()` inspect the first file for selected
tables and generate privacy-safe column profiles with dtype, missingness,
uniqueness, date-format, BvD-ID-like value counts, and operation-readiness
metadata. They do not include source values or example records.

The BvD-ID heuristic is conservative and only flags values that look like a
country-code prefix followed by digits.

`Sftp(offline=True)` skips SFTP login and supports packaged metadata helpers
such as `search_dictionary()`, `table_dates()`, `search_country_codes()`, and
`offline_capabilities()`.

`allow_invalid_bvd_ids=True` lets non-interactive `bvd_list` assignments keep
invalid-looking values and treat them as exact IDs instead of raising.

`batch_bvd_search()` accepts optional `AND_bvd_list` and `OR_bvd_list`
arguments so workbook-driven searches can reuse the layered BvD filter model.

## Current backend behavior

- `process_all()` always returns pandas and may use Polars internally when the
  workload is compatible.
- `pandas_all()` is the explicit pandas backend.
- `polars_all()` is the explicit native Polars backend and supports exact and
  prefix BvD filtering, multi-column BvD filters, layered `AND_bvd_list` /
  `OR_bvd_list` filtering, and year-based `time_period` filtering.
- string queries belong on the pandas path.
- `download_root` controls where remote files are cached. If it is not set, the
  default remains `Data Products/<data_product>/<table>`.
- `resolve_cache_file(file)` returns an absolute source/cache path without
  downloads, directory creation, or selection changes. Timestamped export
  folders remain separate; incomplete selection raises a clear `ValueError`.
- `output_root` controls where auto-generated processed outputs are saved.
  Explicit `destination` values still take precedence.
- `get_column_names()` uses dictionary metadata first and falls back to the
  first source-file schema when metadata is unavailable. This fallback supports
  CSV, Parquet, ORC, Avro, and Excel files; file-derived columns do not include
  packaged definitions.
- `dry_run=True` returns a preflight report without downloading, processing,
  saving, or deleting files. Local source schemas validate selected and filter
  columns; remote files are not downloaded merely for schema validation.
- Requested inputs are resolved individually; missing inputs now raise instead
  of producing partial results. Explicit local files and existing profile caches
  are preserved. Read errors never delete source directories.
- Non-interactive invalid selections raise; rejected BvD/date updates retain the
  previous valid filter. Changing exports keeps the active table catalog aligned
  with the selected export.
- Pandas CSV parsing retains complete logical records without nested parser
  pools. `num_workers` caps file/download workers, not Polars' native CPU pool.
  Configure `POLARS_MAX_THREADS` before import for that pool. Concurrent
  extractions sharing a cache remain unsupported.
- All-files profiling cleans only newly staged files, including on failure;
  empty Parquet shards participate in schema-drift checks.

## Generated reference

::: moodys_datahub.tools.Sftp
    handler: python
    options:
        show_source: false
        show_root_heading: true
        heading_level: 2
        inherited_members: true
        members: true
        members_order: source
