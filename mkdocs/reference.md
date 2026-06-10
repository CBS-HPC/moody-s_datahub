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
  `polars_all()`, `download_all(dry_run=True)`, `profile_table()`,
  `profile_tables()`
- diagnostics: `download_finished`, `last_process_engine`,
  `last_process_reason`
- helper workflows: `search_company_names()`, `search_bvd_changes()`,
  `batch_bvd_search()`, `orbis_to_moodys()`

`search_company_names()` uses indexed RapidFuzz matching with exact-match
short-circuiting, prefix/token/length candidate blocking, and optional scorer
selection through the `scorer` argument.

`profile_table()` and `profile_tables()` inspect the first file for selected
tables and generate privacy-safe column profiles with dtype, missingness,
uniqueness, date-format, BvD-ID-like value counts, and operation-readiness
metadata. They do not include source values or example records.

`Sftp(offline=True)` skips SFTP login and supports packaged metadata helpers
such as `search_dictionary()`, `table_dates()`, `search_country_codes()`, and
`offline_capabilities()`.

`allow_invalid_bvd_ids=True` lets non-interactive `bvd_list` assignments keep
invalid-looking values and treat them as exact IDs instead of raising.

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
- `output_root` controls where auto-generated processed outputs are saved.
  Explicit `destination` values still take precedence.
- `dry_run=True` returns a preflight report without downloading, processing,
  saving, or deleting files.

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
