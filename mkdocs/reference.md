title: Reference

# Reference

This page is kept for compatibility with existing links.

For the maintained API documentation, see `api_reference.md`.

## Public API overview

`moodys_datahub` exposes one stable public entry point: `Sftp`.

- session setup: `Sftp(...)`, `download_root`, `interactive`,
  `tables_available()`, `set_data_product`, `set_table`, `select_data()`
- filtering: `select_cols`, `select_columns()`, `bvd_list`, `AND_bvd_list`,
  `OR_bvd_list`, `time_period`
- processing: `process_one()`, `process_all(dry_run=True)`, `pandas_all()`,
  `polars_all()`, `download_all(dry_run=True)`
- diagnostics: `download_finished`, `last_process_engine`,
  `last_process_reason`
- helper workflows: `search_company_names()`, `search_bvd_changes()`,
  `batch_bvd_search()`, `orbis_to_moodys()`

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
