title: Reference

# API Reference

`moodys_datahub` exposes one stable high-level entry point: `Sftp`.

The implementation is split across internal mixin classes, but users should
interact with the package through `moodys_datahub.Sftp` or
`moodys_datahub.tools.Sftp`.

## Public API overview

### Session setup

- `Sftp(...)`: create a session against an SFTP server or a local export
  repository.
- `tables_available()`: inspect the available products and tables.
- `set_data_product` / `set_table`: set the active product and table directly.
- `select_data()`: open the interactive selector in notebook environments.

### Filtering and metadata

- `select_columns()`: open the interactive column selector.
- `select_cols`: set selected columns directly.
- `bvd_list`: define an exact BvD ID list or a country-prefix search.
- `time_period`: define a year-based filter.
- `search_dictionary()`: search the packaged table dictionary.
- `table_dates()`: inspect date-like columns for the active table.
- `search_country_codes()`: search country-code metadata.

### Processing

- `process_one()`: load a sample from one or more files.
- `process_all()`: run the auto-selecting processing pipeline and always return
  pandas.
- `pandas_all()`: force the pandas processing pipeline explicitly.
- `polars_all()`: run the native Polars processing pipeline and return Polars.
- `download_all()`: download missing files to the local cache.
- `download_finished`: inspect the current download state without relying on the
  internal `_download_finished` attribute.
- `last_process_engine` / `last_process_reason`: inspect which backend was used
  most recently and why.

### Higher-level helpers

- `search_company_names()`: fuzzy-match company names across a selected table.
- `search_bvd_changes()`: resolve BvD ID lineage.
- `batch_bvd_search()`: run batch searches from workbook/text inputs.
- `orbis_to_moodys()`: map Orbis-style headings to DataHub columns.

## Current behavior notes

- `process_all()` and `polars_all()` return `(df, file_names)` and raise
  exceptions on failure instead of returning `None`.
- `save_to` is now documented as `None | "csv" | "xlsx"`.
- `process_all()` prefers Polars when the workload is compatible and records the
  decision on `last_process_engine` and `last_process_reason`.
- `polars_all()` supports exact and prefix BvD filtering, multi-column BvD
  matching, and year-based filtering for `time_period`.
- `process_all()` falls back to pandas for string queries, pandas-only callables,
  `concat_files=False`, custom batching, and unsupported file formats.

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
