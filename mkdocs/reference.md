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
- `process_all()`: run the pandas-based processing pipeline.
- `polars_all()`: run the Polars-based processing pipeline for exact-match
  workloads.
- `download_all()`: download missing files to the local cache.

### Higher-level helpers

- `search_company_names()`: fuzzy-match company names across a selected table.
- `search_bvd_changes()`: resolve BvD ID lineage.
- `batch_bvd_search()`: run batch searches from workbook/text inputs.
- `orbis_to_moodys()`: map Orbis-style headings to DataHub columns.

## Current behavior notes

- `process_all()` and `polars_all()` return `(df, file_names)` and raise
  exceptions on failure instead of returning `None`.
- `save_to` is now documented as `None | "csv" | "xlsx"`.
- `polars_all()` is the faster path for exact BvD membership filtering and now
  supports matching the same BvD list across multiple columns in one pass.
- `polars_all()` does not currently support the year filter used by
  `time_period`.

## Generated reference

::: moodys_datahub.tools.Sftp
    handler: python
    options:
        show_source: false
        show_root_heading: true
        heading_level: 2
