title: API Reference

# API Reference

`moodys_datahub` exposes one stable high-level entry point: `Sftp`.

The implementation is split across internal mixin classes, but the supported
public API is `moodys_datahub.Sftp` / `moodys_datahub.tools.Sftp`.

## Public API overview

### Session setup

- `Sftp(...)`: create a session against SFTP or a local export repository.
- `interactive`: set to `False` to prevent widget prompts in scripts and batch jobs.
- `server_cleanup`: control the server-cleanup prompt (`None`, `True`, or `False`).
- `download_root`: override the root folder used for downloaded remote files.
- `tables_available()`: inspect available products and tables.
- `set_data_product` / `set_table`: set the active product and table directly.
- `select_data()`: open the interactive selector in notebook environments.

### Filtering and metadata

- `select_columns()`: open the interactive column selector.
- `select_cols`: set selected columns directly.
- `bvd_list`: define exact BvD ID filtering or prefix/country-code filtering.
- `AND_bvd_list` / `OR_bvd_list`: add layered BvD clauses that narrow or widen
  the base `bvd_list` filter.
- `time_period`: define year-based filtering.
- `search_dictionary()`: search the packaged data dictionary.
- `table_dates()`: inspect date-like columns for the active table.
- `search_country_codes()`: search packaged country-code metadata.

### Processing

- `process_one()`: load a sample from one or more files.
- `process_all()`: auto-select the processing backend and always return pandas.
- `process_all(dry_run=True)`: validate the planned process workflow without
  downloading, processing, saving, or deleting files.
- `pandas_all()`: force the pandas backend explicitly.
- `polars_all()`: force the native Polars backend explicitly and return Polars.
- `download_all()`: download missing files into the local cache.
- `download_all(dry_run=True)`: validate which files would be downloaded.

### Diagnostics and state

- `download_finished`: inspect the current download state.
- `last_process_engine`: inspect the backend used most recently.
- `last_process_reason`: inspect why the backend was chosen.

### Higher-level helpers

- `search_company_names()`: fuzzy-match company names.
- `search_bvd_changes()`: resolve BvD lineage.
- `batch_bvd_search()`: run workbook-driven batch searches.
- `orbis_to_moodys()`: map Orbis-style headings to DataHub columns.

## Data files

Metadata workbooks live under `src/moodys_datahub/data`:

- `data_dict.xlsx`: broad Moody's DataHub data dictionary. It may include
  products that are not licensed or available in the current SFTP export.
- `data_dict_available.xlsx`: repository workbook with an overview of the data
  products available to the current setup/license. It is intentionally excluded
  from the built wheel unless packaging rules are changed.
- `data_products.xlsx`: packaged product metadata used by helper loaders.
- `date_cols.xlsx`: known date columns used by date filtering helpers.
- `country_codes.xlsx`: country-code metadata used by country/prefix BvD
  lookup.
- `products.xlsx` and `bvd_numbers.txt`: templates used by
  `batch_bvd_search()`.

## Backend behavior

### `process_all()`

`process_all()` is the compatibility API. It always returns:

```python
(pandas_dataframe, file_names)
```

When the workload is compatible, it may use the Polars backend internally and
then convert the final result back to pandas before returning it.

### `pandas_all()`

Use `pandas_all()` when you need pandas-specific semantics such as:

- string queries
- pandas-oriented callable filters
- workloads that require the older pandas pipeline explicitly

### `polars_all()`

Use `polars_all()` when you want the native Polars path. The current Polars
backend supports:

- exact BvD list filtering
- prefix / country-code BvD filtering
- multi-column BvD filtering
- layered `AND_bvd_list` / `OR_bvd_list` filtering
- year-based `time_period` filtering
- `pl.Expr` filters

It does not try to emulate every pandas-specific query style. In particular,
string queries belong on the pandas path.

## Non-interactive and dry-run workflows

Use `interactive=False` when running the package from scripts or batch jobs:

```python
SFTP = Sftp(
    privatekey="user_provided-ssh-key.pem",
    interactive=False,
    server_cleanup=False,
    download_root="/scratch/moody_datahub",
)
SFTP.set_data_product = "Firmographics (Monthly)"
SFTP.set_table = "bvd_id_and_name"
```

If `download_root` is not set, remote downloads use the current default:
`Data Products/<data_product>/<table>`. If it is set, the same product/table
layout is created below the custom root:
`<download_root>/<data_product>/<table>`.

Use `dry_run=True` to validate the planned workflow before it performs side
effects:

```python
report = SFTP.process_all(dry_run=True)
if report.ok:
    df, file_names = SFTP.process_all()
```

The returned report includes the selected backend, resolved files, missing
files, warnings, errors, destination, required columns, and flags such as
`would_prompt`, `would_download`, and `would_write`.

## Backend selection reasons

After `process_all()`, inspect:

- `last_process_engine`
- `last_process_reason`

Common `last_process_reason` values include:

- `compatible`
- `explicit`
- `string_query`
- `callable_query`
- `pool_method`
- `n_batches`
- `concat_files_false`
- `mixed_formats`
- `multi_file_xlsx`

These values are useful when diagnosing why `process_all()` chose pandas or
Polars for a given workload.

## Current behavior notes

- `process_all()` and `polars_all()` return `(df, file_names)` and raise
  exceptions on failure instead of returning `None`.
- `save_to` is `None | "csv" | "xlsx"`.
- `download_all()` updates `download_finished` instead of requiring callers to
  inspect the internal `_download_finished` attribute.
- The current release pins `paramiko==3.5.1` because the SFTP backend still
  depends on `pysftp`, which is not compatible with newer Paramiko releases.

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
