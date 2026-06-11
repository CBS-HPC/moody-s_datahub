title: API Reference

# API Reference

`moodys_datahub` exposes one stable high-level entry point: `Sftp`.

The implementation is split across internal mixin classes, but the supported
public API is `moodys_datahub.Sftp` / `moodys_datahub.tools.Sftp`.

## Public API overview

### Session setup

- `Sftp(...)`: create a session against SFTP or a local export repository.
- `interactive`: set to `False` to prevent widget prompts in scripts and batch jobs.
- `offline`: set to `True` to skip SFTP login and use packaged metadata only.
- `allow_invalid_bvd_ids`: set to `True` to keep invalid-looking `bvd_list`
  values in non-interactive mode and treat them as exact IDs.
- `server_cleanup`: control the server-cleanup prompt (`None`, `True`, or `False`).
- `download_root`: override the root folder used for downloaded remote files.
- `output_root`: override the root folder for auto-generated processed outputs.
- `tables_available()`: inspect available products and tables.
- `offline_capabilities()`: list which methods can run offline, with a local
  repository, or only against the SFTP server.
- `set_data_product` / `set_table`: set the active product and table directly.
- `select_data()`: open the interactive selector in notebook environments.

### Filtering and metadata

- `select_columns()`: open the interactive column selector.
- `select_cols`: set selected columns directly.
- `bvd_list`: define exact BvD ID filtering or prefix/country-code filtering.
- `AND_bvd_list` / `OR_bvd_list`: add layered BvD clauses that narrow or widen
  the base `bvd_list` filter.
- `allow_invalid_bvd_ids`: controls whether non-interactive `bvd_list`
  validation raises or keeps invalid-looking values.
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
- `profile_table()`: inspect the first file for one table and return a
  privacy-safe column profile.
- `profile_tables()`: profile multiple tables, including tables across multiple
  data products.

### Diagnostics and state

- `download_finished`: inspect the current download state.
- `last_process_engine`: inspect the backend used most recently.
- `last_process_reason`: inspect why the backend was chosen.

### Higher-level helpers

- `search_company_names()`: fuzzy-match company names with the indexed
  RapidFuzz matcher. It exact-matches first, narrows candidates with
  prefix/token/length blocking, and accepts scorer names such as `"WRatio"`,
  `"ratio"`, `"token_sort_ratio"`, and `"token_set_ratio"`.
- `search_bvd_changes()`: resolve BvD lineage.
- `batch_bvd_search()`: run workbook-driven batch searches. Optional
  `AND_bvd_list` and `OR_bvd_list` arguments apply layered BvD filters to each
  batch run.
- `orbis_to_moodys()`: map Orbis-style headings to DataHub columns.

### Table profiling

`profile_table()` and `profile_tables()` are designed for planning dataframe
operations before running large extractions. They report dtypes, missingness,
uniqueness, date-format detection, BvD-ID-like value counts, and
operation-readiness flags such as `can_join_key`, `can_numeric_aggregate`, and
`can_date_filter`.

The BvD-ID heuristic is conservative: it only flags values that look like a
country-code prefix followed by digits, which avoids common name/address false
positives.

The reports do not contain source values, examples, top values, or actual
min/max values.

```python
profile = SFTP.profile_tables(
    selections={
        "Firmographics (Monthly)": ["bvd_id_and_name"],
        "Ownership": ["linkswitharchive", "beneficial_owners_10_10"],
    },
    save_report=True,
)
```

### Offline metadata mode

Use `Sftp(offline=True)` when you want packaged metadata helpers without
logging in to the SFTP server.

```python
SFTP = Sftp(offline=True, interactive=False)

dictionary = SFTP.search_dictionary(search_word="revenue")
date_columns = SFTP.table_dates()
capabilities = SFTP.offline_capabilities()
```

Offline mode exposes a packaged product/table catalog derived from
`data_dict.xlsx`. It is useful for dictionary search and planning, but it is
not a licensed remote-availability check.

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
    output_root="/scratch/moody_results",
)
SFTP.set_data_product = "Firmographics (Monthly)"
SFTP.set_table = "bvd_id_and_name"
```

If `download_root` is not set, remote downloads use the current default:
`Data Products/<data_product>/<table>`. If it is set, the same product/table
layout is created below the custom root:
`<download_root>/<data_product>/<table>`.

If `output_root` is set, generated processed outputs use that root. Explicit
`destination` values passed to `process_all()`, `pandas_all()`, or
`polars_all()` take precedence and keep the existing behavior.

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
