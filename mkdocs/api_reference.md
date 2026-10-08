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
- `get_column_names()`: return bundled dictionary columns, or source-file
  columns when dictionary metadata is unavailable.
- `bvd_list`: define exact BvD ID filtering or prefix/country-code filtering.
- `AND_bvd_list` / `OR_bvd_list`: add layered BvD clauses that narrow or widen
  the base `bvd_list` filter.
- `allow_invalid_bvd_ids`: controls whether non-interactive `bvd_list`
  validation raises or keeps invalid-looking values.
- `time_period`: define year-based filtering.
- `search_dictionary()`: search the packaged data dictionary.
- `table_dates()`: inspect date-like columns for the active table.
- `search_country_codes()`: search packaged country-code metadata.

Dictionary metadata is preferred for column definitions. When it has no entry
for a source table, column selection and explicit BvD/date settings can inspect
the first CSV, Parquet, ORC, Avro, or Excel source file. File-derived columns
have no packaged definitions. An explicitly requested date column must exist;
processing raises rather than returning unfiltered data when it does not.

### Processing

- `process_one()`: load a sample from one or more files.
- `process_all()`: auto-select the processing backend and always return pandas.
- `process_all(dry_run=True)`: validate the planned process workflow without
  downloading, processing, saving, or deleting files.
- `pandas_all()`: force the pandas backend explicitly.
- `polars_all()`: force the native Polars backend explicitly and return Polars.
- `download_all()`: download missing files into the local cache.
- `download_all(dry_run=True)`: validate which files would be downloaded.
- `resolve_cache_file(file)`: return the absolute local source/cache path
  before downloading, without creating folders or changing selection.
- `profile_table()`: inspect one first file by default, or use
  `file_scope="all_files"` for bounded aggregate profiling across every source
  file for one table.
- `profile_tables()`: profile multiple tables, including tables across multiple
  data products, with the same `file_scope` option.

### Diagnostics and state

- `download_finished`: inspect the current download state.
- `last_process_engine`: inspect the backend used most recently.
- `last_process_reason`: inspect why the backend was chosen.

### Higher-level helpers

- `search_company_names()`: fuzzy-match company names with the indexed
  RapidFuzz matcher. It exact-matches first, narrows candidates with
  prefix/token/length blocking, and accepts scorer names such as `"WRatio"`,
  `"ratio"`, `"token_sort_ratio"`, and `"token_set_ratio"`. Distinct BvD IDs
  sharing the best normalized name are returned as separate rows. Use
  `country="DK"` for one country or `countries_by_name=["DK", "SE"]` to
  pair each input name with a country.
- `search_bvd_changes()`: resolve BvD lineage.
- `batch_bvd_search()`: run workbook-driven batch searches. Optional
  `AND_bvd_list` and `OR_bvd_list` arguments apply layered BvD filters to each
  batch run.
- `orbis_to_moodys()`: map Orbis-style headings to DataHub columns.

Country-filtered company searches exclude BvD IDs with other recognised
two-letter prefixes before matching. IDs with unrecognised or missing prefixes
remain candidates, marked by `BvD_prefix_status`. Results also include
`Input_index`, `Input_name`, and `Requested_country`; the index keeps repeated
input names with different requested countries separate. Prefixes are a
heuristic and do not verify a firm's actual country.

### Table profiling

`profile_table()` and `profile_tables()` are designed for planning dataframe
operations before running large extractions. They report dtypes, missingness,
uniqueness, date-format detection, BvD-ID-like value counts, and
operation-readiness flags such as `can_join_key`, `can_numeric_aggregate`, and
`can_date_filter`.

The BvD-ID heuristic is conservative: it only flags values that look like a
country-code prefix followed by digits, which avoids common name/address false
positives.

The reports do not contain source values, examples, or top values. All-files
summaries may include aggregate date min/max values, total rows, and exact
canonical BvD-ID cardinality, but never the underlying identifiers.

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

For a timestamped export, the product directory is
`<data_product>_exported YYYY-MM-DD_HH-MM-SS`. Switching table or export
invalidates the old local cache selection and uses the newly selected version.

### Read-only cache inspection

Use the public resolver before a download when a script needs to inspect a
source cache file:

```python
from pathlib import Path

cached_source = Path(SFTP.resolve_cache_file(SFTP.remote_files[0]))
print(cached_source, cached_source.is_file())
```

`resolve_cache_file(file)` returns a string containing the absolute path.
An existing explicit local file takes precedence; otherwise it uses
`local_path`, or derives the selected product/export/table directory under
`download_root` (default `Data Products`). The path may not exist yet.
Resolution never downloads, creates directories, opens prompts, or changes
selection. Without an explicit local file, configured `local_path`, or
complete remote selection, it raises `ValueError` listing missing settings.
The existing platform path-length limit also applies.

This replaces private `_file_exist(file)[0]` calls for pre-download inspection.
Check `Path(...).is_file()` for presence and verify byte size separately when
needed. The private method's boolean identifies directly supplied local files;
it is not a cache-existence or integrity guarantee.

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

For local source files, dry-run also validates required columns against the
schema. It does not download remote files to inspect schemas and reports that
limitation as a warning.

Dry-run uses the same read-only file and destination resolvers as execution.
Unknown inputs, incomplete cache selection, empty explicit files, invalid
backend names, and excessive path lengths block execution. Explicit
`pandas_all()` reports pandas; explicit `polars_all()` plans its concatenating
behavior even if `concat_files=False`. `process_one()` defaults to only the
first file and accepts a filename, `Path`, list, or file index.

### File ownership and migration notes

- Every requested file must resolve; mixed local/cache/remote inputs are kept
  in request order. Unknown filenames now raise instead of being skipped.
- Explicit existing local files are never deleted or replaced by remote size
  checks. Empty explicit files raise; managed partial caches may be re-downloaded.
- Reader errors never delete source directories. `delete_files=True` still
  permits cleanup of managed inputs after successful processing.
- `file_scope="all_files"` profiling preserves preexisting files, cleans its own
  staged downloads and scratch database on failure, and checks schema drift in
  empty Parquet shards too. Once a staged path is released, later files created
  at that path are not reclaimed. Profiling dry-run uses the selected export
  when changing tables within the same product.
- Remote discovery preserves marker files and uses a unique local temporary
  directory. Export deletion remains a separately authorized cleanup operation.
- Invalid or ambiguous non-interactive selections raise rather than retaining
  an unrelated selection. Select an export with `remote_path` before choosing
  another table in that export. Failed BvD/date updates retain valid filters.
- Missing batch templates are created at the requested paths, without
  overwriting unrelated default templates. Layered batch filters are reapplied
  after each product/table selection.

Pandas CSV parsing uses logical-record batches in the current file worker, not
nested row-parser pools. Multiple files can still use bounded file workers.
Mixed inferred types cause a second bounded parse with consistent column types,
preserving leading zeros in textual identifiers. Projection keeps the columns
needed by structured BvD filters on both pandas and Polars paths.
Windows downloads use bounded threads; Unix downloads retain process workers.
Polars' process-wide native thread pool is not resized by `num_workers`: set
`POLARS_MAX_THREADS` before import if an explicit CPU cap is required. Avoid
concurrent extractions using the same cache until cache locking is implemented.

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
