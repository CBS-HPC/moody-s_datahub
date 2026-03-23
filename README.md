# Moody's Datahub

`moodys_datahub` is a Python package for selecting, downloading, and processing
Moody's DataHub exports over SFTP. It is designed for large table exports and
supports parallel download and processing workflows for CSV, Parquet, ORC, and
Avro datasets.

## Main functionality

The package is built around the `Sftp` class and supports the main DataHub
workflow end to end:

- connect to a Moody's DataHub SFTP server or a local export repository
- inspect available data products and tables
- select a product, table, and output columns
- filter by exact BvD ID lists, country-code prefixes, and time periods
- download missing files to a local cache
- process large exports with an explicit pandas pipeline, an explicit Polars
  pipeline, or an auto-selecting wrapper that returns pandas
- run helper workflows such as fuzzy company-name matching, BvD change tracking,
  and Orbis-to-DataHub column mapping

## Typical workflow

### Interactive notebook workflow

```python
from moodys_datahub import Sftp

SFTP = Sftp(privatekey="user_provided-ssh-key.pem")
SFTP.select_data()
SFTP.define_options()
SFTP.select_columns()
SFTP.bvd_list = ["DK28505116", "SE5567031702"]

df, files = SFTP.process_all()
```

### Direct setup workflow

```python
from moodys_datahub import Sftp

SFTP = Sftp(privatekey="user_provided-ssh-key.pem")
SFTP.set_data_product = "Firmographics (Monthly)"
SFTP.set_table = "bvd_id_and_name"
SFTP.select_cols = ["bvd_id_number", "name"]
SFTP.bvd_list = ["DK28505116", "SE5567031702"]

df, files = SFTP.polars_all()
```

Use `select_data()` when you want the interactive widget-based flow in notebook
environments. Use `set_data_product` and `set_table` when you want a fully
scripted workflow.

## Processing backends

Use `process_all()` as the default high-level API. It auto-selects the backend
and always returns a pandas `DataFrame` together with `file_names`.

Use `pandas_all()` when you need pandas semantics explicitly, especially for
string queries and pandas-oriented callables.

Use `polars_all()` when you want the native Polars pipeline and native Polars
return type. The current Polars path supports:

- exact BvD ID filtering
- prefix or country-code BvD filtering
- multi-column BvD membership checks
- year-based `time_period` filtering
- `pl.Expr` filters

After any `process_all()` call, inspect `SFTP.last_process_engine` and
`SFTP.last_process_reason` to see which backend was used and why.

`process_all()` routes to pandas when you use:
- string queries
- pandas-only callables
- `concat_files=False`
- custom `pool_method` or `n_batches`
- unsupported or mixed file formats

### Backend inspection example

```python
df, files = SFTP.process_all()

print(SFTP.last_process_engine)  # "polars" or "pandas"
print(SFTP.last_process_reason)  # e.g. "compatible" or "string_query"
```

## API highlights

The stable public API is centered on `moodys_datahub.Sftp`:

- session setup: `Sftp(...)`, `tables_available()`, `set_data_product`,
  `set_table`, `select_data()`
- filtering: `select_cols`, `select_columns()`, `bvd_list`, `time_period`
- processing: `process_one()`, `process_all()`, `pandas_all()`, `polars_all()`,
  `download_all()`
- diagnostics: `download_finished`, `last_process_engine`,
  `last_process_reason`
- helper workflows: `search_company_names()`, `search_bvd_changes()`,
  `batch_bvd_search()`, `orbis_to_moodys()`

## Installation

### Install from a GitHub release wheel

If you want a pinned wheel from a specific GitHub release, install it directly
from the release assets:

```bash
pip install https://github.com/CBS-HPC/moody-s_datahub/releases/download/v1.0.0/moodys_datahub-1.0.0-py3-none-any.whl
```

### Install from a local wheel

Build the package locally and install the wheel from `dist/`:

```bash
python -m build
pip install dist/moodys_datahub-1.0.0-py3-none-any.whl
```

The package pins `paramiko==3.5.1` because the current `pysftp` dependency is
not compatible with newer Paramiko releases. If you install from source, keep
that pin intact unless the SFTP backend is migrated away from `pysftp`.

## Requirements

- Python 3.9+
- Access to a Moody's DataHub SFTP export
- For CBS users: a personal private key (`.pem`) issued for the CBS SFTP setup

## SFTP Access

CBS users should contact CBS staff to obtain a personal private key used to
authenticate against the CBS SFTP server.

For non-CBS SFTP servers, you also need the server `hostname`, `username`, and
the corresponding authentication credentials.

When connecting to an SFTP server, the package detects export folders and tries
to match them to DataHub products automatically. If multiple products share the
same table name, manual selection is required.

## Format Recommendation

Parquet is the recommended export format. It offers the best performance for
large tables because it is compressed, columnar, and typically split into many
smaller file parts.

CSV is supported, but it is often the slowest option and can produce extremely
large single files.

## System Recommendation

Large DataHub tables benefit from high-memory machines. A practical rule of
thumb in this package is roughly 12 GB of memory per worker.

For CBS users, UCloud machines with many cores, high memory, and strong network
throughput are recommended for large exports.

## Getting Started

- [How to get started](https://cbs-hpc.github.io/moody-s_datahub/mkdocs/how_to_get_started/)
- [Git repository](https://github.com/CBS-HPC/moody-s_datahub)
- [API reference](https://cbs-hpc.github.io/moody-s_datahub/mkdocs/api_reference/)

