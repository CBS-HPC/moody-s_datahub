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
- process large exports with either the pandas pipeline or the faster Polars
  pipeline for exact-match workloads
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

Use `process_all()` when you want automatic backend selection with a pandas
return type. Use `polars_all()` when you want to force the Polars path
explicitly. After processing, inspect `SFTP.last_process_engine` and
`SFTP.last_process_reason` to see which backend was used and why.

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

## Requirements

- Python 3.9+
- Access to a Moody's DataHub SFTP export
- For CBS users: a personal private key (`.pem`) issued for the CBS SFTP setup

Python 3.13 is supported for core package installation, but workflows that rely
on `ray` remain unavailable there until upstream support is available.

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
- [API reference](https://cbs-hpc.github.io/moody-s_datahub/mkdocs/reference/)

