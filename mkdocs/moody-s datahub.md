title: Moody's Datahub
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

```python
from moodys_datahub import Sftp

SFTP = Sftp(privatekey="user_provided-ssh-key.pem")
SFTP.set_data_product = "Firmographics (Monthly)"
SFTP.set_table = "bvd_id_and_name"
SFTP.select_cols = ["bvd_id_number", "name"]
SFTP.bvd_list = ["DK28505116", "SE5567031702"]

df, files = SFTP.polars_all()
```

Use `process_all()` for the pandas-based workflow and `polars_all()` when you
want the faster exact-match path for large BvD ID filters.

## SFTP Access

CBS users should contact [CBS staff](mailto:rdm@cbs.dk) to obtain a personal
private key used to authenticate against the CBS SFTP server.

For non-CBS SFTP servers, you also need the server `hostname`, `username`, and
the corresponding authentication credentials.

When connecting to an SFTP server, the package detects export folders and tries
to match them to DataHub products automatically. If multiple products share the
same table name, manual selection is required.

## Format recommendation

Parquet is the recommended export format. It offers the best performance for
large tables because it is compressed, columnar, and typically split into many
smaller file parts.

CSV is supported, but it is often the slowest option and can produce extremely
large single files.


## System recommendation

Large DataHub tables benefit from high-memory machines. A practical rule of
thumb in this package is roughly 12 GB of memory per worker.

For CBS users, [UCloud](https://cbs-hpc.github.io/HPC_Facilities/UCloud/) with
many cores, high memory, and strong network throughput is recommended for large
exports.


## Getting Started

- [How to get started](/moody-s_datahub/mkdocs/how_to_get_started/)
- [Git repository: moody-s_datahub](https://github.com/CBS-HPC/moody-s_datahub)
- [API reference](/moody-s_datahub/mkdocs/reference/)
