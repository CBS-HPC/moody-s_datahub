# TODO

## Reliability follow-up validation

- Run a live DataHub/UCloud extraction and all-files profiling smoke test after
  the reliability release. Synthetic regression tests cannot prove remote
  permissions, scratch capacity, or production throughput.
- Benchmark bounded pandas CSV parsing against Polars on large selective
  BvD/date workloads, including peak memory and file-worker CPU budgets.
- Before supporting concurrent extractions, add owned partial download files,
  atomic completion/rename, and cache locking. A positive file size alone is
  not proof that another downloader has finished.
- Extend profiling drift checks to physical dtype changes, including empty
  Avro shards, and preflight remote scratch capacity without exposing records.
- Align duplicate-name tie behavior in the older `extra.fuzzy_match()` helper
  across sequential/parallel budgets, or deprecate it in favor of the indexed
  company matcher. The public `Sftp.search_company_names()` uses the newer path.
- Enable GitHub repository variable `PYPI_PUBLISH_ENABLED=true` only after
  configuring the PyPI trusted publisher. GitHub release-wheel delivery does
  not depend on PyPI publication.

## Replace `pysftp` with native `paramiko`

Goal: remove the brittle `pysftp` dependency and stop relying on the `paramiko==3.5.1` compatibility pin for runtime stability.

Proposed approach:
- Add a small internal adapter layer, e.g. `src/moodys_datahub/sftp_backend.py`, that wraps `paramiko.SSHClient` and `open_sftp()`.
- Make the adapter expose the operations the package already uses:
  - `listdir`
  - `listdir_attr`
  - `stat`
  - `get`
  - `remove`
  - `exists`
  - `isdir`
  - context-manager enter/exit
- Refactor `src/moodys_datahub/connection.py` so `_connect()` returns the new adapter instead of `pysftp.Connection`.
- Remove `pysftp.CnOpts()` initialization from `_Connection.__init__()` and configure host key policy lazily when a remote connection is actually opened.
- Keep `local_repo` flows independent from remote-backend initialization so `Sftp(local_repo=...)` works without touching SFTP setup.
- Preserve current insecure-host-key behavior in the first migration step to avoid changing connection semantics during the backend swap.
- After parity is confirmed, remove `pysftp` from `pyproject.toml`.

Suggested test scope:
- mocked `_connect()` parity tests for `tables_available()`, `_get_file()`, `_recursive_collect()`, `_delete_files()`, and `_delete_folders()`
- local-repo initialization test proving no remote backend is touched
- one integration smoke test against a real SFTP target or a disposable test server

## Real-data benchmarking

Goal: validate that the pandas/Polars backend split improves real workloads and does not change results.

Benchmark scope:
- Large exact `bvd_list` lookups
- Prefix/country-code BvD lookups
- `search_company_names()` on representative firm-name inputs
- `search_bvd_changes()` on realistic change-history tables

Benchmark method:
- Run the same workload through `pandas_all()`, `process_all(engine="auto")`, and `polars_all()` where supported.
- Capture:
  - elapsed wall-clock time
  - peak memory
  - row counts
  - key output parity checks
- Record dataset shape for each run:
  - file count
  - file format
  - row count
  - selected columns
  - filter type

Output format:
- Store results in a simple markdown or CSV benchmark report under the repo root or `docs/`.
- Include a short conclusion on where Polars is materially better and where pandas should remain the default.
