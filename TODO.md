# TODO

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
