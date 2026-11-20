# Polars file-only export: exploratory local results

Recorded 2026-11-20 on the `codex/polars-1.5.0` development branch.
These are synthetic Windows measurements, not a production/UCloud benchmark.

## Method

- 1,000,000 synthetic rows, four Parquet shards, 14 columns including ten text
  columns. No researcher data, names or actual BvD identifiers are used.
- Python 3.11.14, Polars 2.1.0, pandas 3.1.1, PyArrow 26.0.1.
- Eight native Polars threads, configured before import; one download worker.
- Every measured extraction runs in a fresh subprocess. Imports are excluded
  from extraction wall time. All methods write their requested Parquet outputs.
- Peak resident memory is sampled every 10 ms and includes the process's
  imported libraries; brief allocation peaks may be missed.
- Three repetitions per method/workload; values below are medians.
- Page cache was not cleared, and other validation work was active. Treat wall
  times as exploratory, not controlled speed comparisons or release targets.
- The exact-ID case supplies 55,000 synthetic IDs and retains 55,000 rows.
  Aggregate output row counts and synthetic row-index sums agree across methods.
- Synthetic data is highly compressible; output characteristics and memory
  savings need not match ownership exports or production storage.

## Measurements

| Workload | Method | Wall time (s) | Sampled peak RSS (MiB) |
| --- | --- | ---: | ---: |
| All rows | `process_all(engine="polars")` | 2.498 | 1209.3 |
| All rows | `polars_all()` | 2.658 | 756.7 |
| All rows | `export_all()` | 2.638 | 649.8 |
| 55,000-ID selection | `process_all(engine="polars")` | 0.679 | 241.2 |
| 55,000-ID selection | `polars_all()` | 0.478 | 234.7 |
| 55,000-ID selection | `export_all()` | 0.717 | 229.3 |

The unfiltered file-only route used about 46% less sampled peak memory than
the pandas-returning route on this fixture. Differences were much smaller
after selective filtering. File-only export was not consistently faster;
strict schema checks and safe output staging also add work.

An additional 1.4.15-source/Polars-2.0 baseline completed aggregate parity
checks. It used Python 3.10 and different dependencies, so its timing is not
used to attribute any improvement specifically to upgrading Polars.

## Reproduce or extend

Run with the project dependencies installed and `PYTHONPATH=src`:

```powershell
$env:PYTHONPATH = "src"
python benchmarks/polars_export.py --rows 1000000 --repeats 3 --threads 8
```

The default includes unfiltered, exact-ID, prefix, layered and date-filtered
cases. Each run generates its own synthetic files and removes them afterward.
The JSON report contains aggregate measurements and phase timings only.
For streaming export, `execute_and_write_seconds` is a combined phase: writing
and computation overlap. Do not add a fictitious standalone collect phase.
QC runs after the measured extraction and has its own timer.

Use `--source-root path/to/baseline/src --methods process polars` to evaluate
an older checkout without changing that checkout. For defensible upgrade
comparisons, hold Python/dependency versions, storage, CPU budget and cache
state constant and run competing workloads separately.

## Still required before production recommendations

- Controlled timing on representative data, including larger shard counts,
  nullable/typed dates and layered filters.
- A measured comparison of auto/prefiltered scan strategies before changing
  any scan default. This release keeps the normal `scan_parquet` strategy.
- Authorized UCloud validation with explicit native CPU budgets and known
  download/spill/output storage capacity.
- Manual Linux compatibility and storage-specific hard-link/failure validation.
  GitHub Actions are disabled by user request; use local/approved compute.

Do not infer a fixed RAM ceiling, a universal speedup, reduced SFTP transfer
volume, or improved full-table profiling/RapidFuzz throughput from this test.
