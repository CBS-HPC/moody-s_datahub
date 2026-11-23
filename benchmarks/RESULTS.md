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

## Consumer-run UCloud validation (reported 2026-11-23)

Moody Agent reported results in its
`docs/handoffs/moodys-datahub-preview-layered-query-2026-11-23.md` for the
exact `1.5.0.dev0` preview wheel, SHA-256
`719db140080e9ece2dabc62af94cc17599d9c0583ba637edfa45fc8dd0e025e0`.

- Separate live-data job `12474603` found bounded two-shard ID/date parity
  across all three candidate APIs and the stable baseline. Hard-link
  publication/safety passed on WekaFS; only aggregates were retrieved.
- Synthetic-only job `12474725` passed requested-column parity against 1.4.15,
  including layered filters, dates, duplicates and nulls, plus export safety
  on Linux/WekaFS, Python 3.12.3 and Polars 2.1.0 with one native thread.
  This rerun mounted neither SFTP credentials nor production storage.
- The earlier fingerprint mismatch was a consumer-harness issue involving
  shared mutable `select_cols` and incidental helper columns. Fresh options
  and frozen comparison columns resolved it without a package runtime change.
- The independent benchmark grouped-query failure is fixed locally: keep a
  base `bvd_query` and configure public `AND_bvd_list` / `OR_bvd_list` settings.
  Failing-first subprocess regressions verify all three APIs with duplicates,
  nulls, OR-only matches and an explicit native thread budget.

These checks do not establish full-scale throughput or RAM ceilings. This
package agent did not perform the live runs or inspect source records.

## Final local benchmark smoke check (2026-11-23)

After fixing the layered configuration, the default five workloads completed
across all three methods (15 fresh subprocesses) on 60,003 synthetic input
rows, with one native thread and one repetition. Python 3.11.14 / Polars 2.1.0
were used; every method agreed on row counts and synthetic row-index sums.

| Workload | Retained rows per method |
| --- | ---: |
| All rows | 60,003 |
| 55,000-ID selection | 55,000 |
| Prefix | 60,003 |
| Layered base/AND/OR | 56,667 |
| ID/date selection | 27,500 |

Aggregate report: `.venv/polars-1.5.0/release-smoke.json`. The subprocess unit
regressions additionally verify exact selected rows, including duplicate and
nullable inputs. This smoke run overlapped compatibility testing; its timings
are not controlled performance measurements or speed claims.

## Follow-up before throughput or scan-default recommendations

- Controlled timing on representative data, including larger shard counts,
  nullable/typed dates and layered filters.
- A measured comparison of auto/prefiltered scan strategies before changing
  any scan default. This release keeps the normal `scan_parquet` strategy.
- Broader UCloud measurements with explicit native CPU budgets and known
  download/spill/output storage capacity. Bounded parity/storage checks passed
  as reported above; representative throughput comparisons remain pending.
- GitHub Actions are disabled by user request; use local/approved compute.

Do not infer a fixed RAM ceiling, a universal speedup, reduced SFTP transfer
volume, or improved full-table profiling/RapidFuzz throughput from this test.
