# Polars Performance Release Plan

Prepared: 2026-11-20. Status: implemented locally; production/release gates pending.

Target: `moodys_datahub` 1.5.0, developed on `codex/polars-1.5.0`.
This is a minor release because it adds a public file-only extraction method.
Reconfirm the next available version and current worktree before implementation.
Planning does not authorize publishing a release.

## Implementation progress

- [x] Baseline: 527 existing tests pass (Python 3.11, Polars 1.35.1).
- [x] Isolated Polars 2.0/Python 3.10 and 2.1/Python 3.11 environments. Full
  suites pass (567 tests at the checkpoint); the expanded 43-case export suite
  also passes in both environments, including three additional regression cases.
- [x] Dependency markers, shared lazy query builder and explicit streaming collect.
- [x] Parquet-only export, read-only preflight and atomic no-overwrite publication.
- [x] Focused regressions and minimum/latest Polars CI configuration.
- [x] README and MkDocs development documentation; stable download links retained.
- [x] Local synthetic benchmark analysis recorded in `benchmarks/RESULTS.md`.
  Memory savings are fixture-specific; controlled throughput conclusions remain pending.
- [x] Candidate wheel/sdist built with `twine check` passing. Package/resource,
  dependency-marker and distribution-exclusion audits pass. Python 3.11
  installed-wheel validation passes all 43 export cases and verifies imports
  from site-packages, not the source checkout.
- [x] Manual Windows/Python 3.12 and 3.13 compatibility: 570 source tests and
  43 installed-wheel export tests pass on each version.
- [ ] Authorized Linux/UCloud storage smoke test. Deferred by the user until
  proper UCloud access is arranged on Monday, 2026-11-23.
- [ ] Final stable version/install-link update, tag and publication after approval.

The working version is `1.5.0.dev0`, not a replacement for published 1.4.15.
Candidate artifacts are under `.venv/polars-1.5.0/dist-final/`, not the stable
`dist/` directory. No tag, push or release publication has been performed.

GitHub Actions were disabled repository-wide on 2026-11-20 at the user's
request to avoid CI costs, and the disabled setting was verified through the
GitHub API. Workflow definitions remain available but must not be re-enabled
without approval. References below to the CI matrix describe the checks to
execute manually on local/approved compute, not a requirement to run Actions.
Build and upload final release artifacts manually while Actions remain off.

### Additional compatibility verification (2026-11-20)

| Platform | Python | Polars | Full source suite | Installed-wheel export suite |
| --- | --- | --- | --- | --- |
| Windows | 3.12.11 | 2.1.0 | 570 passed | 43 passed |
| Windows | 3.13.5 | 2.1.0 | 570 passed | 43 passed |

Both isolated environments use pandas 3.1.1, NumPy 2.5.5 and PyArrow 26.0.1.
The native Polars budget was set to four threads before import; the pool-budget
regressions also test their own budgets in fresh subprocesses. Installed-wheel
runs used `-I` and verified site-packages imports of `1.5.0.dev0`, not source
checkout imports. Dependency checks, Ruff and `twine check` also pass.

Detailed XML records are `.venv/polars-1.5.0/pytest312.xml`, `pytest313.xml`,
`wheel312.xml` and `wheel313.xml`. No package-source, workbook or distribution
changes were required. No GitHub Actions run or UCloud job was started.

These Windows results do not establish Linux/UCloud or production-storage
compatibility. No UCloud smoke test has been performed; access setup is pending
the user's Monday arrangements. Do not replace that gate with a local test.

Verification records are under `.venv/polars-1.5.0/`. Tests use short, fresh
workspace-local `--basetemp` paths: the longer automatic Windows pytest path
eventually exceeded the host's path limit in an existing timestamped-cache
test. That was a runner-path problem, not a CSV/download code change.

### Regression record

- The layered benchmark passed the private grouped-filter representation as
  public `bvd_query`, which all three public processing methods reject. Keep
  the base clause in `bvd_query` and configure `AND_bvd_list` / `OR_bvd_list`
  separately. The subprocess regression in `tests/test_polars_benchmark.py`
  failed for all three methods before this fix; it checks exact selected rows,
  duplicate preservation, null non-matches, source retention and the native
  thread budget. Benchmark examples must exercise supported public inputs.
- Expression projection originally removed non-output columns before a `pl.Expr`
  filter could use them. The shared builder now retains expression roots until
  filtering finishes; a failing-first regression verifies the behavior.
- Arrow can retain a Windows file handle when a corrupt Parquet footer makes
  reader construction fail. Export footer validation now owns an outer Python
  file context, so owned temporary output can be removed even on that failure.
  The footer-failure regression exposed the lock before this fix.
- Existing materialized methods include required filter columns in their
  returned projection. Preserve that behavior; file-only exports apply exactly
  the requested final projection. Parity checks compare the requested columns.

## Goal and boundaries

Use the newer CPU Polars engine and add an extraction route that does not
materialize the complete result merely to save it. Preserve existing callers.

- `process_all()` continues returning `(pandas_dataframe, file_names)`.
- `pandas_all()` and pandas-only query routing remain compatible.
- `polars_all()` continues returning `(polars_dataframe, file_names)` without
  converting its result to pandas. It still materializes the result.
- A new `export_all()` returns output paths, not a DataFrame.
- GPU work is deferred: no CUDA dependencies, GPU dispatch or GPU benchmarks.
- No SFTP backend migration, concurrent-table execution, metadata workbook
  changes or full-table profiling rewrite belongs in this release.
- Do not change CSV reader/processing behavior or restore the retired CSV
  service. Existing CSV workflows remain covered by regression tests.

## Phase 1: Establish supported Polars versions

1. Record the clean-source baseline, dependency versions and existing test
   results before making changes. Preserve the developer's conda environment;
   use isolated environments for upgrade testing.
2. Validate these proposed dependency markers with actual installations:

   ```toml
   "polars>=2.0,<2.1; python_version < '3.11'",
   "polars>=2.1,<3; python_version >= '3.11'",
   ```

   Polars 2.1.0 requires Python >=3.11; Polars 2.0.0 supports Python >=3.10.
   Keep the package's Python >=3.10 promise rather than silently raising it.
   Document that Python 3.10 does not receive the 2.1-specific improvements.
3. Test the minimum and latest permitted dependency versions. Do not add
   speculative compatibility layers for untested older Polars releases.
4. Review the 2.0 migration changes affecting membership coercion, dates,
   concatenation, Arrow extension types, empty inputs and incidental row order.
   Preserve identifier strings and duplicates; do not accept lossy conversions.

Exit: existing behavior passes on Python 3.10/Polars 2.0 and supported newer
Python versions/Polars 2.1. Dependency resolution succeeds on Linux and Windows.

## Phase 2: Share lazy query construction

1. Extract the existing scan/projection/BvD/date/expression construction from
   `_load_pl()` into one private helper returning a `pl.LazyFrame`.
2. Keep `_load_pl()` as the materializing caller used by existing methods.
   The export path will use the same query builder but a streaming file sink.
   Reuse existing filter normalization; do not duplicate AND/OR semantics.
3. Preserve required filter columns until filtering completes, even when they
   are absent from the requested output. Preserve BvD-before-date construction
   without making unsupported promises about the optimizer's execution order.
4. Preserve typed dates where safe; use explicit string parsing where needed.
   Keep existing null/date-selection behavior and pandas parity tests.
5. Use the streaming execution engine deliberately for this release, validated
   on both supported Polars lines. Do not claim every operation can spill or
   that a collected result can exceed available RAM.

Exit: existing materialized results, return types and save behavior remain
compatible. There is one filter-building implementation, not two.

## Phase 3: Add a narrow, safe file-only export

Public usage implemented on the development branch:

```python
paths = sftp.export_all(
    destination=r"D:\moody_results\ownership.parquet",
    select_cols=["subsidiary_bvd_id", "shareholder_bvd_id"],
    num_workers=8,
)

report = sftp.export_all(
    destination=r"D:\moody_results\ownership.parquet",
    dry_run=True,
)
```

### Initial contract

- Runtime returns `list[str]`, initially containing one completed Parquet path.
  `dry_run=True` returns the existing style of preflight report instead.
- Accept Parquet sources only initially, from local files or the selected
  remote table. Reject other/mixed formats before downloading.
- Support column selection, exact/prefix BvD clauses, multiple BvD columns,
  configured `AND_bvd_list` / `OR_bvd_list`, date selection and `pl.Expr` queries.
  Null BvD cells remain non-matches under the existing contract.
- Reject string queries and arbitrary callables explicitly, including inherited
  object query settings. Do not silently materialize or fall back to pandas.
- Write through `LazyFrame.sink_parquet()`; do not route through the current
  collect/split/save pipeline or assign a DataFrame to `self.dfs`.
- For this new method, an explicit destination is the final `.parquet` filename.
  With `destination=None`, reuse generated naming under `output_root` or the
  working-directory default. Preserve all existing destination semantics for
  other methods; downloads continue using `download_root` independently.
- Initially produce a single file. Document that the materialized writer's
  `file_size_mb`, `concat_files` and multi-format `output_format` settings do
  not control this explicit Parquet-only method. Never ignore them ambiguously.
- A valid zero-row result creates a readable, schema-preserving Parquet file.

### Safety and failure handling

- Reuse existing source readiness/download validation; never trust a merely
  existing or zero-byte source. Do not redesign the shared download cache here.
- Reject destination/source collisions and an existing final output. Do not
  introduce implicit overwrites or automatic `_copy` recovery.
- Stage output in a uniquely owned temporary sibling file on the same
  filesystem. Publish only after the sink closes and the Parquet footer can
  be read. Use a no-overwrite commit operation; test Windows and Linux behavior.
- On failure, remove only the invocation's owned output temporary file.
  This initial method retains explicit local inputs and managed cache files
  after both success and failure, regardless of `delete_files`.
- Dry-run does not download, write, delete, create directories, spawn pools or
  open widgets. Report checks that need unavailable remote schemas or actual
  writes as unverified; do not promise permissions or capacity from dry-run.
- Streaming avoids full-result materialization; it does not guarantee a fixed
  memory ceiling. Source downloads and spill files still need disk capacity.

Exit: a non-interactive extraction can produce a completed Parquet file without
building the complete result DataFrame and without changing prior object state.

## Phase 4: Add focused verification

Test through the public methods using deterministic, synthetic multi-shard data:

- pandas/Polars/export result parity, including duplicates and selected-column
  order; only require row ordering where the public contract guarantees it;
- 55,000 synthetic lookup IDs, exact/prefix matching, multiple columns, AND/OR
  layers, null cells and nullable/typed date columns;
- empty shards, zero retained rows, missing columns, schema/dtype drift and
  actionable rejection of unsupported queries or input formats;
- generated paths, `output_root`, explicit destinations, separate download
  roots, existing outputs and source/output collisions;
- interrupted/failed sinks, finalization conflicts, owned-temp cleanup, source
  preservation and unchanged prior selection/results;
- dry-run side-effect prohibition and no explicit package-level full collect,
  pandas conversion or materialized chunk writer on the export path;
- worker budget 1 and larger values, no additional file-processing pools, and
  native Polars pool configuration verified in fresh subprocesses.

Extend existing Linux Python 3.10-3.13 and Windows Python 3.13 CI coverage.
Exercise the minimum supported Polars versions explicitly, not only whatever
the resolver happens to choose. Keep noisy performance thresholds out of unit
tests; no live SFTP credentials are required for normal CI.

## Phase 5: Benchmark before making speed claims

1. Compare the existing baseline with the candidate on identical local Parquet
   shards: unfiltered, selective 55,000-ID, country-prefix, layered BvD and date
   filtering, including string-heavy ownership-shaped data.
2. Compare `process_all()`, `polars_all()` and file-only `export_all()` with
   equivalent requested outputs. Include total output-write time; do not
   compare a write-free collection with a completed export as equivalent work.
3. Use repeatable fresh subprocesses and fixed `POLARS_MAX_THREADS` settings.
   Keep download-worker budgets separate. Do not change the native pool after
   import or claim `num_workers` controls it.
4. Record versions, actual thread-pool size, input shape, retained/output counts,
   wall time, peak process memory and output bytes. Separate download time,
   collect, pandas conversion and writing where those are distinct phases.
   For the streaming sink, report combined execute-and-write time; do not
   pretend its overlapping computation and I/O are independent wall timers.
5. Compare default Parquet scanning with `parallel="prefiltered"` experimentally.
   This option is not new to 2.1 and can be slower; do not force it globally.
6. Store aggregate benchmark results only, without source rows, names, BvD IDs
   or query payloads. Label cold/warm-cache and synthetic/real-data results.
7. Run an authorized UCloud/local-data smoke test before recommending production
   rollout; if access is unavailable, record the external validation as pending.

Exit: correctness parity is demonstrated and performance conclusions have
measured evidence. Investigate material regressions before release.

## Phase 6: Documentation and release gates

- Update README, MkDocs overview/getting-started/API/reference pages and the
  changelog. Explain the three workflows and which ones materialize results.
- Document Python/Polars compatibility, streaming-export constraints, native
  CPU configuration, scratch requirements and output failure behavior.
- Keep GPU, larger profiling changes and concurrent extractions out of scope.
- Make focused commits after dependency compatibility, lazy-query refactoring,
  export/tests and documentation/benchmark conclusions.
- Only after implementation approval and successful verification: select the
  final version, update version/install links, rebuild wheel/sdist, run Ruff,
  the full CI matrix, `twine check` and installed-wheel/package-content checks.
- Before publication, audit staged files and retain the v1.4.15 tag/assets
  unchanged. Tag, push and publish only when explicitly authorized; PyPI
  publication still requires the repository's trusted-publisher configuration.

## Reviewed upstream references

Checked on 2026-11-20; recheck at implementation time.

- [Polars 2.1.0 release notes](https://github.com/pola-rs/polars/releases/tag/py-2.1.0)
- [Polars 2.1.0 Python requirements](https://pypi.org/project/polars/2.1.0/)
- [Polars 2.0.0 Python requirements](https://pypi.org/project/polars/2.0.0/)
- [Polars 2.0 migration guide](https://docs.pola.rs/releases/upgrade/2/)
- [Streaming Parquet sink](https://docs.pola.rs/api/python/stable/reference/api/polars.LazyFrame.sink_parquet.html)
- [Parquet scan strategies](https://docs.pola.rs/api/python/stable/reference/api/polars.scan_parquet.html)
