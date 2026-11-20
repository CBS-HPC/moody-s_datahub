"""Reproducible synthetic, local-only CPU/RSS benchmark (not a CI speed test).

Run from the repository root with PYTHONPATH=src. Each measured extraction runs
in a fresh subprocess with POLARS_MAX_THREADS set before importing Polars.
"""

from __future__ import annotations

import argparse
import contextlib
import json
import os
import platform
import subprocess
import sys
import tempfile
import threading
import time
import warnings
from pathlib import Path


def worker(args):
    import polars as pl
    import psutil
    import pyarrow.parquet as pq

    from moodys_datahub import Sftp
    from moodys_datahub import process as process_module

    phases = {}

    def timed(module, name, label):
        original = getattr(module, name)

        def call(*positional, **keywords):
            start = time.perf_counter()
            try:
                return original(*positional, **keywords)
            finally:
                phases[label] = phases.get(label, 0) + time.perf_counter() - start

        setattr(module, name, call)

    timed(process_module, "_load_pl", "query_collect_seconds")
    timed(process_module, "_save_chunks", "output_write_seconds")
    if args.method == "export":
        from moodys_datahub import export as export_module

        timed(export_module, "_sink_export", "execute_and_write_seconds")
    timed(pl.DataFrame, "to_pandas", "pandas_conversion_seconds")
    files = sorted(str(path) for path in Path(args.data).glob("*.parquet"))
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)
        obj = Sftp(offline=True, interactive=False)
    obj._max_path_length = 10000
    obj.output_format = [".parquet"]
    obj.file_size_mb = 1_000_000
    options = {}
    if args.workload in {"ids", "layered", "dates"}:
        options["bvd_query"] = ([f"DK{i:08}" for i in range(55_000)], "bvd_id_number")
    if args.workload == "prefix":
        options["bvd_query"] = (["DK"], "bvd_id_number", "prefix")
    if args.workload == "layered":
        options["bvd_query"] = {
            "base": options["bvd_query"],
            "and": [(["SE"], "owner", "prefix")],
            "or": [(["NO"], "owner", "prefix")],
        }
    if args.workload == "dates":
        options["date_query"] = [2024, 2025, "information_date", "remove"]
    timed(obj, "download_all", "download_validation_seconds")
    process = psutil.Process()
    peak = [process.memory_info().rss]
    stop = threading.Event()

    def sample():
        while not stop.wait(0.01):
            peak[0] = max(peak[0], process.memory_info().rss)

    monitor = threading.Thread(target=sample, daemon=True)
    monitor.start()
    start = time.perf_counter()
    try:
        with open(os.devnull, "w") as quiet, contextlib.redirect_stdout(quiet):
            if args.method == "export":
                paths = obj.export_all(
                    files=files, destination=args.destination, num_workers=1, **options
                )
            elif args.method == "polars":
                _, paths = obj.polars_all(
                    files=files, destination=args.destination, num_workers=1, **options
                )
            else:
                _, paths = obj.process_all(
                    files=files,
                    destination=args.destination,
                    engine="polars",
                    num_workers=1,
                    **options,
                )
        elapsed = time.perf_counter() - start
        peak[0] = max(peak[0], process.memory_info().rss)
    finally:
        stop.set()
        monitor.join()
    qc_start = time.perf_counter()
    rows = sum(pq.ParquetFile(path).metadata.num_rows for path in paths)
    # Aggregate check only; never include source strings or identifier values.
    checksum = pl.scan_parquet(paths).select(pl.col("row").sum()).collect().item()
    print(
        json.dumps(
            {
                "method": args.method,
                "workload": args.workload,
                "python": platform.python_version(),
                "polars": pl.__version__,
                "platform": platform.system(),
                "thread_pool_size": pl.thread_pool_size(),
                "input_files": len(files),
                "input_rows": args.rows,
                "retained_rows": rows,
                "row_index_sum": checksum,
                "wall_seconds": elapsed,
                "peak_rss_mib": peak[0] / 1024**2,
                "output_bytes": sum(Path(path).stat().st_size for path in paths),
                "qc_seconds": time.perf_counter() - qc_start,
                **phases,
            }
        )
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows", type=int, default=1_000_000)
    parser.add_argument("--threads", type=int, default=8)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument(
        "--methods",
        nargs="+",
        choices=["process", "polars", "export"],
        default=["process", "polars", "export"],
    )
    parser.add_argument(
        "--source-root",
        type=Path,
        help="Optional baseline src directory for child imports",
    )
    parser.add_argument(
        "--report", type=Path, default=Path(".venv/polars-benchmark.json")
    )
    parser.add_argument(
        "--workloads", nargs="+", default=["all", "ids", "prefix", "layered", "dates"]
    )
    parser.add_argument("--worker", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--method", choices=["process", "polars", "export"])
    parser.add_argument("--workload")
    parser.add_argument("--data")
    parser.add_argument("--destination")
    args = parser.parse_args()
    if args.worker:
        worker(args)
        return
    if min(args.rows, args.threads, args.repeats) < 1:
        parser.error("rows, threads and repeats must be positive")
    import polars as pl

    args.report.parent.mkdir(parents=True, exist_ok=True)
    results = []
    with tempfile.TemporaryDirectory(prefix="polars-benchmark-") as directory:
        root = Path(directory)
        data = root / "sources"
        data.mkdir()
        for i in range(4):
            start, end = args.rows * i // 4, args.rows * (i + 1) // 4
            frame = pl.DataFrame(
                {"row": pl.arange(start, end, eager=True)}
            ).with_columns(
                (pl.lit("DK") + pl.col("row").cast(pl.String).str.zfill(8)).alias(
                    "bvd_id_number"
                ),
                pl.when(pl.col("row") % 3 == 0)
                .then(pl.lit("NO000001"))
                .otherwise(pl.lit("SE000001"))
                .alias("owner"),
                pl.when(pl.col("row") % 2 == 0)
                .then(pl.lit("2025-01-01"))
                .otherwise(pl.lit("2023-01-01"))
                .alias("information_date"),
                *[
                    (
                        pl.lit(f"Synthetic text {j}: ")
                        + (pl.col("row") % 100_000).cast(pl.String)
                    ).alias(f"text_{j}")
                    for j in range(10)
                ],
            )
            frame.write_parquet(data / f"part-{i}.parquet")
            del frame
        env = {**os.environ, "POLARS_MAX_THREADS": str(args.threads)}
        if args.source_root is not None:
            env["PYTHONPATH"] = str(args.source_root.resolve())
        for repeat in range(args.repeats):
            for workload in args.workloads:
                checks = []
                for method in args.methods:
                    target = root / f"{repeat}-{workload}-{method}.parquet"
                    result = subprocess.run(
                        [
                            sys.executable,
                            str(Path(__file__).resolve()),
                            "--worker",
                            "--method",
                            method,
                            "--workload",
                            workload,
                            "--data",
                            str(data),
                            "--destination",
                            str(target),
                            "--rows",
                            str(args.rows),
                        ],
                        env=env,
                        text=True,
                        capture_output=True,
                        check=True,
                    )
                    measurement = json.loads(result.stdout)
                    measurement["repeat"] = repeat + 1
                    checks.append(
                        (measurement["retained_rows"], measurement["row_index_sum"])
                    )
                    results.append(measurement)
                    args.report.write_text(
                        json.dumps(results, indent=2) + "\n", encoding="utf-8"
                    )
                    print(
                        f"{repeat + 1} {workload} {method}: {measurement['wall_seconds']:.3f}s, {measurement['peak_rss_mib']:.1f} MiB",
                        flush=True,
                    )
                if len(set(checks)) != 1:
                    raise RuntimeError("Extraction aggregate parity failed")


if __name__ == "__main__":
    main()
