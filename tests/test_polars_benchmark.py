import json
import os
import subprocess
import sys
from pathlib import Path

import polars as pl
import pytest


@pytest.mark.parametrize("method", ["process", "polars", "export"])
def test_layered_benchmark_uses_public_filter_configuration(tmp_path, method):
    root = Path(__file__).resolve().parents[1]
    data = tmp_path / "sources"
    data.mkdir()
    frame = pl.DataFrame(
        {
            "row": [0, 1, 2, 3, 4, 4, 5, 6],
            "bvd_id_number": [
                "DK00000000",
                "DK00000001",
                "DK99999999",
                "DK99999999",
                "DK00000004",
                "DK00000004",
                None,
                "DK00000006",
            ],
            "owner": [
                "SE000001",
                None,
                "SE000001",
                "NO000001",
                "SE000001",
                "SE000001",
                "NO000001",
                "FI000001",
            ],
        }
    )
    for i, part in enumerate([frame.head(4), frame.tail(4)]):
        part.write_parquet(data / f"part-{i}.parquet")

    result = subprocess.run(
        [
            sys.executable,
            str(root / "benchmarks" / "polars_export.py"),
            "--worker",
            "--method",
            method,
            "--workload",
            "layered",
            "--data",
            str(data),
            "--destination",
            str(tmp_path / "result.parquet"),
            "--rows",
            str(frame.height),
        ],
        env={**os.environ, "PYTHONPATH": str(root / "src"), "POLARS_MAX_THREADS": "1"},
        text=True,
        capture_output=True,
        timeout=120,
    )
    assert result.returncode == 0, result.stderr
    report = json.loads(result.stdout)
    assert report["thread_pool_size"] == 1
    assert report["retained_rows"] == 5
    assert report["row_index_sum"] == 16
    output = pl.read_parquet(sorted(tmp_path.glob("result*.parquet")))
    assert output["row"].sort().to_list() == [0, 3, 4, 4, 5]
    assert pl.read_parquet(sorted(data.glob("*.parquet"))).equals(frame)
