import json
import os
import subprocess
import sys
from datetime import date
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import polars as pl
import pyarrow as pa
import pytest

from moodys_datahub import Sftp
from moodys_datahub.utils import _build_pl_query


@pytest.fixture
def client(tmp_path, monkeypatch):
    obj = Sftp(offline=True, interactive=False, output_root=tmp_path / "results")
    obj._max_path_length = 10000

    def forbidden(*args, **kwargs):
        pytest.fail("Local export and dry-run must not connect or open widgets")

    monkeypatch.setattr(obj, "_connect", forbidden)
    monkeypatch.setattr(obj, "select_data", forbidden)
    return obj


@pytest.fixture
def shards(tmp_path):
    frame = pl.DataFrame(
        {
            "row": [1, 2, 3, 4, 5, 6],
            "base": ["DK111", "US222", None, "DK111", "DK111", "DK111"],
            "other": [None, "DK999", None, None, None, None],
            "owner": ["SE111", None, "SE111", "SE111", None, "SE111"],
            "override": [None, "DK888", None, None, "DK888", None],
            "when": [
                date(2025, 1, 1),
                None,
                date(2025, 1, 1),
                date(2024, 1, 1),
                date(2025, 1, 1),
                date(2025, 1, 1),
            ],
        }
    )
    paths = []
    for i, part in enumerate([frame.head(3), frame.clear(), frame.tail(3)]):
        path = tmp_path / f"part-{i}.parquet"
        part.write_parquet(path)
        paths.append(str(path))
    return paths


def test_shared_builder_is_lazy_and_preserves_expression_columns(shards):
    query = _build_pl_query(
        shards, select_cols=["row"], query=pl.col("base") == "DK111"
    )
    assert isinstance(query, pl.LazyFrame)
    assert query.collect(engine="streaming")["row"].to_list() == [1, 4, 5, 6]


def test_export_matches_materialized_paths_without_changing_state(
    client, shards, tmp_path
):
    client.output_format = None
    client._bvd_list = [["DK111", "DK999"], ["base", "other"], None]
    client._and_bvd_list = [[["SE111"], "owner", "exact"]]
    client._or_bvd_list = [[["DK888"], "override", "exact"]]
    client._time_period = [2025, 2025, "when", "keep"]
    pandas_result, _ = client.process_all(
        files=shards, select_cols=["row"], engine="polars", num_workers=1
    )
    polars_result, _ = client.polars_all(
        files=shards, select_cols=["row"], num_workers=1
    )
    previous = client.dfs
    client.concat_files = False
    client.delete_files = True
    target = tmp_path / "results" / "links.parquet"
    paths = client.export_all(
        files=shards, select_cols=["row"], destination=target, num_workers=1
    )
    assert isinstance(pandas_result, pd.DataFrame)
    assert isinstance(polars_result, pl.DataFrame)
    assert paths == [str(target)]
    # Existing materialized methods also return their required filter columns.
    assert pl.read_parquet(paths[0]).equals(polars_result.select("row"))
    assert pandas_result["row"].tolist() == [1, 2, 5, 6]
    assert client.dfs is previous
    assert client.concat_files is False
    assert all(Path(path).is_file() for path in shards)
    assert not list(target.parent.glob("*.partial"))


def test_export_does_not_use_materialized_pipeline(
    client, shards, tmp_path, monkeypatch
):
    def forbidden(*args, **kwargs):
        pytest.fail("Export must bypass materialized result/saving methods")

    monkeypatch.setattr(client, "polars_all", forbidden)
    monkeypatch.setattr(client, "pandas_all", forbidden)
    monkeypatch.setattr("moodys_datahub.process._load_pl", forbidden)
    monkeypatch.setattr("moodys_datahub.process._save_chunks", forbidden)
    monkeypatch.setattr(pl.DataFrame, "to_pandas", forbidden)
    paths = client.export_all(files=shards, destination=tmp_path / "out.parquet")
    assert pl.read_parquet(paths[0]).height == 6


@pytest.mark.parametrize("workers", [1, 8])
def test_export_uses_bounded_download_and_native_sink(
    client, shards, tmp_path, monkeypatch, workers
):
    calls = []
    monkeypatch.setattr(client, "download_all", lambda **kwargs: calls.append(kwargs))
    client.export_all(
        files=shards, destination=tmp_path / "out.parquet", num_workers=workers
    )
    assert calls == [{"files": shards, "num_workers": workers, "async_mode": False}]


def test_export_dry_run_has_no_side_effects(client, shards, monkeypatch):
    def forbidden(*args, **kwargs):
        pytest.fail("Dry-run must not write, download, collect or start workers")

    monkeypatch.setattr(client, "download_all", forbidden)
    monkeypatch.setattr(pl.LazyFrame, "collect", forbidden)
    monkeypatch.setattr(pl.LazyFrame, "sink_parquet", forbidden)
    monkeypatch.setattr(Path, "mkdir", forbidden)
    report = client.export_all(files=shards, dry_run=True)
    assert report.ok and report.engine == "polars"
    assert report.would_write and not report.would_download
    assert report.destination.endswith(".parquet")
    assert Path(report.destination).parent == Path(client._output_root)
    assert not Path(client._output_root).exists()


@pytest.mark.parametrize("query", ["row > 1", lambda df: df, 42])
def test_export_rejects_inherited_unsupported_queries_before_download(
    client, shards, monkeypatch, query
):
    client.query = query
    monkeypatch.setattr(
        client, "download_all", lambda **kwargs: pytest.fail("download")
    )
    report = client.export_all(files=shards, dry_run=True)
    assert not report.ok
    with pytest.raises(ValueError, match="query"):
        client.export_all(files=shards)


@pytest.mark.parametrize("extension", ["csv", "avro", "xlsx"])
def test_export_rejects_non_parquet_before_download(client, tmp_path, extension):
    source = tmp_path / f"source.{extension}"
    source.write_bytes(b"not a parquet file")
    report = client.export_all(files=[str(source)], dry_run=True)
    assert not report.ok
    assert any("Parquet" in error for error in report.errors)


@pytest.mark.parametrize("zero_rows", [False, True])
def test_export_empty_results_keep_schema(client, shards, tmp_path, zero_rows):
    paths = client.export_all(
        files=shards[1:2] if not zero_rows else shards,
        query=pl.col("row") < 0,
        destination=tmp_path / "empty.parquet",
    )
    result = pl.read_parquet(paths[0])
    assert result.height == 0
    assert result.schema == pl.read_parquet_schema(shards[0])


def test_export_preflight_checks_expression_and_selected_columns(client, shards):
    report = client.export_all(
        files=shards, select_cols=["row"], query=pl.col("missing") > 1, dry_run=True
    )
    assert not report.ok and "missing" in " ".join(report.errors)


@pytest.mark.parametrize("change", ["name", "dtype"])
def test_export_rejects_schema_drift(client, shards, tmp_path, change):
    path = tmp_path / "drift.parquet"
    frame = pl.read_parquet(shards[0])
    if change == "name":
        frame = frame.rename({"base": "different"})
    else:
        frame = frame.with_columns(pl.col("row").cast(pl.String))
    frame.write_parquet(path)
    report = client.export_all(files=[shards[0], str(path)], dry_run=True)
    assert not report.ok and "schema" in " ".join(report.errors).lower()


def test_export_refuses_existing_output_and_source_collision(client, shards, tmp_path):
    for target in [Path(shards[0]), tmp_path / "exists.parquet"]:
        if not target.exists():
            target.write_bytes(b"keep me")
        original = target.read_bytes()
        report = client.export_all(files=shards, destination=target, dry_run=True)
        assert not report.ok
        with pytest.raises(ValueError):
            client.export_all(files=shards, destination=target)
        assert target.read_bytes() == original


@pytest.mark.parametrize("failure", [OSError("disk full"), KeyboardInterrupt()])
def test_export_sink_failure_cleans_only_owned_temp(
    client, shards, tmp_path, monkeypatch, failure
):
    target = tmp_path / "out.parquet"
    other_temp = tmp_path / ".someone-else.partial"
    other_temp.write_bytes(b"keep")

    def fail(self, path, **kwargs):
        Path(path).write_bytes(b"incomplete")
        raise failure

    monkeypatch.setattr(pl.LazyFrame, "sink_parquet", fail)
    with pytest.raises(type(failure)):
        client.export_all(files=shards, destination=target)
    assert not target.exists()
    assert list(tmp_path.glob("*.partial")) == [other_temp]
    assert all(Path(path).is_file() for path in shards)


def test_export_does_not_overwrite_output_created_during_execution(
    client, shards, tmp_path, monkeypatch
):
    target = tmp_path / "out.parquet"
    link = os.link

    def race(source, destination, **kwargs):
        Path(destination).write_bytes(b"another export")
        link(source, destination, **kwargs)

    monkeypatch.setattr("moodys_datahub.export.os.link", race)
    with pytest.raises(FileExistsError):
        client.export_all(files=shards, destination=target)
    assert target.read_bytes() == b"another export"
    assert not list(tmp_path.glob("*.partial"))


def test_export_large_bvd_list_preserves_duplicates_and_null_nonmatches(
    client, tmp_path
):
    source = tmp_path / "large.parquet"
    pl.DataFrame(
        {"id": ["DK00001", None, "DK54999", "US99999", "DK00001"]}
    ).write_parquet(source)
    paths = client.export_all(
        files=[str(source)],
        bvd_query=([f"DK{i:05}" for i in range(55000)], "id"),
        destination=tmp_path / "out.parquet",
    )
    assert pl.read_parquet(paths[0])["id"].to_list() == [
        "DK00001",
        "DK54999",
        "DK00001",
    ]


def test_export_prefix_across_columns(client, shards, tmp_path):
    paths = client.export_all(
        files=shards,
        bvd_query=(["DK"], ["base", "other"], "prefix"),
        select_cols=["row"],
        destination=tmp_path / "out.parquet",
    )
    assert pl.read_parquet(paths[0])["row"].to_list() == [1, 2, 4, 5, 6]


@pytest.mark.parametrize("budget", [1, 3])
def test_polars_native_budget_is_initialized_in_fresh_process(budget):
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            "import polars as pl; import json; print(json.dumps(pl.thread_pool_size()))",
        ],
        env={**os.environ, "POLARS_MAX_THREADS": str(budget)},
        check=True,
        text=True,
        capture_output=True,
    )
    assert json.loads(result.stdout) == budget


def test_export_local_does_not_create_file_processing_pool(
    client, shards, tmp_path, monkeypatch
):
    monkeypatch.setattr(
        "moodys_datahub.process._run_parallel", lambda **kwargs: pytest.fail("pool")
    )
    paths = client.export_all(
        files=shards, destination=tmp_path / "out.parquet", num_workers=8
    )
    assert pl.read_parquet(paths[0]).height == 6


def test_export_missing_remote_dry_run_uses_separate_roots(
    client, tmp_path, monkeypatch
):
    client._remote_files = ["one.parquet"]
    client._remote_path = "/export/table"
    client._set_data_product = "Product"
    client._set_table = "Table"
    client._download_root = str(tmp_path / "cache")
    report = client.export_all(dry_run=True)
    assert report.ok and report.would_download
    assert str(Path(report.files[0])).startswith(client._download_root)
    assert Path(report.destination).parent == Path(client._output_root)
    assert not Path(client._download_root).exists()
    assert any("deferred" in warning.lower() for warning in report.warnings)


def test_export_generated_output_matches_preflight(client, shards):
    report = client.export_all(files=shards, dry_run=True)
    paths = client.export_all(files=shards, num_workers=1)
    assert paths == [report.destination]


@pytest.mark.parametrize("suffix", [".csv", ""])
def test_export_requires_parquet_destination(client, shards, tmp_path, suffix):
    report = client.export_all(
        files=shards, destination=tmp_path / ("output" + suffix), dry_run=True
    )
    assert not report.ok


def test_export_empty_or_corrupt_input_is_blocked_without_writes(client, tmp_path):
    for content in [b"", b"not parquet"]:
        source = tmp_path / "bad.parquet"
        source.write_bytes(content)
        report = client.export_all(files=[str(source)], dry_run=True)
        assert not report.ok
        assert not Path(client._output_root).exists()
        assert source.read_bytes() == content
    assert not client.export_all(files=[], dry_run=True).ok


def test_export_footer_failure_cleans_staged_output(
    client, shards, tmp_path, monkeypatch
):
    target = tmp_path / "out.parquet"
    monkeypatch.setattr(
        pl.LazyFrame,
        "sink_parquet",
        lambda self, path, **kwargs: Path(path).write_bytes(b"bad"),
    )
    with pytest.raises(pa.ArrowInvalid, match="Parquet|parquet"):
        client.export_all(files=shards, destination=target)
    assert not target.exists()
    assert not list(tmp_path.glob("*.partial"))


def test_export_publication_failure_never_falls_back_to_replace(
    client, shards, tmp_path, monkeypatch
):
    target = tmp_path / "out.parquet"

    def fail(*args, **kwargs):
        raise OSError("filesystem does not support hard links")

    monkeypatch.setattr("moodys_datahub.export.os.link", fail)
    with pytest.raises(OSError, match="hard links"):
        client.export_all(files=shards, destination=target)
    assert not target.exists()
    assert not list(tmp_path.glob("*.partial"))


def test_export_preserves_valid_object_after_failed_preflight(client, shards, tmp_path):
    previous = pl.DataFrame({"previous": [1]})
    client.dfs = previous
    query = pl.col("base") == "DK111"
    client.query = query
    old_selection = (client._select_cols, client._remote_path, client._set_table)
    with pytest.raises(ValueError):
        client.export_all(files=shards, destination=tmp_path / "out.csv")
    assert client.dfs is previous and client.query is query
    assert (
        client._select_cols,
        client._remote_path,
        client._set_table,
    ) == old_selection


def test_export_revalidates_missing_sources_after_download(
    client, shards, tmp_path, monkeypatch
):
    monkeypatch.setattr(
        client, "download_all", lambda **kwargs: Path(shards[0]).write_bytes(b"")
    )
    with pytest.raises(ValueError, match="incomplete"):
        client.export_all(files=shards, destination=tmp_path / "out.parquet")
    assert not (tmp_path / "out.parquet").exists()


def test_export_waits_for_active_downloads(client, shards, tmp_path, monkeypatch):
    monkeypatch.setattr(client, "_check_download", lambda files: False)
    with pytest.raises(TimeoutError):
        client.export_all(files=shards, destination=tmp_path / "out.parquet")
    assert not (tmp_path / "out.parquet").exists()


@pytest.mark.parametrize("dtype", [pl.Date, pl.Datetime("ms"), pl.String])
def test_export_date_filter_handles_typed_and_string_columns(client, tmp_path, dtype):
    source = tmp_path / "dates.parquet"
    pl.DataFrame(
        {"when": [date(2024, 1, 1), None, date(2025, 1, 1)], "row": [1, 2, 3]}
    ).with_columns(pl.col("when").cast(dtype)).write_parquet(source)
    paths = client.export_all(
        files=[str(source)],
        date_query=[2025, 2025, "when", "keep"],
        select_cols=["row"],
        destination=tmp_path / "out.parquet",
    )
    assert pl.read_parquet(paths[0])["row"].to_list() == [2, 3]


def test_export_is_listed_as_conditionally_offline_safe(client):
    capabilities = client.offline_capabilities()
    row = capabilities.loc[capabilities["method"] == "export_all"].iloc[0]
    assert row["offline_safe"] == "conditional"
    assert row["local_repo_safe"]


def test_export_selected_remote_shard_uses_download_cache(
    client, shards, tmp_path, monkeypatch
):
    content = Path(shards[0]).read_bytes()
    client._remote_files = ["remote.parquet"]
    client._remote_path = "/export/Table"
    client._set_data_product = "Product"
    client._set_table = "Table"
    client._download_root = str(tmp_path / "cache")
    monkeypatch.setattr(
        client, "_remote_file_sizes", lambda files: {"remote.parquet": len(content)}
    )

    class Remote:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def get(self, remote, local):
            assert remote == "/export/Table/remote.parquet"
            Path(local).write_bytes(content)

        def stat(self, path):
            return SimpleNamespace(st_size=len(content), st_mtime=0)

    monkeypatch.setattr(client, "_connect", Remote)
    target = tmp_path / "results" / "remote.parquet"
    paths = client.export_all(destination=target, num_workers=1)
    assert paths == [str(target)]
    assert pl.read_parquet(paths[0]).height == 3
    assert Path(client.resolve_cache_file("remote.parquet")).read_bytes() == content
    assert client._remote_path == "/export/Table"
    assert client._set_table == "Table"


@pytest.mark.parametrize("arguments", [[], ["unused"]])
def test_export_rejects_unused_inherited_query_args(client, shards, arguments):
    client.query_args = arguments
    report = client.export_all(files=shards, dry_run=True)
    assert not report.ok
    assert any("query_args" in error for error in report.errors)
