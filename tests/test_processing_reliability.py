import asyncio
from copy import deepcopy
from pathlib import Path

import pandas as pd
import polars as pl
import pytest

from moodys_datahub import Sftp


@pytest.fixture
def client(tmp_path, monkeypatch):
    obj = Sftp(offline=True, interactive=False, output_root=tmp_path / "results")
    obj.output_format = None
    obj._max_path_length = 10000

    def forbidden(*args, **kwargs):
        pytest.fail("Local processing and dry-run must not connect or prompt")

    monkeypatch.setattr(obj, "_connect", forbidden)
    monkeypatch.setattr(obj, "select_data", forbidden)
    return obj


def test_explicit_local_source_is_not_validated_against_remote_size(
    client, tmp_path, monkeypatch
):
    source = tmp_path / "source.csv"
    original = b"value\n123\n"
    source.write_bytes(original)
    client._remote_path = "/unrelated/export"
    client._remote_files = [source.name]
    client._set_data_product = "Product"
    client._set_table = "Table"
    monkeypatch.setattr(client, "_remote_file_sizes", lambda files: {source.name: 2})

    result, _ = client.pandas_all(files=[str(source)], num_workers=1)

    assert result["value"].tolist() == [123]
    assert source.read_bytes() == original


def test_empty_explicit_source_fails_without_deleting(client, tmp_path):
    source = tmp_path / "empty.csv"
    source.touch()
    with pytest.raises(ValueError, match="empty|incomplete"):
        client._get_file(str(source))
    assert source.exists()


@pytest.mark.parametrize("engine", ["pandas", "polars"])
def test_mixed_explicit_and_cache_inputs_keep_all_rows(client, tmp_path, engine):
    source = tmp_path / "one.csv"
    source.write_text("value\n1\n", encoding="utf-8")
    cache = tmp_path / "cache"
    cache.mkdir()
    (cache / "two.csv").write_text("value\n2\n", encoding="utf-8")
    client.local_path = str(cache)
    files = [str(source), "two.csv"]
    report = client.process_all(
        files=files, select_cols=["value"], engine=engine, dry_run=True
    )
    assert report.ok

    result, _ = client.process_all(
        files=files, select_cols=["value"], engine=engine, num_workers=1
    )

    assert result["value"].tolist() == [1, 2]
    assert (cache / "two.csv").exists()


def test_unknown_input_does_not_silently_produce_partial_result(client, tmp_path):
    source = tmp_path / "one.csv"
    source.write_text("value\n1\n", encoding="utf-8")
    with pytest.raises(ValueError, match="missing.csv"):
        client.pandas_all(files=[str(source), "missing.csv"], num_workers=1)


def test_local_only_generated_destination_matches_preflight(client, tmp_path):
    source = tmp_path / "source.csv"
    source.write_text("value\n123\n", encoding="utf-8")
    client.output_format = [".csv"]
    report = client.pandas_all(files=[str(source)], select_cols=["value"], dry_run=True)
    assert report.ok
    assert not Path(client._output_root).exists()

    result, paths = client.pandas_all(
        files=[str(source)], select_cols=["value"], num_workers=1
    )

    assert result["value"].tolist() == [123]
    assert paths == [report.destination + ".csv"]
    assert Path(paths[0]).is_file()


@pytest.mark.parametrize(
    "method,engine", [("pandas_all", "pandas"), ("polars_all", "polars")]
)
def test_explicit_backend_preflight_reports_the_actual_engine(
    client, tmp_path, method, engine
):
    source = tmp_path / "source.csv"
    source.write_text("value\n1\n", encoding="utf-8")
    client.concat_files = False
    report = getattr(client, method)(files=[str(source)], dry_run=True)
    assert report.ok
    assert report.engine == engine


def test_invalid_engine_is_blocked_by_dry_run(client, tmp_path):
    source = tmp_path / "source.csv"
    source.write_text("value\n1\n", encoding="utf-8")
    report = client.process_all(files=[str(source)], engine="invalid", dry_run=True)
    assert not report.ok
    assert any("engine" in error for error in report.errors)


@pytest.mark.parametrize("method", ["process_all", "download_all"])
def test_remote_files_without_selection_are_blocked_read_only(client, method):
    client.remote_files = ["part.csv"]
    state = client.__dict__.copy()
    report = getattr(client, method)(dry_run=True)
    assert not report.ok
    assert any(
        "local_path" in error or "remote_path" in error for error in report.errors
    )
    assert client.__dict__ == state


def test_download_preflight_blocks_unknown_file(client):
    report = client.download_all(files=["unknown.csv"], dry_run=True)
    assert not report.ok
    assert "unknown.csv" in report.missing_files


def test_zero_byte_managed_cache_requires_download(client, tmp_path):
    client.local_path = str(tmp_path / "cache")
    (Path(client.local_path) / "part.csv").touch()
    client._remote_path = "/export/table"
    client.remote_files = ["part.csv"]
    report = client.download_all(dry_run=True)
    assert report.ok
    assert report.would_download
    assert (Path(client.local_path) / "part.csv").exists()


def test_path_length_guard_is_included_in_preflight(client, tmp_path):
    source = tmp_path / "source.csv"
    source.write_text("value\n1\n", encoding="utf-8")
    client._max_path_length = 1
    report = client.process_all(files=[str(source)], dry_run=True)
    assert not report.ok
    assert any("max path length" in error for error in report.errors)


@pytest.mark.parametrize("as_path", [False, True])
def test_process_one_normalizes_single_filename(client, tmp_path, as_path):
    source = tmp_path / "source.csv"
    source.write_text("value\n1\n2\n", encoding="utf-8")
    result = client.process_one(files=source if as_path else str(source), n_rows=1)
    assert result["value"].tolist() == [1]


def test_process_one_default_preflight_only_plans_first_file(client, tmp_path):
    client.local_path = str(tmp_path / "cache")
    client._set_data_product = "Product"
    client._set_table = "Table"
    client._remote_path = "/export/table"
    client.remote_files = ["one.csv", "two.csv"]
    report = client.process_one(dry_run=True)
    assert report.ok
    assert report.files == [str(Path(client.local_path) / "one.csv")]


def test_failed_bvd_assignment_preserves_valid_filters_and_projection(
    client, monkeypatch
):
    client._set_data_product = "Product"
    client._set_table = "Table"
    monkeypatch.setattr(
        client,
        "search_dictionary",
        lambda **kwargs: pd.DataFrame({"Column": ["bvd_id"]}),
    )
    client._select_cols = ["value"]
    client.bvd_list = [["DK111"], "bvd_id"]
    client.AND_bvd_list = [["DK222"], "parent_id"]
    previous = deepcopy(
        (client.bvd_list, client.AND_bvd_list, client.OR_bvd_list, client.select_cols)
    )
    with pytest.raises(ValueError, match="Invalid bvd_list"):
        client.bvd_list = [["12345"], "bvd_id"]
    assert (
        client.bvd_list,
        client.AND_bvd_list,
        client.OR_bvd_list,
        client.select_cols,
    ) == previous


def test_failed_date_assignment_preserves_valid_filters_and_projection(
    client, monkeypatch
):
    client._set_data_product = "Product"
    client._set_table = "Table"
    monkeypatch.setattr(
        client, "table_dates", lambda **kwargs: pd.DataFrame({"Column": ["event_date"]})
    )
    client._select_cols = ["value"]
    client.time_period = [2020, 2021, "event_date"]
    previous = deepcopy((client.time_period, client.select_cols))
    with pytest.raises(ValueError, match="not found"):
        client.time_period = [2020, 2021, "invalid_date"]
    assert (client.time_period, client.select_cols) == previous


def test_polars_expression_stays_blocked_in_pandas_preflight(client, tmp_path):
    source = tmp_path / "source.csv"
    source.write_text("value\n1\n", encoding="utf-8")
    report = client.pandas_all(
        files=[str(source)], query=pl.col("value") > 0, dry_run=True
    )
    assert not report.ok


def test_explicit_pandas_preflight_reports_pandas_even_when_auto_prefers_polars(
    client, tmp_path
):
    source = tmp_path / "source.csv"
    source.write_text("value\n1\n", encoding="utf-8")
    report = client.pandas_all(files=[str(source)], dry_run=True)
    assert report.ok
    assert report.engine == "pandas"


@pytest.mark.parametrize("workers", [1, 2])
def test_raw_cache_file_names_are_not_truncated(client, tmp_path, workers):
    client.local_path = str(tmp_path / "cache")
    files = ["one.csv", "two.csv"]
    for file in files:
        (Path(client.local_path) / file).write_text("value\n1\n", encoding="utf-8")
    result, names = client.pandas_all(
        files=files, num_workers=workers, pool_method="threading"
    )
    assert result.empty
    assert names == [str(Path(client.local_path) / file) for file in files]


def test_windows_download_uses_bounded_threads(client, tmp_path, monkeypatch):
    from types import SimpleNamespace

    client._remote_path = "/export/table"
    client._set_data_product = "Product"
    client._set_table = "Table"
    client.remote_files = ["part.csv"]
    client.local_path = str(tmp_path / "cache")
    monkeypatch.delattr("moodys_datahub.process.os.fork", raising=False)
    monkeypatch.setattr(client, "_remote_file_sizes", lambda files: {"part.csv": 8})

    class SftpFixture:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def get(self, remote, local):
            assert remote == "/export/table/part.csv"
            Path(local).write_bytes(b"value\n1\n")

        def stat(self, remote):
            return SimpleNamespace(st_size=8, st_mtime=123)

    monkeypatch.setattr(client, "_connect", lambda: SftpFixture())
    client.download_all(num_workers=1, async_mode=False)
    assert client.download_finished
    assert (Path(client.local_path) / "part.csv").read_bytes() == b"value\n1\n"


def test_managed_partial_cache_is_still_replaced(client, tmp_path, monkeypatch):
    client.local_path = str(tmp_path / "cache")
    source = Path(client.local_path) / "part.csv"
    source.write_bytes(b"partial")
    client._remote_path = "/export/table"
    monkeypatch.setattr(client, "_remote_file_sizes", lambda files: {"part.csv": 8})
    client._download_retries = 1
    client._download_retry_backoff = 0
    monkeypatch.setattr(
        client, "_connect", lambda: (_ for _ in ()).throw(OSError("unavailable"))
    )
    with pytest.raises(ValueError, match="unavailable"):
        client._get_file("part.csv")
    assert not source.exists()


def test_active_filter_generates_destination_for_cached_pandas_input(
    client, tmp_path, monkeypatch
):
    client.local_path = str(tmp_path / "cache")
    (Path(client.local_path) / "part.csv").write_text(
        "bvd_id,value\nDK111,1\nDK222,2\n", encoding="utf-8"
    )
    client._set_data_product = "Product"
    client._set_table = "Table"
    monkeypatch.setattr(
        client,
        "search_dictionary",
        lambda **kwargs: pd.DataFrame({"Column": ["bvd_id"]}),
    )
    client.bvd_list = [["DK111"], "bvd_id"]
    client.output_format = [".csv"]
    report = client.pandas_all(files=["part.csv"], dry_run=True)
    assert report.ok
    assert report.would_write
    result, names = client.pandas_all(files=["part.csv"], num_workers=1)
    assert result["value"].tolist() == [1]
    assert names == [report.destination + ".csv"]


def test_raw_cache_pandas_preflight_does_not_claim_output_write(client, tmp_path):
    client.local_path = str(tmp_path / "cache")
    (Path(client.local_path) / "part.csv").write_text("value\n1\n", encoding="utf-8")
    client.output_format = [".csv"]
    report = client.pandas_all(files=["part.csv"], dry_run=True)
    assert report.ok
    assert not report.would_write
    assert report.destination is None


def test_preflight_checks_required_columns_in_every_local_shard(client, tmp_path):
    first = tmp_path / "first.parquet"
    second = tmp_path / "second.parquet"
    pd.DataFrame({"value": [1]}).to_parquet(first, index=False)
    pd.DataFrame({"other": [2]}).to_parquet(second, index=False)
    report = client.process_all(
        files=[str(first), str(second)], select_cols=["value"], dry_run=True
    )
    assert not report.ok
    assert any(
        "second.parquet" in error and "value" in error for error in report.errors
    )
    assert first.exists() and second.exists()


def test_interactive_bvd_cancel_preserves_previous_filter(client, monkeypatch):
    client._set_data_product = "Product"
    client._set_table = "Table"
    monkeypatch.setattr(
        client,
        "search_dictionary",
        lambda **kwargs: pd.DataFrame({"Column": ["bvd_id"]}),
    )
    client._select_cols = ["value"]
    client.bvd_list = [["DK111"], "bvd_id"]
    previous = deepcopy((client.bvd_list, client.select_cols))
    client._interactive = True

    class CancelQuestion:
        def __init__(self, *args):
            pass

        async def display_widgets(self):
            return "cancel"

    monkeypatch.setattr("moodys_datahub.process._CustomQuestion", CancelQuestion)

    async def scenario():
        client.bvd_list = [["12345"], "bvd_id"]
        await asyncio.sleep(0)

    asyncio.run(scenario())
    assert (client.bvd_list, client.select_cols) == previous
