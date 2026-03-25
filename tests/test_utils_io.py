from pathlib import Path

import pandas as pd
import polars as pl
import pytest
from pyarrow.lib import ArrowInvalid

from moodys_datahub.utils import (
    _create_chunks,
    _create_workers,
    _date_pl,
    _load_csv_table,
    _load_pd,
    _process_chunk,
    _read_csv_chunk,
    _read_pd,
    _read_pl,
    _run_parallel,
    _save_chunks,
    _save_files_pd,
    _save_files_pl,
    _save_to,
)


class _FixedDatetime:
    @classmethod
    def now(cls):
        return cls()

    def strftime(self, _fmt):
        return "20260101_120000"


def test_create_workers_caps_auto_process_pool(monkeypatch):
    created = {}

    class FakePool:
        def __init__(self, processes):
            created["processes"] = processes

    monkeypatch.setattr(
        "moodys_datahub.utils.psutil.virtual_memory",
        lambda: type("Memory", (), {"total": 96 * (1024**3)})(),
    )
    monkeypatch.setattr("moodys_datahub.utils.cpu_count", lambda: 4)
    monkeypatch.setattr("moodys_datahub.utils.Pool", FakePool)

    worker_pool, method = _create_workers(
        num_workers=-1,
        n_total=10,
        pool_method="spawn",
        query=None,
    )

    assert isinstance(worker_pool, FakePool)
    assert method == "process"
    assert created == {"processes": 4}


def test_create_workers_switches_spawn_query_to_thread_pool(monkeypatch):
    created = {}

    class FakeExecutor:
        def __init__(self, max_workers):
            created["max_workers"] = max_workers

    monkeypatch.setattr("moodys_datahub.utils.ThreadPoolExecutor", FakeExecutor)

    worker_pool, method = _create_workers(
        num_workers=3,
        n_total=10,
        pool_method="spawn",
        query=lambda frame: frame,
    )

    assert isinstance(worker_pool, FakeExecutor)
    assert method == "thread"
    assert created == {"max_workers": 3}


def test_create_workers_defaults_to_threading_without_fork(monkeypatch):
    created = {}

    class FakeExecutor:
        def __init__(self, max_workers):
            created["max_workers"] = max_workers

    monkeypatch.delattr("moodys_datahub.utils.os.fork", raising=False)
    monkeypatch.setattr("moodys_datahub.utils.ThreadPoolExecutor", FakeExecutor)

    worker_pool, method = _create_workers(
        num_workers=8,
        n_total=3,
        pool_method=None,
        query=None,
    )

    assert isinstance(worker_pool, FakeExecutor)
    assert method == "thread"
    assert created == {"max_workers": 3}


def test_run_parallel_process_branch_closes_and_joins(monkeypatch):
    calls = {"closed": False, "joined": False}

    class FakePool:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def map(self, fnc, params, chunksize=None):
            calls["chunksize"] = chunksize
            return [fnc(item) for item in params]

        def close(self):
            calls["closed"] = True

        def join(self):
            calls["joined"] = True

    monkeypatch.setattr(
        "moodys_datahub.utils._create_workers",
        lambda num_workers, n_total, pool_method: (FakePool(), "process"),
    )
    monkeypatch.setattr(
        "moodys_datahub.utils.tqdm",
        lambda iterable, total, mininterval: iterable,
    )

    result = _run_parallel(lambda value: value * 2, [1, 2, 3], n_total=3)

    assert result == [2, 4, 6]
    assert calls == {"closed": True, "joined": True, "chunksize": 1}


def test_run_parallel_thread_branch_maps_without_chunksize(monkeypatch):
    class FakeThreadPool:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def map(self, fnc, params):
            return [fnc(item) for item in params]

    monkeypatch.setattr(
        "moodys_datahub.utils._create_workers",
        lambda num_workers, n_total, pool_method: (FakeThreadPool(), "thread"),
    )
    monkeypatch.setattr(
        "moodys_datahub.utils.tqdm",
        lambda iterable, total, mininterval: iterable,
    )

    result = _run_parallel(lambda value: value + 1, [1, 2, 3], n_total=3)

    assert result == [2, 3, 4]


def test_run_parallel_returns_empty_on_worker_error(monkeypatch, capsys):
    class FailingPool:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def map(self, fnc, params, chunksize=None):
            raise RuntimeError("pool failed")

        def close(self):
            return None

        def join(self):
            return None

    monkeypatch.setattr(
        "moodys_datahub.utils._create_workers",
        lambda num_workers, n_total, pool_method: (FailingPool(), "process"),
    )
    monkeypatch.setattr(
        "moodys_datahub.utils.tqdm",
        lambda iterable, total, mininterval: iterable,
    )

    result = _run_parallel(lambda value: value, [1, 2], n_total=2)

    assert result == []
    assert "Error occurred: pool failed" in capsys.readouterr().out


def test_save_to_writes_pandas_and_polars_csv(monkeypatch, tmp_path, capsys):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr("moodys_datahub.utils.datetime", _FixedDatetime)

    _save_to(pd.DataFrame({"value": [1]}), "pd_result", "csv")
    _save_to(pl.DataFrame({"value": [2]}), "pl_result", "csv")

    assert (tmp_path / "pd_result_20260101_120000.csv").exists()
    assert (tmp_path / "pl_result_20260101_120000.csv").exists()
    output = capsys.readouterr().out
    assert "Results have been saved" in output


def test_save_to_ignores_empty_data_and_invalid_format(monkeypatch, tmp_path, capsys):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr("moodys_datahub.utils.datetime", _FixedDatetime)

    _save_to(pd.DataFrame(), "empty_result", "csv")
    _save_to(pd.DataFrame({"value": [1]}), "bad_result", "json")

    assert not list(tmp_path.glob("empty_result_*"))
    assert not list(tmp_path.glob("bad_result_*"))
    output = capsys.readouterr().out
    assert "df is empty and cannot be saved" in output
    assert "Invalid file_type" in output


def test_save_files_pd_and_pl_write_expected_outputs(tmp_path):
    pd_df = pd.DataFrame({"value": [1, 2]})
    pl_df = pl.DataFrame({"value": [3, 4]})

    pd_files = _save_files_pd(pd_df, str(tmp_path / "pd_frame"), [".csv", ".pickle"])
    pl_files = _save_files_pl(pl_df, str(tmp_path / "pl_frame"), [".csv", ".parquet"])

    assert pd_files == [
        str(tmp_path / "pd_frame.csv"),
        str(tmp_path / "pd_frame.pickle"),
    ]
    assert pl_files == [
        str(tmp_path / "pl_frame.csv"),
        str(tmp_path / "pl_frame.parquet"),
    ]
    assert all(Path(file).exists() for file in pd_files + pl_files)


def test_create_chunks_handles_pandas_and_polars_inputs():
    pd_df = pd.DataFrame({"value": list(range(100))})
    pl_df = pl.DataFrame({"value": list(range(100))})

    pd_chunks = _create_chunks(pd_df, [".csv"], file_size=1)
    pl_chunks = _create_chunks(pl_df, [".parquet"], file_size=1)

    assert pd_chunks[0] >= 1
    assert pd_chunks[1] == 100
    assert pd_chunks[2] >= 1
    assert pl_chunks[0] >= 1
    assert pl_chunks[1] == 100
    assert pl_chunks[2] >= 1


def test_process_chunk_dispatches_by_dataframe_type(monkeypatch):
    calls = []

    monkeypatch.setattr(
        "moodys_datahub.utils._save_files_pd",
        lambda df, file_name, output_format: calls.append(
            ("pd", list(df["value"]), file_name, output_format)
        )
        or [f"{file_name}.csv"],
    )
    monkeypatch.setattr(
        "moodys_datahub.utils._save_files_pl",
        lambda df, file_name, output_format: calls.append(
            ("pl", df["value"].to_list(), file_name, output_format)
        )
        or [f"{file_name}.parquet"],
    )

    pd_result = _process_chunk((1, pd.DataFrame({"value": [1]}), 2, "output", [".csv"]))
    pl_result = _process_chunk(
        (1, pl.DataFrame({"value": [2]}), 1, "output", [".parquet"])
    )

    assert pd_result == ["output_1.csv"]
    assert pl_result == ["output.parquet"]
    assert calls == [
        ("pd", [1], "output_1", [".csv"]),
        ("pl", [2], "output", [".parquet"]),
    ]


def test_save_chunks_concatenates_list_and_uses_sequential_processing(monkeypatch):
    calls = []

    monkeypatch.setattr(
        "moodys_datahub.utils._create_chunks",
        lambda df, output_format, file_size: (2, len(df), 2),
    )
    monkeypatch.setattr(
        "moodys_datahub.utils._process_chunk",
        lambda params: calls.append(params) or [f"{params[3]}_{params[0]}.csv"],
    )

    df, file_names = _save_chunks(
        [pd.DataFrame({"value": [1, 2]}), pd.DataFrame({"value": [3]})],
        "joined",
        [".csv"],
        num_workers=1,
    )

    assert df["value"].tolist() == [1, 2, 3]
    assert file_names == ["joined_1.csv", "joined_2.csv"]
    assert len(calls) == 2


def test_read_pd_and_read_pl_support_csv_and_reject_unknown_formats(tmp_path):
    csv_path = tmp_path / "sample.csv"
    pd.DataFrame({"value": [1], "other": [2]}).to_csv(csv_path, index=False)
    unknown_path = tmp_path / "sample.unknown"
    unknown_path.write_text("x", encoding="utf-8")

    pd_df = _read_pd(str(csv_path), ["value"])
    pl_df = _read_pl([str(csv_path)]).collect()

    assert pd_df.columns.tolist() == ["value"]
    assert pl_df.columns == ["value", "other"]

    try:
        _read_pd(str(unknown_path), None)
    except ValueError as exc:
        assert "Unsupported file format" in str(exc)

    try:
        _read_pl([str(unknown_path)])
    except ValueError as exc:
        assert "Unsupported file format" in str(exc)


def test_date_pl_filters_years_and_keeps_null_rows():
    lazy = pl.DataFrame(
        {
            "closing_date": ["2019-01-01 00:00:00", "2020-06-01 00:00:00", None],
            "value": [1, 2, 3],
        }
    ).lazy()

    result = _date_pl(
        lazy,
        date_col="closing_date",
        start_year=2020,
        end_year=2020,
        nan_action="keep",
    ).collect()

    assert result["value"].to_list() == [2, 3]


def test_load_pd_applies_date_bvd_and_query_filters(tmp_path):
    file_path = tmp_path / "sample.parquet"
    pd.DataFrame(
        {
            "bvd_id": ["A1", "B2", "C3"],
            "closing_date": ["2019-01-01", "2020-06-01", "2020-07-01"],
            "value": [1, 2, 3],
        }
    ).to_parquet(file_path, index=False)

    result = _load_pd(
        str(file_path),
        select_cols=["bvd_id", "closing_date", "value"],
        date_query=[2020, 2020, "closing_date", "remove"],
        bvd_query="bvd_id in ['B2', 'C3']",
        query="value > 2",
    )

    assert result["bvd_id"].tolist() == ["C3"]


def test_load_pd_removes_folder_on_arrow_invalid(monkeypatch, tmp_path):
    data_dir = tmp_path / "broken_dir"
    data_dir.mkdir()
    file_path = data_dir / "broken.parquet"
    file_path.write_text("broken", encoding="utf-8")
    removed = {}

    monkeypatch.setattr(
        "moodys_datahub.utils._read_pd",
        lambda file, select_cols: (_ for _ in ()).throw(ArrowInvalid("bad parquet")),
    )
    monkeypatch.setattr(
        "moodys_datahub.utils.shutil.rmtree",
        lambda path: removed.update({"path": path}),
    )

    with pytest.raises(ValueError, match="folder and sub files"):
        _load_pd(str(file_path))

    assert removed == {"path": str(data_dir)}


def test_read_csv_chunk_applies_selection_and_filters(tmp_path):
    file_path = tmp_path / "chunk.csv"
    pd.DataFrame(
        {
            "bvd_id": ["A1", "B2", "C3"],
            "closing_date": ["2019-01-01", "2020-06-01", None],
            "value": [1, 2, 3],
        }
    ).to_csv(file_path, index=False)

    result = _read_csv_chunk(
        (
            str(file_path),
            0,
            10,
            ["bvd_id", "closing_date", "value"],
            [0, 1, 2],
            [2020, 2020, "closing_date", "keep"],
            "bvd_id in ['B2', 'C3']",
            "value >= 2",
            None,
        )
    )

    assert result["bvd_id"].tolist() == ["B2", "C3"]
    assert result["value"].tolist() == [2, 3]


def test_load_csv_table_validates_columns_and_uses_parallel_reader(monkeypatch, tmp_path):
    file_path = tmp_path / "table.csv"
    pd.DataFrame(
        {
            "bvd_id": ["A1", "B2", "C3", "D4"],
            "value": [1, 2, 3, 4],
        }
    ).to_csv(file_path, index=False)
    captured = {}

    def fake_run_parallel(fnc, params_list, n_total, num_workers, pool_method, msg):
        captured.update(
            {
                "n_total": n_total,
                "num_workers": num_workers,
                "pool_method": pool_method,
                "msg": msg,
                "params_len": len(params_list),
            }
        )
        return [
            pd.DataFrame({"bvd_id": ["A1", "B2"], "value": [1, 2]}),
            pd.DataFrame({"bvd_id": ["C3", "D4"], "value": [3, 4]}),
        ]

    monkeypatch.setattr("moodys_datahub.utils._run_parallel", fake_run_parallel)

    result = _load_csv_table(str(file_path), select_cols=["bvd_id", "value"], num_workers=2)

    assert result["bvd_id"].tolist() == ["A1", "B2", "C3", "D4"]
    assert captured == {
        "n_total": 2,
        "num_workers": 2,
        "pool_method": "process",
        "msg": "Reading chunks",
        "params_len": 2,
    }


def test_load_csv_table_raises_for_missing_columns(tmp_path):
    file_path = tmp_path / "table.csv"
    pd.DataFrame({"bvd_id": ["A1"], "value": [1]}).to_csv(file_path, index=False)

    with pytest.raises(ValueError, match="Columns not found in file"):
        _load_csv_table(str(file_path), select_cols=["missing"], num_workers=1)
