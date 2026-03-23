from pathlib import Path

import pandas as pd
import polars as pl

from moodys_datahub.utils import (
    _create_chunks,
    _process_chunk,
    _read_pd,
    _read_pl,
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
