import pandas as pd
import pytest

from moodys_datahub.process import _Process
from moodys_datahub.tools import Sftp


class DummyProcess(_Process):
    pass


def _make_dummy_process():
    proc = object.__new__(DummyProcess)
    proc.remote_files = ["sample.csv"]
    proc._time_period = [None, None, None, "remove"]
    proc._bvd_list = [None, None, None]
    proc.query = None
    proc.query_args = None
    proc._select_cols = None
    proc.concat_files = True
    proc.output_format = None
    proc.file_size_mb = 100
    proc._set_data_product = "Dummy Product"
    proc._set_table = "dummy_table"
    proc._last_process_engine = None
    proc._last_process_reason = None
    proc._tables_backup = pd.DataFrame({"Data Product": ["Dummy Product"]})
    proc._table_dictionary = None
    proc._table_dates = None
    return proc


def test_select_cols_accepts_valid_columns(monkeypatch):
    proc = _make_dummy_process()

    monkeypatch.setattr(
        DummyProcess,
        "search_dictionary",
        lambda self, **kwargs: pd.DataFrame(
            {
                "Column": ["alpha", "beta"],
                "Definition": ["Alpha", "Beta"],
            }
        ),
    )

    proc.select_cols = ["alpha", "beta"]

    assert set(proc.select_cols) == {"alpha", "beta"}


def test_select_cols_rejects_missing_columns(monkeypatch, capsys):
    proc = _make_dummy_process()

    monkeypatch.setattr(
        DummyProcess,
        "search_dictionary",
        lambda self, **kwargs: pd.DataFrame(
            {
                "Column": ["alpha"],
                "Definition": ["Alpha"],
            }
        ),
    )

    proc.select_cols = ["alpha", "missing"]

    assert proc.select_cols is None
    assert "cannot be found" in capsys.readouterr().out


def test_search_dictionary_letters_only_returns_original_rows():
    proc = _make_dummy_process()
    proc._table_dictionary = pd.DataFrame(
        {
            "Data Product": ["Dummy Product"],
            "Table": ["dummy_table"],
            "Column": ["total_assets"],
            "Definition": ["Total Assets"],
        }
    )

    result = proc.search_dictionary(
        search_word="Total Assets",
        search_cols={
            "Data Product": False,
            "Table": False,
            "Column": False,
            "Definition": True,
        },
        letters_only=True,
        data_product="Dummy Product",
        table="dummy_table",
    )

    assert result["Definition"].tolist() == ["Total Assets"]
    assert result["Column"].tolist() == ["total_assets"]


def test_choose_process_engine_detects_mixed_formats():
    proc = _make_dummy_process()

    engine, reason = proc._choose_process_engine(
        files=["part1.csv", "part2.parquet"],
        raw_bvd_query=None,
        polars_bvd_query=None,
    )

    assert engine == "pandas"
    assert reason == "mixed_formats"


def test_choose_process_engine_detects_multi_file_xlsx():
    proc = _make_dummy_process()

    engine, reason = proc._choose_process_engine(
        files=["part1.xlsx", "part2.xlsx"],
        raw_bvd_query=None,
        polars_bvd_query=None,
    )

    assert engine == "pandas"
    assert reason == "multi_file_xlsx"


def test_normalize_bvd_queries_builds_prefix_query():
    proc = _make_dummy_process()

    pandas_query, polars_query = proc._normalize_bvd_queries(
        [["DK", "SE"], ["bvd_id"], "prefix"]
    )

    assert pandas_query == (
        "bvd_id.str.startswith('DK', na=False) | "
        "bvd_id.str.startswith('SE', na=False)"
    )
    assert polars_query == [["DK", "SE"], "bvd_id", "prefix"]


def test_normalize_bvd_queries_rejects_invalid_mode():
    proc = _make_dummy_process()

    with pytest.raises(ValueError, match="must be 'exact' or 'prefix'"):
        proc._normalize_bvd_queries([["DK"], "bvd_id", "invalid"])


def test_get_column_names_reads_local_parquet_schema(tmp_path):
    parquet_path = tmp_path / "schema.parquet"
    pd.DataFrame({"alpha": [1], "beta": [2]}).to_parquet(parquet_path, index=False)

    class FakeSession:
        remote_files = None

        def _check_args(self, files):
            return files, None

        def _get_file(self, file):
            return file, True

    columns = Sftp.get_column_names(FakeSession(), files=[str(parquet_path)])

    assert columns == ["alpha", "beta"]


def test_orbis_to_moodys_maps_known_headings_and_reports_unknown(tmp_path, monkeypatch):
    orbis_path = tmp_path / "orbis.xlsx"
    pd.DataFrame(
        {
            "Unnamed: 0": [1],
            "Name\nsecondary": ["Acme"],
            "Unknown Heading": ["x"],
        }
    ).to_excel(orbis_path, sheet_name="Results", index=False)

    monkeypatch.setattr(
        "moodys_datahub.tools._table_dictionary",
        lambda: pd.DataFrame(
            {
                "Data Product": ["Firmographics (Monthly)"],
                "Table": ["bvd_id_and_name"],
                "Column": ["Name"],
                "Definition": ["Company Name"],
            }
        ),
    )

    found, not_found = Sftp.orbis_to_moodys(object(), str(orbis_path))

    assert found["heading"].tolist() == ["Name"]
    assert not_found == ["Unknown Heading"]
