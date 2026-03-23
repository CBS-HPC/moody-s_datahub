import pandas as pd
import pytest

from moodys_datahub.connection import _Connection
from moodys_datahub.load_data import _table_match, _table_names
from moodys_datahub.process import _Process
from moodys_datahub.tools import Sftp


class DummyProcess(_Process):
    pass


def _make_metadata_process():
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
    proc._set_data_product = "Prod"
    proc._set_table = "main_table"
    proc._local_repo = None
    proc._local_files = []
    proc._remote_files = []
    proc._remote_path = None
    proc._local_path = None
    proc._time_stamp = None
    proc._download_finished = None
    proc._table_dictionary = pd.DataFrame(
        {
            "Data Product": ["Prod", "Prod", "Other"],
            "Table": ["main_table", "main_table", "other_table"],
            "Column": ["Total Assets", "name", "value"],
            "Definition": ["Total assets", "Company name", "Other value"],
        }
    )
    proc._table_dates = pd.DataFrame(
        {
            "Data Product": ["Prod", "Prod"],
            "Table": ["main_table", "history_table"],
            "Column": ["closing_date", "information_date"],
        }
    )
    proc._tables_backup = pd.DataFrame(
        {
            "Data Product": ["Prod", "Other"],
            "Table": ["main_table", "other_table"],
            "Export": ["exp1", "exp2"],
            "Base Directory": ["base/main_table", "base/other_table"],
            "Top-level Directory": ["top1", "top2"],
            "Timestamp": ["2024-01-01", "2024-02-01"],
        }
    )
    proc._tables_available = proc._tables_backup.copy()
    return proc


def test_table_names_reads_csv_and_deduplicates(tmp_path):
    file_path = tmp_path / "products.csv"
    pd.DataFrame(
        {
            "Data Product": ["Prod", "Prod", "Other"],
            "Top-level Directory": ["dir1", "dir1", "dir2"],
            "Table": ["t1", "t1", "t2"],
        }
    ).to_csv(file_path, index=False)

    out = _table_names(str(file_path))

    assert out.to_dict("records") == [
        {"Data Product": "Prod", "Top-level Directory": "dir1"},
        {"Data Product": "Other", "Top-level Directory": "dir2"},
    ]


def test_table_match_returns_multiple_options_for_complete_match(tmp_path):
    file_path = tmp_path / "products.xlsx"
    pd.DataFrame(
        {
            "Data Product": ["ProdA", "ProdA", "ProdB"],
            "Table": ["table_1", "table_2", "other_table"],
        }
    ).to_excel(file_path, index=False)

    matched_product, matched_tables = _table_match(
        ["table_1.csv", "table_2.csv"], str(file_path)
    )

    assert matched_product == "Multiple_Options: ['ProdA']"
    assert matched_tables == ["table_1", "table_2"]


def test_select_cols_sets_values_when_columns_exist():
    proc = _make_metadata_process()

    proc.select_cols = ["name", "Total Assets"]

    assert set(proc.select_cols) == {"name", "Total Assets"}


def test_select_cols_resets_to_none_when_column_is_missing(capsys):
    proc = _make_metadata_process()

    proc.select_cols = ["missing_column"]

    assert proc.select_cols is None
    assert "cannot be found" in capsys.readouterr().out


def test_search_dictionary_letters_only_returns_original_values():
    proc = _make_metadata_process()

    out = proc.search_dictionary(
        search_word="total-assets",
        search_cols={
            "Data Product": False,
            "Table": False,
            "Column": True,
            "Definition": False,
        },
        letters_only=True,
    )

    assert out["Column"].tolist() == ["Total Assets"]


def test_table_dates_supports_partial_table_match():
    proc = _make_metadata_process()

    out = proc.table_dates(data_product="Prod", table="history")

    assert out["Column"].tolist() == ["information_date"]


def test_search_dictionary_list_tags_each_search_word():
    proc = object.__new__(Sftp)
    proc._set_data_product = "Prod"
    proc._set_table = "main_table"
    proc._table_dictionary = _make_metadata_process()._table_dictionary
    proc._tables_backup = _make_metadata_process()._tables_backup

    out = Sftp._search_dictionary_list(
        proc,
        search_word=["assets", "name"],
        search_cols={
            "Data Product": False,
            "Table": False,
            "Column": True,
            "Definition": False,
        },
    )

    assert set(out["search_word"]) == {"assets", "name"}
    assert set(out["Column"]) == {"Total Assets", "name"}


def test_get_column_names_uses_dictionary_metadata():
    sftp = object.__new__(Sftp)
    sftp._set_table = "main_table"
    sftp.search_dictionary = lambda save_to=None: pd.DataFrame(
        {"Column": ["bvd_id_number", "name"]}
    )

    out = Sftp.get_column_names(sftp)

    assert out == ["bvd_id_number", "name"]


def test_get_column_names_uses_dictionary_metadata_and_saves(monkeypatch):
    sftp = object.__new__(Sftp)
    sftp._set_table = "main_table"

    monkeypatch.setattr(
        Sftp,
        "search_dictionary",
        lambda self, save_to=None: pd.DataFrame({"Column": ["col_a", "col_b"]}),
    )

    saved = {}
    monkeypatch.setattr(
        "moodys_datahub.tools._save_to",
        lambda df, name, save_to: saved.update(
            {"columns": df["Column_Names"].tolist(), "name": name, "save_to": save_to}
        ),
    )

    out = Sftp.get_column_names(sftp, save_to="csv")

    assert out == ["col_a", "col_b"]
    assert saved == {
        "columns": ["col_a", "col_b"],
        "name": "column_names",
        "save_to": "csv",
    }


def test_get_column_names_reads_parquet_schema_from_files(tmp_path):
    file_path = tmp_path / "sample.parquet"
    pd.DataFrame({"col_a": [1], "col_b": [2]}).to_parquet(file_path, index=False)

    sftp = object.__new__(Sftp)
    sftp.remote_files = [str(file_path)]
    sftp._check_args = lambda files: (files, None)
    sftp._get_file = lambda file: (file, False)

    out = Sftp.get_column_names(sftp, files=[str(file_path)])

    assert out == ["col_a", "col_b"]


def test_orbis_to_moodys_maps_known_headers_and_returns_missing(tmp_path, monkeypatch):
    file_path = tmp_path / "orbis.xlsx"
    pd.DataFrame(
        {
            "Unnamed: 0": [1],
            "Total Assets\nEUR": [100],
            "Missing Header": [200],
        }
    ).to_excel(file_path, sheet_name="Results", index=False)

    monkeypatch.setattr(
        "moodys_datahub.tools._table_dictionary",
        lambda: pd.DataFrame(
            {
                "Data Product": ["Prod"],
                "Table": ["main_table"],
                "Column": ["Total Assets"],
                "Definition": ["Total assets definition"],
            }
        ),
    )

    found, not_found = Sftp.orbis_to_moodys(object.__new__(Sftp), str(file_path))

    assert found["heading"].tolist() == ["Total Assets"]
    assert not_found == ["Missing Header"]


def test_search_country_codes_filters_with_custom_columns(monkeypatch, capsys):
    proc = _make_metadata_process()

    monkeypatch.setattr(
        "moodys_datahub.process._country_codes",
        lambda: pd.DataFrame(
            {"Country": ["Denmark", "Congo"], "Code": ["DK", "CG"]}
        ),
    )

    out = proc.search_country_codes(
        search_word="DK", search_cols={"Country": False, "Code": True}
    )

    assert out["Code"].tolist() == ["DK"]
    assert "The following query was executed:" in capsys.readouterr().out


def test_table_search_matches_data_product_and_table():
    sftp = object.__new__(Sftp)
    sftp._tables_available = pd.DataFrame(
        {
            "Data Product": ["Firmographics (Monthly)", "Other Product"],
            "Table": ["bvd_id_and_name", "firm_table"],
        }
    )

    out = Sftp._table_search(sftp, "firm")

    assert len(out) == 2


def test_tables_available_reset_restores_backup(monkeypatch):
    conn = object.__new__(_Connection)
    conn._tables_available = None
    conn._tables_backup = None

    overview_df = pd.DataFrame(
        {
            "Data Product": ["Prod"],
            "Table": ["main_table"],
            "Export": ["exp1"],
            "Base Directory": ["base/main_table"],
            "Top-level Directory": ["top1"],
            "Timestamp": ["2024-01-01"],
        }
    )

    monkeypatch.setattr(
        _Connection,
        "_table_overview",
        lambda self, product_overview=None: (overview_df.copy(), ["old_export"]),
    )
    monkeypatch.setattr(_Connection, "_specify_data_products", lambda self: None)

    first_df, to_delete = conn.tables_available()
    conn._tables_available = pd.DataFrame({"Data Product": ["Changed"]})
    reset_df, _ = conn.tables_available(reset=True)

    assert first_df.equals(overview_df)
    assert reset_df.equals(overview_df)
    assert to_delete == ["old_export"]


def test_tables_available_initial_load_uses_table_overview_and_save(monkeypatch):
    conn = object.__new__(_Connection)
    conn._tables_available = None
    conn._tables_backup = None

    overview_df = pd.DataFrame({"Data Product": ["A"], "Table": ["table_a"]})
    saved = {}

    monkeypatch.setattr(
        _Connection,
        "_table_overview",
        lambda self, product_overview=None: (overview_df, ["old/export"]),
    )
    monkeypatch.setattr(_Connection, "_specify_data_products", lambda self: None)
    monkeypatch.setattr(
        "moodys_datahub.connection._save_to",
        lambda df, name, save_to: saved.update(
            {"df": df.copy(), "name": name, "save_to": save_to}
        ),
    )

    df, to_delete = conn.tables_available(save_to="csv")

    assert df.equals(overview_df)
    assert to_delete == ["old/export"]
    assert conn._tables_backup.equals(overview_df)
    assert saved["df"].equals(overview_df)
    assert saved["name"] == "tables_available"
    assert saved["save_to"] == "csv"


def test_pool_method_rejects_invalid_value():
    conn = object.__new__(_Connection)

    with pytest.raises(ValueError, match="Invalid worker pool method"):
        conn.pool_method = "invalid"


def test_set_data_product_exact_match_updates_timestamp_and_filters_tables(monkeypatch):
    proc = _make_metadata_process()
    proc._set_data_product = None

    monkeypatch.setattr(DummyProcess, "_object_defaults", lambda self: None)

    proc.set_data_product = "Prod"

    assert proc.set_data_product == "Prod"
    assert proc._time_stamp == "2024-01-01"
    assert proc._tables_available["Data Product"].tolist() == ["Prod"]


def test_set_data_product_partial_match_reports_multiple_matches(monkeypatch, capsys):
    proc = _make_metadata_process()
    proc._set_data_product = None
    proc._tables_backup = pd.DataFrame(
        {
            "Data Product": ["Prod Alpha", "Prod Beta"],
            "Table": ["table_a", "table_b"],
            "Export": ["exp1", "exp2"],
            "Base Directory": ["base/a", "base/b"],
            "Top-level Directory": ["top1", "top2"],
            "Timestamp": ["2024-01-01", "2024-01-02"],
        }
    )
    proc._tables_available = proc._tables_backup.copy()

    monkeypatch.setattr(DummyProcess, "_object_defaults", lambda self: None)

    proc.set_data_product = "prod"

    assert proc.set_data_product is None
    assert "Multiple data products partially match" in capsys.readouterr().out


def test_set_table_exact_match_updates_remote_path_and_selection_state(monkeypatch):
    proc = _make_metadata_process()
    proc._set_data_product = None
    proc._set_table = None

    monkeypatch.setattr(DummyProcess, "_object_defaults", lambda self: None)
    monkeypatch.setattr(
        DummyProcess,
        "_check_path",
        lambda self, path, source: ([f"{path}/part.csv"], path),
    )

    proc.set_table = "main_table"

    assert proc.set_table == "main_table"
    assert proc.set_data_product == "Prod"
    assert proc.remote_path == "base/main_table"
    assert proc.remote_files == ["base/main_table/part.csv"]


def test_set_table_partial_match_reports_multiple_tables(monkeypatch, capsys):
    proc = _make_metadata_process()
    proc._set_data_product = None
    proc._set_table = None
    proc._tables_backup = pd.DataFrame(
        {
            "Data Product": ["Prod", "Prod"],
            "Table": ["main_table", "main_history"],
            "Export": ["exp1", "exp1"],
            "Base Directory": ["base/main_table", "base/main_history"],
            "Top-level Directory": ["top1", "top1"],
            "Timestamp": ["2024-01-01", "2024-01-01"],
        }
    )
    proc._tables_available = proc._tables_backup.copy()

    monkeypatch.setattr(DummyProcess, "_object_defaults", lambda self: None)

    proc.set_table = "main"

    assert proc.set_table is None
    assert "Multiple tables partially match" in capsys.readouterr().out


def test_local_path_none_clears_local_and_remote_state():
    proc = _make_metadata_process()
    proc._local_path = "local/path"
    proc._local_files = ["local.csv"]
    proc._remote_path = "remote/path"
    proc._remote_files = ["remote.csv"]

    proc.local_path = None

    assert proc.local_path is None
    assert proc.local_files == []
    assert proc.remote_path is None
    assert proc.remote_files == []


def test_check_path_lists_local_directory(tmp_path):
    proc = _make_metadata_process()
    proc._set_table = None
    (tmp_path / "one.csv").write_text("a\n1\n", encoding="utf-8")
    (tmp_path / "two.csv").write_text("a\n2\n", encoding="utf-8")

    files, path = proc._check_path(str(tmp_path), "local")

    assert set(files) == {"one.csv", "two.csv"}
    assert path == str(tmp_path)


def test_check_path_normalizes_local_file_to_parent_directory(tmp_path):
    proc = _make_metadata_process()
    local_file = tmp_path / "single.csv"
    local_file.write_text("a\n1\n", encoding="utf-8")

    files, path = proc._check_path(str(local_file), "local")

    assert files == ["single.csv"]
    assert path == str(tmp_path)


def test_check_path_creates_missing_local_directory(tmp_path, capsys):
    proc = _make_metadata_process()
    new_dir = tmp_path / "created"

    files, path = proc._check_path(str(new_dir), "local")

    assert files == []
    assert path == str(new_dir)
    assert new_dir.exists()
    assert "created" in capsys.readouterr().out


def test_check_files_rejects_non_string_values():
    proc = _make_metadata_process()

    with pytest.raises(ValueError, match="list of strings"):
        proc._check_files(["ok.csv", 1])


def test_pool_method_falls_back_to_spawn_without_fork(monkeypatch, capsys):
    conn = object.__new__(_Connection)
    conn._pool_method = "threading"

    monkeypatch.delattr("moodys_datahub.connection.os.fork", raising=False)

    conn.pool_method = "fork"

    assert conn.pool_method == "spawn"
    output = capsys.readouterr().out
    assert "not supported" in output
    assert '"spawn" is chosen' in output
