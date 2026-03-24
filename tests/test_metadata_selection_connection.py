import asyncio
import json

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


def test_search_dictionary_list_supports_exact_match_and_letters_only(monkeypatch):
    proc = object.__new__(Sftp)
    proc._set_data_product = "Prod"
    proc._set_table = "main_table"
    proc._tables_backup = pd.DataFrame({"Data Product": ["Prod", "Other"]})
    proc._table_dictionary = pd.DataFrame(
        {
            "Data Product": ["Prod", "Prod", "Other"],
            "Table": ["main_table", "main_table", "other_table"],
            "Column": ["Total Assets", "name", "other"],
            "Definition": ["Total Assets", "Company Name", "Other"],
        }
    )

    out = Sftp._search_dictionary_list(
        proc,
        search_word=["total-assets", "name"],
        search_cols={
            "Data Product": False,
            "Table": False,
            "Column": True,
            "Definition": False,
        },
        letters_only=True,
        exact_match=True,
    )

    assert set(out["search_word"]) == {"totalassets", "name"}
    assert set(out["Column"]) == {"Total Assets", "name"}


def test_search_dictionary_list_returns_empty_for_unknown_table(capsys):
    proc = object.__new__(Sftp)
    proc._set_data_product = "Prod"
    proc._set_table = "main_table"
    proc._tables_backup = pd.DataFrame({"Data Product": ["Prod"]})
    proc._table_dictionary = pd.DataFrame(
        {
            "Data Product": ["Prod"],
            "Table": ["main_table"],
            "Column": ["name"],
            "Definition": ["Company Name"],
        }
    )

    out = Sftp._search_dictionary_list(proc, search_word="name", table="missing_table")

    assert out.empty
    assert "No such Table was found" in capsys.readouterr().out


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


def test_check_path_creates_missing_local_folder(tmp_path, capsys):
    proc = _make_metadata_process()
    new_folder = tmp_path / "created_folder"

    files, path = proc._check_path(str(new_folder), "local")

    assert files == []
    assert path == str(new_folder)
    assert new_folder.exists()
    assert f"Folder '{new_folder}' created." in capsys.readouterr().out


def test_check_path_marks_invalid_remote_path(monkeypatch, capsys):
    class FakeSftp:
        def exists(self, path):
            return False

    proc = _make_metadata_process()
    monkeypatch.setattr(DummyProcess, "_connect", lambda self: FakeSftp())

    files, path = proc._check_path("remote/missing", "remote")

    assert files == []
    assert path is None
    assert "Remote path is invalid" in capsys.readouterr().out


def test_check_files_requires_string_lists():
    proc = _make_metadata_process()

    with pytest.raises(ValueError, match="file list must be a list of strings"):
        proc._check_files(["ok", 1])


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


def test_connection_init_sets_default_state(monkeypatch):
    class FakeCnOpts:
        def __init__(self):
            self.hostkeys = "preset"

    monkeypatch.setattr("moodys_datahub.connection.pysftp.CnOpts", FakeCnOpts)

    conn = _Connection()

    assert conn.connection is None
    assert conn.privatekey is None
    assert conn._cnopts.hostkeys is None
    assert conn.output_format == [".csv"]
    assert conn.file_size_mb == 500
    assert conn.delete_files is False
    assert conn.concat_files is True


def test_connect_passes_credentials_to_pysftp(monkeypatch):
    captured = {}
    sentinel = object()

    def fake_connection(**kwargs):
        captured.update(kwargs)
        return sentinel

    monkeypatch.setattr("moodys_datahub.connection.pysftp.Connection", fake_connection)

    conn = object.__new__(_Connection)
    conn.hostname = "host"
    conn.username = "user"
    conn.port = 22
    conn.privatekey = "key.pem"
    conn._cnopts = object()

    assert conn._connect() is sentinel
    assert captured == {
        "host": "host",
        "username": "user",
        "port": 22,
        "private_key": "key.pem",
        "cnopts": conn._cnopts,
    }


def test_pool_method_rejects_invalid_value():
    conn = object.__new__(_Connection)

    with pytest.raises(ValueError, match="Invalid worker pool method"):
        conn.pool_method = "invalid"


def test_table_overview_reads_sftp_exports_and_resolves_unknown_products(
    monkeypatch, tmp_path
):
    class FakeStat:
        def __init__(self, mtime):
            self.st_mtime = mtime

    class FakeSftp:
        def __init__(self):
            self.removed = []

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def listdir(self, path=None):
            if path is None:
                return ["prod_dir", "unknown_dir"]
            path = path.replace("\\", "/")

            mapping = {
                "prod_dir": ["tnfs", "export_old", "export_new"],
                "prod_dir/tnfs": ["old.tnf", "new.tnf"],
                "prod_dir/export_new": ["main_table.csv", "usd_interim"],
                "unknown_dir": ["unknown_export"],
                "unknown_dir/unknown_export": ["mystery_table.csv"],
            }
            return mapping[path]

        def exists(self, path):
            return path == "prod_dir/tnfs"

        def stat(self, path):
            mapping = {
                "prod_dir/tnfs/old.tnf": 100,
                "prod_dir/tnfs/new.tnf": 200,
            }
            return FakeStat(mapping[path])

        def get(self, remote_path, local_path):
            with open(local_path, "w", encoding="utf-8") as handle:
                json.dump({"DataFolder": "export_new"}, handle)

        def remove(self, path):
            self.removed.append(path)

    fake_sftp = FakeSftp()
    conn = object.__new__(_Connection)
    conn._local_repo = None
    conn._local_path = str(tmp_path)

    monkeypatch.setattr(
        "moodys_datahub.connection._table_names",
        lambda file_name=None: pd.DataFrame(
            {
                "Data Product": ["Known Product"],
                "Top-level Directory": ["prod_dir"],
            }
        ),
    )
    monkeypatch.setattr(_Connection, "_connect", lambda self: fake_sftp)
    monkeypatch.setattr(
        "moodys_datahub.connection._table_match",
        lambda tables: ("Resolved Product", tables),
    )

    df, to_delete = conn._table_overview()

    assert set(df["Data Product"]) == {"Known Product", "Resolved Product"}
    assert set(df["Table"]) == {"main_table", "interim_usd", "mystery_table"}
    assert "prod_dir/export_old" in to_delete
    assert "prod_dir/tnfs/old.tnf" in fake_sftp.removed


def test_server_clean_up_runs_prompt_for_allowed_host(monkeypatch, capsys):
    class FakeQuestion:
        def __init__(self, question, buttons):
            self.question = question
            self.buttons = buttons

        async def display_widgets(self):
            return "ok"

    conn = object.__new__(_Connection)
    conn.hostname = "s-f2112b8b980e44f9a.server.transfer.eu-west-1.amazonaws.com"
    conn.username = "D2vdz8elTWKyuOcC2kMSnw"
    called = {}

    monkeypatch.setattr("moodys_datahub.connection.cpu_count", lambda: 32)
    monkeypatch.setattr("moodys_datahub.connection._CustomQuestion", FakeQuestion)
    monkeypatch.setattr(
        "moodys_datahub.connection.asyncio.ensure_future",
        lambda coro: asyncio.run(coro),
    )
    monkeypatch.setattr(
        _Connection,
        "_remove_exports",
        lambda self, to_delete: called.update({"to_delete": to_delete}),
    )

    conn._server_clean_up(["export_a"])

    assert called == {"to_delete": ["export_a"]}
    assert "DELETING OLD EXPORTS" in capsys.readouterr().out


def test_delete_files_and_folders_use_sftp_remove(monkeypatch, capsys):
    class FakeAttr:
        def __init__(self, filename):
            self.filename = filename

    class FakeSftp:
        def __init__(self):
            self.removed = []

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def remove(self, path):
            self.removed.append(path)

        def listdir_attr(self, path):
            if path == "missing":
                raise FileNotFoundError
            return [FakeAttr("one.csv"), FakeAttr("two.csv")]

    fake_sftp = FakeSftp()
    conn = object.__new__(_Connection)

    monkeypatch.setattr(_Connection, "_connect", lambda self: fake_sftp)

    conn._delete_files(["a.csv", "b.csv"])
    conn._delete_folders("folder_a")
    conn._delete_folders("missing")

    assert fake_sftp.removed[:2] == ["a.csv", "b.csv"]
    assert "folder_a/one.csv" in fake_sftp.removed
    assert "Folder folder_a deleted successfully" in capsys.readouterr().out


def test_object_defaults_resets_runtime_state():
    conn = object.__new__(_Connection)
    conn._select_cols = ["name"]
    conn.query = "value > 1"
    conn.query_args = ["arg"]
    conn._bvd_list = [["DK"], "bvd_id_number", "query"]
    conn._time_period = [2020, 2021, "closing_date", "keep"]
    conn.dfs = pd.DataFrame({"value": [1]})
    conn._local_path = "local/path"
    conn._local_files = ["local.csv"]
    conn._remote_path = "remote/path"
    conn._remote_files = ["remote.csv"]
    conn._set_data_product = "Prod"
    conn._time_stamp = "2024-01-01"
    conn._set_table = "table_a"
    conn._download_finished = True
    conn._last_process_engine = "polars"
    conn._last_process_reason = "compatible"

    conn._object_defaults()

    assert conn._select_cols is None
    assert conn.query is None
    assert conn.query_args is None
    assert conn._bvd_list == [None, None, None]
    assert conn._time_period == [None, None, None, "remove"]
    assert conn.dfs is None
    assert conn._local_path is None
    assert conn._remote_files == []
    assert conn._set_data_product is None
    assert conn._download_finished is None
    assert conn._last_process_engine is None


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


def test_remote_path_falls_back_to_export_match(monkeypatch):
    proc = _make_metadata_process()
    proc._set_data_product = "Missing Product"
    proc._set_table = "main_table"

    monkeypatch.setattr(
        DummyProcess,
        "_check_path",
        lambda self, path, source: (["part.csv"], path),
    )

    proc.remote_path = "exp1"

    assert proc.remote_path == "exp1"
    assert proc.remote_files == ["part.csv"]
    assert proc.set_table is None


def test_select_data_sets_product_and_table_for_single_match(monkeypatch, capsys):
    class FakeSelectData:
        def __init__(self, df, title):
            self.df = df
            self.title = title

        async def display_widgets(self):
            return "Prod", "main_table"

    proc = _make_metadata_process()
    proc._set_data_product = None
    proc._set_table = None
    proc._tables_backup = pd.DataFrame(
        {
            "Data Product": ["Prod"],
            "Table": ["main_table"],
            "Base Directory": ["base/main_table"],
            "Top-level Directory": ["top1"],
            "Export": ["exp1"],
            "Timestamp": ["2024-01-01"],
        }
    )
    proc._tables_available = proc._tables_backup.copy()

    monkeypatch.setattr("moodys_datahub.selection._SelectData", FakeSelectData)
    monkeypatch.setattr(
        "moodys_datahub.selection.asyncio.ensure_future",
        lambda coro: asyncio.run(coro),
    )
    monkeypatch.setattr(
        DummyProcess,
        "_check_path",
        lambda self, path, source: (["part.csv"], path),
    )

    proc.select_data()

    assert proc.set_data_product == "Prod"
    assert proc.set_table == "main_table"
    assert proc.remote_path == "base/main_table"
    output = capsys.readouterr().out
    assert "was set as Data Product" in output
    assert "was set as Table" in output


def test_select_data_uses_directory_selector_when_multiple_exports(monkeypatch):
    class FakeSelectData:
        def __init__(self, df, title):
            self.df = df
            self.title = title

        async def display_widgets(self):
            return "Prod", "main_table"

    proc = _make_metadata_process()
    proc._set_data_product = None
    proc._set_table = None
    proc._tables_backup = pd.DataFrame(
        {
            "Data Product": ["Prod", "Prod"],
            "Table": ["main_table", "main_table"],
            "Base Directory": ["base/main_table/v1", "base/main_table/v2"],
            "Top-level Directory": ["top1", "top2"],
            "Export": ["exp1", "exp2"],
            "Timestamp": ["2024-01-01", "2024-01-02"],
        }
    )
    proc._tables_available = proc._tables_backup.copy()
    captured = {}

    monkeypatch.setattr("moodys_datahub.selection._SelectData", FakeSelectData)
    monkeypatch.setattr(
        "moodys_datahub.selection.asyncio.ensure_future",
        lambda coro: asyncio.run(coro),
    )
    monkeypatch.setattr(
        "moodys_datahub.selection._select_list",
        lambda class_type, values, col_name, title, fnc, n_args: captured.update(
            {
                "class_type": class_type,
                "values": values,
                "col_name": col_name,
                "title": title,
            }
        ),
    )

    proc.select_data()

    assert captured["class_type"] == "_SelectList"
    assert captured["values"] == ["top1", "top2"]
    assert proc.set_data_product == "Prod"
    assert proc.set_table == "main_table"


def test_table_overview_reads_local_repo_exports(tmp_path):
    export_dir = tmp_path / "Prod One_exported 2024-02-03_04-05-06"
    export_dir.mkdir()
    (export_dir / "company_data.csv").write_text("value\n1\n", encoding="utf-8")
    nested_table = export_dir / "ownership_data"
    nested_table.mkdir()

    conn = object.__new__(_Connection)
    conn._local_repo = str(tmp_path)
    conn._local_path = None

    df, to_delete = conn._table_overview()

    assert to_delete == []
    assert set(df["Table"]) == {"company_data", "ownership_data"}
    assert set(df["Data Product"]) == {"Prod One"}
    assert set(df["Export"]) == {str(export_dir)}
    assert set(df["Top-level Directory"]) == {export_dir.name}
    assert set(df["Timestamp"]) == {"2024-02-03 04:05:06"}


def test_recursive_collect_recurses_over_nested_sftp_paths(monkeypatch):
    class FakeAttr:
        def __init__(self, filename):
            self.filename = filename

    class FakeSftp:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def listdir_attr(self, path):
            mapping = {
                "root": [FakeAttr("file.csv"), FakeAttr("sub"), FakeAttr("notes.txt")],
                "root/sub": [FakeAttr("nested.parquet")],
            }
            return mapping[path]

        def isdir(self, path):
            return path == "root/sub"

    conn = object.__new__(_Connection)
    monkeypatch.setattr(_Connection, "_connect", lambda self: FakeSftp())

    files = conn._recursive_collect("root")

    assert files == ["root/file.csv", "root/sub/nested.parquet", "root/notes.txt"]


def test_remove_exports_batches_parallel_delete_calls(monkeypatch):
    conn = object.__new__(_Connection)
    calls = []

    def fake_run_parallel(fnc, params_list, n_total, msg):
        calls.append(
            {
                "fnc": fnc.__name__,
                "params_list": params_list,
                "n_total": n_total,
                "msg": msg,
            }
        )
        if msg == "Collecting files to delete":
            return [["a.csv", "b.csv", "c.csv"]]
        return []

    monkeypatch.setattr("moodys_datahub.connection._run_parallel", fake_run_parallel)

    conn._remove_exports(["export_a"], num_workers=2)

    assert calls[0] == {
        "fnc": "_recursive_collect",
        "params_list": ["export_a"],
        "n_total": 1,
        "msg": "Collecting files to delete",
    }
    assert calls[1]["fnc"] == "_delete_files"
    assert calls[1]["msg"] == "Deleting files"
    assert calls[1]["params_list"] == [["a.csv", "b.csv"], ["c.csv"]]
    assert calls[2] == {
        "fnc": "_delete_folders",
        "params_list": ["export_a"],
        "n_total": 1,
        "msg": "Deleting folders",
    }


def test_specify_data_products_updates_unknown_exports(monkeypatch, tmp_path):
    class FakeDropdown:
        def __init__(self, values, col_names, title):
            self.values = values
            self.col_names = col_names
            self.title = title

        async def display_widgets(self):
            return ["Resolved Product"]

    conn = object.__new__(_Connection)
    conn._tables_available = pd.DataFrame(
        {
            "Data Product": ["Multiple_Options: ['Resolved Product', 'Fallback']"],
            "Table": ["table_a"],
            "Top-level Directory": ["dir_a"],
            "Base Directory": ["base/dir_a"],
            "Timestamp": ["2024-01-01 00:00:00"],
            "Export": ["export_a"],
        }
    )
    conn._tables_backup = conn._tables_available.copy()

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr("moodys_datahub.connection._Multi_dropdown", FakeDropdown)
    monkeypatch.setattr(
        "moodys_datahub.connection.asyncio.ensure_future",
        lambda coro: asyncio.run(coro),
    )

    conn._specify_data_products()

    assert conn._tables_available["Data Product"].tolist() == ["Resolved Product"]
    saved_templates = list(tmp_path.glob("*_data_products.csv"))
    assert len(saved_templates) == 1
