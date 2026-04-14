import asyncio

import pandas as pd
import polars as pl
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
    proc._and_bvd_list = []
    proc._or_bvd_list = []
    proc.query = None
    proc.query_args = None
    proc._select_cols = None
    proc.concat_files = True
    proc.output_format = None
    proc.file_size_mb = 100
    proc.delete_files = False
    proc._set_data_product = "Dummy Product"
    proc._set_table = "dummy_table"
    proc._last_process_engine = None
    proc._last_process_reason = None
    proc._download_finished = True
    proc._local_path = None
    proc._local_files = []
    proc._remote_path = "remote/base"
    proc._remote_files = ["sample.csv"]
    proc._max_path_length = 10000
    proc.delete_files = False
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


def test_time_period_sets_single_date_column_and_updates_select_cols(monkeypatch):
    proc = _make_dummy_process()
    proc._select_cols = ["name"]

    monkeypatch.setattr(
        DummyProcess,
        "table_dates",
        lambda self, **kwargs: pd.DataFrame({"Column": ["closing_date"]}),
    )

    proc.time_period = [2020, 2021]

    assert proc.time_period == [2020, 2021, "closing_date", "remove"]
    assert set(proc.select_cols) == {"name", "closing_date"}


def test_time_period_uses_selector_when_multiple_date_columns(monkeypatch):
    proc = _make_dummy_process()
    captured = {}

    monkeypatch.setattr(
        DummyProcess,
        "table_dates",
        lambda self, **kwargs: pd.DataFrame(
            {"Column": ["closing_date", "information_date"]}
        ),
    )
    monkeypatch.setattr(
        "moodys_datahub.process._select_list",
        lambda class_type, values, col_name, title, fnc, n_args: captured.update(
            {
                "class_type": class_type,
                "values": values,
                "col_name": col_name,
                "title": title,
            }
        ),
    )

    proc.time_period = [2020, 2021]

    assert captured["class_type"] == "_SelectList"
    assert captured["values"] == ["closing_date", "information_date"]
    assert proc.time_period[2] is None


def test_time_period_rejects_unknown_date_column(monkeypatch):
    proc = _make_dummy_process()

    monkeypatch.setattr(
        DummyProcess,
        "table_dates",
        lambda self, **kwargs: pd.DataFrame({"Column": ["closing_date"]}),
    )

    with pytest.raises(ValueError, match="bad_date was not found"):
        proc.time_period = [2020, 2021, "bad_date"]


def test_bvd_list_sets_exact_query_and_updates_select_cols(monkeypatch):
    proc = _make_dummy_process()
    proc._select_cols = ["name"]

    monkeypatch.setattr(
        DummyProcess,
        "search_country_codes",
        lambda self, **kwargs: pd.DataFrame({"Code": ["DK", "SE"]}),
    )
    monkeypatch.setattr(
        DummyProcess,
        "search_dictionary",
        lambda self, **kwargs: pd.DataFrame({"Column": ["bvd_id_number"]}),
    )

    proc.bvd_list = ["BVD1", "BVD2"]

    assert set(proc.bvd_list[0]) == {"BVD1", "BVD2"}
    assert proc.bvd_list[1] == "bvd_id_number"
    assert proc.bvd_list[2].startswith("bvd_id_number in [")
    assert "bvd_id_number" in proc.select_cols


def test_bvd_list_uses_prefix_mode_for_country_codes(monkeypatch):
    proc = _make_dummy_process()

    monkeypatch.setattr(
        DummyProcess,
        "search_country_codes",
        lambda self, **kwargs: pd.DataFrame({"Code": ["DK", "SE"]}),
    )
    monkeypatch.setattr(
        DummyProcess,
        "search_dictionary",
        lambda self, **kwargs: pd.DataFrame({"Column": ["bvd_id_number"]}),
    )

    proc.bvd_list = ["DK", "SE"]

    assert set(proc.bvd_list[0]) == {"DK", "SE"}
    assert proc.bvd_list[1] == "bvd_id_number"
    assert "str.startswith('DK'" in proc.bvd_list[2]
    assert "str.startswith('SE'" in proc.bvd_list[2]


def test_bvd_list_uses_selector_when_multiple_bvd_columns_are_available(monkeypatch):
    proc = _make_dummy_process()
    captured = {}

    monkeypatch.setattr(
        DummyProcess,
        "search_country_codes",
        lambda self, **kwargs: pd.DataFrame({"Code": ["DK", "SE"]}),
    )
    monkeypatch.setattr(
        DummyProcess,
        "search_dictionary",
        lambda self, **kwargs: pd.DataFrame(
            {"Column": ["bvd_id_number", "guo_bvd_id_number"]}
        ),
    )
    monkeypatch.setattr(
        "moodys_datahub.process._select_list",
        lambda class_type, values, col_name, title, fnc, n_args: captured.update(
            {
                "class_type": class_type,
                "values": values,
                "col_name": col_name,
                "title": title,
            }
        ),
    )

    proc.bvd_list = ["BVD1"]

    assert captured["class_type"] == "_SelectMultiple"
    assert captured["values"] == ["bvd_id_number", "guo_bvd_id_number"]
    assert proc.bvd_list[1] is None


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


def test_define_options_applies_widget_config(monkeypatch, capsys):
    class FakeOptions:
        def __init__(self, config):
            self.config = config

        async def display_widgets(self):
            return {
                "delete_files": True,
                "concat_files": False,
                "output_format": [".csv", None],
                "file_size_mb": 250,
            }

    proc = _make_dummy_process()

    monkeypatch.setattr("moodys_datahub.process._SelectOptions", FakeOptions)
    monkeypatch.setattr(
        "moodys_datahub.process.asyncio.ensure_future",
        lambda coro: asyncio.run(coro),
    )

    proc.define_options()

    assert proc.delete_files is True
    assert proc.concat_files is False
    assert proc.output_format == [".csv"]
    assert proc.file_size_mb == 250
    assert "The following options were selected" in capsys.readouterr().out


def test_select_columns_uses_widget_selection_and_merges_required_columns(monkeypatch):
    class FakeSelectMultiple:
        def __init__(self, values, col_name, title):
            self.values = values
            self.col_name = col_name
            self.title = title

        async def display_widgets(self):
            return [self.values[1]]

    proc = _make_dummy_process()
    proc._bvd_list = [["BVD1"], "bvd_id_number", "bvd query"]
    proc._time_period = [2020, 2021, "closing_date", "remove"]

    monkeypatch.setattr("moodys_datahub.process._SelectMultiple", FakeSelectMultiple)
    monkeypatch.setattr(
        "moodys_datahub.process.asyncio.ensure_future",
        lambda coro: asyncio.run(coro),
    )
    monkeypatch.setattr(
        DummyProcess,
        "search_dictionary",
        lambda self, **kwargs: pd.DataFrame(
            {
                "Column": ["name", "value"],
                "Definition": ["Company Name", "Value"],
            }
        ),
    )

    proc.select_columns()

    assert set(proc._select_cols) == {"value", "bvd_id_number", "closing_date"}


def test_search_dictionary_prints_query_and_saves(monkeypatch, capsys):
    proc = _make_dummy_process()
    proc._table_dictionary = pd.DataFrame(
        {
            "Data Product": ["Dummy Product"],
            "Table": ["dummy_table"],
            "Column": ["name"],
            "Definition": ["Company Name"],
        }
    )
    saved = {}

    monkeypatch.setattr(
        "moodys_datahub.process._save_to",
        lambda df, name, save_to: saved.update(
            {"rows": len(df), "name": name, "save_to": save_to}
        ),
    )

    result = proc.search_dictionary(
        search_word="name",
        search_cols={
            "Data Product": False,
            "Table": False,
            "Column": True,
            "Definition": False,
        },
        save_to="csv",
    )

    assert result["Column"].tolist() == ["name"]
    assert saved == {"rows": 1, "name": "dict_search", "save_to": "csv"}
    assert "The following query was executed" in capsys.readouterr().out


def test_table_dates_prints_on_unknown_data_product(monkeypatch, capsys):
    proc = _make_dummy_process()
    proc._table_dates = pd.DataFrame(
        {
            "Data Product": ["Dummy Product"],
            "Table": ["dummy_table"],
            "Column": ["closing_date"],
        }
    )

    result = proc.table_dates(data_product="Missing Product")

    assert result.empty
    assert "No such Data Product was found" in capsys.readouterr().out


def test_table_dates_saves_results(monkeypatch):
    proc = _make_dummy_process()
    proc._table_dates = pd.DataFrame(
        {
            "Data Product": ["Dummy Product"],
            "Table": ["dummy_table"],
            "Column": ["closing_date"],
        }
    )
    saved = {}

    monkeypatch.setattr(
        "moodys_datahub.process._save_to",
        lambda df, name, save_to: saved.update(
            {"rows": len(df), "name": name, "save_to": save_to}
        ),
    )

    result = proc.table_dates(save_to="xlsx")

    assert result["Column"].tolist() == ["closing_date"]
    assert saved == {"rows": 1, "name": "date_cols_search", "save_to": "xlsx"}


def test_search_country_codes_reports_no_match_columns(capsys):
    proc = _make_dummy_process()

    result = proc.search_country_codes(
        search_word="ZZZ",
        search_cols={"Country": False, "Code": True},
    )

    assert result.empty
    assert "No such 'search word' was detected across columns" in capsys.readouterr().out


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


def test_bvd_layer_setters_refresh_required_columns():
    proc = _make_dummy_process()
    proc._select_cols = ["row_id"]
    proc._time_period = [None, None, None, "remove"]
    proc._bvd_list = [["B1"], ["base_a", "base_b"], "base_a in ['B1'] | base_b in ['B1']"]

    proc.AND_bvd_list = [
        [["A1"], ["and_a", "and_b"], "exact"],
        [["A2"], ["and_c"], "prefix"],
    ]
    proc.OR_bvd_list = [[["O1"], ["or_a", "or_b"], "exact"]]

    assert len(proc.AND_bvd_list) == 2
    assert len(proc.OR_bvd_list) == 1
    assert set(proc._required_filter_columns()) == {
        "base_a",
        "base_b",
        "and_a",
        "and_b",
        "and_c",
        "or_a",
        "or_b",
    }
    assert set(proc._select_cols) == {
        "row_id",
        "base_a",
        "base_b",
        "and_a",
        "and_b",
        "and_c",
        "or_a",
        "or_b",
    }


def test_process_all_supports_and_or_bvd_layers_in_pandas_and_polars(tmp_path):
    file_path = tmp_path / "bvd_layers.csv"
    pd.DataFrame(
        {
            "row_id": [1, 2, 3, 4],
            "base_a": ["B1", None, None, "B1"],
            "base_b": [None, "B1", None, None],
            "and_a": ["A1", None, None, None],
            "and_b": [None, "A1", None, None],
            "and_c": [None, None, None, None],
            "or_a": [None, None, None, None],
            "or_b": [None, None, "O1", None],
        }
    ).to_csv(file_path, index=False)

    proc = _make_dummy_process()
    proc._bvd_list = [
        ["B1"],
        ["base_a", "base_b"],
        "base_a in ['B1'] | base_b in ['B1']",
    ]
    proc.AND_bvd_list = [[["A1"], ["and_a", "and_b"], "exact"]]
    proc.OR_bvd_list = [[["O1"], ["or_a", "or_b"], "exact"]]

    pandas_df, _ = proc.process_all(files=[str(file_path)], engine="pandas")
    polars_df, _ = proc.process_all(files=[str(file_path)], engine="polars")
    auto_df, _ = proc.process_all(files=[str(file_path)], engine="auto")

    expected_rows = [1, 2, 3]
    assert pandas_df["row_id"].tolist() == expected_rows
    assert polars_df["row_id"].tolist() == expected_rows
    assert auto_df["row_id"].tolist() == expected_rows
    assert proc.last_process_engine == "polars"


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


def test_copy_obj_resets_defaults_and_triggers_select_data(monkeypatch):
    sftp = object.__new__(Sftp)
    sftp.marker = "original"
    called = {"defaults": 0, "select_data": 0}

    def fake_deepcopy(obj):
        clone = object.__new__(Sftp)
        clone.marker = obj.marker
        clone._object_defaults = lambda: called.update(
            {"defaults": called["defaults"] + 1}
        )
        clone.select_data = lambda: called.update(
            {"select_data": called["select_data"] + 1}
        )
        return clone

    monkeypatch.setattr("moodys_datahub.tools.copy.deepcopy", fake_deepcopy)

    clone = Sftp.copy_obj(sftp)

    assert clone is not sftp
    assert clone.marker == "original"
    assert called == {"defaults": 1, "select_data": 1}


def test_get_column_names_returns_none_when_file_lookup_fails(monkeypatch, capsys):
    class FakeSession:
        remote_files = None

        def _check_args(self, files):
            raise ValueError("missing files")

    out = Sftp.get_column_names(FakeSession(), files=["missing.parquet"])

    assert out is None
    assert "missing files" in capsys.readouterr().out


def test_batch_bvd_search_creates_input_templates_when_missing(monkeypatch, tmp_path):
    products_template = tmp_path / "products_template.xlsx"
    bvd_template = tmp_path / "bvd_numbers_template.txt"
    products_template.write_text("template", encoding="utf-8")
    bvd_template.write_text("numbers", encoding="utf-8")

    class FakeResource:
        def __init__(self, path):
            self.path = path

        def __truediv__(self, other):
            if other == "products.xlsx":
                return FakeResource(products_template)
            return FakeResource(bvd_template)

        def open(self, mode):
            return open(self.path, mode)

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr("moodys_datahub.tools.pkg_resources.files", lambda _: FakeResource(tmp_path))

    Sftp.batch_bvd_search(object(), products="missing_products.xlsx", bvd_numbers="missing_ids.txt")

    assert (tmp_path / "products.xlsx").exists()
    assert (tmp_path / "bvd_numbers.txt").exists()


def test_sftp_init_uses_cbs_fallback_credentials(monkeypatch):
    monkeypatch.setattr("moodys_datahub.connection.pysftp.CnOpts", lambda: type("C", (), {"hostkeys": None})())
    monkeypatch.setattr(Sftp, "_object_defaults", lambda self: None)
    monkeypatch.setattr(Sftp, "tables_available", lambda self, product_overview=None: (pd.DataFrame(), []))
    monkeypatch.setattr(Sftp, "_server_clean_up", lambda self, to_delete: None)

    attempts = []

    def fake_connect(self):
        attempts.append(self.username)
        if self.username == "D2vdz8elTWKyuOcC2kMSnw":
            raise ValueError("first credential fails")
        return object()

    monkeypatch.setattr(Sftp, "_connect", fake_connect)

    sftp = Sftp(privatekey="key.pem")

    assert attempts == ["D2vdz8elTWKyuOcC2kMSnw", "aN54UkFxQPCOIEtmr0FmAQ"]
    assert sftp.hostname == "s-f2112b8b980e44f9a.server.transfer.eu-west-1.amazonaws.com"
    assert sftp.username == "aN54UkFxQPCOIEtmr0FmAQ"


def test_get_file_downloads_remote_file_and_applies_timestamp(monkeypatch, tmp_path):
    class FakeStat:
        st_mtime = 123

    class FakeSftp:
        def __init__(self):
            self.downloads = []

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def get(self, remote_file, local_file):
            self.downloads.append((remote_file, local_file))
            pd.DataFrame({"value": [1]}).to_csv(local_file, index=False)

        def stat(self, remote_file):
            return FakeStat()

    fake_sftp = FakeSftp()
    proc = _make_dummy_process()
    proc._local_path = str(tmp_path)
    proc._remote_path = "remote/base"
    touched = {}

    monkeypatch.setattr(DummyProcess, "_connect", lambda self: fake_sftp)
    monkeypatch.setattr(
        "moodys_datahub.process.os.utime",
        lambda path, times: touched.update({"path": path, "times": times}),
    )

    local_file, flag = proc._get_file("sample.csv")

    assert flag is False
    assert local_file == str(tmp_path / "sample.csv")
    assert fake_sftp.downloads == [("remote/base/sample.csv", str(tmp_path / "sample.csv"))]
    assert touched == {"path": str(tmp_path / "sample.csv"), "times": (123, 123)}


def test_curate_file_saves_split_outputs_and_deletes_new_files(monkeypatch, tmp_path):
    proc = _make_dummy_process()
    proc.concat_files = False
    proc.output_format = [".csv"]
    proc.delete_files = True

    local_file = tmp_path / "sample.csv"
    local_file.write_text("value\n1\n", encoding="utf-8")
    destination = tmp_path / "processed"
    saved = {}
    removed = {}

    monkeypatch.setattr(
        "moodys_datahub.process._load_csv_table",
        lambda **kwargs: pd.DataFrame({"value": [1]}),
    )
    monkeypatch.setattr(
        "moodys_datahub.process._save_files_pd",
        lambda df, file_name, output_format: saved.update(
            {"df": df.copy(), "file_name": file_name, "output_format": output_format}
        )
        or "saved.csv",
    )
    monkeypatch.setattr(
        "moodys_datahub.process.os.remove",
        lambda path: removed.update({"path": path}),
    )

    df, file_name = proc._curate_file(
        flag=False,
        destination=str(destination),
        local_file=str(local_file),
        select_cols=["value"],
    )

    assert df is None
    assert file_name == "saved.csv"
    assert saved["df"]["value"].tolist() == [1]
    assert saved["file_name"] == str(destination / "sample")
    assert saved["output_format"] == [".csv"]
    assert removed == {"path": str(local_file)}


def test_process_sequential_collects_dataframes_filenames_and_flags(monkeypatch):
    proc = _make_dummy_process()

    def fake_get_file(self, file):
        if file == "broken.csv":
            raise ValueError("cannot read")
        return f"/tmp/{file}", file == "existing.csv"

    def fake_curate_file(self, **kwargs):
        local_file = kwargs["local_file"]
        if local_file.endswith("new.csv"):
            return pd.DataFrame({"value": [1]}), None
        return None, "saved.csv"

    monkeypatch.setattr(DummyProcess, "_get_file", fake_get_file)
    monkeypatch.setattr(DummyProcess, "_curate_file", fake_curate_file)

    dfs, file_names, flags = proc._process_sequential(
        ["new.csv", "existing.csv", "broken.csv"]
    )

    assert len(dfs) == 1
    assert dfs[0]["value"].tolist() == [1]
    assert file_names == ["saved.csv"]
    assert flags == [False, True]


def test_process_parallel_curates_single_input(monkeypatch):
    proc = _make_dummy_process()

    monkeypatch.setattr(
        DummyProcess, "_get_file", lambda self, file: ("/tmp/sample.csv", True)
    )
    monkeypatch.setattr(
        DummyProcess,
        "_curate_file",
        lambda self, **kwargs: (None, "saved.csv"),
    )

    result = proc._process_parallel(
        ["sample.csv", "dest", ["value"], [None, None, None, "remove"], None, None, None]
    )

    assert result == [None, "saved.csv", True]


def test_process_polars_downloads_and_loads_existing_local_files(monkeypatch, tmp_path):
    proc = _make_dummy_process()
    first = tmp_path / "first.csv"
    second = tmp_path / "second.csv"
    first.write_text("value\n1\n", encoding="utf-8")
    second.write_text("value\n2\n", encoding="utf-8")
    calls = {}

    monkeypatch.setattr(
        DummyProcess,
        "download_all",
        lambda self, **kwargs: calls.update({"download": kwargs}),
    )
    monkeypatch.setattr(
        DummyProcess,
        "_file_exist",
        lambda self, file: (
            str(first) if file == "first.csv" else str(second),
            True,
        ),
    )
    monkeypatch.setattr(
        "moodys_datahub.process._load_pl",
        lambda **kwargs: calls.update({"load": kwargs}) or pl.DataFrame({"value": [1, 2]}),
    )

    result = proc._process_polars(
        files=["first.csv", "second.csv"],
        select_cols=["value"],
        row_limit=1,
    )

    assert result["value"].to_list() == [1, 2]
    assert calls["download"]["async_mode"] is False
    assert calls["load"]["file_list"] == [str(first), str(second)]
    assert calls["load"]["select_cols"] == ["value"]
    assert calls["load"]["row_limit"] == 1


def test_pandas_all_rejects_polars_expression_query():
    proc = _make_dummy_process()

    with pytest.raises(ValueError, match="Polars expressions are not supported"):
        proc.pandas_all(query=pl.col("value") > 1)


def test_pandas_all_raises_timeout_when_downloads_are_incomplete(monkeypatch):
    proc = _make_dummy_process()

    monkeypatch.setattr(DummyProcess, "_check_download", lambda self, files: False)

    with pytest.raises(TimeoutError, match="Files have not finished downloading"):
        proc.pandas_all(files=["sample.csv"])


def test_pandas_all_parallel_batches_and_saves(monkeypatch):
    proc = _make_dummy_process()
    proc.concat_files = True
    proc.output_format = [".csv"]
    run_calls = []

    monkeypatch.setattr(DummyProcess, "_check_download", lambda self, files: True)
    monkeypatch.setattr(
        DummyProcess,
        "_validate_args",
        lambda self, **kwargs: (kwargs["select_cols"], kwargs["files"], kwargs["destination"]),
    )
    monkeypatch.setattr("moodys_datahub.process.set_workers", lambda num_workers, default: 2)

    def fake_run_parallel(**kwargs):
        run_calls.append(kwargs)
        first_file = kwargs["params_list"][0][0]
        if first_file == "a.csv":
            return [
                [pd.DataFrame({"value": [1]}), ["saved_a.csv"], False],
                [pd.DataFrame({"value": [2]}), ["saved_b.csv"], True],
            ]
        return [[pd.DataFrame({"value": [3]}), None, False]]

    monkeypatch.setattr("moodys_datahub.process._run_parallel", fake_run_parallel)
    monkeypatch.setattr(
        "moodys_datahub.process._save_chunks",
        lambda **kwargs: (pd.DataFrame({"value": [1, 2, 3]}), ["joined.csv"]),
    )

    df, file_names = proc.pandas_all(files=["a.csv", "b.csv", "c.csv"], num_workers=2)

    assert len(run_calls) == 2
    assert df["value"].tolist() == [1, 2, 3]
    assert file_names == ["joined.csv"]
    assert proc.last_process_engine == "pandas"
    assert proc.last_process_reason == "direct"


def test_pandas_all_sequential_concat_false_concatenates_frames(monkeypatch):
    proc = _make_dummy_process()
    proc.concat_files = False

    monkeypatch.setattr(DummyProcess, "_check_download", lambda self, files: True)
    monkeypatch.setattr(
        DummyProcess,
        "_validate_args",
        lambda self, **kwargs: (kwargs["select_cols"], kwargs["files"], kwargs["destination"]),
    )
    monkeypatch.setattr("moodys_datahub.process.set_workers", lambda num_workers, default: 1)
    monkeypatch.setattr(
        DummyProcess,
        "_process_sequential",
        lambda self, *args: (
            [pd.DataFrame({"value": [1]}), pd.DataFrame({"value": [2]})],
            ["saved.csv"],
            [False, False],
        ),
    )

    df, file_names = proc.pandas_all(files=["a.csv"], num_workers=1)

    assert df["value"].tolist() == [1, 2]
    assert file_names == ["saved.csv"]


def test_polars_all_saves_results_and_restores_concat_files(monkeypatch):
    proc = _make_dummy_process()
    proc.concat_files = False
    proc.output_format = [".parquet"]
    proc.file_size_mb = 200

    monkeypatch.setattr(DummyProcess, "_check_download", lambda self, files: True)
    monkeypatch.setattr(
        DummyProcess,
        "_validate_args",
        lambda self, **kwargs: (kwargs["select_cols"], kwargs["files"], kwargs["destination"]),
    )
    monkeypatch.setattr(
        DummyProcess,
        "_process_polars",
        lambda self, *args, **kwargs: pl.DataFrame({"value": [1, 2]}),
    )
    monkeypatch.setattr("moodys_datahub.process.set_workers", lambda num_workers, default: 2)
    monkeypatch.setattr(
        "moodys_datahub.process._save_chunks",
        lambda **kwargs: (kwargs["dfs"], ["polars.parquet"]),
    )

    df, file_names = proc.polars_all(files=["sample.csv"], num_workers=2)

    assert isinstance(df, pl.DataFrame)
    assert df["value"].to_list() == [1, 2]
    assert file_names == ["polars.parquet"]
    assert proc.concat_files is False
    assert proc.last_process_engine == "polars"
    assert proc.last_process_reason == "direct"


def test_polars_all_restores_concat_files_on_timeout(monkeypatch):
    proc = _make_dummy_process()
    proc.concat_files = False

    monkeypatch.setattr(DummyProcess, "_check_download", lambda self, files: False)

    with pytest.raises(TimeoutError, match="Files have not finished downloading"):
        proc.polars_all(files=["sample.csv"])

    assert proc.concat_files is False


def test_normalize_bvd_queries_accepts_string_query():
    proc = _make_dummy_process()

    pandas_query, polars_query = proc._normalize_bvd_queries("bvd_id_number in ['A1']")

    assert pandas_query == "bvd_id_number in ['A1']"
    assert polars_query is None


def test_normalize_bvd_queries_normalizes_series_inputs():
    proc = _make_dummy_process()

    pandas_query, polars_query = proc._normalize_bvd_queries(
        [
            pd.Series(["A1", "B2"]),
            pd.Series(["primary_bvd"]),
            "Exact",
        ]
    )

    assert pandas_query == "primary_bvd in ['A1', 'B2']"
    assert polars_query == [["A1", "B2"], "primary_bvd", "exact"]


def test_normalize_bvd_queries_rejects_malformed_length():
    proc = _make_dummy_process()

    with pytest.raises(ValueError, match="bvd_query must be \\[values, columns\\]"):
        proc._normalize_bvd_queries([["A1"]])


def test_choose_process_engine_routes_string_bvd_query_to_pandas():
    proc = _make_dummy_process()

    engine, reason = proc._choose_process_engine(
        files=["one.csv"],
        raw_bvd_query="bvd_id_number in ['A1']",
        polars_bvd_query=None,
    )

    assert engine == "pandas"
    assert reason == "string_bvd_query"


def test_choose_process_engine_routes_unsupported_format_to_pandas():
    proc = _make_dummy_process()

    engine, reason = proc._choose_process_engine(files=["one.orc"])

    assert engine == "pandas"
    assert reason == "unsupported_format:orc"


def test_get_file_wraps_remote_read_errors(tmp_path, monkeypatch):
    proc = _make_dummy_process()
    proc.remote_path = "remote/base"
    local_file = tmp_path / "sample.csv"

    class FailingSftp:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def get(self, remote_file, local_target):
            raise RuntimeError("boom")

    monkeypatch.setattr(DummyProcess, "_file_exist", lambda self, file: (str(local_file), False))
    monkeypatch.setattr(DummyProcess, "_connect", lambda self: FailingSftp())

    with pytest.raises(ValueError, match="Error reading remote file: boom"):
        proc._get_file("sample.csv")


def test_check_args_generates_destination_when_flag_is_true(tmp_path, monkeypatch):
    proc = _make_dummy_process()
    existing_file = tmp_path / "sample.csv"
    existing_file.write_text("value\n1\n", encoding="utf-8")
    proc._local_path = str(tmp_path / "Dummy Product" / "dummy_table")

    monkeypatch.setattr("moodys_datahub.process.datetime", type("FixedDatetime", (), {
        "now": staticmethod(lambda: pd.Timestamp("2026-03-24 12:34:00").to_pydatetime())
    }))

    files, destination = proc._check_args([str(existing_file)], destination=None, flag=True)

    assert files == [str(existing_file)]
    assert destination.endswith("2603241234_base")


def test_check_args_creates_parent_directory_for_concat_output(tmp_path):
    proc = _make_dummy_process()
    proc.concat_files = True
    existing_file = tmp_path / "sample.csv"
    existing_file.write_text("value\n1\n", encoding="utf-8")
    destination = tmp_path / "nested" / "joined.csv"

    files, out_destination = proc._check_args([str(existing_file)], str(destination))

    assert files == [str(existing_file)]
    assert out_destination == str(destination)
    assert destination.parent.exists()


def test_batch_bvd_search_skips_existing_output_files(tmp_path, monkeypatch):
    class FakeRunner:
        def __init__(self):
            self._set_table = None
            self.process_calls = []

        def _object_defaults(self):
            return None

        def __setattr__(self, name, value):
            if name == "set_data_product":
                object.__setattr__(self, "_set_data_product", value)
            elif name == "set_table":
                object.__setattr__(self, "_set_table", value)
            else:
                object.__setattr__(self, name, value)

        def get_column_names(self):
            return ["bvd_id_number"]

        def process_all(self, **kwargs):
            self.process_calls.append(kwargs)

    parent = object.__new__(Sftp)
    runner = FakeRunner()
    products = tmp_path / "products.xlsx"
    bvd_numbers = tmp_path / "bvd_numbers.txt"
    pd.DataFrame(
        {
            "Data Product": ["Prod"],
            "Table": ["table_a"],
            "Column": ["bvd_id_number"],
            "Run": [True],
        }
    ).to_excel(products, index=False)
    bvd_numbers.write_text("BVD1\n", encoding="utf-8")
    (tmp_path / "1_Prod_table_a.csv").write_text("done\n", encoding="utf-8")

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr("moodys_datahub.tools.copy.deepcopy", lambda obj: runner)

    parent.batch_bvd_search(str(products), str(bvd_numbers))

    assert runner.process_calls == []


def test_batch_bvd_search_skips_rows_with_missing_columns(tmp_path, monkeypatch):
    class FakeRunner:
        def __init__(self):
            self._set_table = None
            self.process_calls = []

        def _object_defaults(self):
            return None

        def __setattr__(self, name, value):
            if name == "set_data_product":
                object.__setattr__(self, "_set_data_product", value)
            elif name == "set_table":
                object.__setattr__(self, "_set_table", value)
            else:
                object.__setattr__(self, name, value)

        def get_column_names(self):
            return ["name"]

        def process_all(self, **kwargs):
            self.process_calls.append(kwargs)

    parent = object.__new__(Sftp)
    runner = FakeRunner()
    products = tmp_path / "products.xlsx"
    bvd_numbers = tmp_path / "bvd_numbers.txt"
    pd.DataFrame(
        {
            "Data Product": ["Prod"],
            "Table": ["table_a"],
            "Column": ["bvd_id_number"],
            "Run": [True],
        }
    ).to_excel(products, index=False)
    bvd_numbers.write_text("BVD1\n", encoding="utf-8")

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr("moodys_datahub.tools.copy.deepcopy", lambda obj: runner)

    parent.batch_bvd_search(str(products), str(bvd_numbers))

    assert runner.process_calls == []
