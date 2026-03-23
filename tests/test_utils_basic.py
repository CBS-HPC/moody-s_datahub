import pandas as pd
import polars as pl
import pytest

from moodys_datahub.extra import national_identifer
from moodys_datahub.process import _Process, set_workers
from moodys_datahub.tools import Sftp
from moodys_datahub.utils import (
    _bvd_changes_ray,
    _check_list_format,
    _construct_query,
    _date_pd,
    _fuzzy_match,
    _letters_only_regex,
    _load_pl,
    fuzzy_match_pl,
    fuzzy_query,
)


class DummyProcess(_Process):
    pass


def _make_dummy_process():
    proc = object.__new__(DummyProcess)
    proc.remote_files = ["sample.csv"]
    proc._local_path = None
    proc._local_files = []
    proc._remote_path = None
    proc._remote_files = ["sample.csv"]
    proc._local_repo = None
    proc._time_stamp = None
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
    return proc


def test_letters_only_regex():
    assert _letters_only_regex("Abc-123!") == "abc123"
    assert _letters_only_regex(None) is None


def test_fuzzy_match_handles_exact_fuzzy_and_no_match():
    df = pd.DataFrame(
        {
            "name": ["acme", "beta"],
            "bvd_id": ["BVD1", "BVD2"],
        }
    )
    choices = df["name"].tolist()
    choice_to_index = {choice: idx for idx, choice in enumerate(choices)}

    results = _fuzzy_match(
        (
            ["acme", "betta", "unknown"],
            choices,
            80,
            df,
            "name",
            "bvd_id",
            choice_to_index,
        )
    )

    assert results == [
        ("acme", "acme", 100, "acme", "BVD1"),
        ("betta", "beta", 88.88888888888889, "beta", "BVD2"),
        ("unknown", None, 0, None, None),
    ]


def test_fuzzy_query_remove_str_creates_exact_match_without_pool():
    df = pd.DataFrame(
        {
            "company_name": ["Acme A/S", "Beta ApS"],
            "bvd_id": ["BVD1", "BVD2"],
        }
    )

    out = fuzzy_query(
        df=df,
        names=["Acme"],
        match_column="company_name",
        return_column="bvd_id",
        remove_str=["A/S"],
        num_workers=1,
    )

    assert out.to_dict("records") == [
        {
            "Search_string": "acme",
            "BestMatch": "acme",
            "Score": 100,
            "company_name": "Acme A/S",
            "bvd_id": "BVD1",
        }
    ]


def test_fuzzy_query_clamps_auto_workers_and_uses_pool(monkeypatch):
    df = pd.DataFrame(
        {
            "company_name": ["Acme", "Beta"],
            "bvd_id": ["BVD1", "BVD2"],
        }
    )
    pool_calls = {}

    class FakePool:
        def __init__(self, processes):
            pool_calls["processes"] = processes

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def map(self, func, args_list):
            pool_calls["batch_sizes"] = [len(args[0]) for args in args_list]
            return [func(args) for args in args_list]

    monkeypatch.setattr("moodys_datahub.utils.cpu_count", lambda: 6)
    monkeypatch.setattr("moodys_datahub.utils.Pool", FakePool)

    out = fuzzy_query(
        df=df,
        names=["Acme", "Beta"],
        match_column="company_name",
        return_column="bvd_id",
    )

    assert pool_calls == {"processes": 2, "batch_sizes": [1, 1]}
    assert out["bvd_id"].tolist() == ["BVD1", "BVD2"]


def test_construct_query_exact():
    query = _construct_query("bvd_id", ["A1", "B2"], search_type=False)
    assert query == "bvd_id in ['A1', 'B2']"


def test_construct_query_startswith():
    query = _construct_query(["bvd_id"], ["US", "DK"], search_type=True)
    assert query == (
        "bvd_id.str.startswith('US', na=False) | bvd_id.str.startswith('DK', na=False)"
    )


def test_check_list_format_merges_args():
    result = _check_list_format("a", "b", ["c"])
    assert set(result) == {"a", "b", "c"}


def test_date_pd_filters_years():
    df = pd.DataFrame(
        {
            "closing_date": ["2019-01-01", "2020-06-01", "2021-01-01"],
            "value": [1, 2, 3],
        }
    )
    out = _date_pd(df, date_col="closing_date", start_year=2020, end_year=2020)
    assert out["value"].tolist() == [2]


def test_load_pl_filters_single_bvd_column(tmp_path):
    file_path = tmp_path / "sample.csv"
    pd.DataFrame(
        {
            "bvd_id": ["A1", "B2", "C3"],
            "value": [1, 2, 3],
        }
    ).to_csv(file_path, index=False)

    out = _load_pl(
        file_list=[str(file_path)],
        bvd_query=[["A1", "C3"], "bvd_id"],
    )

    assert out["bvd_id"].to_list() == ["A1", "C3"]
    assert out["value"].to_list() == [1, 3]


def test_load_pl_filters_multiple_bvd_columns_without_duplicates(tmp_path):
    file_path = tmp_path / "sample_multi.csv"
    pd.DataFrame(
        {
            "primary_bvd": ["A1", "B2", "C3"],
            "secondary_bvd": ["X9", "A1", "C3"],
            "value": [1, 2, 3],
        }
    ).to_csv(file_path, index=False)

    out = _load_pl(
        file_list=[str(file_path)],
        bvd_query=[["A1", "C3"], ["primary_bvd", "secondary_bvd"]],
    )

    assert out["value"].to_list() == [1, 2, 3]


def test_load_pl_filters_prefix_across_multiple_columns(tmp_path):
    file_path = tmp_path / "sample_prefix.csv"
    pd.DataFrame(
        {
            "primary_bvd": ["DK100", "SE200", "NO300", "FI400"],
            "secondary_bvd": ["X1", "Y2", "DK999", "SE777"],
            "value": [1, 2, 3, 4],
        }
    ).to_csv(file_path, index=False)

    out = _load_pl(
        file_list=[str(file_path)],
        bvd_query=[["DK", "SE"], ["primary_bvd", "secondary_bvd"], "prefix"],
    )

    assert out["value"].to_list() == [1, 2, 3, 4]


def test_load_pl_filters_dates_and_preserves_selected_output_columns(tmp_path):
    file_path = tmp_path / "sample_dates.csv"
    pd.DataFrame(
        {
            "bvd_id": ["A1", "B2", "C3"],
            "closing_date": ["2019-01-01", "2020-06-01", None],
            "value": [1, 2, 3],
        }
    ).to_csv(file_path, index=False)

    out = _load_pl(
        file_list=[str(file_path)],
        select_cols=["value"],
        date_query=[2020, 2020, "closing_date", "keep"],
        bvd_query=[["B2", "C3"], "bvd_id"],
    )

    assert out.columns == ["value"]
    assert out["value"].to_list() == [2, 3]


def test_load_pl_rejects_string_query(tmp_path):
    file_path = tmp_path / "sample_query.csv"
    pd.DataFrame({"value": [1, 2, 3]}).to_csv(file_path, index=False)

    with pytest.raises(ValueError, match="String queries are not supported"):
        _load_pl(
            file_list=[str(file_path)],
            query="value > 1",
        )


def test_load_pl_accepts_polars_expression_query(tmp_path):
    file_path = tmp_path / "sample_expr.csv"
    pd.DataFrame({"value": [1, 2, 3]}).to_csv(file_path, index=False)

    out = _load_pl(
        file_list=[str(file_path)],
        query=pl.col("value") > 1,
    )

    assert out["value"].to_list() == [2, 3]


def test_process_all_auto_prefers_polars_and_returns_pandas(monkeypatch):
    proc = _make_dummy_process()
    calls = {"pandas": 0, "polars": 0}

    def fake_pandas_all(*args, **kwargs):
        calls["pandas"] += 1
        return pd.DataFrame({"value": [10]}), ["pandas.csv"]

    def fake_polars_all(*args, **kwargs):
        calls["polars"] += 1
        return pl.DataFrame({"value": [10]}), ["polars.csv"]

    monkeypatch.setattr(DummyProcess, "pandas_all", fake_pandas_all)
    monkeypatch.setattr(DummyProcess, "polars_all", fake_polars_all)

    df, file_names = proc.process_all(engine="auto")

    assert calls == {"pandas": 0, "polars": 1}
    assert isinstance(df, pd.DataFrame)
    assert df["value"].tolist() == [10]
    assert file_names == ["polars.csv"]
    assert proc.dfs.equals(df)
    assert proc.last_process_engine == "polars"
    assert proc.last_process_reason == "compatible"


def test_process_all_auto_routes_string_query_to_pandas(monkeypatch):
    proc = _make_dummy_process()
    calls = {"pandas": 0, "polars": 0}

    def fake_pandas_all(*args, **kwargs):
        calls["pandas"] += 1
        df = pd.DataFrame({"value": [1, 2]})
        proc.dfs = df
        return df, ["pandas.csv"]

    def fake_polars_all(*args, **kwargs):
        calls["polars"] += 1
        return pl.DataFrame({"value": [1, 2]}), ["polars.csv"]

    monkeypatch.setattr(DummyProcess, "pandas_all", fake_pandas_all)
    monkeypatch.setattr(DummyProcess, "polars_all", fake_polars_all)

    df, file_names = proc.process_all(engine="auto", query="value > 1")

    assert calls == {"pandas": 1, "polars": 0}
    assert isinstance(df, pd.DataFrame)
    assert df["value"].tolist() == [1, 2]
    assert file_names == ["pandas.csv"]
    assert proc.dfs.equals(df)
    assert proc.last_process_engine == "pandas"
    assert proc.last_process_reason == "string_query"


def test_process_all_explicit_polars_still_returns_pandas(monkeypatch):
    proc = _make_dummy_process()

    def fake_polars_all(*args, **kwargs):
        return pl.DataFrame({"value": [7]}), ["polars.csv"]

    monkeypatch.setattr(DummyProcess, "polars_all", fake_polars_all)

    df, file_names = proc.process_all(engine="polars")

    assert isinstance(df, pd.DataFrame)
    assert df["value"].tolist() == [7]
    assert file_names == ["polars.csv"]
    assert proc.dfs.equals(df)
    assert proc.last_process_engine == "polars"
    assert proc.last_process_reason == "explicit"


def test_process_all_explicit_pandas_sets_explicit_reason(monkeypatch):
    proc = _make_dummy_process()

    def fake_pandas_all(*args, **kwargs):
        return pd.DataFrame({"value": [3]}), ["pandas.csv"]

    monkeypatch.setattr(DummyProcess, "pandas_all", fake_pandas_all)

    df, file_names = proc.process_all(engine="pandas")

    assert isinstance(df, pd.DataFrame)
    assert file_names == ["pandas.csv"]
    assert proc.last_process_engine == "pandas"
    assert proc.last_process_reason == "explicit"


def test_process_all_auto_routes_unmarked_callable_to_pandas(monkeypatch):
    proc = _make_dummy_process()
    calls = {"pandas": 0, "polars": 0}

    def fake_pandas_all(*args, **kwargs):
        calls["pandas"] += 1
        return pd.DataFrame({"value": [5]}), ["pandas.csv"]

    def fake_polars_all(*args, **kwargs):
        calls["polars"] += 1
        return pl.DataFrame({"value": [5]}), ["polars.csv"]

    monkeypatch.setattr(DummyProcess, "pandas_all", fake_pandas_all)
    monkeypatch.setattr(DummyProcess, "polars_all", fake_polars_all)

    df, file_names = proc.process_all(engine="auto", query=lambda df: df)

    assert calls == {"pandas": 1, "polars": 0}
    assert isinstance(df, pd.DataFrame)
    assert file_names == ["pandas.csv"]
    assert proc.last_process_engine == "pandas"
    assert proc.last_process_reason == "callable_query"


def test_process_all_auto_routes_concat_files_false_to_pandas(monkeypatch):
    proc = _make_dummy_process()
    proc.concat_files = False
    calls = {"pandas": 0, "polars": 0}

    def fake_pandas_all(*args, **kwargs):
        calls["pandas"] += 1
        return pd.DataFrame({"value": [9]}), ["pandas.csv"]

    def fake_polars_all(*args, **kwargs):
        calls["polars"] += 1
        return pl.DataFrame({"value": [9]}), ["polars.csv"]

    monkeypatch.setattr(DummyProcess, "pandas_all", fake_pandas_all)
    monkeypatch.setattr(DummyProcess, "polars_all", fake_polars_all)

    df, file_names = proc.process_all(engine="auto")

    assert calls == {"pandas": 1, "polars": 0}
    assert isinstance(df, pd.DataFrame)
    assert file_names == ["pandas.csv"]
    assert proc.last_process_reason == "concat_files_false"


def test_process_all_auto_routes_pool_method_to_pandas(monkeypatch):
    proc = _make_dummy_process()
    calls = {"pandas": 0, "polars": 0}

    def fake_pandas_all(*args, **kwargs):
        calls["pandas"] += 1
        return pd.DataFrame({"value": [11]}), ["pandas.csv"]

    def fake_polars_all(*args, **kwargs):
        calls["polars"] += 1
        return pl.DataFrame({"value": [11]}), ["polars.csv"]

    monkeypatch.setattr(DummyProcess, "pandas_all", fake_pandas_all)
    monkeypatch.setattr(DummyProcess, "polars_all", fake_polars_all)

    df, file_names = proc.process_all(engine="auto", pool_method="spawn")

    assert calls == {"pandas": 1, "polars": 0}
    assert isinstance(df, pd.DataFrame)
    assert file_names == ["pandas.csv"]
    assert proc.last_process_reason == "pool_method"


def test_process_all_auto_routes_n_batches_to_pandas(monkeypatch):
    proc = _make_dummy_process()
    calls = {"pandas": 0, "polars": 0}

    def fake_pandas_all(*args, **kwargs):
        calls["pandas"] += 1
        return pd.DataFrame({"value": [12]}), ["pandas.csv"]

    def fake_polars_all(*args, **kwargs):
        calls["polars"] += 1
        return pl.DataFrame({"value": [12]}), ["polars.csv"]

    monkeypatch.setattr(DummyProcess, "pandas_all", fake_pandas_all)
    monkeypatch.setattr(DummyProcess, "polars_all", fake_polars_all)

    df, file_names = proc.process_all(engine="auto", n_batches=2)

    assert calls == {"pandas": 1, "polars": 0}
    assert isinstance(df, pd.DataFrame)
    assert file_names == ["pandas.csv"]
    assert proc.last_process_reason == "n_batches"


def test_process_all_auto_rejects_polars_expr_when_only_pandas_can_run():
    proc = _make_dummy_process()
    proc.concat_files = False

    with pytest.raises(ValueError, match="cannot fall back to pandas"):
        proc.process_all(engine="auto", query=pl.col("value") > 1)


def test_process_all_explicit_polars_rejects_pandas_only_workloads():
    proc = _make_dummy_process()

    with pytest.raises(
        ValueError, match="cannot use the Polars backend: string queries require"
    ):
        proc.process_all(engine="polars", query="value > 1")


def test_default_polars_bvd_query_detects_prefix_mode():
    proc = _make_dummy_process()
    proc._bvd_list = [
        ["DK", "SE"],
        "bvd_id_number",
        "bvd_id_number.str.startswith('DK', na=False)",
    ]

    assert proc._default_polars_bvd_query() == [
        ["DK", "SE"],
        "bvd_id_number",
        "prefix",
    ]


def test_normalize_bvd_queries_builds_exact_queries():
    proc = _make_dummy_process()

    pandas_query, polars_query = proc._normalize_bvd_queries(
        [["A1", "B2"], ["primary_bvd", "secondary_bvd"], "exact"]
    )

    assert pandas_query == "primary_bvd in ['A1', 'B2'] | secondary_bvd in ['A1', 'B2']"
    assert polars_query == [["A1", "B2"], ["primary_bvd", "secondary_bvd"], "exact"]


def test_normalize_bvd_queries_rejects_invalid_mode():
    proc = _make_dummy_process()

    with pytest.raises(ValueError, match="BvD filter mode must be 'exact' or 'prefix'"):
        proc._normalize_bvd_queries([["A1"], "bvd_id_number", "invalid"])


def test_describe_polars_limitation_formats_unsupported_format():
    proc = _make_dummy_process()

    assert (
        proc._describe_polars_limitation("unsupported_format:orc")
        == "unsupported file format 'orc'"
    )


def test_choose_process_engine_routes_mixed_formats_to_pandas():
    proc = _make_dummy_process()

    engine, reason = proc._choose_process_engine(
        files=["one.csv", "two.parquet"],
        query=None,
    )

    assert engine == "pandas"
    assert reason == "mixed_formats"


def test_choose_process_engine_routes_multi_file_xlsx_to_pandas():
    proc = _make_dummy_process()

    engine, reason = proc._choose_process_engine(
        files=["one.xlsx", "two.xlsx"],
        query=None,
    )

    assert engine == "pandas"
    assert reason == "multi_file_xlsx"


def test_validate_args_passes_generation_flag_to_check_args(monkeypatch):
    proc = _make_dummy_process()
    proc.output_format = [".csv"]
    proc._bvd_list = [None, "bvd_id_number", None]
    proc._time_period = [None, None, None, "remove"]
    captured = {}

    def fake_check_args(files, destination, flag):
        captured["files"] = files
        captured["destination"] = destination
        captured["flag"] = flag
        return files, "generated/output"

    monkeypatch.setattr(proc, "_check_args", fake_check_args)

    select_cols, files, destination = proc._validate_args(
        files=["sample.csv"],
        select_cols=["value"],
    )

    assert set(select_cols) == {"value", "bvd_id_number"}
    assert files == ["sample.csv"]
    assert destination == "generated/output"
    assert captured["flag"] == [".csv"]


def test_set_workers_casts_values_and_falls_back_for_one():
    assert set_workers(4.0, 2) == 4
    assert set_workers(1, 3) == 3


def test_check_args_rejects_empty_file_list():
    proc = _make_dummy_process()
    proc.local_files = []
    proc.remote_files = []

    with pytest.raises(ValueError, match="'files' is a empty list"):
        proc._check_args([])


def test_check_args_requires_table_before_deriving_local_path():
    proc = _make_dummy_process()
    proc.local_files = []
    proc.remote_files = ["remote.csv"]
    proc._local_path = None
    proc._remote_path = "remote/base"
    proc._set_table = None

    with pytest.raises(ValueError, match="Table is not set"):
        proc._check_args(["remote.csv"])


def test_check_args_creates_destination_directory_for_split_outputs(tmp_path):
    proc = _make_dummy_process()
    proc.concat_files = False
    existing_file = tmp_path / "sample.csv"
    existing_file.write_text("value\n1\n", encoding="utf-8")

    destination = tmp_path / "output_dir"
    files, out_destination = proc._check_args([str(existing_file)], str(destination))

    assert files == [str(existing_file)]
    assert out_destination == str(destination)
    assert destination.exists()


def test_check_download_marks_finished_when_files_are_ready(monkeypatch):
    proc = _make_dummy_process()
    proc._download_finished = False
    proc._remote_files = ["sample.csv"]
    monkeypatch.setattr(DummyProcess, "local_files", property(lambda self: ["sample.csv"]))

    assert proc._check_download(["sample.csv"]) is True
    assert proc.download_finished is True


def test_check_download_times_out_when_local_files_never_appear(monkeypatch, capsys):
    proc = _make_dummy_process()
    proc._download_finished = False
    proc._remote_files = ["sample.csv"]
    monkeypatch.setattr(DummyProcess, "local_files", property(lambda self: []))

    timeline = iter([0.0, 5.1])
    monkeypatch.setattr("moodys_datahub.process.time.sleep", lambda _: None)
    monkeypatch.setattr("moodys_datahub.process.time.time", lambda: next(timeline))

    assert proc._check_download(["sample.csv"]) is False
    assert "timeout period" in capsys.readouterr().out


def test_bvd_changes_ray_resolves_terminal_newest_id_across_chain():
    df = pd.DataFrame(
        {
            "old_id": ["A", "B"],
            "new_id": ["B", "C"],
            "change_date": ["2020-01-01", "2021-01-01"],
        }
    )

    new_ids, newest_ids, filtered_df = _bvd_changes_ray(["A"], df)

    assert new_ids == {"A", "B", "C"}
    assert newest_ids == {"A": "C", "B": "C", "C": "C"}
    assert filtered_df["newest_id"].tolist() == ["C", "C"]


def test_bvd_changes_ray_finds_reverse_links_and_keeps_terminal_mapping():
    df = pd.DataFrame(
        {
            "old_id": ["A", "B"],
            "new_id": ["B", "C"],
            "change_date": ["2020-01-01", "2021-01-01"],
        }
    )

    new_ids, newest_ids, filtered_df = _bvd_changes_ray(["C"], df)

    assert new_ids == {"A", "B", "C"}
    assert newest_ids == {"A": "C", "B": "C", "C": "C"}
    assert filtered_df["newest_id"].tolist() == ["C", "C"]


def test_bvd_changes_ray_prefers_latest_change_date_when_old_id_branches():
    df = pd.DataFrame(
        {
            "old_id": ["A", "A"],
            "new_id": ["B", "C"],
            "change_date": ["2020-01-01", "2021-01-01"],
        }
    )

    new_ids, newest_ids, filtered_df = _bvd_changes_ray(["A"], df)

    assert new_ids == {"A", "B", "C"}
    assert newest_ids["A"] == "C"
    assert filtered_df["newest_id"].tolist() == ["C", "C"]


def test_bvd_changes_ray_returns_empty_filtered_df_when_no_changes_found():
    df = pd.DataFrame(
        {
            "old_id": ["A"],
            "new_id": ["B"],
            "change_date": ["2020-01-01"],
        }
    )

    new_ids, newest_ids, filtered_df = _bvd_changes_ray(["Z"], df)

    assert new_ids == {"Z"}
    assert newest_ids == {"Z": "Z"}
    assert filtered_df.empty


def test_bvd_changes_ray_maps_newest_id_from_new_id_when_old_id_is_missing():
    df = pd.DataFrame(
        {
            "old_id": [pd.NA],
            "new_id": ["C"],
            "change_date": ["2020-01-01"],
        }
    )

    new_ids, newest_ids, filtered_df = _bvd_changes_ray(["C"], df)

    assert new_ids == {"C"}
    assert newest_ids == {"C": "C"}
    assert filtered_df["newest_id"].tolist() == ["C"]


def test_fuzzy_match_pl_normalizes_suffixes_for_exact_matches():
    df = pl.DataFrame(
        {
            "name": ["Acme Ltd", "Other Corp"],
            "bvd_id_number": ["BVD1", "BVD2"],
        }
    )

    result = fuzzy_match_pl(
        names=["ACME LTD"],
        df=df,
        match_column="name",
        return_column="bvd_id_number",
        remove_str=["ltd"],
        cut_off=90,
        num_workers=1,
    )

    assert result["Search_string"].tolist() == ["acme"]
    assert result["BestMatch"].tolist() == ["acme"]
    assert result["Score"].tolist() == [100.0]
    assert result["name"].tolist() == ["Acme Ltd"]
    assert result["bvd_id_number"].tolist() == ["BVD1"]


def test_fuzzy_match_pl_returns_best_fuzzy_match_and_no_match_rows():
    df = pl.DataFrame(
        {
            "name": ["Acme Limited", "Beta Group"],
            "bvd_id_number": ["BVD1", "BVD2"],
        }
    )

    result = fuzzy_match_pl(
        names=["acme limted", "unknown entity"],
        df=df,
        match_column="name",
        return_column="bvd_id_number",
        cut_off=70,
        num_workers=1,
    )

    matched_rows = result[result["Search_string"] == "acme limted"]
    no_match_rows = result[result["Search_string"] == "unknown entity"]

    assert matched_rows["BestMatch"].tolist() == ["acme limited"]
    assert matched_rows["bvd_id_number"].tolist() == ["BVD1"]
    assert matched_rows["Score"].iloc[0] >= 70

    assert no_match_rows["BestMatch"].tolist() == [None]
    assert no_match_rows["Score"].tolist() == [0.0]
    assert no_match_rows["name"].tolist() == [None]
    assert no_match_rows["bvd_id_number"].tolist() == [None]


def test_fuzzy_match_pl_widens_candidates_when_blocked_prefix_misses():
    df = pl.DataFrame(
        {
            "name": ["Acme Limited", "Zebra Services"],
            "bvd_id_number": ["BVD1", "BVD2"],
        }
    )

    result = fuzzy_match_pl(
        names=["zcme limited"],
        df=df,
        match_column="name",
        return_column="bvd_id_number",
        cut_off=80,
        num_workers=1,
    )

    assert result["BestMatch"].tolist() == ["acme limited"]
    assert result["bvd_id_number"].tolist() == ["BVD1"]
    assert result["Score"].iloc[0] >= 80


def test_process_one_uses_polars_row_limit_for_single_compatible_file(monkeypatch):
    proc = _make_dummy_process()

    def fake_polars_all(*args, **kwargs):
        assert kwargs["row_limit"] == 2
        proc._last_process_engine = "polars"
        proc._last_process_reason = "direct"
        return pl.DataFrame({"value": [1, 2]}), ["polars.csv"]

    def fail_process_all(*args, **kwargs):  # pragma: no cover - should not be hit
        raise AssertionError("process_all() should not be used for the Polars fast path")

    monkeypatch.setattr(DummyProcess, "polars_all", fake_polars_all)
    monkeypatch.setattr(DummyProcess, "process_all", fail_process_all)

    df = proc.process_one(n_rows=2)

    assert isinstance(df, pd.DataFrame)
    assert df["value"].tolist() == [1, 2]
    assert proc.last_process_engine == "polars"


def test_process_one_resolves_integer_file_index_against_remote_files(monkeypatch):
    proc = _make_dummy_process()
    proc.remote_files = ["first.csv", "second.csv"]

    def fake_polars_all(*args, **kwargs):
        assert kwargs["files"] == ["second.csv"]
        assert kwargs["row_limit"] == 2
        proc._last_process_engine = "polars"
        proc._last_process_reason = "direct"
        return pl.DataFrame({"value": [1, 2]}), ["second.csv"]

    monkeypatch.setattr(DummyProcess, "polars_all", fake_polars_all)

    df = proc.process_one(files=1, n_rows=2)

    assert isinstance(df, pd.DataFrame)
    assert df["value"].tolist() == [1, 2]


def test_process_one_falls_back_to_process_all_for_pandas_only_workloads(monkeypatch):
    proc = _make_dummy_process()
    proc.query = "value > 1"

    def fake_process_all(*args, **kwargs):
        proc._last_process_engine = "pandas"
        proc._last_process_reason = "string_query"
        return pd.DataFrame({"value": [1, 2, 3]}), ["pandas.csv"]

    def fail_polars_all(*args, **kwargs):  # pragma: no cover - should not be hit
        raise AssertionError("polars_all() should not be used for pandas-only workloads")

    monkeypatch.setattr(DummyProcess, "process_all", fake_process_all)
    monkeypatch.setattr(DummyProcess, "polars_all", fail_polars_all)

    df = proc.process_one(n_rows=2)

    assert isinstance(df, pd.DataFrame)
    assert df["value"].tolist() == [1, 2]
    assert proc.last_process_engine == "pandas"


def test_file_exist_preserves_delete_files_for_existing_local_file(tmp_path):
    proc = _make_dummy_process()
    proc.delete_files = True
    proc._max_path_length = 10_000
    proc._local_path = str(tmp_path)

    local_file = tmp_path / "existing.csv"
    local_file.write_text("value\n1\n", encoding="utf-8")

    resolved_file, flag = proc._file_exist(str(local_file))

    assert resolved_file == str(local_file)
    assert flag is True
    assert proc.delete_files is True


def test_download_all_sync_preserves_flags_and_marks_finished(monkeypatch):
    proc = _make_dummy_process()
    proc._set_data_product = "Dummy Product"
    proc._set_table = "dummy_table"
    proc.remote_files = ["remote.csv"]
    proc.delete_files = True
    proc.concat_files = True

    monkeypatch.setattr("moodys_datahub.process.os.fork", lambda: None, raising=False)
    monkeypatch.setattr(DummyProcess, "_check_args", lambda self, files: (files, None))
    monkeypatch.setattr(DummyProcess, "_file_exist", lambda self, file: (file, False))

    calls = {}

    def fake_run_parallel(**kwargs):
        calls["kwargs"] = kwargs
        return []

    monkeypatch.setattr("moodys_datahub.process._run_parallel", fake_run_parallel)

    proc.download_all(async_mode=False, num_workers=1)

    assert calls["kwargs"]["msg"] == "Downloading"
    assert proc.delete_files is True
    assert proc.concat_files is True
    assert proc._download_finished is True


def test_download_all_async_preserves_flags_and_marks_in_progress(monkeypatch):
    proc = _make_dummy_process()
    proc._set_data_product = "Dummy Product"
    proc._set_table = "dummy_table"
    proc.remote_files = ["remote.csv"]
    proc.delete_files = True
    proc.concat_files = False

    monkeypatch.setattr("moodys_datahub.process.os.fork", lambda: None, raising=False)
    monkeypatch.setattr(DummyProcess, "_check_args", lambda self, files: (files, None))
    monkeypatch.setattr(DummyProcess, "_file_exist", lambda self, file: (file, False))

    started = {"value": False}

    class FakeProcess:
        def __init__(self, target=None, kwargs=None):
            self.target = target
            self.kwargs = kwargs

        def start(self):
            started["value"] = True

    monkeypatch.setattr("moodys_datahub.process.Process", FakeProcess)

    proc.download_all(async_mode=True, num_workers=1)

    assert started["value"] is True
    assert proc.delete_files is True
    assert proc.concat_files is False
    assert proc._download_finished is False


def test_download_all_marks_finished_when_files_are_already_local(monkeypatch, capsys):
    proc = _make_dummy_process()
    proc._set_data_product = "Dummy Product"
    proc._set_table = "dummy_table"
    proc.remote_files = ["already.csv"]
    proc._download_finished = None

    monkeypatch.setattr("moodys_datahub.process.os.fork", lambda: None, raising=False)
    monkeypatch.setattr(DummyProcess, "_check_args", lambda self, files: (files, None))
    monkeypatch.setattr(DummyProcess, "_file_exist", lambda self, file: (file, True))
    monkeypatch.setattr("moodys_datahub.process.os.path.exists", lambda path: True)

    proc.download_all(async_mode=False, num_workers=1)

    assert proc._download_finished is True
    assert "already downloaded" in capsys.readouterr().out


def test_download_finished_property_exposes_download_state():
    proc = _make_dummy_process()
    proc._download_finished = False

    assert proc.download_finished is False


def test_search_company_names_prefers_polars_without_pandas_fallback(monkeypatch):
    class FakeSearch:
        def _object_defaults(self):
            pass

        def polars_all(self, num_workers):
            return pl.DataFrame({"name": ["Acme Ltd"], "bvd_id_number": ["BVD1"]}), [
                "firmographics.parquet"
            ]

        def process_all(self, *args, **kwargs):  # pragma: no cover - should not be hit
            raise AssertionError("pandas fallback should not be used")

    fake_search = FakeSearch()

    monkeypatch.setattr("moodys_datahub.tools.copy.deepcopy", lambda obj: fake_search)
    monkeypatch.setattr(pd.DataFrame, "to_csv", lambda self, *args, **kwargs: None)
    monkeypatch.setattr(
        "moodys_datahub.tools.fuzzy_match_pl",
        lambda **kwargs: pd.DataFrame(
            {
                "Search_string": ["acme"],
                "BestMatch": ["acme ltd"],
                "Score": [100.0],
                "name": ["Acme Ltd"],
                "bvd_id_number": ["BVD1"],
            }
        ),
    )

    result = Sftp.search_company_names(object(), names=["Acme"], num_workers=1)

    assert result["bvd_id_number"].tolist() == ["BVD1"]
    assert fake_search.set_data_product == "Firmographics (Monthly)"
    assert fake_search.set_table == "bvd_id_and_name"


def test_search_company_names_falls_back_to_pandas_on_polars_load_error(monkeypatch):
    class FakeSearch:
        def _object_defaults(self):
            pass

        def polars_all(self, num_workers):
            raise ValueError("broken polars load")

        def process_all(self, *args, **kwargs):
            assert kwargs["engine"] == "pandas"
            return (
                pd.DataFrame(
                    {
                        "Search_string": ["acme"],
                        "BestMatch": ["acme ltd"],
                        "Score": [95.0],
                        "name": ["Acme Ltd"],
                        "bvd_id_number": ["BVD1"],
                    }
                ),
                ["firmographics.csv"],
            )

    fake_search = FakeSearch()

    monkeypatch.setattr("moodys_datahub.tools.copy.deepcopy", lambda obj: fake_search)
    monkeypatch.setattr(pd.DataFrame, "to_csv", lambda self, *args, **kwargs: None)
    monkeypatch.setattr(
        "moodys_datahub.tools.fuzzy_match_pl",
        lambda **kwargs: (_ for _ in ()).throw(
            AssertionError("fuzzy_match_pl should not be called after a load failure")
        ),
    )

    result = Sftp.search_company_names(object(), names=["Acme"], num_workers=1)

    assert result["bvd_id_number"].tolist() == ["BVD1"]


def test_search_company_names_propagates_polars_matcher_errors(monkeypatch):
    class FakeSearch:
        def _object_defaults(self):
            pass

        def polars_all(self, num_workers):
            return pl.DataFrame({"name": ["Acme Ltd"], "bvd_id_number": ["BVD1"]}), [
                "firmographics.parquet"
            ]

        def process_all(self, *args, **kwargs):  # pragma: no cover - should not be hit
            raise AssertionError("pandas fallback should not be used")

    fake_search = FakeSearch()

    monkeypatch.setattr("moodys_datahub.tools.copy.deepcopy", lambda obj: fake_search)
    monkeypatch.setattr(pd.DataFrame, "to_csv", lambda self, *args, **kwargs: None)
    monkeypatch.setattr(
        "moodys_datahub.tools.fuzzy_match_pl",
        lambda **kwargs: (_ for _ in ()).throw(RuntimeError("matcher failed")),
    )

    with pytest.raises(RuntimeError, match="matcher failed"):
        Sftp.search_company_names(object(), names=["Acme"], num_workers=1)


def test_search_bvd_changes_uses_auto_backend_wrapper(monkeypatch):
    class FakeSearch:
        def __init__(self):
            self._set_data_product = None
            self._set_table = None
            self._select_cols = None
            self.output_format = [".csv"]

        def _object_defaults(self):
            pass

        @property
        def set_data_product(self):
            return self._set_data_product

        @set_data_product.setter
        def set_data_product(self, value):
            self._set_data_product = value

        @property
        def set_table(self):
            return self._set_table

        @set_table.setter
        def set_table(self, value):
            self._set_table = value

        def process_all(self, *args, **kwargs):
            assert kwargs["engine"] == "auto"
            return (
                pd.DataFrame(
                    {
                        "old_id": ["A"],
                        "new_id": ["B"],
                        "change_date": ["2020-01-01"],
                    }
                ),
                ["changes.csv"],
            )

    fake_search = FakeSearch()
    expected_filtered = pd.DataFrame({"old_id": ["A"], "new_id": ["B"]})

    monkeypatch.setattr("moodys_datahub.tools.copy.deepcopy", lambda obj: fake_search)
    monkeypatch.setattr(
        "moodys_datahub.tools._bvd_changes_ray",
        lambda bvd_list, df, num_workers: (
            {"A", "B"},
            {"A": "B", "B": "B"},
            expected_filtered,
        ),
    )

    new_ids, newest_ids, filtered_df = Sftp.search_bvd_changes(
        object(), ["A"], num_workers=1
    )

    assert new_ids == {"A", "B"}
    assert newest_ids == {"A": "B", "B": "B"}
    assert filtered_df.equals(expected_filtered)
    assert fake_search.set_data_product == "BvD ID Changes"
    assert fake_search.set_table == "bvd_id_changes_full"


def test_national_identifer_returns_dataframe_and_uses_polars_query():
    class FakeSearch:
        def __init__(self):
            self.set_data_product = None
            self.set_table = None

        def process_all(self, *args, **kwargs):
            assert kwargs["select_cols"] == ["bvd_id_number", "national_id_number"]
            assert isinstance(kwargs["query"], pl.Expr)
            return (
                pd.DataFrame(
                    {
                        "bvd_id_number": ["BVD1"],
                        "national_id_number": ["123"],
                    }
                ),
                ["national.csv"],
            )

    class FakeObj:
        def __init__(self):
            self.search = FakeSearch()

        def copy_obj(self):
            return self.search

    result = national_identifer(FakeObj(), national_ids=[123], num_workers=1)

    assert isinstance(result, pd.DataFrame)
    assert result["bvd_id_number"].tolist() == ["BVD1"]


def test_batch_bvd_search_uses_structured_exact_bvd_query(monkeypatch, tmp_path):
    products_path = tmp_path / "products.xlsx"
    bvd_numbers_path = tmp_path / "bvd_numbers.txt"

    pd.DataFrame(
        {
            "Data Product": ["Firmographics (Monthly)"],
            "Table": ["bvd_id_and_name"],
            "Column": ["bvd_id_number, guo_bvd_id_number"],
            "Run": [True],
        }
    ).to_excel(products_path, index=False)
    pd.DataFrame(["DK1", "SE2"]).to_csv(
        bvd_numbers_path, index=False, header=False
    )

    class FakeSearch:
        def __init__(self):
            self._set_data_product = None
            self._set_table = None
            self.calls = []

        def _object_defaults(self):
            pass

        @property
        def set_data_product(self):
            return self._set_data_product

        @set_data_product.setter
        def set_data_product(self, value):
            self._set_data_product = value

        @property
        def set_table(self):
            return self._set_table

        @set_table.setter
        def set_table(self, value):
            self._set_table = value

        def get_column_names(self):
            return ["bvd_id_number", "guo_bvd_id_number", "name"]

        def process_all(self, *args, **kwargs):
            self.calls.append(kwargs)
            return pd.DataFrame({"value": [1]}), ["batch.csv"]

    fake_search = FakeSearch()

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr("moodys_datahub.tools.copy.deepcopy", lambda obj: fake_search)

    Sftp.batch_bvd_search(
        object(),
        products=str(products_path),
        bvd_numbers=str(bvd_numbers_path),
    )

    assert len(fake_search.calls) == 1
    assert fake_search.calls[0]["bvd_query"] == [
        ["DK1", "SE2"],
        ["bvd_id_number", "guo_bvd_id_number"],
        "exact",
    ]
