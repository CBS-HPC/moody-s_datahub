import pandas as pd
import polars as pl
import pytest

from moodys_datahub.process import _Process
from moodys_datahub.utils import (
    _bvd_changes_ray,
    _check_list_format,
    _construct_query,
    _date_pd,
    _letters_only_regex,
    _load_pl,
    fuzzy_match_pl,
)


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
    return proc


def test_letters_only_regex():
    assert _letters_only_regex("Abc-123!") == "abc123"
    assert _letters_only_regex(None) is None


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
