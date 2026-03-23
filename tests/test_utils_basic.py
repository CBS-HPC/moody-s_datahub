import pandas as pd
import polars as pl
import pytest

from moodys_datahub.process import _Process
from moodys_datahub.utils import (
    _check_list_format,
    _construct_query,
    _date_pd,
    _letters_only_regex,
    _load_pl,
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
