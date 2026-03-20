import pandas as pd

from moodys_datahub.utils import (
    _check_list_format,
    _construct_query,
    _date_pd,
    _letters_only_regex,
    _load_pl,
)


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
