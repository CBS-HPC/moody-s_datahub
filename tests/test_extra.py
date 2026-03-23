import pandas as pd

from moodys_datahub.extra import fuzzy_match, year_distribution


def test_year_distribution_prints_frequency_table(capsys):
    df = pd.DataFrame(
        {
            "closing_date": ["01-01-2020", "15-06-2020", "01-01-2021"],
        }
    )

    year_distribution(df)

    output = capsys.readouterr().out
    assert "2020" in output
    assert "2021" in output
    assert "Total" in output


def test_year_distribution_handles_missing_inputs(capsys):
    year_distribution(None)
    no_df_output = capsys.readouterr().out

    year_distribution(pd.DataFrame({"value": [1, 2]}))
    no_date_output = capsys.readouterr().out

    assert "No Dataframe" in no_df_output
    assert "No valid date columns found" in no_date_output


def test_fuzzy_match_returns_exact_and_no_match_rows():
    df = pd.DataFrame(
        {
            "name": ["Acme Ltd", "Beta Group"],
            "bvd_id_number": ["BVD1", "BVD2"],
        }
    )

    result = fuzzy_match(
        df=df,
        names=["acme ltd", "unknown"],
        match_column="name",
        return_column="bvd_id_number",
        cut_off=90,
        num_workers=1,
    )

    exact_row = result[result["Search_string"] == "acme ltd"]
    no_match_row = result[result["Search_string"] == "unknown"]

    assert exact_row["BestMatch"].tolist() == ["acme ltd"]
    assert exact_row["Score"].tolist() == [100]
    assert exact_row["bvd_id_number"].tolist() == ["BVD1"]

    assert no_match_row["BestMatch"].tolist() == [None]
    assert no_match_row["Score"].tolist() == [0]
    assert no_match_row["bvd_id_number"].tolist() == [None]


def test_fuzzy_match_returns_best_fuzzy_match():
    df = pd.DataFrame(
        {
            "name": ["Acme Limited", "Beta Group"],
            "bvd_id_number": ["BVD1", "BVD2"],
        }
    )

    result = fuzzy_match(
        df=df,
        names=["acme limted"],
        match_column="name",
        return_column="bvd_id_number",
        cut_off=70,
        num_workers=1,
    )

    assert result["BestMatch"].tolist() == ["acme limited"]
    assert result["bvd_id_number"].tolist() == ["BVD1"]
    assert result["Score"].iloc[0] >= 70
