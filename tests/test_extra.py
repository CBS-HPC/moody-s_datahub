import pandas as pd

from moodys_datahub.extra import _fuzzy_worker, fuzzy_match, year_distribution


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


def test_fuzzy_worker_applies_remove_str_before_exact_matching():
    df = pd.DataFrame(
        {
            "name": ["Acme Ltd"],
            "bvd_id_number": ["BVD1"],
        }
    )

    result = _fuzzy_worker(
        (["acme"], 80, df, "name", "bvd_id_number", [" ltd"])
    )

    assert result == [("acme", "acme", 100, "Acme Ltd", "BVD1")]


def test_fuzzy_match_parallel_branch_uses_pool(monkeypatch):
    class FakePool:
        def __init__(self, processes):
            self.processes = processes
            captured["processes"] = processes

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def map(self, fn, args_list):
            captured["batches"] = len(args_list)
            return [fn(args) for args in args_list]

    def fake_worker(args):
        names, _cutoff, df_chunk, match_column, return_column, _remove = args
        row = df_chunk.iloc[0]
        score = 100 if row[match_column] == "Acme Ltd" else 50
        return [
            (
                names[0],
                row[match_column].lower(),
                score,
                row[match_column],
                row[return_column],
            )
        ]

    captured = {}
    df = pd.DataFrame(
        {
            "name": ["Acme Ltd", "Beta Group", "Gamma"],
            "bvd_id_number": ["BVD1", "BVD2", "BVD3"],
        }
    )

    monkeypatch.setattr("moodys_datahub.extra.cpu_count", lambda: 4)
    monkeypatch.setattr("moodys_datahub.extra.Pool", FakePool)
    monkeypatch.setattr("moodys_datahub.extra._fuzzy_worker", fake_worker)

    result = fuzzy_match(
        df=df,
        names=["acme ltd"],
        match_column="name",
        return_column="bvd_id_number",
        cut_off=50,
        num_workers=-1,
    )

    assert captured == {"processes": 2, "batches": 2}
    assert result["BestMatch"].tolist() == ["acme ltd"]
    assert result["bvd_id_number"].tolist() == ["BVD1"]
