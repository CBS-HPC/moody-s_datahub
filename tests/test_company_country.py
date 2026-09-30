import pandas as pd
import polars as pl
import pytest

from moodys_datahub.company_country import (
    country_filter_expr,
    country_match_query,
    resolve_country_filters,
)
from moodys_datahub.load_data import _country_codes
from moodys_datahub.tools import Sftp
from moodys_datahub.utils import _load_pd, _load_pl


class FakeSearch:
    def __init__(self, source, fail_polars=False):
        self.source = source
        self.fail_polars = fail_polars
        self.query = None
        self.query_args = None

    def _object_defaults(self):
        self.query = None
        self.query_args = None

    def polars_all(self, num_workers):
        if self.fail_polars:
            raise ValueError("Polars source unavailable")
        assert isinstance(self.query, pl.Expr)
        return self.source.lazy().filter(self.query).collect(), ["firms.parquet"]

    def process_all(self, num_workers, engine):
        assert engine == "pandas"
        source = self.source.to_pandas()
        return self.query(source, *self.query_args), ["firms.csv"]


@pytest.fixture
def sample_source():
    return pl.DataFrame(
        {
            "name": ["Acme", "Acme", "Acme", "Acme", "Nordic Foods"],
            "bvd_id_number": ["DK999", "US888", "X123", "DK999", "DK444"],
        }
    )


def _search(monkeypatch, source, **kwargs):
    fake = FakeSearch(source, **kwargs)
    monkeypatch.setattr("moodys_datahub.tools.copy.deepcopy", lambda obj: fake)
    monkeypatch.setattr(pd.DataFrame, "to_csv", lambda self, *args, **kw: None)
    return fake


def test_country_metadata_preserves_namibia_code():
    catalog = _country_codes()
    assert catalog.loc[catalog["Country"] == "Namibia", "Code"].item() == "NA"

    codes, known = resolve_country_filters(["Acme"], country="Namibia")
    assert codes == ["NA"]
    assert "NA" in known


@pytest.mark.parametrize("fail_polars", [False, True])
def test_single_country_keeps_own_and_unknown_ids(monkeypatch, sample_source, fail_polars):
    fake = _search(monkeypatch, sample_source, fail_polars=fail_polars)

    result = Sftp.search_company_names(
        object(), names=["Acme"], country="Denmark", num_workers=1
    )

    assert set(result["bvd_id_number"]) == {"DK999", "X123"}
    assert result.set_index("bvd_id_number")["BvD_prefix_status"].to_dict() == {
        "DK999": "requested_prefix",
        "X123": "unrecognized_prefix",
    }
    assert result["Requested_country"].tolist() == ["DK", "DK"]
    if fail_polars:
        assert fake.query is country_match_query
    else:
        assert isinstance(fake.query, pl.Expr)


@pytest.mark.parametrize("fail_polars", [False, True])
def test_per_name_countries_keep_repeated_names_distinct(
    monkeypatch, sample_source, fail_polars
):
    _search(monkeypatch, sample_source, fail_polars=fail_polars)

    result = Sftp.search_company_names(
        object(),
        names=["Acme", "Acme"],
        countries_by_name=["DK", "US"],
        num_workers=1,
    )

    assert set(result.loc[result["Input_index"] == 0, "bvd_id_number"]) == {
        "DK999",
        "X123",
    }
    assert set(result.loc[result["Input_index"] == 1, "bvd_id_number"]) == {
        "US888",
        "X123",
    }
    assert result.groupby("Input_index")["Requested_country"].first().to_dict() == {
        0: "DK",
        1: "US",
    }


def test_country_filter_returns_no_match_when_subset_is_empty(monkeypatch):
    source = pl.DataFrame({"name": ["Acme"], "bvd_id_number": ["US888"]})
    _search(monkeypatch, source)

    result = Sftp.search_company_names(
        object(), names=["Acme"], country="DK", num_workers=1
    )

    assert result["bvd_id_number"].isna().all()
    assert result["Score"].tolist() == [0.0]


def test_country_filter_retains_missing_and_short_prefixes(monkeypatch):
    source = pl.DataFrame(
        {"name": ["Acme", "Acme", "Acme"], "bvd_id_number": [None, "X", "US888"]}
    )
    _search(monkeypatch, source)

    result = Sftp.search_company_names(
        object(), names=["Acme"], country="DK", num_workers=1
    )

    assert result["bvd_id_number"].dropna().tolist() == ["X"]
    assert set(result["BvD_prefix_status"]) == {"missing_id", "unrecognized_prefix"}


def test_country_filter_runs_in_local_parquet_loaders(tmp_path, sample_source):
    path = tmp_path / "firms.parquet"
    sample_source.write_parquet(path)
    codes, known = resolve_country_filters(["Acme"], country="DK")
    selected = ["bvd_id_number", "name"]

    polars_result = _load_pl(
        [str(path)],
        select_cols=selected,
        query=country_filter_expr(set(codes), known),
    )
    pandas_result = _load_pd(
        str(path),
        select_cols=selected,
        query=country_match_query,
        query_args=[["Acme"], codes, known, 90.1, None, "WRatio"],
    )

    assert set(polars_result["bvd_id_number"]) == {"DK999", "X123", "DK444"}
    assert set(pandas_result["bvd_id_number"]) == {"DK999", "X123"}


@pytest.mark.parametrize(
    ("country", "countries_by_name", "message"),
    [
        ("XX", None, "Unknown country"),
        (None, ["DK"], "one value per name"),
        ("DK", ["DK", "US"], "either country or countries_by_name"),
    ],
)
def test_country_filter_rejects_invalid_selection(
    country, countries_by_name, message
):
    with pytest.raises(ValueError, match=message):
        Sftp.search_company_names(
            object(),
            names=["Acme", "Beta"],
            country=country,
            countries_by_name=countries_by_name,
            num_workers=1,
        )
