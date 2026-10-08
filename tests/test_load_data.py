import importlib.resources as pkg_resources

import pandas as pd
import pytest

from moodys_datahub.load_data import (
    _country_codes,
    _table_dates,
    _table_dictionary,
    _table_match,
    _table_names,
)


def test_table_names_structure():
    df = _table_names()
    assert {"Data Product", "Top-level Directory"}.issubset(df.columns)
    assert len(df) > 0


def test_packaged_data_products_have_complete_table_metadata():
    df = _table_names()

    assert not df[["Data Product", "Top-level Directory"]].isna().any().any()


def test_ownership_history_links_have_csv_and_parquet_metadata():
    file_path = pkg_resources.files("moodys_datahub.data") / "data_products.xlsx"
    with file_path.open("rb") as handle:
        df = pd.read_excel(handle)
    subset = df[
        (df["Data Product"] == "Ownership History (Semi-Annual)")
        & (df["Table"].isin(["links_2022", "links_2023", "links_2024"]))
    ]

    formats_by_table = subset.groupby("Table")["Format"].apply(set).to_dict()

    assert formats_by_table == {
        "links_2022": {"csv", "parquet"},
        "links_2023": {"csv", "parquet"},
        "links_2024": {"csv", "parquet"},
    }
    assert not subset[["Top-level Directory", "Format"]].isna().any().any()


def test_ownership_history_links_have_date_metadata():
    df = _table_dates()
    subset = df[
        (df["Data Product"] == "Ownership History (Semi-Annual)")
        & (df["Table"].isin(["links_2022", "links_2023", "links_2024"]))
    ]

    assert set(subset["Table"]) == {"links_2022", "links_2023", "links_2024"}
    assert set(subset["Column"]) == {"information_date"}
    assert (
        not subset[["Data Product", "Table", "Column", "Definition"]].isna().any().any()
    )


def test_batch_bvd_search_template_includes_recent_ownership_links():
    file_path = pkg_resources.files("moodys_datahub.data") / "products.xlsx"
    with file_path.open("rb") as handle:
        df = pd.read_excel(handle)

    subset = df[
        (df["Data Product"] == "Ownership History (Semi-Annual)")
        & (df["Table"].isin(["links_2022", "links_2023", "links_2024"]))
    ]

    assert set(subset["Table"]) == {"links_2022", "links_2023", "links_2024"}
    assert set(subset["Column"]) == {"subsidiary_bvd_id,shareholder_bvd_id"}
    assert subset["Run"].eq(True).all()


def test_table_dictionary_structure():
    df = _table_dictionary()
    assert {"Data Product", "Table", "Column", "Definition"}.issubset(df.columns)
    assert len(df) > 0


def test_country_codes_structure():
    df = _country_codes()
    assert "Code" in df.columns
    assert len(df) > 0


def test_table_names_reads_local_csv(tmp_path):
    file_path = tmp_path / "products.csv"
    pd.DataFrame(
        {
            "Data Product": ["A", "A", "B"],
            "Top-level Directory": ["dir-a", "dir-a", "dir-b"],
            "Other": [1, 2, 3],
        }
    ).to_csv(file_path, index=False)

    df = _table_names(str(file_path))

    assert list(df.columns) == ["Data Product", "Top-level Directory"]
    assert len(df) == 2


def test_table_names_rejects_unsupported_file_format(tmp_path):
    file_path = tmp_path / "products.txt"
    file_path.write_text("not-used", encoding="utf-8")

    with pytest.raises(ValueError, match="Unsupported file format"):
        _table_names(str(file_path))


def test_table_match_strips_csv_suffix_and_returns_multiple_options(monkeypatch):
    data_products = pd.DataFrame(
        {
            "Data Product": ["Product A", "Product A", "Product B", "Product B"],
            "Table": ["table_1", "table_2", "table_1", "table_2"],
        }
    )

    monkeypatch.setattr("moodys_datahub.load_data._read_excel", lambda _: data_products)

    data_product, tables = _table_match(["table_1.csv", "table_2.csv"])

    assert data_product == "Multiple_Options: ['Product A', 'Product B']"
    assert tables == ["table_1", "table_2"]


def test_table_match_returns_unknown_for_non_matching_tables(monkeypatch):
    monkeypatch.setattr(
        "moodys_datahub.load_data._read_excel",
        lambda _: pd.DataFrame(
            {
                "Data Product": ["Product A"],
                "Table": ["table_1"],
            }
        ),
    )

    data_product, tables = _table_match(["table_2"])

    assert data_product == "Unknown"
    assert tables == ["table_2"]


def test_table_dictionary_rejects_missing_file(tmp_path):
    with pytest.raises(ValueError, match="data dictionary file was not detected"):
        _table_dictionary(str(tmp_path / "missing.xlsx"))


def test_country_codes_rejects_missing_file(tmp_path):
    with pytest.raises(ValueError, match="country codes"):
        _country_codes(str(tmp_path / "missing.xlsx"))


def test_table_dates_rejects_missing_file(tmp_path):
    with pytest.raises(ValueError, match="table date columns file was not detected"):
        _table_dates(str(tmp_path / "missing.xlsx"))
