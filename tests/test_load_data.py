import importlib.resources as pkg_resources
from pathlib import Path

import pandas as pd
import pytest

from moodys_datahub.load_data import (
    _country_codes,
    _table_dates,
    _table_dictionary,
    _table_match,
    _table_names,
)
from moodys_datahub.process import _filter_metadata_table


def test_table_names_structure():
    df = _table_names()
    assert {"Data Product", "Top-level Directory"}.issubset(df.columns)
    assert len(df) > 0


def test_packaged_data_products_have_complete_table_metadata():
    df = _table_names()

    assert not df[["Data Product", "Top-level Directory"]].isna().any().any()


def test_ownership_history_links_have_only_parquet_metadata():
    file_path = pkg_resources.files("moodys_datahub.data") / "data_products.xlsx"
    with file_path.open("rb") as handle:
        df = pd.read_excel(handle)
    subset = df[
        (df["Data Product"] == "Ownership History (Semi-Annual)")
        & (df["Table"].isin(["links_2022", "links_2023", "links_2024", "links_2025"]))
    ]

    formats_by_table = subset.groupby("Table")["Format"].apply(set).to_dict()

    assert formats_by_table == {
        "links_2022": {"parquet"},
        "links_2023": {"parquet"},
        "links_2024": {"parquet"},
        "links_2025": {"parquet"},
    }
    assert not subset[["Top-level Directory", "Format"]].isna().any().any()


def test_packaged_product_catalog_does_not_advertise_csv_service():
    file_path = pkg_resources.files("moodys_datahub.data") / "data_products.xlsx"
    with file_path.open("rb") as handle:
        catalog = pd.read_excel(handle)
    assert not catalog.empty
    assert set(catalog["Format"].str.lower()) == {"parquet"}


@pytest.mark.parametrize("table", ["status_history", "statushistory"])
def test_firmographics_status_history_dictionary_supports_both_spellings(table):
    dictionary = _table_dictionary()
    metadata = _filter_metadata_table(
        dictionary.loc[dictionary["Data Product"].eq("Firmographics (Monthly)")], table
    )
    assert set(metadata["Column"]) == {
        "bvd_id_number",
        "status",
        "status_date",
        "status_updated_in_orbis",
    }
    assert metadata["Type"].notna().all()


def test_available_dictionary_uses_parquet_status_history_name():
    path = (
        Path(__file__).resolve().parents[1]
        / "src/moodys_datahub/data/data_dict_available.xlsx"
    )
    dictionary = _table_dictionary(str(path))
    metadata = dictionary.loc[
        dictionary["Data Product"].eq("Firmographics (Monthly)")
        & dictionary["Table"].str.contains("status", regex=False)
    ]
    assert len(metadata) == 4
    assert metadata["Table"].eq("statushistory").all()


@pytest.mark.parametrize("table", ["status_history", "statushistory"])
def test_firmographics_status_history_dates_support_both_spellings(table):
    dates = _table_dates()
    metadata = _filter_metadata_table(
        dates.loc[dates["Data Product"].eq("Firmographics (Monthly)")], table
    )
    assert metadata["Column"].tolist() == ["status_updated_in_orbis"]


def test_ownership_history_links_have_date_metadata():
    df = _table_dates()
    subset = df[
        (df["Data Product"] == "Ownership History (Semi-Annual)")
        & (df["Table"].isin(["links_2022", "links_2023", "links_2024", "links_2025"]))
    ]

    assert set(subset["Table"]) == {
        "links_2022",
        "links_2023",
        "links_2024",
        "links_2025",
    }
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
        & (df["Table"].isin(["links_2022", "links_2023", "links_2024", "links_2025"]))
    ]

    assert set(subset["Table"]) == {
        "links_2022",
        "links_2023",
        "links_2024",
        "links_2025",
    }
    assert set(subset["Column"]) == {"subsidiary_bvd_id,shareholder_bvd_id"}
    assert subset["Run"].eq(True).all()


@pytest.mark.parametrize("filename", ["data_dict.xlsx", "data_dict_available.xlsx"])
def test_ownership_history_2025_reuses_shared_dictionary_metadata(filename):
    path = Path(__file__).resolve().parents[1] / "src/moodys_datahub/data" / filename
    dictionary = _table_dictionary(str(path))
    ownership = dictionary.loc[
        dictionary["Data Product"].eq("Ownership History (Semi-Annual)")
    ]

    metadata = _filter_metadata_table(ownership, "links_2025")
    previous = _filter_metadata_table(ownership, "links_2024")

    assert len(metadata) == 18
    assert not metadata["Column"].duplicated().any()
    assert {"subsidiary_bvd_id", "shareholder_bvd_id", "information_date"}.issubset(
        metadata["Column"]
    )
    assert metadata[["Definition", "Type", "Remarks"]].notna().all().all()
    pd.testing.assert_frame_equal(metadata, previous)
    assert _filter_metadata_table(ownership, "links_20250").empty


def test_table_dictionary_structure():
    df = _table_dictionary()
    assert {
        "Data Product",
        "Table",
        "Column",
        "Definition",
        "Type",
        "Remarks",
    }.issubset(df.columns)
    assert len(df) > 0


@pytest.mark.parametrize("filename", ["data_dict.xlsx", "data_dict_available.xlsx"])
def test_dictionary_preserves_provider_types_and_remarks(filename):
    path = Path(__file__).resolve().parents[1] / "src/moodys_datahub/data" / filename
    df = _table_dictionary(str(path))
    assert {"Type", "Remarks"}.issubset(df.columns)

    beneficial = df.loc[
        df["Data Product"].eq("Beneficial Owners (Monthly)")
        & df["Table"].eq("beneficial_owners_10_10")
        & df["Column"].eq("bvd_id_number")
    ]
    assert beneficial["Type"].tolist() == ["string - 50"]
    assert beneficial["Remarks"].str.contains("10%-10%", regex=False).all()

    ownership_date = df.loc[
        df["Data Product"].eq("Ownership History (Semi-Annual)")
        & df["Column"].eq("information_date")
    ]
    assert not ownership_date.empty
    assert ownership_date["Type"].eq("date - 8").all()
    assert ownership_date["Remarks"].str.startswith("Product remarks:").all()

    history = df.loc[
        df["Data Product"].eq("Financials History (Semi-Annual)")
        & df["Table"].str.contains("key_financials", regex=False)
    ]
    assert not history.empty
    assert history["Type"].notna().all()
    assert (
        not history["Remarks"]
        .fillna("")
        .str.contains("maximum period of 10 years", regex=False)
        .any()
    )

    missing_source_table = df.loc[
        df["Table"].eq("orbis_monthly_shareholders_bvdid_uci")
    ]
    assert not missing_source_table.empty
    assert missing_source_table[["Type", "Remarks"]].isna().all().all()


@pytest.mark.parametrize("filename", ["data_dict.xlsx", "data_dict_available.xlsx"])
def test_dictionary_text_has_no_utf8_mojibake(filename):
    path = Path(__file__).resolve().parents[1] / "src/moodys_datahub/data" / filename
    df = _table_dictionary(str(path))
    text = df[["Definition", "Type", "Remarks"]].fillna("").astype(str)
    mojibake = text.apply(lambda col: col.str.contains("â€|â†|Ã"))
    assert not mojibake.any().any()


@pytest.mark.parametrize("filename", ["data_dict.xlsx", "data_dict_available.xlsx"])
def test_dictionary_backfilled_remarks_stay_in_scope(filename):
    path = Path(__file__).resolve().parents[1] / "src/moodys_datahub/data" / filename
    df = _table_dictionary(str(path))
    remarks = df["Remarks"].fillna("")

    detailed_monthly = df["Data Product"].str.startswith(
        "Detailed Financials (Monthly)"
    )
    for group in ["banks_global", "industry_global", "insurances_global"]:
        rows = remarks[detailed_monthly & df["Table"].str.contains(group, regex=False)]
        assert not rows.empty
        assert rows.str.contains("There are 9 tables", regex=False).all()

    other_financials = df["Data Product"].isin(
        ["Financials History (Semi-Annual)", "Interim Financials (Monthly)"]
    )
    assert not remarks[other_financials].str.contains("There are 9 tables").any()

    ownership_dates = df["Data Product"].eq("Key Ownership (Monthly)") & df[
        "Column"
    ].str.endswith("_information_date")
    assert ownership_dates.sum() == 7
    assert remarks[ownership_dates].str.contains("not an export date").all()

    citations = df["Column"].eq("citingpatpublnr")
    if filename == "data_dict.xlsx":
        by_table = dict(zip(df.loc[citations, "Table"], remarks[citations]))
        assert "cited by the selected patent" in by_table["patents_backwardcitations"]
        assert "citing the selected patent" in by_table["patents_forwardcitations"]
    else:
        assert not citations.any()


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
