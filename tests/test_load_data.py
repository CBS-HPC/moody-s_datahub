from moodys_datahub.load_data import _country_codes, _table_dictionary, _table_names


def test_table_names_structure():
    df = _table_names()
    assert {"Data Product", "Top-level Directory"}.issubset(df.columns)
    assert len(df) > 0


def test_table_dictionary_structure():
    df = _table_dictionary()
    assert {"Data Product", "Table", "Column", "Definition"}.issubset(df.columns)
    assert len(df) > 0


def test_country_codes_structure():
    df = _country_codes()
    assert "Code" in df.columns
    assert len(df) > 0
