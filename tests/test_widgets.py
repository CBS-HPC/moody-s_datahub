import asyncio
from types import SimpleNamespace

import pandas as pd
import pytest

from moodys_datahub.widgets import (
    _CustomQuestion,
    _Multi_dropdown,
    _select_bvd,
    _select_date,
    _select_list,
    _select_product,
    _SelectData,
    _SelectList,
    _SelectMultiple,
    _SelectOptions,
)


class _ChangeEvent(dict):
    def __init__(self, new, owner=None):
        super().__init__({"type": "change", "name": "value"})
        self.new = new
        self.owner = owner


def test_select_data_handlers_update_current_selection(monkeypatch):
    monkeypatch.setattr(
        "moodys_datahub.widgets.asyncio.ensure_future", lambda coro: coro.close()
    )

    df = pd.DataFrame(
        {
            "Data Product": ["Prod A", "Prod A", "Prod B"],
            "Table": ["table_1", "table_2", "table_3"],
        }
    )
    widget = _SelectData(df)

    widget.product_dropdown.value = "Prod B"
    asyncio.run(widget._product_change(_ChangeEvent("Prod B")))

    assert widget.selected_product == "Prod B"
    assert list(widget.table_dropdown.options) == ["table_3"]
    assert widget.ok_button.disabled is True

    asyncio.run(widget._table_change(_ChangeEvent("table_3")))

    assert widget.selected_table == "table_3"
    assert widget.ok_button.disabled is False


def test_select_data_ok_cancel_and_display(monkeypatch):
    monkeypatch.setattr("moodys_datahub.widgets.display", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        "moodys_datahub.widgets.asyncio.ensure_future", lambda coro: coro.close()
    )

    df = pd.DataFrame({"Data Product": ["Prod A"], "Table": ["table_1"]})
    widget = _SelectData(df)

    widget._ok_button_click(None)

    assert widget.ok_button.disabled is True
    assert widget.cancel_button.disabled is True
    assert widget.product_dropdown.disabled is True
    assert widget.table_dropdown.disabled is True

    widget = _SelectData(df)
    widget._cancel_button_click(None)

    assert widget.selected_product is None
    assert widget.selected_table is None

    widget = _SelectData(df)
    widget.cancel_button.disabled = True

    assert asyncio.run(widget.display_widgets()) == ("Prod A", "table_1")


def test_select_list_change_cancel_and_display(monkeypatch):
    monkeypatch.setattr("moodys_datahub.widgets.display", lambda *args, **kwargs: None)
    widget = _SelectList(["alpha", "beta"], "Column")

    asyncio.run(widget._list_change(_ChangeEvent("beta")))
    assert widget.selected_value == "beta"
    assert widget.ok_button.disabled is False

    widget._cancel_button_click(None)
    assert widget.selected_value is None
    assert widget.list_dropdown.disabled is True

    widget = _SelectList(["alpha", "beta"], "Column")
    widget.cancel_button.disabled = True
    assert asyncio.run(widget.display_widgets()) == "alpha"


def test_select_multiple_change_cancel_and_display(monkeypatch):
    monkeypatch.setattr("moodys_datahub.widgets.display", lambda *args, **kwargs: None)
    widget = _SelectMultiple(["alpha", "beta", "gamma"], "Column")

    asyncio.run(widget._list_change(_ChangeEvent(("alpha", "gamma"))))
    assert widget.selected_list == ["alpha", "gamma"]
    assert widget.ok_button.disabled is False

    widget._cancel_button_click(None)
    assert widget.selected_list is None
    assert widget.list_select.disabled is True

    widget = _SelectMultiple(["alpha", "beta"], "Column")
    widget.cancel_button.disabled = True
    assert asyncio.run(widget.display_widgets()) == []


def test_multi_dropdown_validates_updates_and_displays(monkeypatch):
    monkeypatch.setattr("moodys_datahub.widgets.display", lambda *args, **kwargs: None)

    widget = _Multi_dropdown([["A", "B"], ["X", "Y"]], ["Left", "Right"], "Pick")

    right_dropdown = widget.dropdown_widgets[1][1]
    asyncio.run(widget._list_change(_ChangeEvent("Y", owner=right_dropdown)))

    assert widget.selected_values == ["A", "Y"]
    assert widget.ok_button.disabled is False

    widget._cancel_button_click(None)
    assert widget.selected_values is None
    assert all(dropdown.disabled for _, dropdown in widget.dropdown_widgets)

    widget = _Multi_dropdown(["A", "B"], ["Only"], "Pick")
    widget.cancel_button.disabled = True
    assert asyncio.run(widget.display_widgets()) == ["A"]


def test_multi_dropdown_rejects_invalid_configuration():
    with pytest.raises(ValueError, match="col_names must be a list"):
        _Multi_dropdown(["A", "B"], "Only", "Pick")

    with pytest.raises(ValueError, match="Length of col_names"):
        _Multi_dropdown(["A", "B"], ["Left", "Right"], "Pick")

    with pytest.raises(ValueError, match="Title must be a string"):
        _Multi_dropdown(["A", "B"], ["Only"], 123)


def test_select_options_ok_cancel_and_display(monkeypatch):
    monkeypatch.setattr("moodys_datahub.widgets.display", lambda *args, **kwargs: None)

    widget = _SelectOptions()
    widget.delete_files_dropdown.value = True
    widget.concat_files_dropdown.value = False
    widget.output_format_multiselect.value = (".csv", ".parquet")
    widget.file_size_input.value = 123

    widget._ok_button_click(None)

    assert widget.config == {
        "delete_files": True,
        "concat_files": False,
        "output_format": [".csv", ".parquet"],
        "file_size_mb": 123,
    }
    assert widget.output_format_multiselect.disabled is True

    widget = _SelectOptions()
    widget._cancel_button_click(None)
    assert widget.config["output_format"] == [".csv"]

    widget = _SelectOptions()
    widget.cancel_button.disabled = True
    assert asyncio.run(widget.display_widgets())["concat_files"] is True


def test_custom_question_returns_future_and_disables_buttons(monkeypatch):
    monkeypatch.setattr("moodys_datahub.widgets.display", lambda *args, **kwargs: None)
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    try:
        question = _CustomQuestion("Proceed?", ["yes", "no"])
        future = question.display_widgets()

        question.on_button_clicked(SimpleNamespace(description="yes"))

        assert future.done() is True
        assert future.result() == "yes"
        assert all(button.disabled for button in question.buttons)
    finally:
        loop.close()
        asyncio.set_event_loop(None)


def test_select_list_helper_runs_callback(monkeypatch):
    captured = {}

    class FakeSelect:
        def __init__(self, values, col_name, title):
            captured["values"] = values
            captured["col_name"] = col_name
            captured["title"] = title

        async def display_widgets(self):
            return "chosen"

    monkeypatch.setattr("moodys_datahub.widgets._SelectList", FakeSelect)
    monkeypatch.setattr(
        "moodys_datahub.widgets.asyncio.ensure_future",
        lambda coro: asyncio.run(coro),
    )

    _select_list(
        "_SelectList",
        ["alpha", "beta"],
        "Column",
        "Pick one",
        lambda selected, bucket: bucket.update({"selected": selected}),
        [captured],
    )

    assert captured == {
        "values": ["alpha", "beta"],
        "col_name": "Column",
        "title": "Pick one",
        "selected": "chosen",
    }


def test_select_bvd_and_select_date_update_filters(capsys):
    bvd_list = [["DK1", "SE2"], None, None]
    time_period = [2020, 2021, None, "remove"]
    select_cols = ["name"]

    _select_bvd(["bvd_id_number"], bvd_list, select_cols, False)
    _select_date("closing_date", time_period, select_cols)

    assert bvd_list[1] == ["bvd_id_number"]
    assert bvd_list[2] == "bvd_id_number in ['DK1', 'SE2']"
    assert time_period[2] == "closing_date"
    output = capsys.readouterr().out
    assert "bvd query has been created" in output
    assert "Period will be selected" in output


def test_select_product_updates_remote_path_and_clears_on_missing(capsys):
    df = pd.DataFrame(
        {
            "Top-level Directory": ["prod/path"],
            "Base Directory": ["remote/base"],
        }
    )
    obj = SimpleNamespace(
        remote_path=None,
        _set_table="table_1",
        _set_data_product="Prod A",
        set_data_product="Prod A",
        set_table="table_1",
    )

    _select_product("prod/path", df, obj)
    assert obj.remote_path == "remote/base"
    assert "was set as Data Product" in capsys.readouterr().out

    _select_product("missing", df, obj)
    assert obj.remote_path is None
    assert obj._set_table is None
    assert obj._set_data_product is None

    obj._set_table = "table_1"
    obj._set_data_product = "Prod A"
    _select_product(None, df, obj)
    assert obj.remote_path is None
