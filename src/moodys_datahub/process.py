import asyncio
import os
import re
import time
from datetime import datetime
from multiprocessing import Process, cpu_count
from pathlib import Path

import numpy as np
import pandas as pd
import polars as pl
import psutil

from .load_data import _country_codes, _table_dates, _table_dictionary
from .selection import _Selection
from .utils import (
    SaveFormat,
    _check_list_format,
    _construct_query,
    _letters_only_regex,
    _load_csv_table,
    _load_pd,
    _load_pl,
    _run_parallel,
    _save_chunks,
    _save_files_pd,
    _save_to,
)
from .widgets import (
    _CustomQuestion,
    _select_bvd,
    _select_date,
    _select_list,
    _SelectMultiple,
    _SelectOptions,
)


class _Process(_Selection):
    def __init__(self):
        # Initialize mixins
        _Selection.__init__(self)

    @property
    def bvd_list(self):
        return self._bvd_list

    @bvd_list.setter
    def bvd_list(self, bvd_list=None):
        def load_bvd_list(file_path, df_bvd, delimiter="\t"):
            # Get the file extension
            file_extension = file_path.split(".")[-1].lower()

            # Load the file based on the extension
            if file_extension == "csv":
                df = pd.read_csv(file_path)
            elif file_extension in ["xls", "xlsx"]:
                df = pd.read_excel(file_path)
            elif file_extension == "txt":
                df = pd.read_csv(file_path, delimiter=delimiter)
            else:
                raise ValueError(f"Unsupported file extension: {file_extension}")

            # Process each column
            for column in df.columns:
                # Convert the column to a list of strings
                bvd_list = df[column].dropna().astype(str).tolist()
                bvd_list = [item for item in bvd_list if item.strip()]

                # Pass through the first function
                bvd_list = _check_list_format(bvd_list)

                # Pass through the second function
                bvd_list, search_type, non_matching_items = check_bvd_format(
                    bvd_list, df_bvd
                )

                # If successful, return the result
                column, _, _ = check_bvd_format([column], df_bvd)
                if column:
                    bvd_list.extend(column)
                    bvd_list = list(set(bvd_list))

                return bvd_list, search_type, non_matching_items

            return bvd_list, search_type, non_matching_items

        def check_bvd_format(bvd_list, df):
            bvd_list = list(set(bvd_list))
            # Check against df['Code'].values
            df_code_values = df["Code"].values
            df_matches = [item for item in bvd_list if item in df_code_values]
            df_match_count = len(df_matches)

            # Check against the regex pattern
            pattern = re.compile(r"^[A-Za-z]+[*]?[A-Za-z]*\d*[-\dA-Za-z]*$")
            regex_matches = [item for item in bvd_list if pattern.match(item)]
            regex_match_count = len(regex_matches)

            # Determine which check has more matches
            if df_match_count >= regex_match_count:
                non_matching_items = [
                    item for item in bvd_list if item not in df_code_values
                ]
                return df_matches, True, non_matching_items
            else:
                non_matching_items = [
                    item for item in bvd_list if not pattern.match(item)
                ]
                return regex_matches, False, non_matching_items

        def set_bvd_list(bvd_list):
            df = self.search_country_codes()

            if (
                self._bvd_list[1] is not None and self._select_cols is not None
            ) and self._bvd_list[1] in self._select_cols:
                self._select_cols.remove(self._bvd_list[1])

            self._bvd_list = [None, None, None]
            search_word = None
            if (isinstance(bvd_list, str)) and os.path.isfile(bvd_list):
                bvd_list, search_type, non_matching_items = load_bvd_list(bvd_list, df)
            elif (isinstance(bvd_list, list) and len(bvd_list) == 2) and (
                isinstance(bvd_list[0], (list, pd.Series, np.ndarray))
                and isinstance(bvd_list[1], str)
            ):
                search_word = bvd_list[1]
                if isinstance(bvd_list[0], (pd.Series, np.ndarray)):
                    bvd_list = bvd_list[0].tolist()
                else:
                    bvd_list = bvd_list[0]
            else:
                if isinstance(bvd_list, (pd.Series, np.ndarray)):
                    bvd_list = bvd_list.tolist()

            bvd_list = _check_list_format(bvd_list)
            bvd_list, search_type, non_matching_items = check_bvd_format(bvd_list, df)

            return bvd_list, search_word, search_type, non_matching_items

        def set_bvd_col(search_word):
            if search_word is None:
                bvd_col = self.search_dictionary()
            else:
                bvd_col = self.search_dictionary(
                    search_word=search_word,
                    search_cols={
                        "Data Product": False,
                        "Table": False,
                        "Column": True,
                        "Definition": False,
                    },
                )

            if bvd_col.empty:
                raise ValueError("No 'bvd' columns were found for this table")

            bvd_col = bvd_col["Column"].unique().tolist()

            if len(bvd_col) > 1:
                if isinstance(search_word, str) and search_word in bvd_col:
                    self._bvd_list[1] = search_word
                else:
                    return bvd_col
            else:
                self._bvd_list[1] = bvd_col[0]

            return False

        async def f_bvd_prompt(bvd_list, non_matching_items):
            question = _CustomQuestion(
                f"The following elements does not seem to match bvd format: {non_matching_items}",
                ["keep", "remove", "cancel"],
            )
            answer = await question.display_widgets()

            if answer == "keep":
                print(f"The following bvd_id_numbers were kept:{non_matching_items}")
                self._bvd_list[0] = bvd_list + non_matching_items

            elif answer == "cancel":
                print("Adding the bvd list has been canceled")
                return
            else:
                print(f"The following bvd_id_numbers were removed:{non_matching_items}")
                self._bvd_list[0] = bvd_list

            if self._set_data_product is None or self._set_table is None:
                self.select_data()

            bvd_col = set_bvd_col(search_word)
            if bvd_col:
                _select_list(
                    "_SelectMultiple",
                    bvd_col,
                    "Columns:",
                    'Select "bvd" Columns to filtrate',
                    _select_bvd,
                    [self._bvd_list, self._select_cols, search_type],
                )
                return

            self._bvd_list[2] = _construct_query(
                self._bvd_list[1], self._bvd_list[0], search_type
            )

            if self._select_cols is not None:
                self._select_cols = _check_list_format(
                    self._select_cols, self._bvd_list[1], self._time_period[2]
                )

        self._bvd_list = [None, None, None]

        if bvd_list is not None:
            bvd_list, search_word, search_type, non_matching_items = set_bvd_list(
                bvd_list
            )

            if len(non_matching_items) > 0:
                asyncio.ensure_future(f_bvd_prompt(bvd_list, non_matching_items))
                return

            self._bvd_list[0] = bvd_list

            if self._set_data_product is None or self._set_table is None:
                self.select_data()

            bvd_col = set_bvd_col(search_word)
            if bvd_col:
                _select_list(
                    "_SelectMultiple",
                    bvd_col,
                    "Columns:",
                    'Select "bvd" Columns to filtrate',
                    _select_bvd,
                    [self._bvd_list, self._select_cols, search_type],
                )
                return

            self._bvd_list[2] = _construct_query(
                self._bvd_list[1], self._bvd_list[0], search_type
            )

        if self._select_cols is not None:
            self._select_cols = _check_list_format(
                self._select_cols, self._bvd_list[1], self._time_period[2]
            )

    @property
    def time_period(self):
        return self._time_period

    @time_period.setter
    def time_period(self, years: list = None):
        def check_year(years):
            # Get the current year
            current_year = datetime.now().year

            # Check if the list has exactly two elements
            if len(years) < 2:
                raise ValueError(
                    "The list must contain at least a start and end year e.g [1998,2005]. It can also contain a column name as a third element [1998,2005,'closing_date']"
                )

            # Initialize start and end year with default values
            start_year = years[0] if years[0] is not None else 1900
            end_year = years[1] if years[1] is not None else current_year

            # Check if years are integers
            if not isinstance(start_year, int) or not isinstance(end_year, int):
                raise ValueError("Both start year and end year must be integers")

            # Check if years are within a valid range
            if start_year < 1900 or start_year > current_year:
                raise ValueError(f"Start year must be between 1900 and {current_year}")
            if end_year < 1900 or end_year > current_year:
                raise ValueError(f"End year must be between 1900  and {current_year}")

            # Check if start year is less than or equal to end year
            if start_year > end_year:
                raise ValueError("Start year must be less than or equal to end year")

            if len(years) == 3:
                return [start_year, end_year, years[2]]
            else:
                return [start_year, end_year, None]

        if years is not None:
            if (
                self._time_period[2] is not None and self._select_cols is not None
            ) and self._time_period[2] in self._select_cols:
                self._select_cols.remove(self._time_period[2])

            self._time_period[:3] = check_year(years)

            if self._set_data_product is None or self._set_table is None:
                self.select_data()

            date_col = self.table_dates(
                data_product=self.set_data_product, table=self._set_table, save_to=None
            )

            if date_col.empty:
                raise ValueError("No data columns were found for this table")

            date_col = date_col["Column"].unique().tolist()

            if (
                self._time_period[2] is not None
                and self._time_period[2] not in date_col
            ):
                raise ValueError(
                    f"{self._time_period[2]} was not found as date related column: {date_col}. Set ._time_period[2] with the correct one"
                )

            elif self._time_period[2] is None and len(date_col) > 1:
                _select_list(
                    "_SelectList",
                    date_col,
                    "Columns:",
                    'Select "date" Column to filtrate',
                    _select_date,
                    [self._time_period, self._select_cols],
                )
                return

            if self._time_period[2] is None:
                self._time_period[2] = date_col[0]
        else:
            self._time_period = [None, None, None, "remove"]
        if self._select_cols is not None:
            self._select_cols = _check_list_format(
                self._select_cols, self._bvd_list[1], self._time_period[2]
            )

    @property
    def select_cols(self):
        return self._select_cols

    @select_cols.setter
    def select_cols(self, select_cols=None):
        if select_cols is not None:
            if self._set_data_product is None or self._set_table is None:
                self.select_data()

            select_cols = _check_list_format(
                select_cols, self._bvd_list[1], self._time_period[2]
            )

            table_cols = self.search_dictionary(
                data_product=self.set_data_product, table=self._set_table, save_to=None
            )

            if table_cols.empty:
                self._select_cols = None
                raise ValueError("No columns were found for this table")

            table_cols = table_cols["Column"].unique().tolist()

            if not all(element in table_cols for element in select_cols):
                not_found = [
                    element for element in select_cols if element not in table_cols
                ]
                print(
                    "The following selected columns cannot be found in the table columns",
                    not_found,
                )
                self._select_cols = None
            else:
                self._select_cols = select_cols
        else:
            self._select_cols = None

    def define_options(self):
        """Open the interactive options widget and apply selected processing options."""

        async def f(self):
            if self.output_format is None:
                self.output_format = [".csv"]

            config = {
                "delete_files": self.delete_files,
                "concat_files": self.concat_files,
                "output_format": self.output_format,
                "file_size_mb": self.file_size_mb,
            }

            Options_obj = _SelectOptions(config)

            config = await Options_obj.display_widgets()

            if config:
                self.delete_files = config["delete_files"]
                self.concat_files = config["concat_files"]
                self.file_size_mb = config["file_size_mb"]
                self.output_format = config["output_format"]

                if len(self.output_format) == 1 and self.output_format[0] is None:
                    self.output_format = None
                elif len(self.output_format) > 1:
                    self.output_format = [
                        x for x in self.output_format if x is not None
                    ]

                print("The following options were selected:")
                print(f"Delete Files: {self.delete_files}")
                print(f"Concatenate Files: {self.output_format}")
                print(f"Output File Size: {self.file_size_mb} MB")

        asyncio.ensure_future(f(self))

    def select_columns(self):
        """Open the interactive column selector for the active data product and table."""

        async def f(self, column, definition):
            combined = [
                f"{col}  -----  {defn}" for col, defn in zip(column, definition)
            ]

            Select_obj = _SelectMultiple(combined, "Columns:", "Select Table Columns")
            selected_list = await Select_obj.display_widgets()
            if selected_list is not None:
                # Create a dictionary to map selected strings to their indices in the combined list
                indices = {
                    item: combined.index(item)
                    for item in selected_list
                    if item in combined
                }

                # Extract selected columns based on indices
                selected_list = [
                    column[indices[item]] for item in selected_list if item in indices
                ]
                self._select_cols = selected_list
                self._select_cols = _check_list_format(
                    self._select_cols, self._bvd_list[1], self._time_period[2]
                )
                print(f"The following columns have been selected: {self._select_cols}")

        if self._set_data_product is None or self._set_table is None:
            self.select_data()

        table_cols = self.search_dictionary(
            data_product=self.set_data_product, table=self._set_table, save_to=None
        )

        if table_cols.empty:
            self._select_cols = None
            raise ValueError("No columns were found for this table")

        column = table_cols["Column"].tolist()
        definition = table_cols["Definition"].tolist()

        asyncio.ensure_future(f(self, column, definition))

    def search_dictionary(
        self,
        save_to: SaveFormat = None,
        search_word=None,
        search_cols: dict | None = None,
        letters_only: bool = False,
        extact_match: bool = False,
        data_product=None,
        table=None,
    ):
        """Search the bundled column dictionary and optionally persist results.

        Args:
            save_to: Output format (`"csv"` or `"xlsx"`). If `None`, nothing is saved.
            search_word: Value to search for across selected columns.
            search_cols: Column inclusion map.
            letters_only: Normalize both dictionary values and search term to alphanumeric.
            extact_match: Use equality instead of contains matching.
            data_product: Restrict search to one data product.
            table: Restrict search to one table.

        Returns:
            A filtered DataFrame, or an empty DataFrame when no matches are found.
        """
        if search_cols is None:
            search_cols = {
                "Data Product": True,
                "Table": True,
                "Column": True,
                "Definition": True,
            }

        if data_product is None and self.set_data_product is not None:
            data_product = self.set_data_product

            if table is None and self.set_table is not None:
                table = self.set_table

        if self._table_dictionary is None:
            self._table_dictionary = _table_dictionary()
        df = self._table_dictionary
        df = df[
            df["Data Product"].isin(
                self._tables_backup["Data Product"].drop_duplicates()
            )
        ]

        if data_product is not None:
            df_product = df.query(f"`Data Product` == '{data_product}'")
            if df_product.empty:
                print("No such Data Product was found. Please set right data product")
                return df_product
            else:
                df = df_product
            search_cols["Data Product"] = False
        if table is not None:
            df_table = df.query(f"`Table` == '{table}'")
            if df_table.empty:
                df_table = df.query(
                    f"`Table`.str.contains('{table}', case=False, na=False,regex=False)"
                )
                if df_table.empty:
                    print("No such Table was found. Please set right table")
                    return df_table
            search_cols["Table"] = False
            df = df_table

        if search_word is not None:
            if letters_only:
                df_backup = df.copy()
                search_word = _letters_only_regex(search_word)
                df = df.map(_letters_only_regex)

            if extact_match:
                base_string = "`{col}` ==  '{{search_word}}'"
            else:
                base_string = "`{col}`.str.contains('{{search_word}}', case=False, na=False,regex=False)"

            search_conditions = " | ".join(
                base_string.format(col=col)
                for col, include in search_cols.items()
                if include
            )
            final_string = search_conditions.format(search_word=search_word)

            df = df.query(final_string)

            if df.empty:
                base_string = "'{col}'"
                search_conditions = " , ".join(
                    base_string.format(col=col)
                    for col, include in search_cols.items()
                    if include
                )
                print(
                    "No such 'search word' was detected across columns: "
                    + search_conditions
                )
                return df

            if letters_only:
                df = df_backup.loc[df.index]

            if save_to:
                print("The following query was executed:" + final_string)

        _save_to(df, "dict_search", save_to)

        return df

    def table_dates(self, save_to: SaveFormat = None, data_product=None, table=None):
        """Return date-related columns for a table and optionally save the result."""

        if data_product is None and self.set_data_product is not None:
            data_product = self.set_data_product

            if table is None and self.set_table is not None:
                table = self.set_table

        if self._table_dates is None:
            self._table_dates = _table_dates()
        df = self._table_dates
        df = df[
            df["Data Product"].isin(
                self._tables_backup["Data Product"].drop_duplicates()
            )
        ]

        if data_product is not None:
            df_product = df.query(f"`Data Product` == '{data_product}'")
            if df_product.empty:
                print("No such Data Product was found. Please set right data product")
                return df_product
            else:
                df = df_product
        if table is not None:
            df_table = df.query(f"`Table` == '{table}'")
            if df_table.empty:
                df_table = df.query(
                    f"`Table`.str.contains('{table}', case=False, na=False,regex=False)"
                )
                if df_table.empty:
                    print("No such Table was found. Please set right table")
                    return df_table
            df = df_table

        _save_to(df, "date_cols_search", save_to)

        return df

    def search_country_codes(self, search_word=None, search_cols: dict | None = None):
        """Search country-code metadata by term across selected columns."""
        if search_cols is None:
            search_cols = {"Country": True, "Code": True}

        df = _country_codes()
        if search_word is not None:
            base_string = "`{col}`.str.contains('{{search_word}}', case=False, na=False,regex=False)"
            search_conditions = " | ".join(
                base_string.format(col=col)
                for col, include in search_cols.items()
                if include
            )
            final_string = search_conditions.format(search_word=search_word)

            df = df.query(final_string)

            if df.empty:
                base_string = "'{col}'"
                search_conditions = " , ".join(
                    base_string.format(col=col)
                    for col, include in search_cols.items()
                    if include
                )
                print(
                    "No such 'search word' was detected across columns: "
                    + search_conditions
                )
                return df
            else:
                print("The following query was executed:" + final_string)

        return df

    def process_one(self, save_to: SaveFormat = None, files=None, n_rows: int = 1000):
        """Process files and return up to `n_rows` rows, optionally saving the sample."""

        if files is None:
            if self._set_data_product is None or self._set_table is None:
                self.select_data()
            files = [self.remote_files[0]]
        elif isinstance(files, int):
            files = [files]

        pandas_bvd_query, polars_bvd_query = self._normalize_bvd_queries()
        chosen_engine, _ = self._choose_process_engine(
            files=files,
            query=self.query,
            raw_bvd_query=None,
            polars_bvd_query=polars_bvd_query,
        )

        if n_rows > 0 and len(files) == 1 and chosen_engine == "polars":
            df, _ = self.polars_all(files=files, num_workers=1, row_limit=n_rows)
            if isinstance(df, pl.DataFrame):
                df = df.to_pandas()
            self.dfs = df
        else:
            df, _ = self.process_all(
                files=files,
                num_workers=len(files),
                bvd_query=pandas_bvd_query,
            )

        if df.empty:
            print("No rows were retained")
            return df

        if n_rows > 0:
            df = df.head(n_rows)

        _save_to(df, "process_one", save_to)
        return df

    def _default_polars_bvd_query(self):
        if self._bvd_list[0] is None or self._bvd_list[1] is None:
            return None

        mode = "exact"
        if isinstance(self._bvd_list[2], str) and ".str.startswith(" in self._bvd_list[2]:
            mode = "prefix"

        return [self._bvd_list[0], self._bvd_list[1], mode]

    def _normalize_bvd_queries(self, bvd_query=None):
        if bvd_query is None:
            return self._bvd_list[2], self._default_polars_bvd_query()

        if isinstance(bvd_query, str):
            return bvd_query, None

        if not isinstance(bvd_query, (list, tuple)):
            raise ValueError(
                "bvd_query must be None, a pandas query string, or [values, columns[, mode]]."
            )

        if len(bvd_query) == 2:
            values, columns = bvd_query
            mode = "exact"
        elif len(bvd_query) == 3:
            values, columns, mode = bvd_query
        else:
            raise ValueError(
                "bvd_query must be [values, columns] or [values, columns, mode]."
            )

        if isinstance(values, str):
            values = [values]
        elif isinstance(values, (pd.Series, np.ndarray)):
            values = values.tolist()
        elif not isinstance(values, list):
            values = [values]

        if isinstance(columns, (pd.Series, np.ndarray)):
            columns = columns.tolist()

        if isinstance(columns, list) and len(columns) == 1:
            columns = columns[0]

        mode = str(mode).lower()
        if mode not in ["exact", "prefix"]:
            raise ValueError("BvD filter mode must be 'exact' or 'prefix'.")

        pandas_bvd_query = _construct_query(columns, values, search_type=mode == "prefix")
        polars_bvd_query = [values, columns, mode]

        return pandas_bvd_query, polars_bvd_query

    def _describe_polars_limitation(self, reason: str) -> str:
        if reason.startswith("unsupported_format:"):
            file_format = reason.split(":", 1)[1]
            return f"unsupported file format '{file_format}'"

        descriptions = {
            "string_query": "string queries require pandas query semantics",
            "string_bvd_query": "string-based bvd_query values cannot be translated safely to Polars",
            "callable_query": "callable query is not marked as Polars-compatible",
            "pool_method": "custom pool_method is only supported by the pandas backend",
            "n_batches": "custom batching is only supported by the pandas backend",
            "concat_files_false": "concat_files=False is only supported by the pandas backend",
            "mixed_formats": "mixed file extensions are only supported by the pandas backend",
            "multi_file_xlsx": "Polars only supports a single XLSX file per call",
        }
        return descriptions.get(reason, reason)

    def _record_process_backend(self, engine: str, reason: str):
        self._last_process_engine = engine
        self._last_process_reason = reason

    def _choose_process_engine(
        self,
        files,
        query=None,
        pool_method=None,
        n_batches: int = None,
        raw_bvd_query=None,
        polars_bvd_query=None,
    ):
        query_supports_polars = callable(query) and getattr(
            query, "_supports_polars", False
        )

        if raw_bvd_query is not None and polars_bvd_query is None:
            return "pandas", "string_bvd_query"

        if isinstance(query, str):
            return "pandas", "string_query"

        if callable(query) and not query_supports_polars:
            return "pandas", "callable_query"

        if pool_method is not None:
            return "pandas", "pool_method"

        if n_batches is not None:
            return "pandas", "n_batches"

        if self.concat_files is False:
            return "pandas", "concat_files_false"

        files = [files] if isinstance(files, (str, os.PathLike)) else files
        file_extensions = {
            os.fspath(file).lower().rsplit(".", 1)[-1]
            for file in files
            if isinstance(file, (str, os.PathLike)) and "." in os.fspath(file)
        }

        if len(file_extensions) != 1:
            return "pandas", "mixed_formats"

        file_extension = next(iter(file_extensions))
        if file_extension not in {"csv", "parquet", "avro", "xlsx"}:
            return "pandas", f"unsupported_format:{file_extension}"

        if file_extension == "xlsx" and len(files) > 1:
            return "pandas", "multi_file_xlsx"

        return "polars", "compatible"

    def pandas_all(
        self,
        files: list = None,
        destination: str = None,
        num_workers: int = -1,
        n_batches: int = None,
        select_cols: list = None,
        date_query=None,
        bvd_query=None,
        query=None,
        query_args: list = None,
        pool_method=None,
    ):
        """Process files with the pandas backend.

        Returns:
            Tuple of `(df, file_names)` where `df` is a DataFrame (possibly empty).

        Raises:
            ValueError: Invalid arguments or file-selection state.
            TimeoutError: Downloads are still in progress beyond timeout.
        """
        self._record_process_backend("pandas", "direct")

        def batch_processing(n_batches: int = None):
            def batch_list(input_list, batch_size):
                """Splits the input list into batches of a given size."""
                batches = []
                for i in range(0, len(input_list), batch_size):
                    batches.append(input_list[i : i + batch_size])
                return batches

            if n_batches is not None and isinstance(n_batches, (int, float)):
                if n_batches < 1:
                    raise ValueError("n_batches must be >= 1")
                batch_size = max(1, len(files) // int(n_batches))
                batches = batch_list(files, batch_size)
            else:
                batches = batch_list(files, num_workers)

            lists = []

            print(f"Processing {len(files)} files in Parallel")

            for index, batch in enumerate(batches, start=1):
                print(f"Processing Batch {index} of {len(batches)}")
                print(f"------ First file: '{batch[0]}'")
                print(f"------ Last file : '{batch[-1]}'")
                params_list = [
                    (
                        file,
                        destination,
                        select_cols,
                        date_query,
                        bvd_query,
                        query,
                        query_args,
                    )
                    for file in batch
                ]
                list_batch = _run_parallel(
                    fnc=self._process_parallel,
                    params_list=params_list,
                    n_total=len(batch),
                    num_workers=num_workers,
                    pool_method=pool_method,
                    msg="Processing",
                )
                lists.extend(list_batch)

            file_names = [elem[1] for elem in lists]
            file_names = [
                file_name[0] for file_name in file_names if file_name is not None
            ]

            dfs = [elem[0] for elem in lists]
            dfs = [df for df in dfs if df is not None]

            flags = [elem[2] for elem in lists]

            return dfs, file_names, flags

        files = self.remote_files if files is None else files
        if isinstance(files, (str, os.PathLike)):
            files = [files]
        date_query = self.time_period if date_query is None else date_query
        bvd_query = self._bvd_list[2] if bvd_query is None else bvd_query
        query = self.query if query is None else query
        query_args = self.query_args if query_args is None else query_args
        select_cols = self._select_cols if select_cols is None else select_cols

        if isinstance(query, pl.Expr):
            raise ValueError(
                "Polars expressions are not supported in pandas_all(). "
                "Use polars_all() or process_all(engine='polars')."
            )

        # To handle executing when download_all() have not finished!
        if not self._check_download(files):
            raise TimeoutError(
                "Files have not finished downloading within the expected timeout."
            )

        select_cols, files, destination = self._validate_args(
            files=files,
            destination=destination,
            select_cols=select_cols,
            date_query=date_query,
            bvd_query=bvd_query,
            query=query,
        )

        # Set num_workers
        num_workers = set_workers(
            num_workers, int(psutil.virtual_memory().total / (1024**3) / 12)
        )

        # Read multithreaded
        if num_workers != 1 and len(files) > 1:
            dfs, file_names, flags = batch_processing(n_batches)
        else:  # Read Sequential
            print(f"Processing  {len(files)} files in sequence")
            dfs, file_names, flags = self._process_sequential(
                files,
                destination,
                select_cols,
                date_query,
                bvd_query,
                query,
                query_args,
                num_workers,
            )

        if self.concat_files and len(dfs) > 0:
            # Concatenate and save when requested.
            self.dfs, file_names = _save_chunks(
                dfs=dfs,
                file_name=destination,
                output_format=self.output_format,
                file_size=self.file_size_mb,
                num_workers=num_workers,
            )
        elif len(dfs) > 0:
            self.dfs = pd.concat(dfs, ignore_index=True)
        else:
            self.dfs = pd.DataFrame()

        return self.dfs, file_names

    def process_all(
        self,
        files: list = None,
        destination: str = None,
        num_workers: int = -1,
        n_batches: int = None,
        select_cols: list = None,
        date_query=None,
        bvd_query=None,
        query=None,
        query_args: list = None,
        pool_method=None,
        engine: str = "auto",
    ):
        """Process files using pandas or Polars while always returning pandas output.

        `engine="auto"` prefers the Polars backend for supported workloads and
        falls back to pandas for features that still depend on pandas semantics.
        """

        files = self.remote_files if files is None else files
        if isinstance(files, (str, os.PathLike)):
            files = [files]
        date_query = self.time_period if date_query is None else date_query
        query = self.query if query is None else query
        query_args = self.query_args if query_args is None else query_args
        select_cols = self._select_cols if select_cols is None else select_cols
        pandas_bvd_query, polars_bvd_query = self._normalize_bvd_queries(bvd_query)

        if engine not in {"auto", "pandas", "polars"}:
            raise ValueError("engine must be 'auto', 'pandas', or 'polars'.")

        if engine == "pandas":
            result = self.pandas_all(
                files=files,
                destination=destination,
                num_workers=num_workers,
                n_batches=n_batches,
                select_cols=select_cols,
                date_query=date_query,
                bvd_query=pandas_bvd_query,
                query=query,
                query_args=query_args,
                pool_method=pool_method,
            )
            self._record_process_backend("pandas", "explicit")
            return result

        chosen_engine, reason = self._choose_process_engine(
            files=files,
            query=query,
            pool_method=pool_method,
            n_batches=n_batches,
            raw_bvd_query=bvd_query,
            polars_bvd_query=polars_bvd_query,
        )

        if engine == "polars" and chosen_engine != "polars":
            raise ValueError(
                "process_all(engine='polars') cannot use the Polars backend: "
                + self._describe_polars_limitation(reason)
            )

        if chosen_engine == "pandas":
            if engine == "auto" and isinstance(query, pl.Expr):
                raise ValueError(
                    "process_all(engine='auto') cannot fall back to pandas because "
                    + self._describe_polars_limitation(reason)
                    + ". Use engine='polars' or a pandas-compatible query."
                )

            result = self.pandas_all(
                files=files,
                destination=destination,
                num_workers=num_workers,
                n_batches=n_batches,
                select_cols=select_cols,
                date_query=date_query,
                bvd_query=pandas_bvd_query,
                query=query,
                query_args=query_args,
                pool_method=pool_method,
            )
            self._record_process_backend("pandas", reason)
            return result

        df, file_names = self.polars_all(
            files=files,
            destination=destination,
            num_workers=num_workers,
            select_cols=select_cols,
            date_query=date_query,
            bvd_query=polars_bvd_query,
            query=query,
            query_args=query_args,
        )

        if isinstance(df, pl.DataFrame):
            df = df.to_pandas()

        self.dfs = df
        self._record_process_backend(
            "polars", "explicit" if engine == "polars" else reason
        )
        return df, file_names

    def polars_all(
        self,
        files: list = None,
        destination: str = None,
        num_workers: int = -1,
        select_cols: list = None,
        date_query=None,
        bvd_query: list | tuple | None = None,
        query=None,
        query_args: list = None,
        row_limit: int | None = None,
    ):
        """Process files with the polars-based pipeline.

        Returns:
            Tuple of `(df, file_names)` where `df` is a DataFrame.

        Raises:
            ValueError: Invalid arguments or file-selection state.
            TimeoutError: Downloads are still in progress beyond timeout.
        """
        self._record_process_backend("polars", "direct")
        files = self.remote_files if files is None else files
        if isinstance(files, (str, os.PathLike)):
            files = [files]
        date_query = self.time_period if date_query is None else date_query
        bvd_query = self._default_polars_bvd_query() if bvd_query is None else bvd_query
        query = self.query if query is None else query
        query_args = self.query_args if query_args is None else query_args
        select_cols = self._select_cols if select_cols is None else select_cols

        current_concat_files = self.concat_files
        self.concat_files = True

        try:
            # To handle executing when download_all() have not finished!
            if not self._check_download(files):
                raise TimeoutError(
                    "Files have not finished downloading within the expected timeout."
                )

            _, files, destination = self._validate_args(
                files=files,
                destination=destination,
                select_cols=select_cols,
                date_query=date_query,
                bvd_query=bvd_query,
                query=query,
            )

            print(f"Processing  {len(files)} files using polars")
            dfs = self._process_polars(
                files,
                destination,
                select_cols,
                date_query,
                bvd_query,
                query,
                query_args,
                row_limit=row_limit,
            )

            # Set num_workers
            num_workers = set_workers(num_workers, int(cpu_count() - 2))

            # Concatenate and save
            self.dfs, file_names = _save_chunks(
                dfs=dfs,
                file_name=destination,
                output_format=self.output_format,
                file_size=self.file_size_mb,
                num_workers=num_workers,
            )
        finally:
            self.concat_files = current_concat_files

        return self.dfs, file_names

    def download_all(
        self, files: list = None, num_workers: int = None, async_mode: bool = True
    ):
        """Download missing remote files to the local path, optionally in async mode."""

        files = files or self.remote_files

        if hasattr(os, "fork"):
            pool_method = "fork"
        else:
            print("Function only works on Unix systems right now")
            return

        if self._set_data_product is None or self._set_table is None:
            self.select_data()

        # Set num_workers
        num_workers = set_workers(num_workers, int(cpu_count() - 2))

        _, _ = self._check_args(files)

        self._download_finished = None
        self.delete_files = False

        missing_files = [
            file for file in files if not os.path.exists(self._file_exist(file)[0])
        ]

        if missing_files:
            print(f"Downloading {len(missing_files)} of {len(files)} files ")

            current_value = self.concat_files

            self.concat_files = False
            if async_mode:
                # process = Process(target=self.process_all, kwargs={'files':missing_files,'num_workers': num_workers, 'pool_method': pool_method,'n_batches':1})
                process = Process(
                    target=_run_parallel,
                    kwargs={
                        "fnc": self._get_file,
                        "params_list": [(file) for file in missing_files],
                        "n_total": len(missing_files),
                        "num_workers": num_workers,
                        "pool_method": pool_method,
                        "msg": "Downloading",
                    },
                )
                process.start()
                self.concat_files = current_value
                self._download_finished = False
            else:
                # self.process_all(files = missing_files, num_workers = num_workers, pool_method = pool_method, n_batches =  1,select_cols=None)

                _run_parallel(
                    fnc=self._get_file,
                    params_list=[(file) for file in missing_files],
                    n_total=len(missing_files),
                    num_workers=num_workers,
                    pool_method=pool_method,
                    msg="Downloading",
                )

            self.concat_files = current_value
        else:
            print("Are files are already downloaded")

    def _file_exist(self, file: str):
        base_path = os.getcwd()
        base_path = base_path.replace("\\", "/")

        if not file.startswith(base_path):
            file = os.path.join(base_path, file)

        if len(file) > self._max_path_length:
            raise ValueError(
                f"file path is longer ({len(file)}) than the max path length of the OS {self._max_path_length}. Set '.local_path' closer to root. The file is : '{file}'"
            )

        if os.path.exists(file):
            self.delete_files = False
            flag = True
        else:
            file = str(Path(self._local_path) / os.path.basename(file))
            # file = self._local_path + "/" + os.path.basename(file)
            flag = False
            if not file.startswith(base_path):
                file = str(Path(base_path) / file)
                # file = base_path + "/" + file

        if len(file) > self._max_path_length:
            raise ValueError(
                f"file path is longer ({len(file)}) than the max path length of the OS {self._max_path_length}. Set '.local_path' closer to root. The file is : '{file}'"
            )

        return file, flag

    def _get_file(self, file: str):
        local_file, flag = self._file_exist(file)

        if not os.path.exists(local_file):
            try:
                with self._connect() as sftp:
                    remote_file = str(self.remote_path + "/" + os.path.basename(file))
                    # remote_file = os.path.normpath(os.path.join(self.remote_path,os.path.basename(file)))
                    sftp.get(remote_file, local_file)
                    file_attributes = sftp.stat(remote_file)
                    time_stamp = file_attributes.st_mtime
                    os.utime(local_file, (time_stamp, time_stamp))
            except Exception as e:
                raise ValueError(f"Error reading remote file: {e}") from e

        return local_file, flag

    def _curate_file(
        self,
        flag: bool,
        destination: str,
        local_file: str,
        select_cols: list,
        date_query: list | None = None,
        bvd_query: str | None = None,
        query=None,
        query_args: list | None = None,
        num_workers: int = -1,
    ):
        if date_query is None:
            date_query = [None, None, None, "remove"]
        df = None
        file_name = None
        if any([select_cols, query, all(date_query), bvd_query]) or flag:
            file_extension = local_file.lower().split(".")[-1]
            if file_extension in ["csv"]:
                df = _load_csv_table(
                    file=local_file,
                    select_cols=select_cols,
                    date_query=date_query,
                    bvd_query=bvd_query,
                    query=query,
                    query_args=query_args,
                    num_workers=num_workers,
                )
            else:
                df = _load_pd(
                    file=local_file,
                    select_cols=select_cols,
                    date_query=date_query,
                    bvd_query=bvd_query,
                    query=query,
                    query_args=query_args,
                )

            if (
                df is not None
                and self.concat_files is False
                and self.output_format is not None
            ) and not flag:
                file_name, _ = os.path.splitext(
                    str(Path(destination) / os.path.basename(local_file))
                )
                # file_name, _ = os.path.splitext(destination + "/" + os.path.basename(local_file))
                file_name = _save_files_pd(df, file_name, self.output_format)
                df = None

            if self.delete_files and not flag:
                try:
                    os.remove(local_file)
                except Exception as e:
                    raise ValueError(f"Error deleting local file: {local_file}") from e
        else:
            file_name = local_file

        return df, file_name

    def _process_sequential(
        self,
        files: list,
        destination: str = None,
        select_cols: list | None = None,
        date_query: list | None = None,
        bvd_query: str | None = None,
        query=None,
        query_args: list | None = None,
        num_workers: int = -1,
    ):
        dfs = []
        file_names = []
        flags = []
        total_files = len(files)
        for i, file in enumerate(files, start=1):
            if total_files > 1:
                print(f"{i} of {total_files} files")
            try:
                local_file, flag = self._get_file(file)
                df, file_name = self._curate_file(
                    flag=flag,
                    destination=destination,
                    local_file=local_file,
                    select_cols=select_cols,
                    date_query=date_query,
                    bvd_query=bvd_query,
                    query=query,
                    query_args=query_args,
                    num_workers=num_workers,
                )
                flags.append(flag)

                if df is not None:
                    dfs.append(df)
                else:
                    file_names.append(file_name)
            except ValueError as e:
                print(e)

        return dfs, file_names, flags

    def _process_polars(
        self,
        files: list = None,
        destination: str = None,
        select_cols: list = None,
        date_query: list | None = None,
        bvd_query: list | tuple | None = None,
        query=None,
        query_args: list = None,
        num_workers: int = -1,
        row_limit: int | None = None,
    ):
        if date_query is None:
            date_query = [None, None, None, "remove"]

        files = files or self.remote_files

        self.download_all(files=files, num_workers=num_workers, async_mode=False)

        print("### Start processing files with polars")

        local_files = [
            self._file_exist(file)[0]
            for file in files
            if os.path.exists(self._file_exist(file)[0])
        ]

        df = _load_pl(
            file_list=local_files,
            select_cols=select_cols,
            date_query=date_query,
            bvd_query=bvd_query,
            query=query,
            query_args=query_args,
            row_limit=row_limit,
        )

        return df

    def _process_parallel(self, inputs_args: list):
        file, destination, select_cols, date_query, bvd_query, query, query_args = (
            inputs_args
        )
        local_file, flag = self._get_file(file)
        df, file_name = self._curate_file(
            flag=flag,
            destination=destination,
            local_file=local_file,
            select_cols=select_cols,
            date_query=date_query,
            bvd_query=bvd_query,
            query=query,
            query_args=query_args,
        )

        return [df, file_name, flag]

    def _check_args(self, files: list, destination=None, flag: bool = False):
        def _detect_files(files):
            def format_timestamp(timestamp: str) -> str:
                formatted_timestamp = timestamp.replace(" ", "_").replace(":", "-")
                return formatted_timestamp

            if isinstance(files, str):
                files = [files]
            elif isinstance(files, list) and len(files) == 0:
                raise ValueError("'files' is a empty list")
            elif not isinstance(files, list):
                raise ValueError("'files' should be str or list formats")

            existing_files = [file for file in files if os.path.exists(file)]
            missing_files = [file for file in files if not os.path.exists(file)]

            if not existing_files:
                if not self.local_files and not self.remote_files:
                    raise ValueError("No local or remote files detected")

                if self._local_path is None and self._remote_path is not None:
                    if self.set_table is None:
                        raise ValueError(
                            "Table is not set. Please select a table first "
                            "before calling process_all (e.g. via define_options/select_columns)."
                        )

                    if self.set_data_product is None:
                        raise ValueError(
                            "Data Product is not set. Please select a data product and table first "
                            "before calling process_all (e.g. via define_options/select_columns)."
                        )

                    if self._time_stamp:
                        folder_name = f"{self.set_data_product}_exported {format_timestamp(self._time_stamp)}"
                    else:
                        folder_name = self.set_data_product

                    path = str(Path("Data Products") / folder_name / self.set_table)

                    # if self._time_stamp and self.set_data_product is not None:
                    #    path = "Data Products" + "/" + self.set_data_product +'_exported '+ format_timestamp(self._time_stamp) + "/" + self.set_table
                    # else:
                    #    path = "Data Products" + "/" + self.set_data_product + "/" + self.set_table

                    self.local_path = path

                missing_files = [
                    file
                    for file in files
                    if file not in self._remote_files and file not in self.local_files
                ]
                existing_files = [
                    file
                    for file in files
                    if file in self._remote_files or file in self.local_files
                ]

            if not existing_files:
                raise ValueError("Requested files cannot be found locally or remotely")

            return existing_files, missing_files

        files, missing_files = _detect_files(files)

        if missing_files:
            print("Missing files:")
            for file in missing_files:
                print(file)

        if destination is None and flag:
            current_time = datetime.now()
            timestamp_str = current_time.strftime("%y%m%d%H%M")

            if self._remote_path is not None:
                suffix = os.path.basename(self._remote_path)
            else:
                suffix = os.path.basename(self._local_path)

            destination = f"{timestamp_str}_{suffix}"

            base_path = os.getcwd()
            base_path = base_path.replace("\\", "/")

            destination = str(Path(base_path) / destination)
            # destination = base_path + "/" + destination

        if self.concat_files is False and destination is not None:
            if not os.path.exists(destination):
                os.makedirs(destination)
        elif self.concat_files is True and destination is not None:
            parent_directory = os.path.dirname(destination)

            if parent_directory and not os.path.exists(parent_directory):
                os.makedirs(parent_directory)

        return files, destination

    def _check_download(self, files):
        # To handle executing when download_all() have not finished!
        if self._download_finished is False and all(
            file in self._remote_files for file in files
        ):
            start_time = time.time()
            timeout = 5
            files_not_ready = not all(file in self.local_files for file in files)
            while files_not_ready:
                time.sleep(0.1)
                files_not_ready = not all(file in self.local_files for file in files)
                if time.time() - start_time >= timeout:
                    print(
                        f"Files have not finished downloading within the timeout period of {timeout} seconds."
                    )
                    return False

            self._download_finished = True
        return True

    def _validate_args(
        self,
        files: list = None,
        destination: str = None,
        select_cols: list = None,
        date_query=None,
        bvd_query=None,
        query=None,
    ):
        if select_cols is not None:
            select_cols = _check_list_format(
                select_cols, self._bvd_list[1], self._time_period[2]
            )

        has_select_cols = select_cols is not None and len(select_cols) > 0
        has_query = query is not None
        has_date_query = date_query is not None and all(date_query)
        has_bvd_query = bvd_query is not None

        flag = (
            any([has_select_cols, has_query, has_date_query, has_bvd_query])
            and self.output_format
        )
        files, destination = self._check_args(files, destination, flag)
        return select_cols, files, destination


def set_workers(num_workers, default_value: int):
    if isinstance(num_workers, (int, float, complex)) and num_workers != 1:
        num_workers = int(num_workers)
    else:
        num_workers = -1

    if num_workers < 1:
        num_workers = default_value

    return num_workers
