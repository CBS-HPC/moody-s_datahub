import copy
import importlib.resources as pkg_resources
import os
import shutil
from datetime import datetime
from multiprocessing import cpu_count
from pathlib import Path

import pandas as pd
import polars as pl
import pyarrow.parquet as pq

from .load_data import _table_dictionary
from .process import _Process
from .utils import (
    SaveFormat,
    _bvd_changes_ray,
    _letters_only_regex,
    _read_pd,
    _read_pl,
    _save_to,
    fuzzy_match_pl,
    fuzzy_query,
    profile_dataframe,
    save_profile_report,
)


# Defining Sftp Class
class Sftp(_Process):
    """High-level API for selecting, downloading, and processing DataHub exports."""

    def __init__(
        self,
        hostname: str = None,
        username: str = None,
        port: int = 22,
        privatekey: str = None,
        data_product_template: str = None,
        local_repo: str = None,
        download_root: str = None,
        output_root: str = None,
        server_cleanup: bool | None = None,
        interactive: bool = True,
        offline: bool = False,
        allow_invalid_bvd_ids: bool = False,
    ):
        """Initialize SFTP credentials, load table metadata, and apply server cleanup policy.

        Args:
            hostname: Optional SFTP hostname.
            username: Optional SFTP username.
            port: SFTP port.
            privatekey: Path to private key.
            data_product_template: Optional product template path.
            local_repo: Optional local export repository.
            download_root: Optional root directory for downloaded remote files.
                Defaults to ``Data Products`` in the current working directory.
            output_root: Optional root directory for auto-generated processed
                outputs. Explicit ``destination`` values still take precedence.
            server_cleanup: Cleanup response mode for CBS server prompt.
                - ``None``: keep interactive prompt behavior.
                - ``True``: auto-approve cleanup when prompt would be shown.
                - ``False``: skip cleanup prompt and deletion.
            interactive: Enable widget-based interactive flows. Set to ``False``
                for script/batch execution to avoid widget prompts.
            offline: Initialize without SFTP login. Packaged metadata helpers
                remain available, but remote discovery/download methods require
                a normal SFTP or ``local_repo`` session.
            allow_invalid_bvd_ids: In non-interactive mode, keep values that do
                not match the built-in BvD/country-code format check and treat
                them as exact IDs instead of raising.
        """

        # Initialize mixins
        _Process.__init__(self)

        self.privatekey: str = privatekey
        self.port: int = port
        self._interactive: bool = interactive
        self._offline: bool = offline
        self.allow_invalid_bvd_ids: bool = allow_invalid_bvd_ids
        self._download_root: str | None = (
            os.path.abspath(download_root) if download_root else None
        )
        self._output_root: str | None = (
            os.path.abspath(output_root) if output_root else None
        )

        # Try connecting to CBS servers unless explicitly running offline.
        if offline:
            self.hostname = hostname
            self.username = username
        elif privatekey and all([hostname, username]) is False:
            usernames = ["D2vdz8elTWKyuOcC2kMSnw", "aN54UkFxQPCOIEtmr0FmAQ"]
            for username in usernames:
                self.hostname: str = (
                    "s-f2112b8b980e44f9a.server.transfer.eu-west-1.amazonaws.com"
                )
                self.username: str = username
                try:
                    self._connect()
                    break
                except Exception:
                    pass
        else:
            self.hostname: str = hostname
            self.username: str = username

        if local_repo:
            local_repo = os.path.abspath(local_repo)
            if os.path.exists(local_repo):
                self._local_repo = local_repo
            else:
                print(f"Provided local_repo does not exist: {local_repo}")
                return
        else:
            self._local_repo: str = None

        self._object_defaults()

        if offline:
            dictionary = _table_dictionary()
            self._table_dictionary = dictionary
            self._tables_available = (
                dictionary[["Data Product", "Table"]].drop_duplicates().copy()
            )
            self._tables_available["Base Directory"] = None
            self._tables_available["Timestamp"] = None
            self._tables_available["Export"] = "packaged_dictionary"
            self._tables_available["Top-level Directory"] = None
            self._tables_backup = self._tables_available.copy()
            to_delete = []
        else:
            _, to_delete = self.tables_available(product_overview=data_product_template)

        if server_cleanup is None and self._interactive is False:
            server_cleanup = False

        if not offline:
            self._server_clean_up(to_delete, prompt_response=server_cleanup)

    def copy_obj(self):
        """Return a deep copy with defaults restored and interactive data selection triggered."""
        SFTP = copy.deepcopy(self)
        SFTP._object_defaults()
        SFTP.select_data()

        return SFTP

    def offline_capabilities(self):
        """Return which high-level methods can run without an SFTP login."""
        rows = [
            {
                "method": "search_dictionary",
                "offline_safe": True,
                "local_repo_safe": True,
                "server_required": False,
                "notes": "Uses packaged data_dict.xlsx metadata.",
            },
            {
                "method": "table_dates",
                "offline_safe": True,
                "local_repo_safe": True,
                "server_required": False,
                "notes": "Uses packaged date_cols.xlsx metadata.",
            },
            {
                "method": "search_country_codes",
                "offline_safe": True,
                "local_repo_safe": True,
                "server_required": False,
                "notes": "Uses packaged country_codes.xlsx metadata.",
            },
            {
                "method": "company_suffix",
                "offline_safe": True,
                "local_repo_safe": True,
                "server_required": False,
                "notes": "Returns a built-in list.",
            },
            {
                "method": "orbis_to_moodys",
                "offline_safe": True,
                "local_repo_safe": True,
                "server_required": False,
                "notes": "Uses local input files and packaged dictionary metadata.",
            },
            {
                "method": "get_column_names",
                "offline_safe": True,
                "local_repo_safe": True,
                "server_required": False,
                "notes": "Offline with dictionary metadata or local parquet files.",
            },
            {
                "method": "process_one/process_all/pandas_all/polars_all",
                "offline_safe": "conditional",
                "local_repo_safe": True,
                "server_required": "conditional",
                "notes": "No login needed when files are local or local_repo is configured.",
            },
            {
                "method": "profile_table/profile_tables",
                "offline_safe": "conditional",
                "local_repo_safe": True,
                "server_required": "conditional",
                "notes": "No login needed when profiled files are local or local_repo is configured.",
            },
            {
                "method": "search_company_names/search_bvd_changes",
                "offline_safe": "conditional",
                "local_repo_safe": True,
                "server_required": "conditional",
                "notes": "Need local data files or local_repo; otherwise remote files are required.",
            },
            {
                "method": "download_all",
                "offline_safe": False,
                "local_repo_safe": False,
                "server_required": True,
                "notes": "Downloads missing files from SFTP.",
            },
            {
                "method": "tables_available",
                "offline_safe": "packaged_catalog",
                "local_repo_safe": True,
                "server_required": "conditional",
                "notes": "Offline mode exposes packaged product/table metadata, not licensed remote availability.",
            },
        ]
        return pd.DataFrame(rows)

    def orbis_to_moodys(self, file):
        """Map Orbis result-column headers to DataHub dictionary columns."""

        def _load_orbis_file(file):
            df = pd.read_excel(file, sheet_name="Results")

            # Get the headings (column names) from the DataFrame
            headings = df.columns.tolist()

            # Process headings to keep only the first line if they contain multiple lines
            processed_headings = [heading.split("\n")[0] for heading in headings]

            # Keep only unique headings
            unique_headings = list(set(processed_headings))
            unique_headings.remove("Unnamed: 0")
            return unique_headings

        def sort_by(df):
            # Sort by 'Data Product'
            df_sorted = df.sort_values(by="Data Product")

            # Count unique headings for each 'Data Product'
            grouped = (
                df_sorted.groupby("Data Product")["heading"].nunique().reset_index()
            )
            grouped.columns = ["Data Product", "unique_headings"]

            # Sort 'Data Product' based on the number of unique headings in descending order
            sorted_products = grouped.sort_values(
                by="unique_headings", ascending=False
            )["Data Product"]

            # Reorder the original DataFrame based on the sorted 'Data Product'
            df_reordered = pd.concat(
                [
                    df_sorted[df_sorted["Data Product"] == product]
                    for product in sorted_products
                ],
                ignore_index=True,
            )
            return df_reordered

        headings = _load_orbis_file(file)
        headings_processed = [_letters_only_regex(heading) for heading in headings]

        df = _table_dictionary()
        df["letters_only"] = df["Column"].apply(_letters_only_regex)

        found = []
        not_found = []
        for heading, heading_processed in zip(headings, headings_processed):
            df_sel = df.query(f"`letters_only` == '{heading_processed}'")

            if df_sel.empty:
                not_found.append(heading)
            else:
                df_sel = df_sel.copy()  # Avoid SettingWithCopyWarning
                df_sel["heading"] = heading
                found.append(df_sel)

        # Concatenate all found DataFrames if needed
        if found:
            found = pd.concat(found, ignore_index=True)
            found = sort_by(found)
        else:
            found = pd.DataFrame()

        return found, not_found

    def get_column_names(self, save_to: SaveFormat = None, files=None):
        """Return table column names from dictionary metadata or from parquet file schema."""

        def from_dictionary(self):
            if self.set_table is not None:
                df = self.search_dictionary(save_to=None)
                column_names = df["Column"].to_list()
                return column_names
            else:
                return None

        def from_files(self, files):
            if files is None and self.remote_files is None:
                raise ValueError("No files were added")
            elif files is None and self.remote_files is not None:
                files = self.remote_files

            try:
                file, _ = self._check_args([files[0]])
                file, _ = self._get_file(file[0])
                parquet_file = pq.ParquetFile(file)
                # Get the column names
                column_names = parquet_file.schema.names
                return column_names
            except ValueError as e:
                print(e)
                return None

        if files is not None:
            column_names = from_files(self, files)
        else:
            column_names = from_dictionary(self)

        if column_names is not None:
            df = pd.DataFrame({"Column_Names": column_names})
            _save_to(df, "column_names", save_to)

        return column_names

    def _read_profile_file(self, file):
        """Read one local file for profiling without applying filters."""
        try:
            source = _read_pl([file])
            if isinstance(source, pl.LazyFrame):
                return source.collect()
            return source
        except Exception:
            return _read_pd(file, select_cols=None)

    @staticmethod
    def _safe_report_name(value):
        value = str(value or "profile").strip()
        value = value.replace("\\", "_").replace("/", "_")
        return "".join(
            char if char.isalnum() or char in "._- " else "_" for char in value
        )

    def _default_profile_report_path(self, data_product=None, table=None, multiple=False):
        base = Path("Table Profiles")
        if multiple:
            name = self._safe_report_name(data_product or "moodys_table_profiles")
            return str(base / f"{name}_profiles.xlsx")
        product = self._safe_report_name(data_product or self.set_data_product)
        table_name = self._safe_report_name(table or self.set_table)
        return str(base / product / f"{table_name}_profile.xlsx")

    def profile_table(
        self,
        data_product: str = None,
        table: str = None,
        file: str | int = None,
        operation_hints: bool = True,
        save_report: bool = False,
        report_path: str = None,
        dry_run: bool = False,
    ):
        """Profile the first file for a table without exposing source values."""

        profiler = copy.deepcopy(self)
        if data_product is not None:
            profiler.set_data_product = data_product
        if table is not None:
            profiler.set_table = table

        if profiler.set_data_product is None or profiler.set_table is None:
            if not getattr(profiler, "_interactive", True):
                raise ValueError(
                    "profile_table() requires data_product/table in non-interactive mode."
                )
            profiler.select_data()

        if file is None:
            if not profiler.remote_files:
                raise ValueError("No remote files detected for the selected table.")
            selected_file = profiler.remote_files[0]
        elif isinstance(file, int):
            selected_file = profiler.remote_files[file]
        else:
            selected_file = file

        if dry_run:
            return pd.DataFrame(
                [
                    {
                        "data_product": profiler.set_data_product,
                        "table": profiler.set_table,
                        "file_name": os.path.basename(selected_file),
                        "sample_strategy": "first_file",
                        "would_download": selected_file in profiler.remote_files,
                        "would_profile": True,
                        "would_write_report": bool(save_report or report_path),
                        "report_path": report_path
                        or (
                            self._default_profile_report_path(
                                profiler.set_data_product, profiler.set_table
                            )
                            if save_report
                            else None
                        ),
                    }
                ]
            )

        files, _ = profiler._check_args([selected_file])
        local_file, _ = profiler._get_file(files[0])
        df = profiler._read_profile_file(local_file)
        profile = profile_dataframe(
            df,
            data_product=profiler.set_data_product,
            table=profiler.set_table,
            file_name=os.path.basename(local_file),
            sample_strategy="first_file",
            operation_hints=operation_hints,
        )

        if save_report or report_path:
            if report_path is None:
                report_path = self._default_profile_report_path(
                    profiler.set_data_product, profiler.set_table
                )
            written_path = save_profile_report(
                profile,
                report_path,
                report_info={
                    "data_product": profiler.set_data_product,
                    "table": profiler.set_table,
                    "sample_strategy": "first_file",
                },
            )
            profile.attrs["report_path"] = written_path

        return profile

    def profile_tables(
        self,
        selections: dict = None,
        data_product: str = None,
        data_products: list = None,
        tables: list | str = None,
        operation_hints: bool = True,
        save_report: bool = False,
        report_path: str = None,
        dry_run: bool = False,
    ):
        """Profile first files across one or more data products and tables."""

        def tables_for_product(product):
            if tables == "all":
                df = self._tables_backup.query(f"`Data Product` == '{product}'")
                return sorted(df["Table"].dropna().unique().tolist())
            if isinstance(tables, str):
                return [tables]
            if tables is not None:
                return list(tables)
            if product == self.set_data_product and self.set_table is not None:
                return [self.set_table]
            raise ValueError(
                "profile_tables() requires tables, tables='all', or selections."
            )

        if selections is not None:
            resolved = {
                product: ([value] if isinstance(value, str) else list(value))
                for product, value in selections.items()
            }
        elif data_products is not None:
            resolved = {product: tables_for_product(product) for product in data_products}
        else:
            product = data_product or self.set_data_product
            if product is None:
                raise ValueError(
                    "profile_tables() requires selections, data_product, data_products, "
                    "or an already selected data product."
                )
            resolved = {product: tables_for_product(product)}

        profiles = []
        for product, product_tables in resolved.items():
            for table in product_tables:
                profiles.append(
                    self.profile_table(
                        data_product=product,
                        table=table,
                        operation_hints=operation_hints,
                        save_report=False,
                        dry_run=dry_run,
                    )
                )

        combined = pd.concat(profiles, ignore_index=True) if profiles else pd.DataFrame()
        if (save_report or report_path) and not dry_run:
            if report_path is None:
                product_name = next(iter(resolved.keys())) if len(resolved) == 1 else None
                report_path = self._default_profile_report_path(
                    data_product=product_name, multiple=True
                )
            written_path = save_profile_report(
                combined,
                report_path,
                report_info={
                    "profile_scope": "multiple_tables",
                    "sample_strategy": "first_file",
                },
            )
            combined.attrs["report_path"] = written_path

        return combined

    def search_company_names(
        self,
        names: list,
        num_workers: int = -1,
        cut_off: int = 90.1,
        company_suffixes: list = None,
        scorer: str = "WRatio",
    ):
        """Fuzzy-match company names against firmographics.

        The Polars path loads only the firm name and BvD ID columns, then uses
        an indexed RapidFuzz matcher with exact-match short-circuiting and
        prefix/token/length candidate blocking.
        """

        # Determine the number of workers if not specified
        if not num_workers or num_workers < 0:
            num_workers = max(1, cpu_count() - 2)

        SFTP = copy.deepcopy(self)
        SFTP._object_defaults()

        SFTP.set_data_product = "Firmographics (Monthly)"
        SFTP.set_table = "bvd_id_and_name"
        SFTP._select_cols = ["bvd_id_number", "name"]
        SFTP.output_format = None
        try:
            df_polars, _ = SFTP.polars_all(num_workers=num_workers)
        except (
            ImportError,
            OSError,
            TimeoutError,
            ValueError,
            pl.exceptions.PolarsError,
        ) as exc:
            print(f"Falling back to pandas fuzzy matching: {exc}")
            SFTP.query = fuzzy_query
            SFTP.query_args = [
                names,
                "name",
                "bvd_id_number",
                cut_off,
                company_suffixes,
                1,
                scorer,
            ]
            df, _ = SFTP.process_all(num_workers=num_workers, engine="pandas")
        else:
            df = fuzzy_match_pl(
                names=names,
                df=df_polars,
                match_column="name",
                return_column="bvd_id_number",
                cut_off=cut_off,
                remove_str=company_suffixes,
                num_workers=num_workers,
                scorer=scorer,
            )

        # Finder de bedste matches på tværs af "file parts"
        max_scores = df.groupby("Search_string", as_index=False)["Score"].max()
        best_matches = pd.merge(df, max_scores, on=["Search_string", "Score"])

        # Keep only unique rows
        best_matches = best_matches.drop_duplicates()
        best_matches = best_matches.reset_index(drop=True)

        # save to csv
        current_time = datetime.now()
        timestamp_str = current_time.strftime("%y%m%d%H%M")
        best_matches.to_csv(f"{timestamp_str}_company_name_search.csv")

        return best_matches

    def batch_bvd_search(
        self, products: str = "products.xlsx", bvd_numbers: str = "bvd_numbers.txt"
    ):
        def check_file_exists(base_name, extension=".csv", max_attempts=100):
            # Check for "filename.csv"
            if os.path.exists(base_name + extension):
                return base_name + extension

            # Check for "filename_1.csv", "filename_2.csv", ..., up to max_attempts
            for i in range(1, max_attempts + 1):
                file_name = f"{base_name}_{i}{extension}"
                if os.path.exists(file_name):
                    return file_name

        if not os.path.exists(products) or not os.path.exists(bvd_numbers):
            files = []
            if not os.path.exists(products):
                products_file = (
                    pkg_resources.files("moodys_datahub.data") / "products.xlsx"
                )
                with (
                    products_file.open("rb") as src,
                    open("products.xlsx", "wb") as target_file,
                ):
                    shutil.copyfileobj(src, target_file)
                    files.append(target_file)
            if not os.path.exists(bvd_numbers):
                bvd_file = (
                    pkg_resources.files("moodys_datahub.data") / "bvd_numbers.txt"
                )
                with (
                    bvd_file.open("rb") as src,
                    open("bvd_numbers.txt", "wb") as target_file,
                ):
                    shutil.copyfileobj(src, target_file)
                    files.append(target_file)
            print(
                f"The following input templates have been create: {files}. Please fill out and re-run the function"
            )
            return

        df = pd.read_excel(products)
        # Convert columns A, B, and C to lists
        data_products = df["Data Product"].tolist()
        tables = df["Table"].tolist()
        columns = df["Column"].tolist()
        to_runs = df["Run"].tolist()

        df = pd.read_csv(bvd_numbers, header=None)
        bvd_numbers = df[0].tolist()

        SFTP = copy.deepcopy(self)
        SFTP._object_defaults()

        in_complete = []
        # Loop through both lists together
        n = 0
        for data_product, table, column, to_run in zip(
            data_products, tables, columns, to_runs
        ):
            if to_run:
                n = n + 1
                file_name = f"{n}_{data_product}_{table}"

                existing_file = check_file_exists(file_name)

                if existing_file:
                    continue

                print(f"{n} : {data_product} : {table}")
                SFTP.set_data_product = data_product
                SFTP.set_table = table

                if SFTP._set_table is not None:
                    available_cols = SFTP.get_column_names()
                    if isinstance(column, str) and "," in column:
                        column = [word.strip() for word in column.split(",")]
                    else:
                        column = [column]

                    if all(col in available_cols for col in column):
                        SFTP.process_all(
                            destination=file_name,
                            bvd_query=[bvd_numbers, column, "exact"],
                        )
                    else:
                        in_complete.append([n, data_product, table, available_cols])
                else:
                    in_complete.append([n, data_product, table, "Not found"])

    def company_suffix(self):
        company_suffixes = [
            # Without punctuation
            "inc",
            "incorporated",
            "ltd",
            "limited",
            "llc",
            "plc",
            "corp",
            "corporation",
            "co",
            "company",
            "llp",
            "gmbh",
            "ag",
            "sa",
            "sas",
            "pty ltd",
            "bv",
            "oy",
            "as",
            "nv",
            "kk",
            "srl",
            "sp z oo",
            "sc",
            "ou",
            # With punctuation
            "inc.",
            "ltd.",
            "llc.",
            "plc.",
            "corp.",
            "co.",
            "llp.",
            "gmbh.",
            "ag.",
            "s.a.",
            "s.a.s.",
            "pty ltd.",
            "b.v.",
            "oy.",
            "a/s",
            "n.v.",
            "k.k.",
            "s.r.l.",
            "sp. z o.o.",
            "s.c.",
            "oü",
        ]
        return company_suffixes

    def search_bvd_changes(self, bvd_list: list, num_workers: int = -1):
        """Resolve BvD ID lineage and return discovered IDs plus matched change rows."""

        # Determine the number of workers if not specified
        if not num_workers or num_workers < 0:
            num_workers = max(1, cpu_count() - 2)

        SFTP = copy.deepcopy(self)
        SFTP._object_defaults()

        SFTP.set_data_product = "BvD ID Changes"
        SFTP.set_table = "bvd_id_changes_full"
        SFTP._select_cols = ["old_id", "new_id", "change_date"]
        SFTP.output_format = None
        df, _ = SFTP.process_all(num_workers=num_workers, engine="auto")

        new_ids, newest_ids, filtered_df = _bvd_changes_ray(bvd_list, df, num_workers)

        return new_ids, newest_ids, filtered_df

    def _table_search(self, search_word):
        filtered_df = self._tables_available.query(
            f"`Data Product`.str.contains('{search_word}', case=False, na=False,regex=False) | `Table`.str.contains('{search_word}', case=False, na=False,regex=False)"
        )
        return filtered_df

    # Under development
    def _search_dictionary_list(
        self,
        save_to: SaveFormat = None,
        search_word=None,
        search_cols: dict | None = None,
        letters_only: bool = False,
        exact_match: bool = False,
        data_product=None,
        table=None,
    ):
        """Search dictionary across one or more terms and append the triggering term.

        Returns:
            A DataFrame with matching dictionary rows and a `search_word` column.
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
        if self._tables_backup is not None:
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
                    f"`Table`.str.contains('{table}', case=False, na=False, regex=False)"
                )
                if df_table.empty:
                    print("No such Table was found. Please set right table")
                    return df_table
            search_cols["Table"] = False
            df = df_table

        if search_word is not None:
            if letters_only:
                df_backup = df.copy()
                df = df.map(_letters_only_regex)

            if not isinstance(search_word, list):
                search_word = [search_word]

            results = []

            for word in search_word:
                if letters_only:
                    word = _letters_only_regex(word)

                if exact_match:
                    base_string = "`{col}` == '{{word}}'"
                else:
                    base_string = "`{col}`.str.contains('{{word}}', case=False, na=False, regex=False)"

                search_conditions = " | ".join(
                    base_string.format(col=col)
                    for col, include in search_cols.items()
                    if include
                )
                final_string = search_conditions.format(word=word)

                result_df = df.query(final_string)

                if result_df.empty:
                    base_string = "'{col}'"
                    search_conditions = " , ".join(
                        base_string.format(col=col)
                        for col, include in search_cols.items()
                        if include
                    )
                    print(
                        f"No such '{word}' was detected across columns: "
                        + search_conditions
                    )
                else:
                    if letters_only:
                        result_df = df_backup.loc[result_df.index]
                    else:
                        result_df = result_df.copy()
                    result_df["search_word"] = word
                    results.append(result_df)

            if results:
                df = pd.concat(results, ignore_index=True)
            else:
                df = pd.DataFrame()

            # if letters_only:
            #    df = df_backup.loc[df.index]

            if save_to:
                print(
                    f"The following query was executed for each word in search_word: {search_word} : "
                )

        _save_to(df, "dict_search", save_to)

        return df
