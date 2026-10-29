from __future__ import annotations

import ast
import asyncio
import contextlib
import json
import os
import re
import sys
import time
from datetime import datetime
from multiprocessing import cpu_count
from pathlib import Path
from stat import S_ISDIR
from tempfile import TemporaryDirectory
from typing import Literal

import pandas as pd
import pysftp

from .load_data import _table_match, _table_names
from .utils import (
    SaveFormat,
    _check_list_format,
    _join_remote_path,
    _normalize_remote_path,
    _run_parallel,
    _save_to,
)
from .widgets import _CustomQuestion, _Multi_dropdown

_CBS_HOSTNAME = "s-f2112b8b980e44f9a.server.transfer.eu-west-1.amazonaws.com"
_CBS_USERNAME = "D2vdz8elTWKyuOcC2kMSnw"

_DELETE_PLAN_COLUMNS = [
    "requested",
    "top_level_directory",
    "data_product",
    "tables",
    "files",
    "size_mb",
    "status",
]


def _is_remote_dir(sftp, attr, path):
    """Return whether a ``listdir_attr`` entry is a directory."""
    mode = getattr(attr, "st_mode", None)
    if mode is None:
        return sftp.isdir(path)
    return S_ISDIR(mode)


# Defining Sftp Class
class _Connection:
    def __init__(self):
        self.connection: object = None
        self.privatekey: str = None
        self.port: int = None
        self._cnopts = pysftp.CnOpts()
        self._cnopts.hostkeys = None
        self.hostname: str = None
        self.username: str = None
        self._local_repo: str = None

        if hasattr(os, "fork"):
            self._pool_method = "fork"
            self._max_path_length = 256
        else:
            self._pool_method = "threading"
            self._max_path_length = 256

        if sys.platform.startswith("linux"):
            self._max_path_length = 4096
        elif sys.platform == "darwin":
            self._max_path_length = 1024
        elif sys.platform == "win32":
            self._max_path_length = 256

        self._tables_available = None
        self._tables_backup = None
        self._table_dictionary = None
        self._table_dates = None
        self.output_format: list = [".csv"]
        self.file_size_mb: int = 500
        self.delete_files: bool = False
        self.concat_files: bool = True
        self._last_process_engine: str = None
        self._last_process_reason: str = None

    # pool method
    @property
    def pool_method(self):
        return self._pool_method

    @pool_method.setter
    def pool_method(self, method: Literal["fork", "threading", "spawn"]):
        """Set the parallel worker backend."""
        if method not in ["fork", "threading", "spawn"]:
            raise ValueError(
                "Invalid worker pool method. Use 'fork', 'threading', or 'spawn'."
            )

        if not hasattr(os, "fork") and method == "fork":
            print("fork() processes are not supported by OS")
            method = "spawn"

        print(f'"{method}" is chosen as worker pool method')
        self._pool_method = method

    @property
    def last_process_engine(self):
        """Return the backend used by the most recent processing call."""
        return self._last_process_engine

    @property
    def last_process_reason(self):
        """Return why the most recent processing call chose its backend."""
        return self._last_process_reason

    @property
    def download_finished(self):
        """Return the most recent download state."""
        return self._download_finished

    def tables_available(
        self,
        product_overview=None,
        save_to: SaveFormat = None,
        reset: bool = False,
    ):
        """Load available products/tables from SFTP or local repository metadata."""
        to_delete = []
        if self._tables_available is None and self._tables_backup is None:
            self._tables_available, to_delete = self._table_overview(
                product_overview=product_overview
            )
            self._tables_backup = self._tables_available.copy()

        elif reset:
            self._tables_available = self._tables_backup.copy()

        # Specify unknown data product exports
        self._specify_data_products()

        _save_to(self._tables_available, "tables_available", save_to)

        return self._tables_available.copy(), to_delete

    def _connect(self):
        """Create and return a configured SFTP connection."""
        if self.hostname == _CBS_HOSTNAME and self.username == "aN54UkFxQPCOIEtmr0FmAQ":
            raise ValueError(
                "The CBS CSV SFTP service has been decommissioned. "
                "Use the default CBS Parquet service instead."
            )
        sftp = pysftp.Connection(
            host=self.hostname,
            username=self.username,
            port=self.port,
            private_key=self.privatekey,
            cnopts=self._cnopts,
        )
        return sftp

    def _table_overview(self, product_overview=None):
        def resolve_export_path(sftp, product_path, data_folder):
            product_path = _normalize_remote_path(product_path)
            data_folder = _normalize_remote_path(data_folder)
            if data_folder is None:
                return product_path

            product_key = product_path.strip("/")
            data_key = data_folder.strip("/")

            if data_folder.startswith("/"):
                candidates = [
                    data_folder,
                    _join_remote_path(product_path, data_folder),
                    data_key,
                ]
            elif data_key == product_key or data_key.startswith(f"{product_key}/"):
                candidates = [data_folder, data_key]
            else:
                candidates = [
                    _join_remote_path(product_path, data_folder),
                    data_folder,
                    data_key,
                ]

            seen = set()
            for candidate in candidates:
                if candidate and candidate not in seen:
                    seen.add(candidate)
                    if sftp.exists(candidate):
                        return candidate

            return candidates[0]

        def check_sftp(product_overview, sftp):
            product_paths = sftp.listdir()
            newest_exports = []
            time_stamp = []
            data_products = []
            to_delete = []

            for product_path in product_paths:
                sel_product = product_overview.loc[
                    product_overview["Top-level Directory"] == product_path,
                    "Data Product",
                ]
                if len(sel_product) == 0:
                    data_products.append(None)
                else:
                    data_products.append(sel_product.values[0])

                # tnfs_folder = str(Path(product_path) / "tnfs")
                tnfs_folder = _join_remote_path(product_path, "tnfs")

                export_paths = sftp.listdir(product_path)

                if not sftp.exists(tnfs_folder):
                    # path = str(Path(product_path) / export_paths[0])
                    path = _join_remote_path(product_path, export_paths[0])
                    newest_exports.append(path)
                    time_stamp.append(None)
                    continue

                # Get all .tnfs files in the 'tnfs' folder
                tnfs_files = [
                    f for f in sftp.listdir(tnfs_folder) if f.endswith(".tnf")
                ]
                if not tnfs_files:
                    print(tnfs_files)
                    print("Error")

                # Initialize variables to keep track of the newest .tnfs file
                newest_tnfs_file = None
                newest_mtime = float("-inf")

                # Determine the newest .tnfs file
                for tnfs_file in tnfs_files:
                    # tnfs_file_path  = str(Path(tnfs_folder) / tnfs_file)
                    tnfs_file_path = _join_remote_path(tnfs_folder, tnfs_file)
                    file_attributes = sftp.stat(tnfs_file_path)
                    mtime = file_attributes.st_mtime
                    if mtime > newest_mtime:
                        newest_mtime = mtime
                        newest_tnfs_file = tnfs_file_path

                if newest_tnfs_file:
                    time_stamp.append(
                        time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(newest_mtime))
                    )
                    # Own the download location and clean it on success or failure.
                    with TemporaryDirectory(prefix="moodys-datahub-") as temp_dir:
                        local_file = str(Path(temp_dir) / "marker.tnf")
                        sftp.get(newest_tnfs_file, local_file)
                        with open(local_file, encoding="utf-8") as f:
                            tnfs_data = json.load(f)
                        newest_export = resolve_export_path(
                            sftp, product_path, tnfs_data.get("DataFolder")
                        )
                        newest_exports.append(newest_export)

                    for export_path in export_paths:
                        # export_path = str(Path(product_path) / export_path)
                        export_path = _join_remote_path(product_path, export_path)
                        if export_path != newest_export and export_path != tnfs_folder:
                            to_delete.append(export_path)

            # Create a DataFrame from the lists
            df = pd.DataFrame(
                {
                    "Data Product": data_products,
                    "Top-level Directory": product_paths,
                    "Newest Export": newest_exports,
                    "Timestamp": time_stamp,
                }
            )

            return df, to_delete

        def check_local(local_path: str = None):
            product_paths = [
                name
                for name in os.listdir(local_path)
                if (Path(local_path) / name).is_dir()
            ]
            newest_exports = []
            time_stamps = []
            data_products = []
            for product_path in product_paths:
                newest_exports.append(str(Path(local_path) / product_path))

                filename = os.path.basename(
                    product_path
                )  # Extract the last part of the path
                # Split by '_exported' and strip spaces
                parts = filename.split("_exported", 1)

                # Extract data_product
                data_products.append(parts[0].strip())

                # Extract timestamp and normalize it
                timestamp = parts[1].strip() if len(parts) > 1 else ""

                # Convert timestamp format
                timestamp = re.sub(r"[_]", " ", timestamp)  # Replace "_" with " "
                try:
                    timestamp = datetime.strptime(timestamp, "%Y-%m-%d %H-%M-%S")
                    timestamp = timestamp.strftime("%Y-%m-%d %H:%M:%S")
                except ValueError:
                    timestamp = None  # Handle cases where format doesn't match
                time_stamps.append(timestamp)

            # Create a DataFrame from the lists
            df = pd.DataFrame(
                {
                    "Data Product": data_products,
                    "Top-level Directory": product_paths,
                    "Newest Export": newest_exports,
                    "Timestamp": time_stamps,
                }
            )

            return df, []

        def compile_table(df, sftp=None):
            data = []
            for _, row in df.iterrows():
                data_product = row["Data Product"]
                timestamp = row["Timestamp"]
                main_directory = row["Top-level Directory"]
                export = row["Newest Export"]

                if sftp:
                    tables = sftp.listdir(export)
                    # full_paths  = [str(Path(export) / table) for table in tables]
                    full_paths = [_join_remote_path(export, table) for table in tables]

                else:
                    tables = os.listdir(export)
                    # full_paths  = [os.path.normpath(os.path.join(export ,table)) for table in tables]
                    full_paths = [str(Path(export) / table) for table in tables]

                full_paths = [
                    os.path.dirname(full_path)
                    if full_path.endswith(".csv")
                    else full_path
                    for full_path in full_paths
                ]
                tables = [
                    table[:-4] if table.endswith(".csv") else table for table in tables
                ]

                tables = [
                    table.replace("usd_interim", "interim_usd").replace(
                        "eur_interim", "interim_eur"
                    )
                    for table in tables
                ]

                if pd.isna(data_product):
                    data_product, tables = _table_match(tables)

                # Append a dictionary with export, timestamp, and modified_list to the list
                for full_path, table in zip(full_paths, tables):
                    data.append(
                        {
                            "Data Product": data_product,
                            "Table": table,
                            "Base Directory": full_path,
                            "Timestamp": timestamp,
                            "Export": export,
                            "Top-level Directory": main_directory,
                        }
                    )

            # Create a DataFrame from the list of dictionaries
            df = pd.DataFrame(
                data,
                columns=[
                    "Data Product",
                    "Table",
                    "Base Directory",
                    "Timestamp",
                    "Export",
                    "Top-level Directory",
                ],
            )

            return df

        if not self._local_repo:
            print("Retrieving Data Product overview from SFTP..wait a moment")
            product_overview = _table_names(file_name=product_overview)
            with self._connect() as sftp:
                df, to_delete = check_sftp(product_overview, sftp)

                df = compile_table(df, sftp)

        else:
            print(f"Retrieving Data Product overview from {self._local_repo}")
            df, to_delete = check_local(self._local_repo)

            df = compile_table(df)
        return df, to_delete

    def _server_clean_up(self, to_delete, prompt_response: bool | None = None):
        async def f(self, to_delete):
            question = _CustomQuestion(
                "Please help maintain the server by deleting old exports? It may take a few minutes",
                ["ok", "no"],
            )
            result = await question.display_widgets()

            if result == "ok":
                print("------------------  DELETING OLD EXPORTS FROM SFTP")
                self._remove_exports(to_delete)

        if to_delete is None:
            _, to_delete = self._table_overview()

        to_delete = _check_list_format(to_delete)

        if len(to_delete) == 0:
            return
        should_prompt = (
            self.hostname == _CBS_HOSTNAME
            and self.username == _CBS_USERNAME
            and int(cpu_count()) >= 32
        )

        if not should_prompt:
            return

        # Allow non-interactive automation for login workflows.
        if prompt_response is False:
            return
        if prompt_response is True:
            print("------------------  DELETING OLD EXPORTS FROM SFTP")
            self._remove_exports(to_delete)
            return

        if prompt_response is None and not getattr(self, "_interactive", True):
            return

        if prompt_response is None:
            asyncio.ensure_future(f(self, to_delete))

    def delete_top_level_directories(
        self,
        directories: str | list[str],
        dry_run: bool = True,
        confirm: bool = False,
        num_workers: int | None = None,
    ) -> pd.DataFrame:
        """Plan or permanently delete whole top-level directories on the SFTP server.

        Args:
            directories: Exact top-level directory names (the ``Top-level
                Directory`` column of ``tables_available()``) or exact ``Data
                Product`` names, which resolve to their top-level directories.
                Only folders directly under the SFTP login directory qualify.
            dry_run: When ``True`` (default), only report what would be deleted.
            confirm: Must be ``True`` together with ``dry_run=False`` to delete.
                Deletion removes every export, table and marker file in the
                directory and cannot be undone.
            num_workers: Optional number of parallel file-deletion batches.

        Returns:
            DataFrame with one row per requested directory: the requested name,
            data product, tables, file count, size in MB and status
            (``would_delete``, ``deleted``, ``failed``, ``not_found`` or
            ``not_a_directory``).

        Raises:
            ValueError: Invalid names, a non-SFTP session, missing confirmation,
                or (when deleting) any name that cannot be resolved. Nothing is
                deleted in these cases.
            RuntimeError: Deletion started but a directory still exists afterwards.
        """
        if isinstance(directories, str):
            directories = [directories]
        if not isinstance(directories, (list, tuple)) or not all(
            isinstance(name, str) for name in directories
        ):
            raise ValueError("directories must be a string or a list of strings.")
        requested = list(dict.fromkeys(directories))
        if not requested:
            raise ValueError(
                "Provide at least one top-level directory or data product name."
            )

        invalid = [
            name
            for name in requested
            if not name.strip()
            or name.strip() in {".", ".."}
            or "/" in name
            or "\\" in name
        ]
        if invalid:
            raise ValueError(
                "Top-level directories must be exact names directly under the SFTP "
                f"login directory, without path separators: {invalid}"
            )

        if getattr(self, "_offline", False) or self._local_repo:
            raise ValueError(
                "Deleting top-level directories requires an SFTP session; offline "
                "and local_repo sessions cannot delete remote directories."
            )

        if not dry_run and not confirm:
            raise ValueError(
                "Deleting top-level directories is permanent. Review the dry run, "
                "then call again with dry_run=False and confirm=True."
            )

        overview = (
            self._tables_backup
            if self._tables_backup is not None
            else self._tables_available
        )
        if overview is None or "Top-level Directory" not in overview.columns:
            overview = _table_names()

        rows = []
        seen = set()
        with self._connect() as sftp:
            top_level = set(sftp.listdir())
            for name in requested:
                if name in top_level:
                    targets = [name]
                else:
                    matches = overview.loc[
                        overview["Data Product"] == name, "Top-level Directory"
                    ]
                    targets = sorted(
                        d for d in matches.dropna().unique() if d in top_level
                    )

                if not targets:
                    rows.append({"requested": name, "status": "not_found"})
                    continue

                for directory in targets:
                    if directory in seen:
                        continue
                    seen.add(directory)
                    row = {"requested": name, "top_level_directory": directory}

                    if not sftp.isdir(directory):
                        rows.append({**row, "status": "not_a_directory"})
                        continue

                    in_dir = overview["Top-level Directory"] == directory
                    products = overview.loc[in_dir, "Data Product"].dropna()
                    tables = (
                        overview.loc[in_dir, "Table"].dropna()
                        if "Table" in overview.columns
                        else pd.Series(dtype=str)
                    )
                    n_files, n_bytes = self._directory_inventory(sftp, directory)
                    rows.append(
                        {
                            **row,
                            "data_product": ", ".join(
                                sorted(products.astype(str).unique())
                            )
                            or None,
                            "tables": ", ".join(sorted(tables.astype(str).unique()))
                            or None,
                            "files": n_files,
                            "size_mb": round(n_bytes / 1024**2, 1),
                            "status": "would_delete",
                        }
                    )

        plan = pd.DataFrame(rows, columns=_DELETE_PLAN_COLUMNS)
        to_delete = plan["status"].eq("would_delete")
        targets = plan.loc[to_delete, "top_level_directory"].tolist()
        unresolved = plan.loc[~to_delete, "requested"].tolist()

        if dry_run:
            print(
                f"Dry run: {len(targets)} top-level directories "
                f"({int(plan.loc[to_delete, 'files'].sum())} files, "
                f"{plan.loc[to_delete, 'size_mb'].sum():.1f} MB) would be deleted. "
                "Nothing was deleted."
            )
            if unresolved:
                print(f"Not found or not a directory: {unresolved}")
            return plan

        if unresolved:
            raise ValueError(
                "Nothing was deleted. These names did not resolve to top-level "
                f"directories on the SFTP server: {unresolved}"
            )

        print(f"------------------  DELETING {len(targets)} TOP-LEVEL DIRECTORIES")
        error = None
        try:
            self._remove_exports(targets, num_workers=num_workers)
        except Exception as exc:
            error = exc

        with self._connect() as sftp:
            remaining = {directory for directory in targets if sftp.exists(directory)}

        plan.loc[to_delete, "status"] = plan.loc[to_delete, "top_level_directory"].map(
            lambda directory: "failed" if directory in remaining else "deleted"
        )

        deleted = set(targets) - remaining
        for attr in ("_tables_available", "_tables_backup"):
            df = getattr(self, attr, None)
            if df is not None and "Top-level Directory" in df.columns:
                setattr(
                    self, attr, df.loc[~df["Top-level Directory"].isin(deleted)].copy()
                )

        if remaining:
            raise RuntimeError(
                f"Deletion did not complete for: {sorted(remaining)}. Deleted: "
                f"{sorted(deleted)}. Run the dry run again to see what remains."
            ) from error
        if error is not None:
            print(f"Deletion reported an error but no directories remain: {error}")

        print(f"Deleted {len(deleted)} top-level directories: {sorted(deleted)}")
        return plan

    @staticmethod
    def _directory_inventory(sftp, path):
        """Count files and bytes below a remote directory."""
        n_files = 0
        n_bytes = 0
        pending = [path]
        while pending:
            current = pending.pop()
            for attr in sftp.listdir_attr(current):
                full_path = _join_remote_path(current, attr.filename)
                if _is_remote_dir(sftp, attr, full_path):
                    pending.append(full_path)
                else:
                    n_files += 1
                    n_bytes += int(getattr(attr, "st_size", 0) or 0)
        return n_files, n_bytes

    def _remove_exports(self, to_delete=None, num_workers=None):
        def batch_list(input_list, num_batches):
            """Splits the input list into a specified number of batches."""
            # Calculate the batch size based on the total number of elements and the number of batches

            if len(input_list) <= num_batches:
                num_batches = len(input_list)
            batch_size = len(input_list) // num_batches

            # If there is a remainder, some batches will have one extra element
            remainder = len(input_list) % num_batches

            batches = []
            start = 0
            for i in range(num_batches):
                # Calculate the end index for each batch
                end = start + batch_size + (1 if i < remainder else 0)
                batches.append(input_list[start:end])
                start = end

            return batches

        # Detecting files to delete
        lists = _run_parallel(
            fnc=self._recursive_collect,
            params_list=to_delete,
            n_total=len(to_delete),
            msg="Collecting files to delete",
        )
        file_paths = [item for sublist in lists for item in sublist]

        # Define worker
        if num_workers is None:
            num_workers = int(cpu_count() - 2)

        if isinstance(num_workers, (int, float, complex)) and num_workers != 1:
            num_workers = int(num_workers)
        num_workers = max(1, num_workers)

        # Batch and delete files; a folder tree may contain no files at all.
        if file_paths:
            batches = batch_list(file_paths, num_workers)
            _run_parallel(
                fnc=self._delete_files,
                params_list=batches,
                n_total=len(batches),
                msg="Deleting files",
            )

        # Deleting the remaining (empty) folder trees
        _run_parallel(
            fnc=self._delete_folders,
            params_list=to_delete,
            n_total=len(to_delete),
            msg="Deleting folders",
        )

    def _recursive_collect(
        self, path, extensions: tuple = (".parquet", ".csv", ".orc", ".avro")
    ):
        file_paths = []
        with self._connect() as sftp:
            for file_attr in sftp.listdir_attr(path):
                # full_path = str(Path(path) / file_attr.filename)
                full_path = _join_remote_path(path, file_attr.filename)

                # Check if the file ends with any of the specified extensions
                if full_path.endswith(extensions):
                    file_paths.append(full_path)
                elif sftp.isdir(full_path):
                    subfolder_paths = self._recursive_collect(full_path, extensions)
                    file_paths.extend(subfolder_paths)
                else:
                    file_paths.append(full_path)

        return file_paths

    def _delete_files(self, files):
        with self._connect() as sftp:
            for file in files:
                sftp.remove(file)

    def _delete_folders(self, folder_path: str = None):
        """Delete a remote folder tree: files first, then folders deepest-first."""

        def recursive_delete(sftp, path):
            try:
                entries = sftp.listdir_attr(path)
            except FileNotFoundError:
                return False
            for file_attr in entries:
                full_path = _join_remote_path(path, file_attr.filename)
                if _is_remote_dir(sftp, file_attr, full_path):
                    recursive_delete(sftp, full_path)
                else:
                    with contextlib.suppress(FileNotFoundError):
                        sftp.remove(full_path)
            # Object-store backed servers may drop empty folders implicitly.
            with contextlib.suppress(FileNotFoundError):
                sftp.rmdir(path)
            return True

        with self._connect() as sftp:
            try:
                existed = recursive_delete(sftp, folder_path)
            except Exception as e:
                print(f"Failed to delete folder {folder_path}: {e}")
                return
        if existed:
            print(f"Folder {folder_path} deleted successfully")
        else:
            print(f"Folder {folder_path} not found")

    def _specify_data_products(self):
        def extract_options(row):
            if "Multiple_Options: " in row:
                # Extract the substring starting after "Multiple_Options: "
                list_str = row.split("Multiple_Options: ")[1]
                # Convert the extracted substring to a Python list using ast.literal_eval
                try:
                    options_list = ast.literal_eval(list_str)
                    return options_list
                except (SyntaxError, ValueError):
                    print(f"Error parsing list from row: {row}")
                    return None
            else:
                return None

        # Function to check if a row contains "Multiple_Options: "
        def contains_multiple_options(row):
            return "Multiple_Options: " in row

        async def f(self, df, df_multiple):
            selected_values = None

            # Keep only columns '1' and '2'
            df_multiple = df_multiple[["Data Product", "Top-level Directory"]]

            # Remove duplicate rows based on columns '1' and '2'
            df_multiple = df_multiple.drop_duplicates()

            # Apply the function to the column '1' and store the results in a new column 'Options_List'
            df_multiple["Options_List"] = df_multiple["Data Product"].apply(
                extract_options
            )

            Select_obj = _Multi_dropdown(
                df_multiple["Options_List"].to_list(),
                df_multiple["Top-level Directory"].to_list(),
                "Specify 'Data Product' for unknown exports:",
            )

            selected_values = await Select_obj.display_widgets()

            if selected_values:
                df_multiple["Data Product"] = selected_values

                # Create a mapping from df1
                mapping = pd.Series(
                    df_multiple["Data Product"].values,
                    index=df_multiple["Top-level Directory"],
                ).to_dict()

                # Update df2['2'] based on the mapping
                df["Data Product"] = (
                    df["Top-level Directory"]
                    .map(mapping)
                    .combine_first(df["Data Product"])
                )

                self._tables_available = df
                self._tables_backup = df

                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                filename = f"{timestamp}_data_products.csv"
                df = df[["Data Product", "Table", "Top-level Directory"]]
                df.to_csv(filename)
                print(f"Data Product template has been saved to: {filename}")

        df = self._tables_available.copy()

        # Filter rows where column '1' contains "Multiple_Options: "
        df_multiple = df[df["Data Product"].apply(contains_multiple_options)].copy()

        if not df_multiple.empty:
            if not getattr(self, "_interactive", True):
                exports = df_multiple["Top-level Directory"].drop_duplicates().tolist()
                raise ValueError(
                    "Multiple unresolved data products require interactive selection. "
                    "Run with interactive=True and create a template, then pass it via "
                    f"data_product_template. Affected exports: {exports}"
                )
            asyncio.ensure_future(f(self, df, df_multiple))

    def _object_defaults(self):
        self._select_cols: list = None
        self.query = None
        self.query_args: list = None
        self._bvd_list: list = [None, None, None]
        self._and_bvd_list: list = []
        self._or_bvd_list: list = []
        self._time_period: list = [None, None, None, "remove"]
        self.dfs = None
        self._local_path: str = None
        self._local_files: list = []
        self._remote_path: str = None
        self._remote_files: list = []
        self._set_data_product: str = None
        self._time_stamp: str = None
        self._set_table: str = None
        self._download_finished = None
        self._last_process_engine = None
        self._last_process_reason = None
