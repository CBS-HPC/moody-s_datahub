import asyncio
import os
import time
from pathlib import Path

import pandas as pd

from .connection import _Connection
from .utils import _join_remote_path, _normalize_remote_path
from .widgets import _select_list, _select_product, _SelectData


class _Selection(_Connection):
    def __init__(self):
        # Initialize mixins
        _Connection.__init__(self)

    # Local path and files
    @property
    def local_path(self):
        return self._local_path

    @local_path.setter
    def local_path(self, path):
        """Set the active local path and refresh tracked local files."""
        if path is None:
            self._remote_files = []
            self._remote_path = None
            self._local_files = []
            self._local_path = None
        elif path is not self._local_path:
            self._local_files, self._local_path = self._check_path(path, "local")

    @property
    def local_files(self):
        self._local_files, self._local_path = self._check_path(
            self._local_path, "local"
        )
        return self._local_files

    @local_files.setter
    def local_files(self, value):
        self._local_files = self._check_files(value)

    @property
    def remote_path(self):
        return self._remote_path

    @remote_path.setter
    def remote_path(self, path):
        """Set the active remote path and align selected product/table metadata."""

        if path is None:
            self._object_defaults()

        elif path != self.remote_path:
            inventory = self._tables_backup
            if inventory is None:
                inventory = self._tables_available
            selection_path = (
                str(Path(path)) if self._local_repo else _normalize_remote_path(path)
            )
            if selection_path is None and not getattr(self, "_interactive", True):
                raise ValueError(f"Remote path is invalid: {path!r}")
            metadata = inventory.loc[
                (inventory["Base Directory"] == selection_path)
                | (inventory["Export"] == selection_path)
            ]
            # Prefer export metadata before _check_path falls back to file stat.
            previous_timestamp = self._time_stamp
            self._time_stamp = None if metadata.empty else metadata["Timestamp"].iloc[0]
            try:
                remote_files, remote_path = self._check_path(
                    path, None if self._local_repo else "remote"
                )
                if remote_path is None and not getattr(self, "_interactive", True):
                    raise ValueError(f"Remote path is invalid: {path!r}")
            except Exception:
                self._time_stamp = previous_timestamp
                raise

            self._local_files = []
            self._local_path = None
            self._remote_files, self._remote_path = remote_files, remote_path

            if self._remote_path:
                df = inventory.loc[inventory["Base Directory"] == self._remote_path]

                if df.empty:
                    df = inventory.loc[inventory["Export"] == self._remote_path]
                    self._set_table = None
                else:
                    if self._set_table not in df["Table"].values:
                        self._set_table = df["Table"].iloc[0]

                if not df.empty:
                    if self._set_data_product not in df["Data Product"].values:
                        self._set_data_product = df["Data Product"].iloc[0]
                    # Keep every table in the chosen export, not the prior catalog.
                    self._tables_available = inventory.loc[
                        inventory["Export"].isin(df["Export"])
                    ].copy()
                    self._time_stamp = df["Timestamp"].iloc[0]

    @property
    def remote_files(self):
        return self._remote_files

    @remote_files.setter
    def remote_files(self, value):
        self._remote_files = self._check_files(value)

    @property
    def set_data_product(self):
        return self._set_data_product

    @set_data_product.setter
    def set_data_product(self, product):
        """Set the current data product and reset dependent state as needed."""

        if product is None:
            self._tables_available = self._tables_backup.copy()
            self._object_defaults()
            return

        if product != self._set_data_product:
            inventory = self._tables_backup
            df = inventory.loc[inventory["Data Product"] == product]

            if df.empty:
                df = inventory.loc[
                    inventory["Data Product"].str.contains(
                        str(product), case=False, na=False, regex=False
                    )
                ]
                if df.empty:
                    message = (
                        f"No such Data Product was found for {product!r}. "
                        "Please set the right data product."
                    )
                else:
                    matches = df["Data Product"].drop_duplicates().tolist()
                    match_description = (
                        "Multiple data products partially match"
                        if len(matches) > 1
                        else "One data product partially match"
                    )
                    message = (
                        f"{match_description} '{product}' : {matches}. "
                        "Please set right data product"
                    )
                if not getattr(self, "_interactive", True):
                    raise ValueError(message)
                self._tables_available = inventory.copy()
                print(message)

            elif len(df["Export"].unique()) > 1:
                matches = df[["Data Product", "Export"]].drop_duplicates()
                message = f"Multiple version of '{product}' are detected: {matches['Data Product'].tolist()} with export paths ('Export') {matches['Export'].tolist()} .Please Set the '.remote_path' property with the correct 'Export' Path"
                if not getattr(self, "_interactive", True):
                    raise ValueError(message)
                self._tables_available = inventory.copy()
                print(message)
            else:
                self._object_defaults()
                self._tables_available = df.copy()
                self._set_data_product = product
                self._time_stamp = df["Timestamp"].iloc[0]

    @property
    def set_table(self):
        return self._set_table

    @set_table.setter
    def set_table(self, table):
        """Set the active table and derive matching data product/path context."""

        if table is None:
            self._object_defaults()
        elif table != self._set_table:
            df = self._tables_available.loc[self._tables_available["Table"] == table]
            if self._set_data_product is not None:
                df = df.loc[df["Data Product"] == self._set_data_product]

            if df.empty:
                if not getattr(self, "_interactive", True):
                    raise ValueError(
                        f"No exact Table match was found for {table!r} in the active "
                        "catalog. Choose an exact table name from tables_available(); "
                        "to change exports, set remote_path explicitly first."
                    )
                df = self._tables_available.loc[
                    self._tables_available["Table"].str.contains(
                        str(table), case=False, na=False, regex=False
                    )
                ]
                if len(df) > 1:
                    matches = df[["Data Product", "Table"]].drop_duplicates()
                    print(
                        f"Multiple tables partially match '{table}' : {matches['Table'].tolist()} from {matches['Data Product'].tolist()}. Please set right table"
                    )
                elif df.empty:
                    print("No such Table was found. Please set right table")
                self._set_table = None
            elif len(df) > 1:
                if not getattr(self, "_interactive", True):
                    raise ValueError(
                        f"Multiple tables match {table!r}. Set set_data_product and "
                        "remote_path explicitly to select one export."
                    )
                if self._set_data_product is None:
                    matches = df[["Data Product", "Table"]].drop_duplicates()
                    print(
                        f"Multiple tables match '{table}' : {matches['Table'].tolist()} from {matches['Data Product'].tolist()}. Please set Data Product using the '.set_data_product' property"
                    )
                elif len(df["Export"].unique()) > 1:
                    matches = df[["Data Product", "Table", "Export"]].drop_duplicates()
                    print(
                        f"Multiple version of '{table}' are detected: {matches['Table'].tolist()} from {matches['Data Product'].tolist()} with export paths ('Base Directory') {matches['Base Directory'].tolist()} .Please Set the '.remote_path' property with the correct 'Base Directory' Path"
                    )
                self._set_table = None
            else:
                self._object_defaults()
                self._set_table = table
                self._set_data_product = df["Data Product"].iloc[0]
                self._time_stamp = df["Timestamp"].iloc[0]
                base_directory = df["Base Directory"].iloc[0]
                if pd.notna(base_directory):
                    self.remote_path = base_directory

    def select_data(self):
        """Open an interactive selector for data product and table."""
        if not getattr(self, "_interactive", True):
            raise ValueError(
                "select_data() is unavailable in non-interactive mode. "
                "Set set_data_product and set_table explicitly."
            )

        async def f(self):
            Select_obj = _SelectData(
                self._tables_backup, "Select Data Product and Table"
            )
            selected_product, selected_table = await Select_obj.display_widgets()
            df = self._tables_backup.copy()
            df = (
                df[["Data Product", "Table", "Base Directory", "Top-level Directory"]]
                .query(
                    f"`Data Product` == '{selected_product}' & `Table` == '{selected_table}'"
                )
                .drop_duplicates()
            )
            if len(df) > 1:
                options = df["Top-level Directory"].tolist()
                product = df["Data Product"].drop_duplicates().tolist()
                msg = f"Multiple data products match '{product[0]}'. Please set right data product:"
                self._set_table = selected_table
                self._set_data_product = selected_product
                _select_list(
                    "_SelectList",
                    options,
                    f"'{product[0]}':",
                    msg,
                    _select_product,
                    [df, self],
                )
            elif len(df) == 1:
                self.set_data_product = selected_product
                self.set_table = selected_table
                print(f"{self.set_data_product} was set as Data Product")
                print(f"{self.set_table} was set as Table")

        self._download_finished = None

        asyncio.ensure_future(f(self))

    def _check_path(self, path, mode=None):
        files = []
        if path is not None:
            if mode == "local" or mode is None:
                if os.path.exists(path):
                    if os.path.isdir(path):
                        files = os.listdir(path)
                    elif os.path.isfile(path):
                        files = [os.path.basename(path)]
                        path = os.path.dirname(path)
                else:  # added
                    if mode is None:
                        mode = "remote"
                    else:
                        os.makedirs(path)
                        print(f"Folder '{path}' created.")

            if mode == "remote":
                path = _normalize_remote_path(path)
                sftp = self._connect()

                if sftp.exists(path):
                    files = sftp.listdir(path)
                    if not files:
                        files = [os.path.basename(path)]
                        path = os.path.dirname(path)
                else:
                    print(f"Remote path is invalid: '{path}'")
                    path = None

            if len(files) > 1 and self._set_table:
                matched_file = f"{self._set_table}.csv"
                if matched_file in files:
                    files = [matched_file]

            if mode == "remote" and not self._time_stamp:
                if files and files[0] is not None:
                    file_attributes = sftp.stat(_join_remote_path(path, files[0]))
                    self._time_stamp = time.strftime(
                        "%Y-%m-%d %H:%M:%S", time.localtime(file_attributes.st_mtime)
                    )
                else:
                    print("WARNING: Cannot get timestamp, files is empty or None")
        else:
            files = []

        if path is not None:
            if mode == "remote":
                path = _normalize_remote_path(path)
            else:
                path = str(Path(path))

        return files, path

    def _check_files(self, value):
        if isinstance(value, list) and all(isinstance(item, str) for item in value):
            return value
        else:
            raise ValueError("file list must be a list of strings")
