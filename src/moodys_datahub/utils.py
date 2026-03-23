import os
import re
import shlex
import shutil
import subprocess
import sys
from collections import defaultdict, deque
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from math import ceil
from multiprocessing import Pool, cpu_count
from typing import Literal

import fastavro
import numpy as np
import pandas as pd
import polars as pl
import psutil
import pyarrow
from rapidfuzz import process
from tqdm import tqdm

SaveFormat = Literal["xlsx", "csv"] | None


# Dependency functions
def _create_workers(
    num_workers: int = -1, n_total: int = None, pool_method=None, query=None
):
    if num_workers < 1:
        num_workers = int(psutil.virtual_memory().total / (1024**3) / 12)

    if num_workers > int(cpu_count()):
        num_workers = int(cpu_count())

    if num_workers > n_total:
        num_workers = int(n_total)

    print(f"------ Creating Worker Pool of {num_workers}")

    if pool_method is None:
        if hasattr(os, "fork"):
            pool_method = "fork"
        else:
            pool_method = "threading"

    method = "process"
    if pool_method == "spawn" and query is not None:
        print('The custom function (query) is not supported by "spawn" proceses')
        if not hasattr(os, "fork"):
            print(
                'Switching worker pool to "ThreadPoolExecutor(max_workers=num_workers)" which is under Global Interpreter Lock (GIL) - I/O operations should still speed up'
            )
            method = "thread"
    elif pool_method == "threading":
        method = "thread"

    if method == "process":
        worker_pool = Pool(processes=num_workers)
    elif method == "thread":
        worker_pool = ThreadPoolExecutor(max_workers=num_workers)

    return worker_pool, method


def _run_parallel(
    fnc,
    params_list: list,
    n_total: int,
    num_workers: int = -1,
    pool_method: str = None,
    msg: str = "Process",
):
    worker_pool, method = _create_workers(num_workers, n_total, pool_method)
    lists = []
    try:
        with worker_pool as pool:
            print(f"------ {msg} {n_total} files in parallel")
            if method == "process":
                lists = list(
                    tqdm(
                        pool.map(fnc, params_list, chunksize=1),
                        total=n_total,
                        mininterval=0.1,
                    )
                )
            else:
                lists = list(
                    tqdm(pool.map(fnc, params_list), total=n_total, mininterval=0.1)
                )
    except Exception as e:
        print(f"Error occurred: {e}")

    finally:
        if method == "process":
            worker_pool.close()
            worker_pool.join()

    return lists


def _save_files_pl(df: pl.DataFrame, file_name: str, output_format: list | None = None):
    if output_format is None:
        output_format = [".parquet"]

    def replace_columns_with_na(df, replacement_value: str = "N/A"):
        return df.with_columns(
            [
                pl.when(pl.col(col).is_null().all())
                .then(replacement_value)
                .otherwise(pl.col(col))
                .alias(col)
                for col in df.columns
            ]
        )

    file_names = []

    try:
        for extension in output_format:
            current_file = f"{file_name}{extension}"

            if extension == ".csv":
                df.write_csv(current_file)
            elif extension == ".xlsx":
                df.write_excel(current_file)
            elif extension == ".parquet":
                df.write_parquet(current_file)
            elif extension == ".ipc":  # Arrow IPC (alternative to pickle)
                df.write_ipc(current_file)
            elif extension == ".dta":
                df = replace_columns_with_na(
                    df, replacement_value="N/A"
                )  # Handle empty columns for Stata
                df.write_stata(current_file)
            else:
                print(f"Unsupported format: {extension}")
                continue

            file_names.append(current_file)

    except PermissionError as e:
        print(
            f"PermissionError: {e}. Check if you have the necessary permissions to write to the specified location."
        )

        if not file_name.endswith("_copy"):
            print(f'Saving "{file_name}" as "{file_name}_copy" instead')
            return _save_files_pl(df, file_name + "_copy", output_format)
        else:
            print(f'"{file_name}" was not saved')

    return file_names


def _save_files_pd(df: pd.DataFrame, file_name: str, output_format: list | None = None):
    if output_format is None:
        output_format = [".parquet"]

    def replace_columns_with_na(df, replacement_value: str = "N/A"):
        for column_name in df.columns:
            if df[column_name].isna().all():
                df[column_name] = replacement_value
        return df

    file_names = []
    try:
        for extension in output_format:
            if extension == ".csv":
                current_file = file_name + ".csv"
                df.to_csv(current_file, index=False)
            elif extension == ".xlsx":
                current_file = file_name + ".xlsx"
                df.to_excel(current_file, index=False)
            elif extension == ".parquet":
                current_file = file_name + ".parquet"
                df.to_parquet(current_file)
            elif extension == ".pickle":
                current_file = file_name + ".pickle"
                df.to_pickle(current_file)
            elif extension == ".dta":
                current_file = file_name + ".dta"
                df = replace_columns_with_na(
                    df, replacement_value="N/A"
                )  # .dta format does not like empty columns so these are removed
                df.to_stata(current_file)
            file_names.append(current_file)

    except PermissionError as e:
        print(
            f"PermissionError: {e}. Check if you have the necessary permissions to write to the specified location."
        )

        if not file_name.endswith("_copy"):
            print(f'Saving "{file_name}" as "{file_name}_copy" instead')
            current_file = _save_files_pd(df, file_name + "_copy", output_format)
            file_names.append(current_file)
        else:
            print(f'"{file_name}" was not saved')

    return file_names


def _create_chunks(df, output_format: list | None = None, file_size: int = 100):
    if output_format is None:
        output_format = [".parquet"]

    def memory_usage_pl(df: pl.DataFrame):
        """
        Estimate the memory usage of a Polars DataFrame.
        This will sum the size of each column in the DataFrame.
        """
        column_sizes = []
        for col in df.columns:
            dtype = df[col].dtype
            num_rows = df.height
            if dtype == pl.Int32:
                column_sizes.append(num_rows * 4)  # 4 bytes per Int32
            elif dtype == pl.Int64:
                column_sizes.append(num_rows * 8)  # 8 bytes per Int64
            elif dtype == pl.Float32:
                column_sizes.append(num_rows * 4)  # 4 bytes per Float32
            elif dtype == pl.Float64:
                column_sizes.append(num_rows * 8)  # 8 bytes per Float64
            elif dtype == pl.Utf8:
                # UTF-8 strings are variable-length, but we estimate an average string length
                avg_str_len = 50  # You can adjust this value if needed
                column_sizes.append(num_rows * avg_str_len)
            else:
                column_sizes.append(num_rows * 8)  # For other types (e.g., Boolean)

        return sum(column_sizes)

    total_rows = len(df)
    if ".xlsx" in output_format:
        chunk_size = 1_000_000  # ValueError: This sheet is too large! Your sheet size is: 1926781, 4 Max sheet size is: 1048576, 1
    else:
        # Rough size factor to assure that compressed files of "file_size" size.
        if ".dta" in output_format or ".pickle" in output_format:
            size_factor = 1.5
        elif ".csv" in output_format:
            size_factor = 3
        elif ".parquet" in output_format:
            size_factor = 12

        # Convert maximum file size to bytes
        file_size = file_size * 1024 * 1024 * size_factor

        if isinstance(df, pd.DataFrame):
            n_chunks = int(df.memory_usage(deep=True).sum() / file_size)
        elif isinstance(df, pl.DataFrame):
            n_chunks = int(memory_usage_pl(df) / file_size)

        if n_chunks == 0:
            n_chunks = 1
        chunk_size = int(total_rows / n_chunks)

        n_chunks = pd.Series(np.ceil(total_rows / chunk_size)).astype(int)
        n_chunks = int(n_chunks.iloc[0])
        if n_chunks == 0:
            n_chunks = 1

        return n_chunks, total_rows, chunk_size


def _process_chunk(params):
    i, chunk, n_chunks, file_name, output_format = params
    if n_chunks > 1:
        file_part = f"{file_name}_{i}"
    else:
        file_part = file_name
    if isinstance(chunk, pd.DataFrame):
        file_name = _save_files_pd(chunk, file_part, output_format)
    elif isinstance(chunk, pl.DataFrame):
        file_name = _save_files_pl(chunk, file_part, output_format)

    return file_name


def _save_chunks(
    dfs: list,
    file_name: str,
    output_format: list | None = None,
    file_size: int = 100,
    num_workers: int = 1,
):
    if output_format is None:
        output_format = [".csv"]
    num_workers = int(num_workers)
    file_names = None
    if len(dfs) > 0:
        if isinstance(dfs, list):
            print("------ Concatenating fileparts")
            dfs = pd.concat(dfs, ignore_index=True)

        if output_format is None or file_name is None:
            return dfs, file_names

        elif len(dfs) == 0:
            print("No rows have been retained")
            return dfs, file_names

        print("------ Saving files")
        n_chunks, total_rows, chunk_size = _create_chunks(dfs, output_format, file_size)
        total_rows = len(dfs)

        # Read multithreaded
        if (
            isinstance(num_workers, (int, float, complex))
            and num_workers != 1
            and n_chunks > 1
        ):
            print(f"Saving {n_chunks} files")
            if isinstance(dfs, pd.DataFrame):
                params_list = [
                    (
                        i,
                        dfs[start : min(start + chunk_size, total_rows)].copy(),
                        n_chunks,
                        file_name,
                        output_format,
                    )
                    for i, start in enumerate(range(0, total_rows, chunk_size), start=1)
                ]
            elif isinstance(dfs, pl.DataFrame):
                params_list = [
                    (
                        i,
                        dfs.slice(start, chunk_size),
                        n_chunks,
                        file_name,
                        output_format,
                    )
                    for i, start in enumerate(range(0, total_rows, chunk_size), start=1)
                ]

            file_names = _run_parallel(
                fnc=_process_chunk,
                params_list=params_list,
                n_total=n_chunks,
                num_workers=num_workers,
                msg="Saving",
            )
        else:
            file_names = []
            for i, start in enumerate(range(0, total_rows, chunk_size), start=1):
                print(f" {i} of {n_chunks} files")

                if isinstance(dfs, pd.DataFrame):
                    current_file = _process_chunk(
                        [
                            i,
                            dfs[start : min(start + chunk_size, total_rows)].copy(),
                            n_chunks,
                            file_name,
                            output_format,
                        ]
                    )
                elif isinstance(dfs, pl.DataFrame):
                    current_file = _process_chunk(
                        [
                            i,
                            dfs.slice(start, chunk_size),
                            n_chunks,
                            file_name,
                            output_format,
                        ]
                    )

                file_names.append(current_file)

        file_names = [
            item for sublist in file_names for item in sublist if item is not None
        ]

    return dfs, file_names


def _load_pd(
    file: str,
    select_cols=None,
    date_query: list | None = None,
    bvd_query: str = None,
    query=None,
    query_args: list | None = None,
):
    if date_query is None:
        date_query = [None, None, None, "remove"]
    try:
        df = _read_pd(file, select_cols)

        if df.empty:
            print(f"{os.path.basename(file)} empty after column selection")
            return df
    except pyarrow.lib.ArrowInvalid as e:
        folder_path = os.path.dirname(file)
        if os.path.exists(folder_path) and os.path.isdir(folder_path):
            shutil.rmtree(folder_path)

        raise ValueError(
            f"Error reading {os.path.basename(file)} folder and sub files {folder_path} has been removed): {e}"
        ) from e
    except Exception as e:
        raise ValueError(f"Error reading file: {e}") from e

    if all(date_query):
        try:
            df = _date_pd(
                df,
                date_col=date_query[2],
                start_year=date_query[0],
                end_year=date_query[1],
                nan_action=date_query[3],
            )
        except Exception as e:
            raise ValueError(f"Error while date selection: {e}") from e
        if df.empty:
            print(f"{os.path.basename(file)} empty after date selection")
            return df

    if bvd_query is not None:
        try:
            df = df.query(bvd_query)
        except Exception as e:
            raise ValueError(f"Error while bvd filtration: {e}") from e
        if df.empty:
            print(f"{os.path.basename(file)} empty after bvd selection")
            return df

    # Apply function or query to filter df
    if query is not None:
        if isinstance(query, type(lambda: None)):
            try:
                df = query(df, *query_args) if query_args else query(df)
            except Exception as e:
                raise ValueError(
                    f"Error curating file with custom function: {e}"
                ) from e
        elif isinstance(query, str):
            try:
                df = df.query(query)
            except Exception as e:
                raise ValueError(f"Error curating file with pd.query(): {e}") from e
        if df.empty:
            print(f"{os.path.basename(file)} empty after query filtering")
    return df


def _load_pl(
    file_list: list,
    select_cols=None,
    date_query=None,
    bvd_query=None,
    query=None,
    query_args=None,
    row_limit: int | None = None,
):
    if date_query is None:
        date_query = [None, None, None, "remove"]
    """
    Load multiple files into a Polars LazyFrame and apply optional filtering.

    Args:
    - file_list (list): List of file paths.
    - select_cols (list, optional): Columns to select.
    - date_query (list, optional): [start_year, end_year, date_column, nan_action].
    - bvd_query (tuple, optional): (values_list, column_name or column_names[, mode]).
    - query (callable or Polars expression, optional): Additional filter query.
    - query_args (tuple, optional): Arguments for function-based query.

    Returns:
    - Polars DataFrame
    """

    def _normalize_pl_bvd_query(bvd_query):
        if len(bvd_query) == 2:
            values, columns = bvd_query
            mode = "exact"
        elif len(bvd_query) == 3:
            values, columns, mode = bvd_query
        else:
            raise ValueError(
                "bvd_query must be [values, columns] or [values, columns, mode]."
            )

        if isinstance(values, (pd.Series, np.ndarray)):
            values = values.tolist()
        elif not isinstance(values, list):
            values = [values]

        values = [str(value) for value in values]

        if isinstance(columns, str):
            columns = [columns]
        elif isinstance(columns, (pd.Series, np.ndarray)):
            columns = columns.tolist()
        elif not isinstance(columns, list):
            columns = [columns]

        if mode not in ["exact", "prefix"]:
            raise ValueError("BvD filter mode must be 'exact' or 'prefix'.")

        return values, columns, mode

    def _resolve_required_columns(select_cols, date_query, bvd_query):
        output_cols = None
        required_cols = None

        if select_cols is not None:
            if isinstance(select_cols, str):
                output_cols = [select_cols]
            else:
                output_cols = list(select_cols)
            required_cols = list(output_cols)

        date_col = None
        if date_query is not None and all(date_query[:3]):
            date_col = date_query[2]

        if required_cols is not None and bvd_query is not None:
            _, bvd_cols, _ = _normalize_pl_bvd_query(bvd_query)
            required_cols.extend(bvd_cols)

        if required_cols is not None and date_col is not None:
            required_cols.append(date_col)

        if required_cols is not None:
            required_cols = list(dict.fromkeys(required_cols))

        return output_cols, required_cols, date_col

    def _apply_pl_bvd_filter(df_lazy, bvd_query):
        values, columns, mode = _normalize_pl_bvd_query(bvd_query)

        if mode == "exact":
            filters = [
                pl.col(column).cast(pl.Utf8, strict=False).is_in(values)
                for column in columns
            ]
        else:
            filters = [
                pl.col(column).cast(pl.Utf8, strict=False).str.starts_with(value)
                for column in columns
                for value in values
            ]

        if not filters:
            return df_lazy

        return df_lazy.filter(pl.any_horizontal(filters))

    try:
        # Load files lazily
        df_lazy = _read_pl(file_list)

        output_cols, required_cols, date_col = _resolve_required_columns(
            select_cols=select_cols,
            date_query=date_query,
            bvd_query=bvd_query,
        )

        if required_cols is not None:
            df_lazy = df_lazy.select(required_cols)

        # Apply BVD query filter before date parsing to cut row counts early.
        if bvd_query is not None:
            df_lazy = _apply_pl_bvd_filter(df_lazy, bvd_query)

        if all(date_query[:3]):
            df_lazy = _date_pl(
                df_lazy,
                date_col=date_col,
                start_year=date_query[0],
                end_year=date_query[1],
                nan_action=date_query[3],
            )

        # Apply query function or filter
        if query is not None:
            if callable(query):
                df_lazy = query(df_lazy, *query_args) if query_args else query(df_lazy)
            elif isinstance(query, pl.Expr):
                df_lazy = df_lazy.filter(query)
            elif isinstance(query, str):
                raise ValueError(
                    "String queries are not supported in polars_all(). "
                    "Use process_all(), a polars expression, or a callable."
                )

        if output_cols is not None:
            df_lazy = df_lazy.select(output_cols)

        if row_limit is not None and row_limit >= 0:
            df_lazy = df_lazy.limit(int(row_limit))

        # Collect the results (triggering execution)
        df = df_lazy.collect()

        return df

    except ValueError:
        raise
    except Exception as e:
        raise RuntimeError(f"Error processing files: {file_list}") from e


def _read_csv_chunk(params):
    (
        file,
        chunk_idx,
        chunk_size,
        select_cols,
        col_index,
        date_query,
        bvd_query,
        query,
        query_args,
    ) = params
    try:
        df = pd.read_csv(
            file, low_memory=False, skiprows=chunk_idx * chunk_size, nrows=chunk_size
        )
    except Exception as e:
        raise ValueError(f"Error while reading chunk: {e}") from e

    if select_cols is not None:
        df = df.iloc[:, col_index]
        df.columns = select_cols

    if all(date_query):
        try:
            df = _date_pd(
                df,
                date_col=date_query[2],
                start_year=date_query[0],
                end_year=date_query[1],
                nan_action=date_query[3],
            )
        except Exception as e:
            raise ValueError(f"Error while date selection: {e}") from e
        if df.empty:
            print(f"{os.path.basename(file)} empty after date selection")
            return df
    if bvd_query is not None:
        try:
            df = df.query(bvd_query)
        except Exception as e:
            raise ValueError(f"Error while bvd filtration: {e}") from e
        if df.empty:
            print(f"{os.path.basename(file)} empty after bvd selection")
            return df

    # Apply function or query to filter df
    if query is not None:
        if isinstance(query, type(lambda: None)):
            try:
                df = query(df, *query_args) if query_args else query(df)
            except Exception as e:
                raise ValueError(
                    f"Error curating file with custom function: {e}"
                ) from e
        elif isinstance(query, str):
            try:
                df = df.query(query)
            except Exception as e:
                raise ValueError(f"Error curating file with pd.query(): {e}") from e
        if df.empty:
            print(f"{os.path.basename(file)} empty after query filtering")
    return df


def _load_csv_table(
    file: str,
    select_cols=None,
    date_query: list | None = None,
    bvd_query: str | None = None,
    query=None,
    query_args: list | None = None,
    num_workers: int = -1,
):
    if date_query is None:
        date_query = [None, None, None, "remove"]

    def check_cols(file, select_cols):
        col_index = None
        # Read a small chunk to get column names if needed
        header_chunk = pd.read_csv(file, nrows=0)  # Read only header
        available_cols = header_chunk.columns.tolist()

        if select_cols is None:
            select_cols = available_cols

        if not set(select_cols).issubset(available_cols):
            missing_cols = set(select_cols) - set(available_cols)
            raise ValueError(f"Columns not found in file: {missing_cols}")

        # Find indices of select_cols
        col_index = [available_cols.index(col) for col in select_cols]
        select_cols = [available_cols[i] for i in col_index]

        return select_cols, col_index

    if num_workers < 1:
        num_workers = int(psutil.virtual_memory().total / (1024**3) / 12)

    # check if the requested columns exist
    select_cols, col_index = check_cols(file, select_cols)

    # Step 1: Determine the total number of rows using subprocess
    if sys.platform.startswith("linux") or sys.platform == "darwin":
        safe_file_path = shlex.quote(file)
        num_lines = (
            int(
                subprocess.check_output(f"wc -l {safe_file_path}", shell=True).split()[
                    0
                ]
            )
            - 1
        )
    elif sys.platform == "win32":

        def count_lines_chunk(file_path):
            # Open the file in binary mode for faster reading
            with open(file_path, "rb") as f:
                # Use os.read to read large chunks of the file at once (64KB in this case)
                buffer_size = 1024 * 64  # 64 KB
                read_chunk = f.read(buffer_size)
                count = 0
                while read_chunk:
                    # Count the number of newlines in each chunk
                    count += read_chunk.count(b"\n")
                    read_chunk = f.read(buffer_size)
            return count

        num_lines = count_lines_chunk(file) - 1

    # Step 2: Calculate the chunk size to create 64 chunks
    chunk_size = num_lines // num_workers

    # Step 3: Prepare the params_list
    params_list = [
        (
            file,
            i,
            chunk_size,
            select_cols,
            col_index,
            date_query,
            bvd_query,
            query,
            query_args,
        )
        for i in range(num_workers)
    ]

    # Step 4: Use _run_parallel to read the DataFrame in parallel
    chunks = _run_parallel(
        _read_csv_chunk,
        params_list,
        n_total=num_workers,
        num_workers=num_workers,
        pool_method="process",
        msg="Reading chunks",
    )

    # Step 5: Concatenate all chunks into a single DataFrame
    df = pd.concat(chunks, ignore_index=True)

    return df


def _save_to(df, filename, format: SaveFormat | bool = None):
    if format is False:
        format = None

    if df is None or (
        isinstance(df, (pd.DataFrame, pl.DataFrame))
        and (df.empty if isinstance(df, pd.DataFrame) else df.is_empty())
    ):
        print("df is empty and cannot be saved")
        return

    def check_format(file_type):
        allowed_values = {None, "xlsx", "csv"}
        if file_type not in allowed_values:
            print(
                f"Invalid file_type: {file_type}. Allowed values are None, 'xlsx', or 'csv'."
            )

    check_format(format)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    if format == "xlsx":
        filename = f"{filename}_{timestamp}.xlsx"
        if isinstance(df, pd.DataFrame):
            df.to_excel(filename, index=False)
        else:  # Convert Polars to Pandas before saving
            df.to_pandas().to_excel(filename, index=False)

    elif format == "csv":
        filename = f"{filename}_{timestamp}.csv"
        if isinstance(df, pd.DataFrame):
            df.to_csv(filename, index=False)
        else:  # Convert Polars to Pandas before saving
            df.write_csv(filename)
    else:
        filename = None

    if filename is not None:
        print(f"Results have been saved to '{filename}'")


def _check_list_format(values, *args):
    # Convert the input value to a list if it's a string
    if isinstance(values, str):
        values = [values]
    elif values is None:
        values = []
    elif isinstance(values, list):
        # Check if all elements in the list are strings
        if not all(isinstance(value, str) for value in values):
            raise ValueError("Not all inputs to the list are in str format")
    else:
        raise ValueError("Input list is in the wrong format.")

    # Check additional arguments
    for arg in args:
        if arg is not None:
            if isinstance(arg, str):
                # Convert single string to a list
                arg = [arg]
            if isinstance(arg, list):
                # Iterate and check each string
                for item in arg:
                    if not isinstance(item, str):
                        raise ValueError("All items in the list must be strings")
                    if item not in values:
                        values.append(item)
            else:
                raise ValueError(
                    "Additional arguments must be either None, a string, or a list of strings"
                )

    # Retain unique values before returning
    return list(set(values))


def _date_pd(
    df,
    date_col=None,
    start_year: int = None,
    end_year: int = None,
    nan_action: str = "remove",
):
    """
    Filter DataFrame based on a date column and optional start/end years.

    Args:
    df (pd.DataFrame): The DataFrame to filter.
    date_col (str): The name of the date column in the DataFrame.
    start_year (int, optional): The starting year for filtering (inclusive). Defaults to None (no lower bound).
    end_year (int, optional): The ending year for filtering (inclusive). Defaults to None (no upper bound).

    Returns:
    pd.DataFrame: Filtered DataFrame based on the date and optional year filters.
    """

    pd.options.mode.copy_on_write = True

    if date_col is None:
        columns_to_check = ["closing_date", "information_date"]
    else:
        columns_to_check = [date_col]

    date_col = next((col for col in columns_to_check if col in df.columns), None)

    if not date_col:
        print("No valid date columns found")
        return df

    # Separate rows with NaNs in the date column
    if nan_action == "keep":
        nan_rows = df[df[date_col].isna()]
    df = df.dropna(subset=[date_col])

    try:
        df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
    except ValueError as e:
        print(f"{e}")
        return df

    date_filter = (df[date_col].dt.year >= start_year) & (
        df[date_col].dt.year <= end_year
    )

    if date_filter.any():
        df = df.loc[date_filter]
    else:
        df = pd.DataFrame()

    # Add back the NaN rows
    if nan_action == "keep":
        if not nan_rows.empty:
            df = pd.concat([df, nan_rows], sort=False)
            df = df.sort_index()

    return df


def _date_pl(
    df,
    date_col=None,
    start_year: int = None,
    end_year: int = None,
    nan_action: str = "remove",
):
    """
    Filter DataFrame based on a date column and optional start/end years.

    Args:
    df (pl.DataFrame): The DataFrame to filter.
    date_col (str): The name of the date column in the DataFrame.
    start_year (int, optional): The starting year for filtering (inclusive). Defaults to None (no lower bound).
    end_year (int, optional): The ending year for filtering (inclusive). Defaults to None (no upper bound).

    Returns:
    pl.DataFrame: Filtered DataFrame based on the date and optional year filters.
    """

    available_cols = df.collect_schema().names()

    if date_col is None:
        columns_to_check = ["closing_date", "information_date"]
    else:
        columns_to_check = [date_col]

    date_col = next((col for col in columns_to_check if col in available_cols), None)

    if not date_col:
        print("No valid date columns found")
        return df

    df = df.with_columns(
        pl.col(date_col)
        .cast(pl.Utf8, strict=False)
        .str.to_datetime(exact=False, strict=False)
        .alias(date_col)
    )

    year_filter = pl.lit(True)
    if start_year is not None and end_year is not None:
        year_filter = year_filter & (pl.col(date_col).dt.year() >= start_year)
        year_filter = year_filter & (pl.col(date_col).dt.year() <= end_year)
    elif start_year is not None:
        year_filter = year_filter & (pl.col(date_col).dt.year() >= start_year)
    elif end_year is not None:
        year_filter = year_filter & (pl.col(date_col).dt.year() <= end_year)

    null_filter = pl.col(date_col).is_null()

    if nan_action == "keep":
        df = df.filter(null_filter | year_filter)
    else:
        df = df.filter(~null_filter & year_filter)

    return df


def _construct_query(bvd_cols, bvd_list, search_type):
    conditions = []
    if isinstance(bvd_cols, str):
        bvd_cols = [bvd_cols]

    for bvd_col in bvd_cols:
        if search_type:
            for substring in bvd_list:
                condition = f"{bvd_col}.str.startswith('{substring}', na=False)"
                conditions.append(condition)
        else:
            condition = f"{bvd_col} in {bvd_list}"
            conditions.append(condition)
    query = " | ".join(conditions)  # Combine conditions using OR (|)
    return query


def _letters_only_regex(text):
    """Converts the title to lowercase and removes non-alphanumeric characters."""
    if isinstance(text, str):
        return re.sub(r"[^a-zA-Z0-9]", "", text.lower())
    else:
        return text


def _fuzzy_match(args):
    """
    Worker function to perform fuzzy matching for each batch of names.
    """
    name_batch, choices, cut_off, df, match_column, return_column, choice_to_index = (
        args
    )
    results = []

    for name in name_batch:
        # First, check if an exact match exists in the choices
        if name in choice_to_index:
            match_index = choice_to_index[name]
            match_value = df.iloc[match_index][match_column]
            return_value = df.iloc[match_index][return_column]
            results.append(
                (name, name, 100, match_value, return_value)
            )  # Exact match with score 100
        else:
            # Perform fuzzy matching if no exact match is found
            match_obj = process.extractOne(name, choices, score_cutoff=cut_off)
            if match_obj:
                match, score, match_index = match_obj
                match_value = df.iloc[match_index][match_column]
                return_value = df.iloc[match_index][return_column]
                results.append((name, match, score, match_value, return_value))
            else:
                results.append((name, None, 0, None, None))

    return results


def fuzzy_query(
    df: pd.DataFrame,
    names: list,
    match_column: str = None,
    return_column: str = None,
    cut_off: int = 50,
    remove_str: list = None,
    num_workers: int = None,
):
    """
    Perform fuzzy string matching with a list of input strings against a specific column in a DataFrame.
    """

    def remove_suffixes(choices, suffixes):
        for i, choice in enumerate(choices):
            choice_lower = (
                choice.lower()
            )  # convert to lowercase for case-insensitive comparison
            for suffix in suffixes:
                if choice_lower.endswith(suffix.lower()):
                    # Remove the suffix from the end of the string
                    choices[i] = choice_lower[: -len(suffix)].strip()
        return choices

    def remove_substrings(choices, substrings):
        for substring in substrings:
            choices = [choice.replace(substring.lower(), "") for choice in choices]
        return choices

    choices = [choice.lower() for choice in df[match_column].tolist()]
    names = [name.lower() for name in names]

    if remove_str:
        names = remove_suffixes(names, remove_str)
        choices = remove_suffixes(choices, remove_str)

    # Create a mapping of choice to index for fast exact match lookup
    choice_to_index = {choice: i for i, choice in enumerate(choices)}

    # Determine the number of workers if not specified
    if not num_workers or num_workers < 0:
        num_workers = max(1, cpu_count() - 2)

    # Ensure number of workers is not greater than the number of names
    if len(names) < num_workers:
        num_workers = len(names)

    # Parallel processing
    matches = []
    if num_workers > 1:
        # Split names into batches according to the number of workers
        batch_size = ceil(len(names) / num_workers)
        name_batches = [
            names[i : i + batch_size] for i in range(0, len(names), batch_size)
        ]

        args_list = [
            (batch, choices, cut_off, df, match_column, return_column, choice_to_index)
            for batch in name_batches
        ]

        with Pool(processes=num_workers) as pool:
            results = pool.map(_fuzzy_match, args_list)
            for result_batch in results:
                matches.extend(result_batch)
    else:
        # If single worker, process in a simple loop
        matches.extend(
            _fuzzy_match(
                (
                    names,
                    choices,
                    cut_off,
                    df,
                    match_column,
                    return_column,
                    choice_to_index,
                )
            )
        )

    # Create the result DataFrame
    result_df = pd.DataFrame(
        matches,
        columns=["Search_string", "BestMatch", "Score", match_column, return_column],
    )

    return result_df


def fuzzy_match_pl(
    names: list,
    file_list: list = None,
    df=None,
    match_column: str = None,
    return_column: str = None,
    cut_off: int = 50,
    remove_str: list = None,
    num_workers: int = None,
):
    def normalize_company_name(value):
        if pd.isna(value):
            return None

        normalized = str(value).lower()
        original_lower = normalized

        if remove_str:
            for suffix in remove_str:
                suffix = str(suffix).lower()
                if suffix and original_lower.endswith(suffix):
                    normalized = original_lower[: -len(suffix)].strip()

        return normalized

    def block_key(value, length):
        if value is None:
            return ""
        letters_only = _letters_only_regex(value)
        return letters_only[:length]

    def prepare_source_frame():
        if df is not None:
            source = df
        elif file_list is not None:
            source = _read_pl(file_list)
        else:
            raise ValueError("Either 'df' or 'file_list' must be provided.")

        if isinstance(source, pl.LazyFrame):
            source = source.select([match_column, return_column]).collect()
        elif isinstance(source, pl.DataFrame):
            source = source.select([match_column, return_column])
        elif isinstance(source, pd.DataFrame):
            source = pl.from_pandas(source[[match_column, return_column]].copy())
        else:
            raise ValueError("'df' must be a pandas or polars DataFrame.")

        return source.to_pandas()

    try:
        if not num_workers or num_workers < 0:
            num_workers = max(1, cpu_count() - 2)

        source_df = prepare_source_frame()
        source_df = source_df.dropna(subset=[match_column]).reset_index(drop=True)
        source_df["BestMatch"] = source_df[match_column].map(normalize_company_name)
        source_df = source_df.dropna(subset=["BestMatch"]).reset_index(drop=True)
        source_df["_source_idx"] = source_df.index
        source_df["_prefix3"] = source_df["BestMatch"].map(lambda value: block_key(value, 3))
        source_df["_prefix1"] = source_df["BestMatch"].map(lambda value: block_key(value, 1))
        source_df["_name_len"] = source_df["BestMatch"].str.len()

        # Match the pandas exact-match behavior by keeping the last occurrence.
        source_df = source_df.drop_duplicates(subset=["BestMatch"], keep="last").reset_index(
            drop=True
        )

        exact_lookup = {
            row["BestMatch"]: row
            for row in source_df[
                [match_column, return_column, "BestMatch", "_prefix3", "_prefix1", "_name_len"]
            ].to_dict("records")
        }

        prefix3_map = defaultdict(list)
        prefix1_map = defaultdict(list)
        candidate_records = source_df[
            [match_column, return_column, "BestMatch", "_prefix3", "_prefix1", "_name_len"]
        ].to_dict("records")
        for record in candidate_records:
            prefix3_map[record["_prefix3"]].append(record)
            prefix1_map[record["_prefix1"]].append(record)

        def dedupe_candidates(candidates):
            seen = set()
            unique_candidates = []
            for candidate in candidates:
                key = candidate["BestMatch"]
                if key not in seen:
                    seen.add(key)
                    unique_candidates.append(candidate)
            return unique_candidates

        def filter_by_length(candidates, search_string):
            name_length = len(search_string)
            max_delta = max(3, min(10, int(ceil(name_length / 3))))
            filtered = [
                candidate
                for candidate in candidates
                if abs(candidate["_name_len"] - name_length) <= max_delta
            ]
            return filtered or candidates

        def candidate_tiers(search_string):
            prefix3 = block_key(search_string, 3)
            prefix1 = block_key(search_string, 1)
            tiers = []

            prefix3_candidates = dedupe_candidates(prefix3_map.get(prefix3, []))
            prefix1_candidates = dedupe_candidates(prefix1_map.get(prefix1, []))

            if prefix3_candidates:
                tiers.append(filter_by_length(prefix3_candidates, search_string))

            if prefix1_candidates:
                filtered_prefix1 = filter_by_length(prefix1_candidates, search_string)
                if not tiers or filtered_prefix1 != tiers[-1]:
                    tiers.append(filtered_prefix1)

            all_length_candidates = filter_by_length(candidate_records, search_string)
            if not tiers or all_length_candidates != tiers[-1]:
                tiers.append(all_length_candidates)

            if tiers[-1] != candidate_records:
                tiers.append(candidate_records)

            return tiers

        def score_candidate_set(search_string, candidates):
            choices = [candidate["BestMatch"] for candidate in candidates]

            if len(choices) > 2000:
                best_match = process.extractOne(
                    search_string, choices, score_cutoff=cut_off
                )
                if best_match is None:
                    return []

                _, best_score, candidate_idx = best_match
                candidate = candidates[candidate_idx]
                return [
                    (
                        search_string,
                        candidate["BestMatch"],
                        float(best_score),
                        candidate[match_column],
                        candidate[return_column],
                    )
                ]

            matches = process.extract(
                search_string,
                choices,
                score_cutoff=cut_off,
                limit=len(choices),
            )

            if not matches:
                return []

            best_score = matches[0][1]
            best_candidates = [
                candidates[candidate_idx]
                for _, score, candidate_idx in matches
                if score == best_score
            ]
            return [
                (
                    search_string,
                    candidate["BestMatch"],
                    float(best_score),
                    candidate[match_column],
                    candidate[return_column],
                )
                for candidate in best_candidates
            ]

        def score_search_strings(search_batch):
            result_rows = []
            for search_string in search_batch:
                exact_match = exact_lookup.get(search_string)
                if exact_match is not None:
                    result_rows.append(
                        (
                            search_string,
                            search_string,
                            100.0,
                            exact_match[match_column],
                            exact_match[return_column],
                        )
                    )
                    continue

                matched_rows = []
                for candidates in candidate_tiers(search_string):
                    matched_rows = score_candidate_set(search_string, candidates)
                    if matched_rows:
                        break

                if not matched_rows:
                    result_rows.append((search_string, None, 0.0, None, None))
                    continue

                result_rows.extend(matched_rows)

            return result_rows

        search_names = [normalize_company_name(name) for name in names]
        search_names = [name for name in search_names if name is not None]

        if len(search_names) < num_workers:
            num_workers = len(search_names) or 1

        if num_workers > 1 and len(search_names) > 1:
            batch_size = ceil(len(search_names) / num_workers)
            search_batches = [
                search_names[i : i + batch_size]
                for i in range(0, len(search_names), batch_size)
            ]
            matches = []
            with ThreadPoolExecutor(max_workers=num_workers) as pool:
                for batch_matches in pool.map(score_search_strings, search_batches):
                    matches.extend(batch_matches)
        else:
            matches = score_search_strings(search_names)

        return pd.DataFrame(
            matches,
            columns=["Search_string", "BestMatch", "Score", match_column, return_column],
        )

    except Exception as e:
        raise RuntimeError(f"Error processing fuzzy company matching: {file_list}") from e

def _bvd_changes_ray(initial_ids, df, num_workers: int = -1):
    """Resolve connected BvD ID lineage and map each ID to its terminal newest ID.

    The `num_workers` argument is retained for API compatibility but is no longer used.
    """

    initial_ids = {bvd_id for bvd_id in set(initial_ids) if pd.notna(bvd_id)}
    if not initial_ids:
        return set(), {}, df.iloc[0:0].copy()

    graph_df = df[["old_id", "new_id"]].copy()
    if "change_date" in df.columns:
        graph_df.loc[:, "_change_date"] = pd.to_datetime(
            df["change_date"], errors="coerce"
        )
    else:
        graph_df.loc[:, "_change_date"] = pd.NaT

    graph_df = graph_df.dropna(subset=["old_id", "new_id"])

    neighbors = defaultdict(set)
    edge_dates = {}

    for old_id, new_id, change_date in graph_df.itertuples(index=False):
        neighbors[old_id].add(new_id)
        neighbors[new_id].add(old_id)

        edge_key = (old_id, new_id)
        previous_date = edge_dates.get(edge_key)
        if previous_date is None or (
            pd.notna(change_date)
            and (pd.isna(previous_date) or change_date > previous_date)
        ):
            edge_dates[edge_key] = change_date

    discovered_ids = set(initial_ids)
    queue = deque(initial_ids)

    while queue:
        current_id = queue.popleft()
        for neighbor in neighbors.get(current_id, ()):
            if neighbor not in discovered_ids:
                discovered_ids.add(neighbor)
                queue.append(neighbor)

    outgoing_edges = defaultdict(list)
    for (old_id, new_id), change_date in edge_dates.items():
        if old_id in discovered_ids and new_id in discovered_ids:
            outgoing_edges[old_id].append((change_date, new_id))

    def _edge_sort_key(item):
        change_date, new_id = item
        has_date = 1 if pd.notna(change_date) else 0
        normalized_date = change_date if pd.notna(change_date) else pd.Timestamp.min
        return (has_date, normalized_date, str(new_id))

    for old_id in outgoing_edges:
        outgoing_edges[old_id].sort(key=_edge_sort_key, reverse=True)

    newest_ids = {}
    visiting = set()

    def resolve_newest_id(current_id):
        if current_id in newest_ids:
            return newest_ids[current_id]

        if current_id in visiting:
            return current_id

        visiting.add(current_id)
        next_ids = [new_id for _, new_id in outgoing_edges.get(current_id, [])]

        if not next_ids:
            resolved_id = current_id
        else:
            resolved_id = resolve_newest_id(next_ids[0])

        visiting.remove(current_id)
        newest_ids[current_id] = resolved_id
        return resolved_id

    for bvd_id in discovered_ids:
        resolve_newest_id(bvd_id)

    filtered_df = df[
        df["old_id"].isin(discovered_ids) | df["new_id"].isin(discovered_ids)
    ].copy()

    if not filtered_df.empty:
        filtered_df.loc[:, "newest_id"] = filtered_df.apply(
            lambda row: newest_ids.get(
                row["old_id"], newest_ids.get(row["new_id"])
            ),
            axis=1,
        )

    no_iter = 1 + max(0, len(discovered_ids) - len(initial_ids))
    print(f"Bvd_id changes were found at {no_iter} steps")

    if initial_ids == discovered_ids:
        print("No changes were found for the provided bvd_ids")

    return discovered_ids, newest_ids, filtered_df


def _read_pd(file, select_cols):
    """Load multiple files lazily based on extension."""

    def read_avro(file):
        df = []
        with open(file, "rb") as avro_file:
            avro_reader = fastavro.reader(avro_file)
            for record in avro_reader:
                df.append(record)
        df = pd.DataFrame(df)

        return df

    read_functions = {
        "csv": pd.read_csv,
        "xlsx": pd.read_excel,
        "parquet": pd.read_parquet,
        "orc": pd.read_orc,
        "avro": read_avro,
    }

    file_ext = file.lower().split(".")[-1]

    if file_ext not in read_functions:
        raise ValueError(f"Unsupported file format: {file_ext}")

    read_function = read_functions[file_ext]

    if select_cols is None:
        df = read_function(file)
    else:
        if file_ext in ["csv", "xlsx"]:
            df = read_function(file, usecols=select_cols)
        elif file_ext in ["parquet", "orc"]:
            df = read_function(file, columns=select_cols)
    return df


def _read_pl(files):
    """Load multiple files lazily based on extension."""

    # Read Avro function
    def read_avro(files):
        df_list = []
        for file in files:
            with open(file, "rb") as avro_file:
                avro_reader = fastavro.reader(avro_file)
                df_list.extend(avro_reader)  # Collect all records
        return pl.LazyFrame(df_list)  # Convert to LazyFrame

    read_functions = {
        "csv": lambda files: pl.scan_csv(files),
        "xlsx": lambda files: pl.read_excel(
            files[0]
        ).lazy(),  # Only supports single files
        "parquet": lambda files: pl.scan_parquet(files),
        "avro": read_avro,
    }

    first_file = files[0]
    file_ext = first_file.lower().split(".")[-1]

    if file_ext not in read_functions:
        raise ValueError(f"Unsupported file format: {file_ext}")

    return read_functions[file_ext](files)
