import os
import re
import shlex
import shutil
import subprocess
import sys
import warnings
from collections import defaultdict, deque
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from functools import lru_cache
from math import ceil
from multiprocessing import Pool, cpu_count
from pathlib import Path
from typing import Callable, Literal

import fastavro
import numpy as np
import pandas as pd
import polars as pl
import psutil
import pyarrow
from rapidfuzz import fuzz, process
from tqdm import tqdm

from .load_data import _country_codes

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
    pool_method: str = "threading",
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
                pool_method=pool_method,
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
    - bvd_query (tuple or dict, optional):
      single clause as `(values_list, column_name or column_names[, mode])` or
      grouped clauses as `{"base": ..., "and": [...], "or": [...]}`.
    - query (callable or Polars expression, optional): Additional filter query.
    - query_args (tuple, optional): Arguments for function-based query.

    Returns:
    - Polars DataFrame
    """

    def _normalize_pl_bvd_query(bvd_query):
        if bvd_query is None:
            return None

        if isinstance(bvd_query, dict):
            return {
                "base": _normalize_bvd_clause(bvd_query.get("base")),
                "and": _normalize_bvd_clause_group(bvd_query.get("and")),
                "or": _normalize_bvd_clause_group(bvd_query.get("or")),
            }

        if isinstance(bvd_query, (list, tuple)):
            return {"base": _normalize_bvd_clause(bvd_query), "and": [], "or": []}

        raise ValueError(
            "bvd_query must be [values, columns[, mode]] or a grouped clause dict."
        )

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

        normalized_bvd_query = _normalize_pl_bvd_query(bvd_query)
        if required_cols is not None and normalized_bvd_query is not None:
            required_cols.extend(
                _collect_bvd_clause_columns(
                    normalized_bvd_query["base"],
                    normalized_bvd_query["and"],
                    normalized_bvd_query["or"],
                )
                or []
            )

        if required_cols is not None and date_col is not None:
            required_cols.append(date_col)

        if required_cols is not None:
            required_cols = list(dict.fromkeys(required_cols))

        return output_cols, required_cols, date_col

    def _apply_pl_bvd_filter(df_lazy, bvd_query):
        normalized_bvd_query = _normalize_pl_bvd_query(bvd_query)
        if normalized_bvd_query is None:
            return df_lazy

        expr = _build_bvd_filter_expr(
            base_clause=normalized_bvd_query["base"],
            and_clauses=normalized_bvd_query["and"],
            or_clauses=normalized_bvd_query["or"],
        )
        if expr is None:
            return df_lazy

        return df_lazy.filter(expr)

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


def _normalize_bvd_clause(clause):
    if clause is None:
        return None

    if isinstance(clause, dict):
        values = clause.get("values")
        columns = clause.get("columns")
        mode = clause.get("mode", "exact")
    elif isinstance(clause, (list, tuple)) and len(clause) in [2, 3]:
        values, columns = clause[:2]
        mode = clause[2] if len(clause) == 3 else "exact"
    else:
        raise ValueError(
            "BvD clauses must be [values, columns] or [values, columns, mode]."
        )

    if isinstance(values, str):
        values = [values]
    elif isinstance(values, (pd.Series, np.ndarray)):
        values = values.tolist()
    elif not isinstance(values, list):
        values = [values]

    if isinstance(columns, str):
        columns = [columns]
    elif isinstance(columns, (pd.Series, np.ndarray)):
        columns = columns.tolist()
    elif not isinstance(columns, list):
        columns = [columns]

    values = [value for value in values if value is not None and str(value).strip()]
    columns = [column for column in columns if column is not None and str(column).strip()]

    if not values or not columns:
        return None

    mode = str(mode).lower()
    if mode not in ["exact", "prefix"]:
        raise ValueError("BvD filter mode must be 'exact' or 'prefix'.")

    return {"values": values, "columns": columns, "mode": mode}


def _normalize_bvd_clause_group(clauses):
    if clauses is None:
        return []

    if isinstance(clauses, (list, tuple)) and len(clauses) == 0:
        return []

    if isinstance(clauses, dict):
        normalized = _normalize_bvd_clause(clauses)
        return [normalized] if normalized is not None else []

    if isinstance(clauses, (list, tuple)):
        group = []
        is_group = len(clauses) > 0

        if is_group:
            for clause in clauses:
                try:
                    normalized_clause = _normalize_bvd_clause(clause)
                except ValueError:
                    is_group = False
                    break

                if normalized_clause is None:
                    is_group = False
                    break

                group.append(normalized_clause)

        if is_group:
            return group

        normalized = _normalize_bvd_clause(clauses)
        return [normalized] if normalized is not None else []

    normalized = _normalize_bvd_clause(clauses)
    return [normalized] if normalized is not None else []


def _build_bvd_clause_query(clause):
    clause = _normalize_bvd_clause(clause)
    if clause is None:
        return None

    query = _construct_query(
        clause["columns"], clause["values"], search_type=clause["mode"] == "prefix"
    )
    return query


def _build_bvd_clause_group_query(clauses):
    clauses = _normalize_bvd_clause_group(clauses)
    queries = []
    for clause in clauses:
        query = _build_bvd_clause_query(clause)
        if query is not None:
            queries.append(query)
    if not queries:
        return None
    if len(queries) == 1:
        return queries[0]
    return " | ".join(queries)


def _build_bvd_clause_group_and_query(clauses):
    clauses = _normalize_bvd_clause_group(clauses)
    queries = []
    for clause in clauses:
        query = _build_bvd_clause_query(clause)
        if query is not None:
            queries.append(query)
    if not queries:
        return None
    if len(queries) == 1:
        return queries[0]
    return " & ".join(queries)


def _build_bvd_filter_query(base_clause=None, and_clauses=None, or_clauses=None):
    base_query = _build_bvd_clause_query(base_clause)
    and_query = _build_bvd_clause_group_and_query(and_clauses)
    or_query = _build_bvd_clause_group_query(or_clauses)

    base_parts = [f"({query})" for query in [base_query, and_query] if query is not None]
    base_and_query = " & ".join(base_parts) if base_parts else None

    if base_and_query is not None and or_query is not None:
        return f"({base_and_query}) | ({or_query})"
    if base_and_query is not None:
        return base_and_query
    return or_query


def _collect_bvd_clause_columns(base_clause=None, and_clauses=None, or_clauses=None):
    columns = []
    for clause in [
        _normalize_bvd_clause(base_clause),
        *_normalize_bvd_clause_group(and_clauses),
        *_normalize_bvd_clause_group(or_clauses),
    ]:
        if clause is not None:
            columns.extend(clause["columns"])

    return list(dict.fromkeys(columns)) if columns else None


def _build_bvd_clause_expr(clause):
    clause = _normalize_bvd_clause(clause)
    if clause is None:
        return None

    exprs = []
    for column in clause["columns"]:
        col_expr = pl.col(column).cast(pl.Utf8, strict=False)
        if clause["mode"] == "exact":
            exprs.append(col_expr.is_in(clause["values"]))
        else:
            exprs.extend([col_expr.str.starts_with(value) for value in clause["values"]])

    if not exprs:
        return None
    if len(exprs) == 1:
        return exprs[0]
    return pl.any_horizontal(exprs)


def _build_bvd_filter_expr(base_clause=None, and_clauses=None, or_clauses=None):
    clause_exprs = []
    base_expr = _build_bvd_clause_expr(base_clause)
    if base_expr is not None:
        clause_exprs.append(base_expr)
    clause_exprs.extend(
        expr
        for expr in (_build_bvd_clause_expr(clause) for clause in _normalize_bvd_clause_group(and_clauses))
        if expr is not None
    )

    and_expr = None
    if clause_exprs:
        and_expr = clause_exprs[0]
        for expr in clause_exprs[1:]:
            and_expr = and_expr & expr

    or_exprs = [
        expr
        for expr in (_build_bvd_clause_expr(clause) for clause in _normalize_bvd_clause_group(or_clauses))
        if expr is not None
    ]

    or_expr = None
    if or_exprs:
        or_expr = or_exprs[0]
        for expr in or_exprs[1:]:
            or_expr = or_expr | expr

    if and_expr is not None and or_expr is not None:
        return and_expr | or_expr
    if and_expr is not None:
        return and_expr
    return or_expr


def _letters_only_regex(text):
    """Converts the title to lowercase and removes non-alphanumeric characters."""
    if isinstance(text, str):
        return re.sub(r"[^a-zA-Z0-9]", "", text.lower())
    else:
        return text


_FUZZY_SCORERS: dict[str, Callable] = {
    "WRatio": fuzz.WRatio,
    "ratio": fuzz.ratio,
    "partial_ratio": fuzz.partial_ratio,
    "token_sort_ratio": fuzz.token_sort_ratio,
    "token_set_ratio": fuzz.token_set_ratio,
    "token_ratio": fuzz.token_ratio,
    "QRatio": fuzz.QRatio,
}


def _resolve_fuzzy_scorer(scorer: str | Callable | None) -> Callable:
    """Return a RapidFuzz scorer from a name or callable."""
    if scorer is None:
        return fuzz.WRatio
    if callable(scorer):
        return scorer
    try:
        return _FUZZY_SCORERS[scorer]
    except KeyError as exc:
        valid = ", ".join(sorted(_FUZZY_SCORERS))
        raise ValueError(f"Unknown fuzzy scorer '{scorer}'. Valid scorers: {valid}.") from exc


def _fuzzy_match(args):
    """
    Worker function to perform fuzzy matching for each batch of names.
    """
    if len(args) == 7:
        name_batch, choices, cut_off, df, match_column, return_column, choice_to_index = args
        scorer = fuzz.WRatio
    else:
        (
            name_batch,
            choices,
            cut_off,
            df,
            match_column,
            return_column,
            choice_to_index,
            scorer,
        ) = args
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
            match_obj = process.extractOne(
                name, choices, scorer=scorer, score_cutoff=cut_off
            )
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
    scorer: str | Callable | None = None,
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

    scorer = _resolve_fuzzy_scorer(scorer)
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
            (
                batch,
                choices,
                cut_off,
                df,
                match_column,
                return_column,
                choice_to_index,
                scorer,
            )
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
                    scorer,
                )
            )
        )

    # Create the result DataFrame
    result_df = pd.DataFrame(
        matches,
        columns=["Search_string", "BestMatch", "Score", match_column, return_column],
    )

    return result_df


class CompanyNameFuzzyMatcher:
    """Reusable fuzzy company-name matcher with indexed candidate blocking."""

    def __init__(
        self,
        df,
        match_column: str,
        return_column: str,
        remove_str: list = None,
        scorer: str | Callable | None = None,
        max_cdist_cells: int = 2_000_000,
    ):
        self.match_column = match_column
        self.return_column = return_column
        self.remove_str = remove_str or []
        self.scorer = _resolve_fuzzy_scorer(scorer)
        self.max_cdist_cells = max_cdist_cells
        self.records = self._prepare_records(df)
        self.exact_lookup = {record["BestMatch"]: record for record in self.records}
        self.prefix3_map = defaultdict(list)
        self.prefix1_map = defaultdict(list)
        self.token_map = defaultdict(list)
        for idx, record in enumerate(self.records):
            self.prefix3_map[record["_prefix3"]].append(idx)
            self.prefix1_map[record["_prefix1"]].append(idx)
            for token in record["_tokens"]:
                self.token_map[token].append(idx)
        self.all_indices = tuple(range(len(self.records)))

    def normalize_company_name(self, value):
        if pd.isna(value):
            return None

        normalized = str(value).lower()
        normalized = re.sub(r"[^\w\s]", " ", normalized)
        normalized = re.sub(r"\s+", " ", normalized).strip()
        original_lower = normalized

        for suffix in self.remove_str:
            suffix = re.sub(r"[^\w\s]", " ", str(suffix).lower())
            suffix = re.sub(r"\s+", " ", suffix).strip()
            if suffix and original_lower.endswith(suffix):
                normalized = original_lower[: -len(suffix)].strip()
                break

        return normalized or None

    @staticmethod
    def _block_key(value, length):
        if value is None:
            return ""
        letters_only = _letters_only_regex(value)
        return letters_only[:length]

    @staticmethod
    def _tokens(value):
        if value is None:
            return tuple()
        return tuple(token for token in value.split() if len(token) > 1)

    def _prepare_records(self, df):
        source_df = df.dropna(subset=[self.match_column]).reset_index(drop=True).copy()
        source_df["BestMatch"] = source_df[self.match_column].map(
            self.normalize_company_name
        )
        source_df = source_df.dropna(subset=["BestMatch"]).reset_index(drop=True)
        source_df["_prefix3"] = source_df["BestMatch"].map(
            lambda value: self._block_key(value, 3)
        )
        source_df["_prefix1"] = source_df["BestMatch"].map(
            lambda value: self._block_key(value, 1)
        )
        source_df["_name_len"] = source_df["BestMatch"].str.len()
        source_df["_tokens"] = source_df["BestMatch"].map(self._tokens)

        # Match the pandas exact-match behavior by keeping the last occurrence.
        source_df = source_df.drop_duplicates(subset=["BestMatch"], keep="last")
        source_df = source_df.reset_index(drop=True)
        columns = [
            self.match_column,
            self.return_column,
            "BestMatch",
            "_prefix3",
            "_prefix1",
            "_name_len",
            "_tokens",
        ]
        return source_df[columns].to_dict("records")

    @staticmethod
    def _dedupe_indices(indices):
        seen = set()
        unique_indices = []
        for idx in indices:
            if idx not in seen:
                seen.add(idx)
                unique_indices.append(idx)
        return tuple(unique_indices)

    def _filter_by_length(self, indices, search_string):
        name_length = len(search_string)
        max_delta = max(3, min(10, int(ceil(name_length / 3))))
        filtered = tuple(
            idx
            for idx in indices
            if abs(self.records[idx]["_name_len"] - name_length) <= max_delta
        )
        return filtered or tuple(indices)

    def _candidate_tiers(self, search_string):
        prefix3 = self._block_key(search_string, 3)
        prefix1 = self._block_key(search_string, 1)
        tokens = self._tokens(search_string)
        tiers = []

        prefix3_candidates = self._dedupe_indices(self.prefix3_map.get(prefix3, []))
        if prefix3_candidates:
            tiers.append(self._filter_by_length(prefix3_candidates, search_string))

        token_candidates = []
        for token in tokens:
            token_candidates.extend(self.token_map.get(token, []))
        token_candidates = self._dedupe_indices(token_candidates)
        if token_candidates:
            tier = self._filter_by_length(token_candidates, search_string)
            if not tiers or tier != tiers[-1]:
                tiers.append(tier)

        prefix1_candidates = self._dedupe_indices(self.prefix1_map.get(prefix1, []))
        if prefix1_candidates:
            tier = self._filter_by_length(prefix1_candidates, search_string)
            if not tiers or tier != tiers[-1]:
                tiers.append(tier)

        length_candidates = self._filter_by_length(self.all_indices, search_string)
        if not tiers or length_candidates != tiers[-1]:
            tiers.append(length_candidates)

        if tiers[-1] != self.all_indices:
            tiers.append(self.all_indices)

        return tiers

    def _rows_from_scores(self, search_strings, candidate_indices, scores):
        rows_by_search = {search_string: [] for search_string in search_strings}
        for row_idx, search_string in enumerate(search_strings):
            row_scores = np.asarray(scores[row_idx])
            if row_scores.size == 0:
                continue
            best_score = float(row_scores.max())
            if best_score < self._effective_cutoff:
                continue
            best_positions = np.flatnonzero(row_scores == best_score)
            for position in best_positions:
                candidate = self.records[candidate_indices[int(position)]]
                rows_by_search[search_string].append(
                    (
                        search_string,
                        candidate["BestMatch"],
                        best_score,
                        candidate[self.match_column],
                        candidate[self.return_column],
                    )
                )
        return rows_by_search

    def _score_group(self, search_strings, candidate_indices, cut_off, workers):
        if not search_strings or not candidate_indices:
            return {search_string: [] for search_string in search_strings}

        cells = len(search_strings) * len(candidate_indices)
        choices = [self.records[idx]["BestMatch"] for idx in candidate_indices]
        if cells <= self.max_cdist_cells:
            self._effective_cutoff = cut_off
            scores = process.cdist(
                search_strings,
                choices,
                scorer=self.scorer,
                score_cutoff=cut_off,
                workers=workers,
            )
            return self._rows_from_scores(search_strings, candidate_indices, scores)

        rows_by_search = {}
        for search_string in search_strings:
            best_match = process.extractOne(
                search_string,
                choices,
                scorer=self.scorer,
                score_cutoff=cut_off,
            )
            if best_match is None:
                rows_by_search[search_string] = []
                continue
            _, score, candidate_idx = best_match
            candidate = self.records[candidate_indices[candidate_idx]]
            rows_by_search[search_string] = [
                (
                    search_string,
                    candidate["BestMatch"],
                    float(score),
                    candidate[self.match_column],
                    candidate[self.return_column],
                )
            ]
        return rows_by_search

    def search(self, names: list, cut_off: int = 50, num_workers: int = None):
        if not num_workers or num_workers < 0:
            num_workers = max(1, cpu_count() - 2)

        search_names = [self.normalize_company_name(name) for name in names]
        search_names = [name for name in search_names if name is not None]
        if not search_names:
            return pd.DataFrame(
                columns=[
                    "Search_string",
                    "BestMatch",
                    "Score",
                    self.match_column,
                    self.return_column,
                ]
            )

        matches = []
        unresolved = []
        tier_lookup = {}
        for search_string in search_names:
            exact_match = self.exact_lookup.get(search_string)
            if exact_match is not None:
                matches.append(
                    (
                        search_string,
                        search_string,
                        100.0,
                        exact_match[self.match_column],
                        exact_match[self.return_column],
                    )
                )
            else:
                tiers = self._candidate_tiers(search_string)
                tier_lookup[search_string] = tiers
                unresolved.append(search_string)

        max_workers = min(max(1, num_workers), len(unresolved) or 1)
        max_tiers = max((len(tiers) for tiers in tier_lookup.values()), default=0)
        for tier_idx in range(max_tiers):
            if not unresolved:
                break

            grouped = defaultdict(list)
            next_unresolved = []
            skipped = set()
            for search_string in unresolved:
                tiers = tier_lookup[search_string]
                if tier_idx >= len(tiers):
                    next_unresolved.append(search_string)
                    skipped.add(search_string)
                    continue
                grouped[tiers[tier_idx]].append(search_string)

            tier_results = {}
            for candidate_indices, grouped_searches in grouped.items():
                result = self._score_group(
                    grouped_searches,
                    candidate_indices,
                    cut_off=cut_off,
                    workers=max_workers,
                )
                tier_results.update(result)

            for search_string in unresolved:
                if search_string in skipped:
                    continue
                rows = tier_results.get(search_string, [])
                if rows:
                    matches.extend(rows)
                else:
                    next_unresolved.append(search_string)
            unresolved = next_unresolved

        for search_string in unresolved:
            matches.append((search_string, None, 0.0, None, None))

        return pd.DataFrame(
            matches,
            columns=[
                "Search_string",
                "BestMatch",
                "Score",
                self.match_column,
                self.return_column,
            ],
        )


def fuzzy_match_pl(
    names: list,
    file_list: list = None,
    df=None,
    match_column: str = None,
    return_column: str = None,
    cut_off: int = 50,
    remove_str: list = None,
    num_workers: int = None,
    scorer: str | Callable | None = None,
    max_cdist_cells: int = 2_000_000,
):
    """Fuzzy-match names against a pandas/Polars frame using indexed blocking."""

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
        source_df = prepare_source_frame()
        matcher = CompanyNameFuzzyMatcher(
            source_df,
            match_column=match_column,
            return_column=return_column,
            remove_str=remove_str,
            scorer=scorer,
            max_cdist_cells=max_cdist_cells,
        )
        return matcher.search(names=names, cut_off=cut_off, num_workers=num_workers)
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
        filtered_df.loc[:, "newest_id"] = filtered_df["old_id"].map(newest_ids)
        missing_newest = filtered_df["newest_id"].isna()
        filtered_df.loc[missing_newest, "newest_id"] = filtered_df.loc[
            missing_newest, "new_id"
        ].map(newest_ids)
        filtered_df.loc[:, "newest_id"] = filtered_df["newest_id"].where(
            filtered_df["newest_id"].notna(), filtered_df["new_id"]
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


_DATE_FORMATS = [
    ("%Y-%m-%d", "date", False, False),
    ("%Y%m%d", "date", False, False),
    ("%Y-%m", "year_month", False, False),
    ("%Y", "year", False, False),
    ("%d/%m/%Y", "date", True, False),
    ("%m/%d/%Y", "date", True, False),
    ("%d-%m-%Y", "date", True, False),
    ("%m-%d-%Y", "date", True, False),
    ("%Y-%m-%d %H:%M:%S", "datetime", False, True),
    ("%Y-%m-%dT%H:%M:%S", "datetime", False, True),
    ("%Y-%m-%dT%H:%M:%SZ", "datetime", False, True),
    ("%Y-%m-%d %H:%M:%S%z", "datetime", False, True),
    ("%Y-%m-%dT%H:%M:%S%z", "datetime", False, True),
]


def _profile_date_format(series: pd.Series) -> dict:
    """Infer date parse metadata without returning any source values."""
    non_null = series.dropna()
    result = {
        "date_format": None,
        "date_format_confidence": 0.0,
        "date_format_ambiguous": False,
        "date_granularity": None,
        "date_parseable_pct": 0.0,
        "has_time_component": False,
        "has_timezone": False,
        "date_filter_strategy": None,
    }
    if non_null.empty:
        return result

    if pd.api.types.is_datetime64_any_dtype(series):
        result.update(
            {
                "date_format": "native",
                "date_format_confidence": 1.0,
                "date_granularity": "datetime",
                "date_parseable_pct": 1.0,
                "has_time_component": True,
                "date_filter_strategy": "native",
            }
        )
        return result

    values = non_null.astype(str).str.strip()
    values = values[values != ""]
    total = len(values)
    if total == 0:
        return result

    best = None
    scores = {}
    for fmt, granularity, ambiguous, has_time in _DATE_FORMATS:
        parsed = pd.to_datetime(values, format=fmt, errors="coerce")
        score = float(parsed.notna().mean())
        scores[fmt] = score
        if best is None or score > best[1]:
            best = (fmt, score, granularity, ambiguous, has_time)

    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)
        fallback_parsed = pd.to_datetime(values, errors="coerce")
    fallback_score = float(fallback_parsed.notna().mean())
    if best is None or fallback_score > best[1]:
        best = ("mixed", fallback_score, "mixed", False, False)

    fmt, score, granularity, ambiguous, has_time = best
    if score == 0:
        return result

    tied_formats = [candidate for candidate, value in scores.items() if value == score]
    is_ambiguous = ambiguous or (
        "%d/%m/%Y" in tied_formats and "%m/%d/%Y" in tied_formats
    )
    has_timezone = bool(
        values.str.contains(r"(?:Z|[+-]\d{2}:?\d{2})$", regex=True).any()
    )
    result.update(
        {
            "date_format": fmt,
            "date_format_confidence": score,
            "date_format_ambiguous": is_ambiguous,
            "date_granularity": granularity,
            "date_parseable_pct": score,
            "has_time_component": has_time
            or bool(values.str.contains(r"\d{1,2}:\d{2}", regex=True).any()),
            "has_timezone": has_timezone,
            "date_filter_strategy": f"parse_with_format:{fmt}"
            if fmt not in {"mixed", None}
            else "mixed_or_inferred",
        }
    )
    return result


def _infer_logical_type(series: pd.Series, date_profile: dict) -> str:
    """Infer an operation-oriented logical type from dtype and safe stats."""
    dtype = series.dtype
    non_null = series.dropna()
    if pd.api.types.is_bool_dtype(dtype):
        return "boolean"
    if pd.api.types.is_integer_dtype(dtype):
        return "integer"
    if pd.api.types.is_float_dtype(dtype):
        return "float"
    if pd.api.types.is_datetime64_any_dtype(dtype):
        return "datetime"
    if date_profile["date_parseable_pct"] >= 0.9:
        return "date" if date_profile["date_granularity"] != "datetime" else "datetime"
    if non_null.empty:
        return "unknown"

    unique_pct = non_null.nunique(dropna=True) / len(non_null)
    as_text = non_null.astype(str)
    alpha_num_pct = as_text.str.contains(r"[A-Za-z]").mean()
    digit_pct = as_text.str.contains(r"\d").mean()
    if unique_pct >= 0.8 and (alpha_num_pct > 0 or digit_pct > 0):
        return "identifier"
    if unique_pct <= 0.2:
        return "categorical"
    return "string"


@lru_cache(maxsize=1)
def _bvd_country_prefixes() -> set[str]:
    """Return known country-code prefixes used by Moody's BvD IDs."""
    try:
        codes = _country_codes()["Code"].dropna().astype(str).str.upper().str.strip()
        return {code for code in codes if code}
    except Exception:
        return set()


def _looks_like_bvd_id(value) -> bool:
    """Return True when a value looks like a real BvD ID, not just text."""
    if value is None or pd.isna(value):
        return False

    text = str(value).strip().upper()
    if not text:
        return False

    normalized = re.sub(r"[\s\-_./]", "", text)
    if len(normalized) < 5:
        return False

    prefix = normalized[:2]
    if prefix not in _bvd_country_prefixes():
        return False

    body = normalized[2:]
    if not body.isdigit():
        return False

    if len(body) < 3:
        return False

    return True


def _profile_bvd_id_format(series: pd.Series) -> dict:
    """Count values that look like BvD IDs without returning source values."""
    non_null = series.dropna()
    result = {
        "bvd_id_like_count": 0,
        "bvd_id_like_pct": 0.0,
        "contains_bvd_id_like_values": False,
        "mostly_bvd_id_like": False,
    }
    if non_null.empty:
        return result

    values = non_null.astype(str).str.strip()
    values = values[values != ""]
    if values.empty:
        return result

    matches = values.map(_looks_like_bvd_id)
    count = int(matches.sum())
    pct = count / len(values)
    result.update(
        {
            "bvd_id_like_count": count,
            "bvd_id_like_pct": pct,
            "contains_bvd_id_like_values": count > 0,
            "mostly_bvd_id_like": pct >= 0.8,
        }
    )
    return result


def profile_dataframe(
    df,
    data_product: str = None,
    table: str = None,
    file_name: str = None,
    sample_strategy: str = "first_file",
    operation_hints: bool = True,
) -> pd.DataFrame:
    """Create a privacy-safe column profile for a pandas or Polars DataFrame."""
    if isinstance(df, pl.LazyFrame):
        df = df.collect()
    if isinstance(df, pl.DataFrame):
        df = df.to_pandas()
    if not isinstance(df, pd.DataFrame):
        raise ValueError("'df' must be a pandas or Polars DataFrame.")

    sampled_rows = len(df)
    column_count = len(df.columns)
    rows = []
    for column in df.columns:
        series = df[column]
        non_null_count = int(series.notna().sum())
        missing_count = int(series.isna().sum())
        missing_pct = missing_count / sampled_rows if sampled_rows else 0.0
        unique_count = int(series.nunique(dropna=True))
        unique_pct = unique_count / non_null_count if non_null_count else 0.0

        string_lengths = series.dropna().astype(str).str.len()
        date_profile = _profile_date_format(series)
        bvd_profile = _profile_bvd_id_format(series)
        logical_type = _infer_logical_type(series, date_profile)
        nullable = missing_count > 0

        can_date_filter = logical_type in {"date", "datetime"} and (
            date_profile["date_parseable_pct"] >= 0.9
            or date_profile["date_filter_strategy"] == "native"
        )
        can_numeric_aggregate = logical_type in {"integer", "float"} and non_null_count > 0
        can_filter_prefix = logical_type in {"string", "identifier", "categorical"}
        can_join_key = (
            logical_type in {"identifier", "integer", "string"}
            and missing_pct <= 0.01
            and unique_pct >= 0.8
        )
        can_groupby = (
            logical_type in {"categorical", "identifier", "string", "boolean", "date"}
            and unique_pct <= 0.5
            and non_null_count > 0
        )

        notes = []
        if nullable:
            notes.append("contains_nulls")
        if can_date_filter:
            notes.append("date_filter_supported")
        if can_join_key:
            notes.append("candidate_join_key")
        if can_numeric_aggregate:
            notes.append("numeric_aggregation_supported")
        if logical_type == "identifier":
            notes.append("candidate_identifier")
        if bvd_profile["mostly_bvd_id_like"]:
            notes.append("candidate_bvd_id_column")

        row = {
            "data_product": data_product,
            "table": table,
            "file_name": file_name,
            "sample_strategy": sample_strategy,
            "sampled_rows": sampled_rows,
            "column_count": column_count,
            "column": column,
            "physical_dtype": str(series.dtype),
            "logical_type": logical_type,
            "nullable": nullable,
            "non_null_count": non_null_count,
            "missing_count": missing_count,
            "missing_pct": missing_pct,
            "unique_count": unique_count,
            "unique_pct": unique_pct,
            "string_min_length": int(string_lengths.min())
            if not string_lengths.empty
            else None,
            "string_max_length": int(string_lengths.max())
            if not string_lengths.empty
            else None,
            "string_mean_length": float(string_lengths.mean())
            if not string_lengths.empty
            else None,
            **date_profile,
            **bvd_profile,
            "can_filter_exact": logical_type != "unknown" and non_null_count > 0,
            "can_filter_prefix": can_filter_prefix,
            "can_groupby": can_groupby,
            "can_join_key": can_join_key,
            "can_numeric_aggregate": can_numeric_aggregate,
            "can_date_filter": can_date_filter,
            "needs_null_handling": nullable,
            "operation_notes": ";".join(notes) if operation_hints else None,
        }
        rows.append(row)

    return pd.DataFrame(rows)


def save_profile_report(
    profile: pd.DataFrame,
    report_path: str | os.PathLike,
    report_info: dict | None = None,
) -> str:
    """Save a privacy-safe table profile report and return the written path."""
    report_path = Path(report_path)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    suffix = report_path.suffix.lower()
    if suffix == ".csv":
        profile.to_csv(report_path, index=False)
    elif suffix == ".parquet":
        profile.to_parquet(report_path, index=False)
    elif suffix in {"", ".xlsx"}:
        if suffix == "":
            report_path = report_path.with_suffix(".xlsx")
        summary_cols = [
            "data_product",
            "table",
            "file_name",
            "sample_strategy",
            "sampled_rows",
            "column_count",
        ]
        summary = profile[summary_cols].drop_duplicates()
        date_formats = profile[
            profile["date_format"].notna() | profile["can_date_filter"]
        ].copy()
        bvd_id_columns = profile[profile["mostly_bvd_id_like"]].copy()
        operation_cols = [
            "data_product",
            "table",
            "column",
            "logical_type",
            "nullable",
            "missing_pct",
            "unique_pct",
            "bvd_id_like_pct",
            "mostly_bvd_id_like",
            "can_filter_exact",
            "can_filter_prefix",
            "can_groupby",
            "can_join_key",
            "can_numeric_aggregate",
            "can_date_filter",
            "operation_notes",
        ]
        with pd.ExcelWriter(report_path) as writer:
            profile.to_excel(writer, sheet_name="columns", index=False)
            summary.to_excel(writer, sheet_name="summary", index=False)
            date_formats.to_excel(writer, sheet_name="date_formats", index=False)
            bvd_id_columns.to_excel(writer, sheet_name="bvd_id_columns", index=False)
            profile[operation_cols].to_excel(
                writer, sheet_name="operation_hints", index=False
            )
            info = {
                "profile_created_at": datetime.now().isoformat(timespec="seconds"),
                "privacy": "No source values, examples, top values, or min/max values are included.",
            }
            if report_info:
                info.update(report_info)
            pd.DataFrame([info]).to_excel(writer, sheet_name="report_info", index=False)
    else:
        raise ValueError("Report path must end with .xlsx, .csv, or .parquet.")

    return str(report_path)
