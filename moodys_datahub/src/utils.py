import sys
import shutil
import importlib
import shlex
import subprocess
import os
import re
import psutil
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from multiprocessing import Pool, cpu_count

# Check and install required libraries

required_libraries = ['pandas','pyarrow','fastparquet','fastavro','openpyxl','tqdm','asyncio','rapidfuzz','polars',"polars_ds",'ray','modin[ray]'] 
for lib in required_libraries:
    try:
        importlib.import_module(lib)
    except ImportError:
        print(f"Installing {lib}...")
        subprocess.check_call([sys.executable, '-m', 'pip', 'install', lib])
subprocess.run(['pip', 'install', '-U', 'ipywidgets'])

import ray
import modin.pandas as pd
import pandas as pd
import pyarrow
from tqdm import tqdm
import fastavro
import numpy as np
from IPython.display import display
from rapidfuzz import process
from math import ceil
import polars as pl
import polars_ds as pds




# Dependency functions
def _create_workers(num_workers:int = -1,n_total:int=None,pool_method = None  ,query = None):

    if num_workers < 1:
            num_workers =int(psutil.virtual_memory().total/ (1024 ** 3)/12)

    if num_workers > int(cpu_count()):
        num_workers = int(cpu_count())

    if num_workers > n_total:
        num_workers = int(n_total)
     

    print(f"------ Creating Worker Pool of {num_workers}")

    if pool_method is None:
        if hasattr(os, 'fork'):
            pool_method = 'fork'
        else:
            pool_method = 'threading'

    method = 'process'
    if pool_method == 'spawn' and query is not None:
        print('The custom function (query) is not supported by "spawn" proceses')
        if not hasattr(os, 'fork'):
           print('Switching worker pool to "ThreadPoolExecutor(max_workers=num_workers)" which is under Global Interpreter Lock (GIL) - I/O operations should still speed up')
           method = 'thread'
    elif pool_method == 'threading':
        method = 'thread' 

    if method == 'process':
        worker_pool = Pool(processes=num_workers)
    elif method == 'thread':
        worker_pool = ThreadPoolExecutor(max_workers=num_workers)
        
    return worker_pool, method 

def _run_parallel(fnc,params_list:list,n_total:int,num_workers:int=-1,pool_method:str= None,msg:str= 'Process'):
        
    worker_pool, method = _create_workers(num_workers,n_total,pool_method)   
    lists  = []
    try:
        with worker_pool as pool:
            print(f'------ {msg} {n_total} files in parallel')
            if method == 'process':
                lists = list(tqdm(pool.map(fnc, params_list, chunksize=1), total=n_total, mininterval=0.1))
            else:
                lists = list(tqdm(pool.map(fnc, params_list)             , total=n_total, mininterval=0.1))
    except Exception as e:
        print(f"Error occurred: {e}")
    
    finally:
        if method == 'process':
            worker_pool.close()
            worker_pool.join()

    return lists

def _save_files_pl(df: pl.DataFrame, file_name: str, output_format: list = [".parquet"]):
    def replace_columns_with_na(df, replacement_value: str = "N/A"):
        return df.with_columns([
            pl.when(pl.col(col).is_null().all()).then(replacement_value).otherwise(pl.col(col)).alias(col)
            for col in df.columns
        ])
    
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
                df = replace_columns_with_na(df, replacement_value="N/A")  # Handle empty columns for Stata
                df.write_stata(current_file)
            else:
                print(f"Unsupported format: {extension}")
                continue
            
            file_names.append(current_file)
    
    except PermissionError as e:
        print(f"PermissionError: {e}. Check if you have the necessary permissions to write to the specified location.")
        
        if not file_name.endswith("_copy"):
            print(f'Saving "{file_name}" as "{file_name}_copy" instead')
            return _save_files_pl(df, file_name + "_copy", output_format)
        else:
            print(f'"{file_name}" was not saved')

    return file_names

def _save_files_pd(df:pd.DataFrame, file_name:str, output_format:list = ['.parquet']):
    def replace_columns_with_na(df, replacement_value:str ='N/A'):
        for column_name in df.columns:
            if df[column_name].isna().all():
                df[column_name] = replacement_value
        return df
    
    file_names = []
    try:
        for extension in output_format:
            if extension == '.csv':
                current_file = file_name + '.csv'
                df.to_csv(current_file,index=False)
            elif extension == '.xlsx':
                current_file = file_name + '.xlsx'
                df.to_excel(current_file,index=False)
            elif extension == '.parquet':
                current_file = file_name + '.parquet'
                df.to_parquet(current_file)
            elif extension == '.pickle':
                current_file = file_name + '.pickle'
                df.to_pickle(current_file) 
            elif extension == '.dta':
                current_file = file_name + '.dta'              
                df = replace_columns_with_na(df, replacement_value='N/A') # .dta format does not like empty columns so these are removed
                df.to_stata(current_file)
            file_names.append(current_file)

    except PermissionError as e:
        print(f"PermissionError: {e}. Check if you have the necessary permissions to write to the specified location.")
        
        if not file_name.endswith('_copy'):
            print(f'Saving "{file_name}" as "{file_name}_copy" instead')
            current_file = _save_files_pd(df,file_name + "_copy",output_format)
            file_names.append(current_file)
        else: 
            print(f'"{file_name}" was not saved')

    return file_names
    
def _create_chunks(df, output_format:list = ['.parquet'],file_size:int = 100):

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
    if  '.xlsx' in output_format:
        chunk_size = 1_000_000  # ValueError: This sheet is too large! Your sheet size is: 1926781, 4 Max sheet size is: 1048576, 1
    else:
           # Rough size factor to assure that compressed files of "file_size" size.
        if '.dta' in output_format or '.pickle' in output_format:
            size_factor = 1.5 
        elif '.csv' in output_format:
            size_factor =  3
        elif '.parquet' in output_format:
            size_factor =  12

        # Convert maximum file size to bytes
        file_size = file_size * 1024 * 1024 *size_factor

        if isinstance(df, pd.DataFrame):
            n_chunks = int(df.memory_usage(deep=True).sum()/file_size)
        elif isinstance(df, pl.DataFrame):
            n_chunks = int(memory_usage_pl(df)/file_size)
    
        if n_chunks == 0:
            n_chunks = 1
        chunk_size = int(total_rows /n_chunks)


        n_chunks = pd.Series(np.ceil(total_rows /chunk_size)).astype(int)
        n_chunks = int(n_chunks.iloc[0])
        if n_chunks == 0:
            n_chunks = 1

        return n_chunks,total_rows,chunk_size

def _process_chunk(params):
    i, chunk, n_chunks, file_name, output_format = params
    if n_chunks > 1:
        file_part = f'{file_name}_{i}'
    else:
        file_part = file_name
    if isinstance(chunk, pd.DataFrame):    
        file_name = _save_files_pd(chunk, file_part, output_format)
    elif isinstance(chunk, pl.DataFrame):
        file_name = _save_files_pl(chunk, file_part, output_format)

    return file_name

def _save_chunks(dfs:list, file_name:str, output_format:list = ['.csv'] , file_size:int = 100,num_workers:int = 1):
    num_workers = int(num_workers) 
    file_names = None
    if len(dfs) > 0: 
        if isinstance(dfs, list):
            print('------ Concatenating fileparts')
            dfs = pd.concat(dfs, ignore_index=True)
      
        if output_format is None or file_name is None:
            return dfs, file_names
        
        elif len(dfs) == 0:
            print('No rows have been retained')
            return dfs, file_names
        
        print('------ Saving files')
        n_chunks,total_rows,chunk_size = _create_chunks(dfs, output_format,file_size)
        total_rows = len(dfs)
        
        # Read multithreaded
        if isinstance(num_workers, (int, float, complex))and num_workers != 1 and n_chunks > 1:
            print(f"Saving {n_chunks} files")
            if isinstance(dfs, pd.DataFrame):  
                params_list =   [(i,dfs[start:min(start + chunk_size, total_rows)].copy(), n_chunks, file_name, output_format) for i, start in enumerate(range(0, total_rows, chunk_size),start=1)]
            elif isinstance(dfs, pl.DataFrame):
                params_list =   [(i,dfs.slice(start, chunk_size), n_chunks, file_name, output_format) for i, start in enumerate(range(0, total_rows, chunk_size),start=1)]
            
            file_names = _run_parallel(fnc=_process_chunk,params_list=params_list,n_total=n_chunks,num_workers=num_workers,msg='Saving') 
        else:
            file_names = []
            for i, start in enumerate(range(0, total_rows, chunk_size),start=1):
                    print(f" {i} of {n_chunks} files")
                    
                    if isinstance(dfs, pd.DataFrame): 
                        current_file = _process_chunk([i, dfs[start:min(start + chunk_size, total_rows)].copy(), n_chunks, file_name, output_format])
                    elif isinstance(dfs, pl.DataFrame):
                        current_file = _process_chunk([i, dfs.slice(start, chunk_size), n_chunks, file_name, output_format])  
                    
                    file_names.append(current_file)

        file_names = [item for sublist in file_names for item in sublist if item is not None]

    return dfs, file_names

def _load_pd(file:str,select_cols = None, date_query:list=[None,None,None,"remove"], bvd_query:str=None, query = None, query_args:list = None):
    try:
        df =_read_pd(file,select_cols)

        if df.empty:
            print(f"{os.path.basename(file)} empty after column selection")
            return df
    except pyarrow.lib.ArrowInvalid as e:
        folder_path = os.path.dirname(file)
        if os.path.exists(folder_path) and os.path.isdir(folder_path):
            shutil.rmtree(folder_path)

        raise ValueError(f"Error reading {os.path.basename(file)} folder and sub files {folder_path} has been removed): {e}")
    except Exception as e:
        raise ValueError(f"Error reading file: {e}")

    if all(date_query):
        try:
            df = _date_pd(df, date_col= date_query[2],  start_year = date_query[0], end_year = date_query[1],nan_action=date_query[3])
        except Exception as e:
            raise ValueError(f"Error while date selection: {e}") 
        if df.empty:
            print(f"{os.path.basename(file)} empty after date selection")
            return df
    
    if bvd_query is not None:
        try:
            df = df.query(bvd_query)
        except Exception as e:
            raise ValueError(f"Error while bvd filtration: {e}")    
        if df.empty:
            print(f"{os.path.basename(file)} empty after bvd selection")
            return df
    
    # Apply function or query to filter df
    if query is not None:
        if isinstance(query, type(lambda: None)):
            try:
                df = query(df, *query_args) if query_args else query(df)
            except Exception as e:
                raise ValueError(f"Error curating file with custom function: {e}")
        elif isinstance(query,str):
            try:
                df = df.query(query)
            except Exception as e:
                raise ValueError(f"Error curating file with pd.query(): {e}")
        if df.empty:
            print(f"{os.path.basename(file)} empty after query filtering")
    return df

def _load_pl(file_list: list, select_cols=None, date_query=[None, None, None, "remove"], bvd_query=None, query=None, query_args=None):
    """
    Load multiple files into a Polars LazyFrame and apply optional filtering.

    Parameters:
    - file_list (list): List of file paths.
    - select_cols (list, optional): Columns to select.
    - date_query (list, optional): [start_year, end_year, date_column, nan_action].
    - bvd_query (tuple, optional): (values_list, column_name).
    - query (callable or str, optional): Function or string-based filter query.
    - query_args (tuple, optional): Arguments for function-based query.

    Returns:
    - Polars DataFrame
    """
    try:
        # Load files lazily
        df_lazy = _read_pl(file_list)

        # Select specific columns
        if select_cols is not None:
            df_lazy = df_lazy.select(select_cols)

        # Apply date filter if needed
        if all(date_query[:3]):  # Ensure the first three elements are not None
            raise ValueError("Year filter does not work for polars.. please use '.process_all()'")
   
            #df_lazy = _date_pl(
            #    df_lazy, date_col=date_query[2], start_year=date_query[0], end_year=date_query[1], nan_action=date_query[3]
            #)

        # Apply BVD query filter if provided
        if bvd_query is not None and len(bvd_query) == 2:
            df_lazy = df_lazy.filter(pl.col(bvd_query[1]).is_in(bvd_query[0]))
        
        # Apply query function or filter
        if query is not None:
            if isinstance(query, type(lambda: None)):
                df_lazy = query(df_lazy, *query_args) if query_args else query(df_lazy)
            elif isinstance(query, pl.Expr):
                df_lazy = df_lazy.filter(query)

        # Collect the results (triggering execution)
        df = df_lazy.collect()

        return df

    except Exception as e:
        raise RuntimeError(f"Error processing files: {file_list}") from e

def _read_csv_chunk(params):
    # FIX ME !!! - Implementering af log-function!!
    file, chunk_idx, chunk_size, select_cols, col_index, date_query, bvd_query, query, query_args = params
    try:
        df = pd.read_csv(file, low_memory=False, skiprows=chunk_idx * chunk_size, nrows=chunk_size)
    except Exception as e:
            raise ValueError(f"Error while reading chunk: {e}") 

    if select_cols is not None:
            df = df.iloc[:,col_index]
            df.columns = select_cols

    if all(date_query):
        try:
            df = _date_pd(df, date_col= date_query[2],  start_year = date_query[0], end_year = date_query[1],nan_action=date_query[3])
        except Exception as e:
            raise ValueError(f"Error while date selection: {e}") 
        if df.empty:
            print(f"{os.path.basename(file)} empty after date selection")
            return df
    if bvd_query is not None:
        try:
            df = df.query(bvd_query)
        except Exception as e:
            raise ValueError(f"Error while bvd filtration: {e}")    
        if df.empty:
            print(f"{os.path.basename(file)} empty after bvd selection")
            return df
    
    # Apply function or query to filter df
    if query is not None:
        if isinstance(query, type(lambda: None)):
            try:
                df = query(df, *query_args) if query_args else query(df)
            except Exception as e:
                raise ValueError(f"Error curating file with custom function: {e}")
        elif isinstance(query,str):
            try:
                df = df.query(query)
            except Exception as e:
                raise ValueError(f"Error curating file with pd.query(): {e}")
        if df.empty:
            print(f"{os.path.basename(file)} empty after query filtering")
    return df   
  
def _load_csv_table(file:str,select_cols = None, date_query:list=[None,None,None,"remove"], bvd_query:str=None, query = None, query_args:list = None,num_workers:int = -1):

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

        return select_cols,col_index


    if num_workers < 1:
        num_workers =int(psutil.virtual_memory().total/ (1024 ** 3)/12)

    # check if the requested columns exist
    select_cols,col_index = check_cols(file,select_cols)

    # Step 1: Determine the total number of rows using subprocess
    if sys.platform.startswith('linux') or sys.platform == 'darwin':
            safe_file_path = shlex.quote(file)
            num_lines = int(subprocess.check_output(f"wc -l {safe_file_path}", shell=True).split()[0]) - 1
    elif sys.platform == 'win32':
            def count_lines_chunk(file_path):
                # Open the file in binary mode for faster reading
                with open(file_path, 'rb') as f:
                    # Use os.read to read large chunks of the file at once (64KB in this case)
                    buffer_size = 1024 * 64  # 64 KB
                    read_chunk = f.read(buffer_size)
                    count = 0
                    while read_chunk:
                        # Count the number of newlines in each chunk
                        count += read_chunk.count(b'\n')
                        read_chunk = f.read(buffer_size)
                return count
            
            num_lines = count_lines_chunk(file) - 1 
        
    # Step 2: Calculate the chunk size to create 64 chunks
    chunk_size = num_lines // num_workers

    # Step 3: Prepare the params_list
    params_list = [(file, i, chunk_size, select_cols, col_index, date_query, bvd_query, query, query_args) for i in range(num_workers)]

    # Step 4: Use _run_parallel to read the DataFrame in parallel
    chunks = _run_parallel(_read_csv_chunk, params_list, n_total=num_workers, num_workers=num_workers, pool_method='process', msg='Reading chunks')

    # Step 5: Concatenate all chunks into a single DataFrame
    df = pd.concat(chunks, ignore_index=True)

    return df

def _save_to(df, filename, format):
    
    if df is None or (isinstance(df, (pd.DataFrame, pl.DataFrame)) and (df.empty if isinstance(df, pd.DataFrame) else df.is_empty())):
        print("df is empty and cannot be saved")
        return

    def check_format(file_type):
        allowed_values = {False, 'xlsx', 'csv'}
        if file_type not in allowed_values:
            print(f"Invalid file_type: {file_type}. Allowed values are False, 'xlsx', or 'csv'.")

    check_format(format)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    if format == 'xlsx':
        filename = f"{filename}_{timestamp}.xlsx"
        if isinstance(df, pd.DataFrame):
            df.to_excel(filename, index=False)
        else:  # Convert Polars to Pandas before saving
            df.to_pandas().to_excel(filename, index=False)
    
    elif format == 'csv':
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
    elif isinstance(values, list): 
        # Check if all elements in the list are strings
        if not all(isinstance(value, str) for value in values):
            raise ValueError("Not all inputs to the list are in str format")
    elif values is not None:
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
                raise ValueError("Additional arguments must be either None, a string, or a list of strings")
    
    # Retain unique values before returning
    return list(set(values))

def _date_pd(df, date_col = None,  start_year:int = None, end_year:int = None,nan_action:str= 'remove'):
    """
    Filter DataFrame based on a date column and optional start/end years.

    Parameters:
    df (pd.DataFrame): The DataFrame to filter.
    date_col (str): The name of the date column in the DataFrame.
    start_year (int, optional): The starting year for filtering (inclusive). Defaults to None (no lower bound).
    end_year (int, optional): The ending year for filtering (inclusive). Defaults to None (no upper bound).

    Returns:
    pd.DataFrame: Filtered DataFrame based on the date and optional year filters.
    """
     
    pd.options.mode.copy_on_write = True
    
    if date_col is None:
        columns_to_check = ['closing_date', 'information_date']
    else:
        columns_to_check = [date_col]

    date_col = next((col for col in columns_to_check if col in df.columns), None)
    
    if not date_col:
        print('No valid date columns found')
        return df

    # Separate rows with NaNs in the date column
    if nan_action == 'keep':
        nan_rows = df[df[date_col].isna()]
    df = df.dropna(subset=[date_col])
                           
    try:
        df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
    except ValueError as e:
        print(f"{e}")
        return df 
    
    date_filter = (df[date_col].dt.year >= start_year) & (df[date_col].dt.year <= end_year)
    # FIX ME [!!] make into pandas query! 

    if date_filter.any():  
        df = df.loc[date_filter]
    else:
        df = pd.DataFrame() 

    # Add back the NaN rows
    if nan_action == 'keep':
        if not nan_rows.empty:
            df = pd.concat([df, nan_rows], sort=False)
            df = df.sort_index()
   
    return df

# FIX ME - Not working
def _date_pl(df, date_col=None, start_year:int=None, end_year:int=None, nan_action:str='remove'):
    """
    Filter DataFrame based on a date column and optional start/end years.

    Parameters:
    df (pl.DataFrame): The DataFrame to filter.
    date_col (str): The name of the date column in the DataFrame.
    start_year (int, optional): The starting year for filtering (inclusive). Defaults to None (no lower bound).
    end_year (int, optional): The ending year for filtering (inclusive). Defaults to None (no upper bound).

    Returns:
    pl.DataFrame: Filtered DataFrame based on the date and optional year filters.
    """

    if date_col is None:
        columns_to_check = ['closing_date', 'information_date']
    else:
        columns_to_check = [date_col]

    date_col = next((col for col in columns_to_check if col in df.columns), None)
    
    if not date_col:
        print('No valid date columns found')
        return df
    
    # Separate rows with null values in the date column (Polars uses is_null to check for NaNs)
    if nan_action == 'keep':
        nan_rows = df.filter(pl.col(date_col).is_null())
    
    # Remove rows with null values in the date column (if nan_action is 'remove')
    df = df.filter(pl.col(date_col).is_not_null())
    
    # Get the first row efficiently
    first_row = df.select(date_col).fetch(1)

    # Extract the first element
    if not first_row.is_empty():
        first_element = first_row[date_col].to_list()[0]
    else:
        first_element = None

    # Check the type and cast if necessary
    if first_element is not None and not isinstance(first_element, str):
        df = df.with_columns(pl.col(date_col).cast(pl.Utf8))
        
    # Apply the conversion to the date column lazily
    df = df.with_columns(
        #pl.col(date_col).str.strptime(pl.Datetime, format="%Y-%m-%d") 
        pl.col(date_col).str.to_datetime(exact=False, strict=False).alias(date_col)
    )

    # Apply the date filtering for year range (inclusive)
    if start_year is not None and end_year is not None:
        df = df.filter(
            (pl.col(date_col).dt.year() >= start_year) & (pl.col(date_col).dt.year() <= end_year)
        )
    elif start_year is not None:
        df = df.filter(pl.col(date_col).dt.year() >= start_year)
    elif end_year is not None:
        df = df.filter(pl.col(date_col).dt.year() <= end_year)
    
    # Add back the NaN rows if 'keep' action is specified
    if nan_action == 'keep' and nan_rows is not None:
        df = df.vstack(nan_rows)
    
    return df

def _construct_query(bvd_cols,bvd_list,search_type):
    conditions = []
    if isinstance(bvd_cols,str):
        bvd_cols = [bvd_cols]

    for bvd_col in bvd_cols:
        if search_type:
            for substring in bvd_list:
                condition = f"{bvd_col}.str.startswith('{substring}', na=False)"
                conditions.append(condition)
        else:
            condition  = f"{bvd_col} in {bvd_list}" 
            conditions.append(condition)
    query = " | ".join(conditions)  # Combine conditions using OR (|)
    return query
   
def _letters_only_regex(text):
    """Converts the title to lowercase and removes non-alphanumeric characters."""
    if isinstance(text,str):
        return re.sub(r'[^a-zA-Z0-9]', '', text.lower())
    else:
        return text


def _fuzzy_match(args):
    """
    Worker function to perform fuzzy matching for each batch of names.
    """
    name_batch, choices, cut_off, df, match_column, return_column, choice_to_index = args
    results = []
    
    for name in name_batch:
        # First, check if an exact match exists in the choices
        if name in choice_to_index:
            match_index = choice_to_index[name]
            match_value = df.iloc[match_index][match_column]
            return_value = df.iloc[match_index][return_column]
            results.append((name, name, 100, match_value, return_value))  # Exact match with score 100
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

def fuzzy_query(df: pd.DataFrame, names: list, match_column: str = None, return_column: str = None, cut_off: int = 50, remove_str: list = None, num_workers: int = None):
    """
    Perform fuzzy string matching with a list of input strings against a specific column in a DataFrame.
    """

    def remove_suffixes(choices, suffixes):
        for i, choice in enumerate(choices):
            choice_lower = choice.lower()  # convert to lowercase for case-insensitive comparison
            for suffix in suffixes:
                if choice_lower.endswith(suffix.lower()):
                    # Remove the suffix from the end of the string
                    choices[i] = choice_lower[:-len(suffix)].strip()
        return choices

    def remove_substrings(choices, substrings):
        for substring in substrings:
            choices = [choice.replace(substring.lower(), '') for choice in choices]
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
        name_batches = [names[i:i + batch_size] for i in range(0, len(names), batch_size)]

        args_list = [(batch, choices, cut_off, df, match_column, return_column, choice_to_index) for batch in name_batches]

        with Pool(processes=num_workers) as pool:
            results = pool.map(_fuzzy_match, args_list)
            for result_batch in results:
                matches.extend(result_batch)
    else:
        # If single worker, process in a simple loop
        matches.extend(_fuzzy_match((names, choices, cut_off, df, match_column, return_column, choice_to_index)))

    # Create the result DataFrame
    result_df = pd.DataFrame(matches, columns=['Search_string', 'BestMatch', 'Score', match_column, return_column])

    return result_df

# FIX ME
def fuzzy_match_pl(names: list,file_list:list=None, match_column: str = None, return_column: str = None, cut_off: int = 0.50, remove_str: list = None):
    try:

        df_lazy = _read_pl(file_list)

        df_lazy = df_lazy.with_columns(pl.col(match_column).alias('BestMatch').str.to_lowercase())

        names_lower = [s.lower() for s in names]

        # Apply remove_str filtering if provided
        if remove_str:
            # Filter out rows where match_column ends with any of the substrings in remove_str
            for substr in remove_str:

                df_lazy = df_lazy.with_columns(pl.col('BestMatch').str.replace_all(substr, '').alias('BestMatch'))
                #df_lazy = df_lazy.filter(~pl.col('BestMatch').str.ends_with(substr))
                
                # Remove the suffix from each string if it ends with the specified suffix
                #names_lower = [s[:-len(substr)] if s.endswith(substr) else s for s in names_lower]
                names_lower = [name.replace(substr, '') for name in names_lower]
        
        # Filter rows where match_column values are in names
        df_matches = df_lazy.filter(pl.col('BestMatch').is_in(names_lower))

         # Add a new column 'score' with a constant value of 100
        df_matches = df_matches.with_columns(pl.lit(1.0000).alias('Score'))

        df_matches = df_matches.with_columns(pl.col('BestMatch').alias('Search_string'))

        # Select only the match_column and return_column
        select_cols = [pl.col(['Search_string', 'BestMatch', 'Score'])]

        if match_column:
            select_cols.append(pl.col(match_column))
        if return_column:
            select_cols.append(pl.col(return_column))

        df_matches = df_matches.select(select_cols)

        # Collect the results
        df_matches = df_matches.collect()

        # Convert df_matches 'Search_string' column to a set
        df_matches_set = set(df_matches['BestMatch'])

        # Find names not in df_matches
        names_fuzzy = [name for name in names_lower if name not in df_matches_set]

        df_names_fuzzy = pl.DataFrame({'Search_string': names_fuzzy})
       
        names_lazy = df_names_fuzzy.lazy()

        # Filter rows where 'Search_string' values are NOT in names
        df_fuzzy = df_lazy.filter(~pl.col('BestMatch').is_in(names_lower))
     
        # Perform a cross join and calculate similarity scores
        df_fuzzy = df_fuzzy.join_where(
            names_lazy,
            pl.lit(True),  # Always true to generate all combinations
            #how="cross"
        ).with_columns(pds.str_fuzz('Search_string', 'BestMatch').alias("Score"))
  
        # Filter rows where the 'fuzz' score is greater than or equal to 90
        df_fuzzy = df_fuzzy.filter(pl.col("Score") >= cut_off)
        
        df_fuzzy = df_fuzzy.select(select_cols).collect()

        result_df = pl.concat([df_matches, df_fuzzy])

        return result_df 

    except Exception as e:
        raise RuntimeError(f"Error processing files: {file_list}") from e



# FIX ME
def _bvd_changes_ray_not_working(initial_ids, df, num_workers=-1):
    """
    Track the newest IDs using Modin and Ray.

    Parameters:
    - initial_ids (set): Set of initial IDs.
    - df (DataFrame): Modin DataFrame containing 'old_id' and 'new_id' columns.
    - num_workers (int): Number of workers for Ray. Defaults to using all available CPUs minus two.

    Returns:
    - new_ids (set): Set of all discovered IDs.
    - newest_ids (dict): Mapping of each ID to its newest ID.
    - df (DataFrame): Filtered DataFrame with an additional 'newest_id' column.
    """
    if num_workers < 1:
        num_workers = max(1, os.cpu_count() - 2)
    
    ray.init(num_cpus=num_workers)

    new_ids = set(initial_ids)
    newest_ids = {id: id for id in new_ids}
    current_ids = set()
    found_new = True

    while found_new:
        found_new = False
        if not current_ids:
            current_ids = new_ids.copy()
        else:
            current_ids = new_ids - current_ids

        # Filter for old_id and new_id matches
        old_matches = df[df['old_id'].isin(current_ids)]
        new_matches = df[df['new_id'].isin(current_ids)]

        if old_matches.empty and new_matches.empty:
            break

        # Extract unique old_id and new_id values
        old_id = set(old_matches['old_id'].unique())
        new_id = set(new_matches['new_id'].unique())

        # Determine new IDs not already in new_ids
        new_old_ids = old_id - new_ids
        new_new_ids = new_id - new_ids

        # Check if new IDs were found
        found_new = bool(new_old_ids or new_new_ids)

         # Update new_ids set
        new_ids.update(new_old_ids)
        new_ids.update(new_new_ids)

        # Update newest_ids mapping
        newest_ids.update(old_matches.set_index('old_id')['new_id'].to_dict())
        newest_ids.update(old_matches.set_index('new_id')['new_id'].to_dict())

      


    # Re-filter data based on new_ids
    df = df[(df['old_id'].isin(new_ids)) | (df['new_id'].isin(new_ids))]

    # Map newest IDs using dictionary lookup
    df['newest_id'] = df.apply(
        lambda row: newest_ids.get(row['old_id'], newest_ids.get(row['new_id'])),
        axis=1
    )

    ray.shutdown()
    return new_ids, newest_ids, df

def _bvd_changes_ray(initial_ids, df,num_workers:int=-1):
    import modin.pandas as pd

    if not num_workers or num_workers < 0:
            num_workers = max(1, cpu_count() - 2)
    ray.init(num_cpus=num_workers) 
    
    new_ids = set(initial_ids)
    newest_ids = {id: id for id in new_ids}
    current_ids = set()  # Keep track of processed IDs
    found_new = True
    
    while found_new:
        found_new = False

        if not current_ids:
            current_ids = new_ids.copy()
        else:
            current_ids = new_ids - current_ids

        # Use pandas isin to check for matching old_id and new_id in a vectorized way
        old_matches = df[df['old_id'].isin(current_ids)]
        new_matches = df[df['new_id'].isin(current_ids)]

        # Process old_id matches to find corresponding new_ids
        for _, row in old_matches.iterrows():
            new_id = row['new_id']
            old_id = row['old_id']
            if new_id not in new_ids:
                new_ids.add(new_id)
                found_new = True
                newest_ids[old_id] = new_id
                newest_ids[new_id] = new_id

        # Process new_id matches to find corresponding old_ids
        for _, row in new_matches.iterrows():
            old_id = row['old_id']
            new_id = row['new_id']
            if old_id not in new_ids:
                new_ids.add(old_id)
                found_new = True
                newest_ids[old_id] = newest_ids[new_id]

    # Filter the DataFrame based on new_ids
    df = df[df['old_id'].isin(new_ids) | df['new_id'].isin(new_ids)]

    # Map newest IDs to the filtered DataFrame
    df.loc[:, 'newest_id'] = df.apply(
        lambda row: newest_ids.get(row['old_id'], newest_ids.get(row['new_id'])), axis=1
    )
    ray.shutdown()
    import pandas as pd
    return new_ids, newest_ids, df

# FIX ME
def _bvd_changes_pl(initial_ids,file_list):
    """
    Load multiple files lazily and track the newest IDs.

    Parameters:
    - file_list (list): List of file paths.
    - initial_ids (set): Set of initial IDs.

    Returns:
    - Polars LazyFrame with updated newest IDs.
    """
    # Load files lazily
    df_lazy = _read_pl(file_list)

    new_ids = set(initial_ids)
    newest_ids = {id: id for id in new_ids}
    current_ids = set()
    found_new = True

    while found_new:
        found_new = False
        if not current_ids:
            current_ids = new_ids.copy()
        else:
            current_ids = new_ids - current_ids

        # Filter for old_id and new_id matches
        old_id_matches = df_lazy.filter(pl.col("old_id").is_in(current_ids)).collect()
        new_id_matches = df_lazy.filter(pl.col("new_id").is_in(current_ids)).collect()

        # Process old_id matches to update newest_ids
        for row in old_id_matches.iter_rows(named=True):
            old_id, new_id = row["old_id"], row["new_id"]
            if new_id not in new_ids:
                new_ids.add(new_id)
                found_new = True
                newest_ids[old_id] = new_id
                newest_ids[new_id] = new_id

        # Process new_id matches to update newest_ids
        for row in new_id_matches.iter_rows(named=True):
            old_id, new_id = row["old_id"], row["new_id"]
            if old_id not in new_ids:
                new_ids.add(old_id)
                found_new = True
                newest_ids[old_id] = newest_ids[new_id]

    # Re-filter data based on new_ids
    df_lazy = df_lazy.filter(pl.col("old_id").is_in(new_ids) | pl.col("new_id").is_in(new_ids))

    # Map newest IDs using dictionary lookup
    df_lazy = df_lazy.with_columns(
        pl.when(pl.col("old_id").is_in(newest_ids))
        .then(pl.col("old_id").map_dict(newest_ids))
        .otherwise(pl.col("new_id").map_dict(newest_ids))
        .alias("newest_id")
    )

    df_lazy.collect()
    return df_lazy
# FIX ME
def _bvd_changes_pl_not_tested(initial_ids, file_list):
    """
    Load multiple files lazily and track the newest IDs.

    Parameters:
    - initial_ids (set): Set of initial IDs.
    - file_list (list): List of file paths.

    Returns:
    - Polars LazyFrame with updated newest IDs.
    """
    # Load files lazily
    df_lazy = _read_pl(file_list)

    new_ids = set(initial_ids)
    newest_ids = {id: id for id in new_ids}
    current_ids = set()
    found_new = True

    while found_new:
        found_new = False
        if not current_ids:
            current_ids = new_ids.copy()
        else:
            current_ids = new_ids - current_ids

        # Filter for old_id and new_id matches
        matches = df_lazy.filter(
            pl.col("old_id").is_in(current_ids) | pl.col("new_id").is_in(current_ids)
        ).collect()

        if matches.is_empty():
            break

        # Extract unique old_id and new_id values
        old_ids = set(matches.select("old_id").unique().to_series())
        new_ids = set(matches.select("new_id").unique().to_series())

        # Determine new IDs not already in new_ids
        new_old_ids = old_ids - new_ids
        new_new_ids = new_ids - new_ids

        # Update newest_ids mapping
        newest_ids.update(
            {row["old_id"]: row["new_id"] for row in matches.iter_rows(named=True)}
        )

        # Update new_ids set
        new_ids.update(new_old_ids)
        new_ids.update(new_new_ids)

        # Check if new IDs were found
        found_new = bool(new_old_ids or new_new_ids)

    # Re-filter data based on new_ids
    df_lazy = df_lazy.filter(
        pl.col("old_id").is_in(new_ids) | pl.col("new_id").is_in(new_ids)
    )

    # Map newest IDs using dictionary lookup
    df_lazy = df_lazy.with_columns(
        pl.when(pl.col("old_id").is_in(newest_ids))
        .then(pl.col("old_id").map_dict(newest_ids))
        .otherwise(pl.col("new_id").map_dict(newest_ids))
        .alias("newest_id")
    )

    return df_lazy

def _read_pd(file,select_cols):
    """Load multiple files lazily based on extension."""
    
    def read_avro(file):
        df = []
        with open(file, 'rb') as avro_file:
            avro_reader = fastavro.reader(avro_file)
            for record in avro_reader:
                df.append(record)
        df = pd.DataFrame(df)

        return df 
     
    read_functions = {
        'csv': pd.read_csv,
        'xlsx': pd.read_excel,
        'parquet': pd.read_parquet,
        'orc': pd.read_orc,
        'avro': read_avro,
    }
        
    file_ext = file.lower().split(".")[-1]

    if file_ext not in read_functions:
        raise ValueError(f"Unsupported file format: {file_ext}")
    
    read_function = read_functions[file_ext]

    if select_cols is None:
        df = read_function(file)
    else:  
        if file_ext in ['csv','xlsx']:
            df = read_function(file, usecols = select_cols)
        elif file_ext in ['parquet','orc']:
            df = read_function(file, columns = select_cols)
    return df

def _read_pl(files):
    """Load multiple files lazily based on extension."""
    
    # Read Avro function
    def read_avro(files):
        df_list = []
        for file in files:
            with open(file, 'rb') as avro_file:
                avro_reader = fastavro.reader(avro_file)
                df_list.extend(avro_reader)  # Collect all records
        return pl.LazyFrame(df_list)  # Convert to LazyFrame

    read_functions = {
        "csv": lambda files: pl.scan_csv(files),
        "xlsx": lambda files: pl.read_excel(files[0]).lazy(),  # Only supports single files
        "parquet": lambda files: pl.scan_parquet(files),
        "avro": read_avro,
    }
        
    first_file = files[0]
    file_ext = first_file.lower().split(".")[-1]

    if file_ext not in read_functions:
        raise ValueError(f"Unsupported file format: {file_ext}")

    return read_functions[file_ext](files)