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

required_libraries = ['pandas','pyarrow','fastparquet','fastavro','openpyxl','tqdm','asyncio','rapidfuzz'] 
for lib in required_libraries:
    try:
        importlib.import_module(lib)
    except ImportError:
        print(f"Installing {lib}...")
        subprocess.check_call([sys.executable, '-m', 'pip', 'install', lib])
subprocess.run(['pip', 'install', '-U', 'ipywidgets'])

import ray
import pandas as pd
import pyarrow
from tqdm import tqdm
import fastavro
import numpy as np
from IPython.display import display
from rapidfuzz import process
from math import ceil


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

def _save_files(df:pd.DataFrame, file_name:str, output_format:list = ['.parquet']):
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
            current_file = _save_files(df,file_name + "_copy",output_format)
            file_names.append(current_file)
        else: 
            print(f'"{file_name}" was not saved')

    return file_names
    
def _create_chunks(dfs:list, output_format:list = ['.parquet'],file_size:int = 100):
    total_rows = len(dfs)
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

        n_chunks = int(dfs.memory_usage(deep=True).sum()/file_size)
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
        
    file_name = _save_files(chunk, file_part, output_format)

    return file_name

def _save_chunks(dfs:list, file_name:str, output_format:list = ['.csv'] , file_size:int = 100,num_workers:int = 1):
    num_workers = int(num_workers) 
    file_names = None
    if dfs:
        print('------ Concatenating fileparts')
        #dfs = pd_modin.concat(dfs, ignore_index=True)
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
        if isinstance(num_workers, (int, float, complex))and num_workers != 1:
            #num_workers = int(num_workers) 
            print(f"Saving {n_chunks} files")

            params_list =   [(i,dfs[start:min(start + chunk_size, total_rows)].copy(), n_chunks, file_name, output_format) for i, start in enumerate(range(0, total_rows, chunk_size),start=1)]
            file_names = _run_parallel(fnc=_process_chunk,params_list=params_list,n_total=n_chunks,num_workers=num_workers,msg='Saving') 
        else:
            file_names =[]
            for i, start in enumerate(range(0, total_rows, chunk_size),start=1):
                    print(f" {i} of {n_chunks} files")
                    current_file = _process_chunk([i, dfs[start:min(start + chunk_size, total_rows)].copy(), n_chunks, file_name, output_format])
                    file_names.append(current_file)

        file_names = [item for sublist in file_names for item in sublist if item is not None]
    return dfs, file_names

def _load_table(file:str,select_cols = None, date_query:list=[None,None,None,"remove"], bvd_query:str=None, query = None, query_args:list = None):
    # FIX ME !!! - Implementering af log-function!!
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

    file_extension = file.lower().split('.')[-1]

    if file_extension not in read_functions:
        raise ValueError(f"Unsupported file format: {file_extension}")

    read_function = read_functions[file_extension]

    try:
        if select_cols is None:
            df = read_function(file)
        else:  
            if file_extension in ['csv','xlsx']:
                df = read_function(file, usecols = select_cols)
            elif file_extension in ['parquet','orc']:
                df = read_function(file, columns = select_cols)
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
            df = _date_fnc(df, date_col= date_query[2],  start_year = date_query[0], end_year = date_query[1],nan_action=date_query[3])
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
            df = _date_fnc(df, date_col= date_query[2],  start_year = date_query[0], end_year = date_query[1],nan_action=date_query[3])
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

def _save_to(df,filename,format):

    if df is None:
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
        df.to_excel(filename)
    elif format == 'csv':
        filename = f"{filename}_{timestamp}.csv"
        df.to_csv(filename)
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
            
    
    return values

def _date_fnc(df, date_col = None,  start_year:int = None, end_year:int = None,nan_action:str= 'remove'):
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

def _bvd_changes_ray(initial_ids, df,num_workers:int=-1):
    
    subprocess.run(['pip', 'install','ray'])
    subprocess.run(['pip', 'install','modin[ray]'])
    #import ray
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
        old_id_matches = df[df['old_id'].isin(current_ids)]
        new_id_matches = df[df['new_id'].isin(current_ids)]

        # Process old_id matches to find corresponding new_ids
        for _, row in old_id_matches.iterrows():
            new_id = row['new_id']
            old_id = row['old_id']
            if new_id not in new_ids:
                new_ids.add(new_id)
                found_new = True
                newest_ids[old_id] = new_id
                newest_ids[new_id] = new_id

        # Process new_id matches to find corresponding old_ids
        for _, row in new_id_matches.iterrows():
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
