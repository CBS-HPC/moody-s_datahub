import sys
import shutil
import time
import json
import importlib
import subprocess
import os
import re
import psutil
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from multiprocessing import Pool, cpu_count, Process
import importlib.resources as pkg_resources
import copy


# Check and install required libraries
required_libraries = ['pandas', 'pysftp','pyarrow','fastparquet','fastavro','openpyxl','tqdm','asyncio'] 
for lib in required_libraries:
    try:
        importlib.import_module(lib)
    except ImportError:
        print(f"Installing {lib}...")
        subprocess.check_call([sys.executable, '-m', 'pip', 'install', lib])
subprocess.run(['pip', 'install', '-U', 'ipywidgets'])


import pandas as pd
import pyarrow
import pyarrow.parquet as pq 
import pysftp
from tqdm import tqdm
import fastavro
import numpy as np
import ipywidgets as widgets
from IPython.display import display
import asyncio

# Defining Sftp Class
class Sftp:
    def __init__(self, hostname:str = "s-f2112b8b980e44f9a.server.transfer.eu-west-1.amazonaws.com", username:str = "D2vdz8elTWKyuOcC2kMSnw", port:int = 22, privatekey:str = None):
        """Constructor Method

         ### Sftp Class Object Variables        
        - `connection`: None or pysftp.Connection object. Represents the current SFTP connection.
        
        - `hostname`: str. Hostname of the SFTP server (Default connects to CBS SFTP server).
        - `username`: str. Username for authentication (Default connects to CBS SFTP server).
        - `privatekey`: str or None. Path to the private key file for authentication (Valid private key is needed to access SFTP server)
        - `port`: int. Port number for the SFTP connection (default is 22).

        - `output_format`: list of str. List of supported output file formats (e.g., ['.csv', '.parquet']).
        - `file_size_mb`: int. Maximum file size in MB for before splitting output files..
        - `delete_files`: bool. Flag indicating whether to delete processed files.
        - `concat_files`: bool. Flag indicating whether to concatenate processed files.
        - `select_cols`: list or None. List of columns to select during file operations.
        - `query`: str, fnc or None. Query string or function for filtering data.
        - `query_args`: list or None. list of arguments for the query string or function.
        - `dfs`: None or DataFrame. Stores concatenated DataFrames if concatenation is enabled.
    
        """

        # Set connection object to None (initial value)
        self.connection: object = None
        self.hostname: str = hostname
        self.username: str = username
        self.privatekey: str = privatekey
        self.port: int = port
        self._cnopts = pysftp.CnOpts()
        self._cnopts.hostkeys = None
        
        self.output_format: list =  ['.csv'] 
        self.file_size_mb:int = 500
        self.delete_files: bool = False
        self.concat_files: bool = True
        self._select_cols: list = None 
        self.query = None
        self.query_args: list = None
        self._bvd_list: list = [None,None,None]
        self._time_period: list = [None,None,None,"remove"]
        self.dfs = None

        self._local_path: str = None
        self._local_files: list = []

        self._remote_path: str = None
        self._remote_files: list = []

        self._tables_available = None
        self._tables_backup = None
        self._set_data_product:str = None
        self._time_stamp:str = None
        self._set_table:str = None
        self._table_dictionary = None
        self._table_dates = None
        self._download_finished = None
        
        if hasattr(os, 'fork'):
            self._pool_method = 'fork'
            self._max_path_length = 256
        else:
            self._pool_method = 'threading'
            self._max_path_length = 256

        if sys.platform.startswith('linux'):
            self._max_path_length = 4096
        elif sys.platform == 'darwin':
            self._max_path_length = 1024
        elif sys.platform == 'win32':
            self._max_path_length = 256

    # pool method
    @property
    def pool_method(self):
       return self._pool_method
    
    @pool_method.setter
    def pool_method(self,method:str):
        """
        Get or set the worker pool method for concurrent operations.

        Input Variables:
        - `self`: Implicit reference to the instance.
        - `method` (str): Worker pool method (`'fork'`, `'threading'`, `'spawn'`).

        Returns:
        - Current worker pool method (`'fork'`, `'threading'`, `'spawn'`).
        """
        if not method in ['fork','theading','spawn']:
            print('invalid worker pool method')
            method = 'fork'

        if not hasattr(os, 'fork') and method =='fork':
            print('fork() processes are not supported by OS')
            method = 'spawn'
         
        print(f'"{method}" is chosen as worker pool method')
        self._pool_method == method

    # Local path and files
    @property
    def local_path(self):
       return self._local_path
    
    @local_path.setter
    def local_path(self, path):
        """
        Get or set the local path for operations.

        Input Variables:
        - `self`: Implicit reference to the instance.
        - `path` (str): Local path to set or retrieve.

        Returns:
        - Current local path.
        """
        if path is None:
            self._remote_files = []
            self._remote_path  = None    
            self._local_files  = []
            self._local_path   = None
        elif path is not self._local_path:
            self._local_files, self._local_path = self._check_path(path,"local")
    
    @property
    def local_files(self):
        self._local_files, self._local_path = self._check_path(self._local_path,"local")
        return self._local_files

    @local_files.setter
    def local_files(self, value):
        self._local_files = self._check_files(value)

    @property
    def remote_path(self):
       return self._remote_path

    @remote_path.setter
    def remote_path(self, path):
        """
        Get or set the remote path for operations.

        Input Variables:
        - `self`: Implicit reference to the instance.
        - `path` (str): Remote path to set or retrieve.

        Returns:
        - Current remote path.
        """
       
        if path is None:
            self._remote_files = []
            self._remote_path  = None    
            self._local_files  = []
            self._local_path   = None
            self._set_data_product = None
            self._set_table = None

        elif path is not self.remote_path:
            self._local_files  = []
            self._local_path   = None
            self._remote_files, self._remote_path = self._check_path(path,"remote")

            if self._remote_path is not None:
                if self._tables_available is None and self._tables_backup is None:
                    self.tables_available(save_to=False)

                df = self._tables_available.query(f"`Base Directory` == '{self._remote_path}'")
        
                if df.empty:
                    df = self._tables_available.query(f"`Export` == '{self._remote_path}'")
                    self._set_table = None
                else:                 
                    if (self._set_table and self._set_table not in df['Table'].values) or not self._set_table:
                        self._set_table = df['Table'].iloc[0]
                
                if not df.empty:
                    if (self._set_data_product and self._set_data_product not in df['Data Product'].values) or not self._set_data_product:    
                        self.set_data_product = df['Data Product'].iloc[0]
 
 
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
        """
        Set or retrieve the current data product.

        Input Variables:
        - `self`: Implicit reference to the instance.
        - `product` (str): Data product name to set or retrieve.

        Returns:
        - Current data product.
        """
        if self._tables_available is None and self._tables_backup is None:
                self.tables_available(save_to=False)

        if (product is None) or (product is not self._set_data_product):
            self._tables_available = self._tables_backup

        if product is None:
            self._set_data_product = None
            self._set_table = None
            self.remote_path = None
            self._time_stamp = None
            self._select_cols = None 
            self.query = None
            self.query_args = None

        elif product is not self._set_data_product:
            
            df = self._tables_available.query(f"`Data Product` == '{product}'")

            if df.empty:
                df = self._tables_available.query(f"`Data Product`.str.contains('{product}', case=False, na=False,regex=False)")
                if df.empty:  
                    print("No such Data Product was found. Please set right data product")
                else:
                    matches   = df[['Data Product']].drop_duplicates()
                    if len(matches) >1:
                        print(f"Multiple data products partionally match '{product}' : {matches['Data Product'].tolist()}. Please set right data product" )
                    else:
                        print(f"One data product partionally match '{product}' : {matches['Data Product'].tolist()}. Please set right data product")

            elif len(df['Export'].unique()) > 1:
                matches   = df[['Data Product','Export']].drop_duplicates()              
                print(f"Multiple version of '{product}' are detected: {matches['Data Product'].tolist()} with export paths ('Export') {matches['Export'].tolist()} .Please Set the '.remote_path' property with the correct 'Export' Path")                
            else:
                self._tables_available = df
                self._set_data_product = product
                self._set_table = None
                self._select_cols = None 
                self.query = None
                self.query_args = None
                #self.remote_path = df['Export'].iloc[0]
                self._time_stamp = df['Timestamp'].iloc[0]

    @property
    def set_table(self):
        return self._set_table

    @set_table.setter
    def set_table(self, table):
        """
        Set or retrieve the current table.

        Input Variables:
        - `self`: Implicit reference to the instance.
        - `table` (str): Table name to set or retrieve.

        Returns:
        - Current table.
        """
        if self._tables_available is None and self._tables_backup is None:
                self.tables_available(save_to=False)

        if table is None:
            self._set_table = None
            self.remote_path = None
            self._select_cols = None 
            self.query = None
            self.query_args = None

        elif table is not self._set_table:
            
            if self._set_data_product is None:
                df = self._tables_available.query(f"`Table` == '{table}'")
            else:
                df = self._tables_available.query(f"`Table` == '{table}' &  `Data Product` == '{self._set_data_product}'")

            if df.empty:
                df = self._tables_available.query(f"`Table`.str.contains('{table}', case=False, na=False,regex=False)")
                if len(df) >1:
                    matches   = df[['Data Product','Table']].drop_duplicates()
                    print(f"Multiple tables partionally match '{table}' : {matches['Table'].tolist()} from {matches['Data Product'].tolist()}. Please set right table" )
                elif df.empty:    
                    print("No such Table was found. Please set right table")
                self._set_table = None
            elif len(df) > 1:
                if self._set_data_product is None: 
                    matches   = df[['Data Product','Table']].drop_duplicates()
                    print(f"Multiple tables match '{table}' : {matches['Table'].tolist()} from {matches['Data Product'].tolist()}. Please set Data Product using the '.set_data_product' property")
          
                elif len(df['Export'].unique()) > 1:
                    matches   = df[['Data Product','Table','Export']].drop_duplicates()
                    print(f"Multiple version of '{table}' are detected: {matches['Table'].tolist()} from {matches['Data Product'].tolist()} with export paths ('Base Directory') {matches['Base Directory'].tolist()} .Please Set the '.remote_path' property with the correct 'Base Directory' Path")
                self._set_table = None    
            else:
                self._set_table = table
                self.set_data_product = df['Data Product'].iloc[0]
                self.remote_path = df['Base Directory'].iloc[0]
                self._select_cols = None 
                self.query = None
                self.query_args = None
   
    @property
    def bvd_list(self):
        return self._bvd_list
       
    @bvd_list.setter
    def bvd_list(self, bvd_list = None):
        def load_bvd_list(file_path, df_bvd ,delimiter='\t'):
            # Get the file extension
            file_extension = file_path.split('.')[-1].lower()
            
            # Load the file based on the extension
            if file_extension == 'csv':
                df = pd.read_csv(file_path)
            elif file_extension in ['xls', 'xlsx']:
                df = pd.read_excel(file_path)
            elif file_extension == 'txt':
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
                    bvd_list, search_type, non_matching_items = check_bvd_format(bvd_list, df_bvd)
                    
                    # If successful, return the result
                    if search_type is not None:
                        return bvd_list, search_type, non_matching_items

            return  bvd_list, search_type, non_matching_items  
            
        def check_bvd_format(bvd_list, df):
            bvd_list = list(set(bvd_list))
            # Check against df['Code'].values
            df_code_values = df['Code'].values
            df_matches = [item for item in bvd_list if item in df_code_values]
            df_match_count = len(df_matches)
            
            # Check against the regex pattern
            pattern = re.compile(r'^[A-Za-z]+[*]?[A-Za-z]*\d*[-\dA-Za-z]*$')
            regex_matches = [item for item in bvd_list if pattern.match(item)]
            regex_match_count = len(regex_matches)
            
            # Determine which check has more matches
            if df_match_count >= regex_match_count:
                non_matching_items = [item for item in bvd_list if item not in df_code_values]
                return df_matches, True if df_match_count == len(bvd_list) else None, non_matching_items
            else:
                non_matching_items = [item for item in bvd_list if not pattern.match(item)]
                return regex_matches, False if regex_match_count == len(bvd_list) else None, non_matching_items

        if bvd_list is not None:
            df =self.search_country_codes()

            if self._bvd_list[1] is not None and self._select_cols is not None:
                self._select_cols.remove(self._bvd_list[1])

            self._bvd_list = [None,None,None]
            search_word = None
            if (isinstance(bvd_list,str)) and os.path.isfile(bvd_list):
                bvd_list, search_type,non_matching_items = load_bvd_list(bvd_list,df)
            elif (isinstance(bvd_list,list) and len(bvd_list)==2) and (isinstance(bvd_list[0],(list, pd.Series, np.ndarray)) and isinstance(bvd_list[1],str)):
                search_word =  bvd_list[1]
                if isinstance(bvd_list[0],(pd.Series, np.ndarray)):
                    bvd_list = bvd_list[0].tolist()
                else:
                    bvd_list = bvd_list[0]
            else:
                if isinstance(bvd_list,(pd.Series, np.ndarray)):
                    bvd_list = bvd_list.tolist()

            bvd_list = _check_list_format(bvd_list)
            bvd_list, search_type,non_matching_items = check_bvd_format(bvd_list,df)

            if search_type is None:
                raise ValueError(f"The following elements does not seem to match bvd format:{non_matching_items}")

            if self._set_data_product is None or self._set_table is None:
                self.select_data()
            
            if search_word is None:
                bvd_col = self.search_dictionary()
            else:
                bvd_col = self.search_dictionary(search_word = search_word,search_cols={'Data Product':False,'Table':False,'Column':True,'Definition':False})

            if bvd_col.empty:
                raise ValueError("No 'bvd' columns were found for this table")

            bvd_col = bvd_col['Column'].unique().tolist()
            
            self._bvd_list[0]  = bvd_list
            
            if len(bvd_col) > 1:
                if isinstance(search_word, str) and search_word in bvd_col:
                    self._bvd_list[1] = search_word
                else:
                    _select_list('dropdown2',bvd_col,'bvd columns',_select_bvd,[self._bvd_list,self._select_cols, search_type])
                    return
            else:    
                self._bvd_list[1]  = bvd_col[0]

            self._bvd_list[2] = _construct_query(self._bvd_list[1],self._bvd_list[0],search_type)
        else:
            self._bvd_list = [None,None,None]
        
        if self._select_cols  is not None:
            self._select_cols = _check_list_format(self._select_cols,self._bvd_list[1],self._time_period[2])

    @property
    def time_period(self):
        return self._time_period
    
    @time_period.setter
    def time_period(self,years: list = None):
        def check_year(years):
            # Get the current year
            current_year = datetime.now().year
            
            # Check if the list has exactly two elements
            if len(years) <2:
                raise ValueError("The list must contain at least a start and end year e.g [1998,2005]. It can also contain a column name as a third element [1998,2005,'closing_date']")

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
            if self._time_period[2] is not None and self._select_cols is not None:
                self._select_cols.remove(self._time_period[2])
            
            self._time_period = check_year(years)
            self._time_period.append("remove")
            
            if self._set_data_product is None or self._set_table is None:
                self.select_data()

            date_col = self.table_dates(data_product=self.set_data_product,table = self._set_table,save_to=False)

            if date_col.empty:
                raise ValueError("No data columns were found for this table")

            date_col = date_col['Column'].unique().tolist()
            
            if self._time_period[2] is not None and self._time_period[2] not in date_col:
                raise ValueError(f"{self._time_period[2]} was not found as date related column: {date_col}. Set ._time_period[2] with the correct one") 
            
            elif self._time_period[2] is None and len(date_col) > 1:
                _select_list('dropdown',date_col,'Date columns',_select_date,[self._time_period,self._select_cols])
                return          

            if self._time_period[2] is None:
                self._time_period[2] = date_col[0]
        else:
            self._time_period =[None,None,None,"remove"]
        if self._select_cols  is not None:
            self._select_cols = _check_list_format(self._select_cols,self._bvd_list[1],self._time_period[2])

    @property
    def select_cols(self):
        return self._select_cols
    
    @select_cols.setter
    def select_cols(self,select_cols = None):
        
        if select_cols is not None:
            if self._set_data_product is None or self._set_table is None:
                self.select_data()

            select_cols = _check_list_format(select_cols,self._bvd_list[1],self._time_period[2])

            table_cols = self.search_dictionary(data_product=self.set_data_product,table = self._set_table,save_to=False)

            if table_cols.empty:
                self._select_cols = None
                raise ValueError("No columns were found for this table")

            table_cols = table_cols['Column'].unique().tolist()

            if not all(element in table_cols for element in select_cols):
                not_found = [element for element in table_cols if element not in select_cols]
                print("The following selected columns cannot be found in the table columns", not_found)
                self._select_cols = None
            else:
                self._select_cols = select_cols
        else:
            self._select_cols = None

    def connect(self):
        """
        Establish an SFTP connection.

        Input Variables:
        - `self`: Implicit reference to the instance.

        Returns:
        - SFTP connection object.
        """
        sftp = pysftp.Connection(host=self.hostname , username=self.username ,port = self.port ,private_key=self.privatekey, cnopts=self._cnopts)
        return sftp

    def select_data(self):
        """
        Asynchronously select and set the data product and table using interactive widgets.

        This method initializes an instance of `_SelectData` with `_tables_backup`, displays interactive widgets
        to allow the user to select a data product and table, and sets these selections to `self.set_data_product`
        and `self.set_table`, respectively. It also prints the selected data product and table.

        The method ensures that `_tables_available` or `_tables_backup` is populated by calling `tables_available()`
        if they are not already set.

        Notes:
        - This method uses `asyncio.ensure_future` to run the asynchronous function `f` which handles the widget interaction.

        Example:
            self.select_data()
        """
           
        async def f(self):
            Select_obj = _SelectData(self._tables_backup)
            selected_product, selected_table = await Select_obj.display_widgets()
   
            self.set_data_product = selected_product
            self.set_table = selected_table

            print(f"{self.set_data_product} was set as Data Product")
            print(f"{self.set_table} was set as Table")

        if self._tables_available is None and self._tables_backup is None:
                self.tables_available(save_to=False)
        
        asyncio.ensure_future(f(self))

    def select_columns(self):
        """
        Asynchronously select and set columns for a specified data product and table using interactive widgets.

        This method performs the following steps:
        1. Checks if the data product and table are set. If not, it calls `select_data()` to set them.
        2. Searches the dictionary for columns corresponding to the set data product and table.
        3. Displays an interactive widget for the user to select columns based on their names and definitions.
        4. Sets the selected columns to `self._select_cols` and prints the selected columns.

        If no columns are found for the specified table, a `ValueError` is raised.

        Args:
        - `self`: Implicit reference to the instance.

        Notes:
        - This method uses `asyncio.ensure_future` to run the asynchronous function `f` which handles the widget interaction.
        - The function `f` combines column names and definitions for display, maps selected items to their indices,
        and then extracts the selected columns based on these indices.

        Raises:
        - `ValueError`: If no columns are found for the specified table.

        Example:
            self.select_columns()
        """
        async def f(self,column, definition):

            combined = [f"{col}  -----  {defn}" for col, defn in zip(column, definition)]
            
            Select_obj = _SelectMultiple(combined,'columns')
            selected_list = await Select_obj.display_widgets()
            if selected_list is not None:

                # Create a dictionary to map selected strings to their indices in the combined list
                indices = {item: combined.index(item) for item in selected_list if item in combined}

                # Extract selected columns based on indices
                selected_list = [column[indices[item]] for item in selected_list if item in indices]
                self._select_cols = selected_list
                self._select_cols = _check_list_format(self._select_cols,self._bvd_list[1],self._time_period[2])
                print(f"The following columns have been selected: {self._select_cols}")
        
        if self._set_data_product is None or self._set_table is None:
            self.select_data()
        
        table_cols = self.search_dictionary(data_product=self.set_data_product,table = self._set_table,save_to=False)

        if table_cols.empty:
            self._select_cols = None
            raise ValueError("No columns were found for this table")

        column = table_cols['Column'].tolist()
        definition = table_cols['Definition'].tolist()

        asyncio.ensure_future(f(self, column, definition)) 

    def copy_obj(self):
        """
        Create a deep copy of the current Sftp instance with optional updates.

        Input Variables:
        - `self`: Implicit reference to the instance.
    
        Returns:
        - Deep copy of the current Sftp instance with optional updates.
        """
        new_obj = copy.deepcopy(self)

        new_obj.select_data()
        new_obj.bvd_list = None
        new_obj.time_period = None
        new_obj.select_cols = None
        
        return new_obj

    def table_dates(self,save_to:str=False, data_product = None,table = None):
        """
        Retrieve and save the available date columns for a specified data product and table.

        This method performs the following steps:
        1. Ensures that the available tables and table dates are loaded.
        2. Filters the dates data by the specified data product and table, if provided.
        3. Optionally saves the filtered results to a specified format.

        Args:
        - `self`: Implicit reference to the instance.
        - `save_to` (str, optional): Format to save results. If False, results are not saved (default is False).
        - `data_product` (str, optional): Specific data product to filter results by. If None, defaults to `self.set_data_product`.
        - `table` (str, optional): Specific table to filter results by. If None, defaults to `self.set_table`.

        Returns:
        - pandas.DataFrame: A DataFrame containing the filtered dates for the specified data product and table. If no results are found, an empty DataFrame is returned.

        Notes:
        - If `data_product` is provided and does not match any records, a message is printed and an empty DataFrame is returned.
        - If `table` is provided and does not match any records, it attempts to perform a case-insensitive partial match search.
        - If `save_to` is specified, the query results are saved in the format specified.

        Example:
            df = self.table_dates(save_to='csv', data_product='Product1', table='TableA')
        """    
        if self._tables_available is None and self._tables_backup is None:
                self.tables_available(save_to=False)

        if data_product is None and self.set_data_product is not None:
            data_product = self.set_data_product

            if table is None and self.set_table is not None:
                table = self.set_table

        if self._table_dates is None:
            self._table_dates = _table_dates()        
        df = self._table_dates
        df = df[df['Data Product'].isin(self._tables_backup['Data Product'].drop_duplicates())]

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
                df_table = df.query(f"`Table`.str.contains('{table}', case=False, na=False,regex=False)")
                if df_table.empty:
                    print("No such Table was found. Please set right table")
                    return df_table
            df = df_table
   
        _save_to(df,'date_cols_search',save_to)

        return df    
    
    # Under development
    def _search_dictionary_list(self, save_to:str=False, search_word=None, search_cols={'Data Product':True, 'Table':True, 'Column':True, 'Definition':True}, letters_only:bool=False, exact_match:bool=False, data_product = None, table = None):
        """
        Search for a term in a column/variable dictionary and save results to a file.

        Args:
        - `self`: Implicit reference to the instance.
        - `save_to` (str, optional): Format to save results. If False, results are not saved (default is False).
        - `search_word` (str or list of str, optional): Search term(s). If None, no term is searched.
        - `search_cols` (dict, optional): Dictionary indicating which columns to search. Columns are 'Data Product', 'Table', 'Column', and 'Definition' with default value as True for each.
        - `letters_only` (bool, optional): If True, search only for alphabetic characters in the search term (default is False).
        - `exact_match` (bool, optional): If True, search for an exact match of the search term. Otherwise, search for partial matches (default is False).
        - `data_product` (str, optional): Specific data product to filter results by. If None, no filtering by data product (default is None).
        - `table` (str, optional): Specific table to filter results by. If None, no filtering by table (default is None).

        Returns:
        - pandas.DataFrame: A DataFrame containing the search results. If no results are found, an empty DataFrame is returned.

        Notes:
        - If `data_product` is provided and does not match any records, a message is printed and an empty DataFrame is returned.
        - If `table` is provided and does not match any records, it attempts to perform a case-insensitive partial match search.
        - If `search_word` is provided and no matches are found, a message is printed indicating no results were found.
        - If `letters_only` is True, the search term is processed to include only alphabetic characters before searching.
        - If `save_to` is specified, the query results are saved in the format specified.
        """
    
        if self._tables_available is None and self._tables_backup is None:
            self.tables_available(save_to=False)

        if data_product is None and self.set_data_product is not None:
            data_product = self.set_data_product

        if table is None and self.set_table is not None:
            table = self.set_table

        if self._table_dictionary is None:
            self._table_dictionary = _table_dictionary()        
        
        df = self._table_dictionary
        df = df[df['Data Product'].isin(self._tables_backup['Data Product'].drop_duplicates())]

        if data_product is not None:
            df_product = df.query(f"`Data Product` == '{data_product}'")
            if df_product.empty:
                print("No such Data Product was found. Please set right data product")
                return df_product
            else:
                df = df_product
            search_cols['Data Product'] = False
        
        if table is not None:
            df_table = df.query(f"`Table` == '{table}'")
            if df_table.empty:   
                df_table = df.query(f"`Table`.str.contains('{table}', case=False, na=False, regex=False)")
                if df_table.empty:
                    print("No such Table was found. Please set right table")
                    return df_table
            search_cols['Table'] = False
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
                    
                search_conditions = " | ".join(base_string.format(col=col) for col, include in search_cols.items() if include)
                final_string = search_conditions.format(word=word)

                result_df = df.query(final_string)

                if result_df.empty:
                    base_string = "'{col}'"
                    search_conditions = " , ".join(base_string.format(col=col) for col, include in search_cols.items() if include)
                    print(f"No such '{word}' was detected across columns: " + search_conditions)
                else:
                    if letters_only:
                      result_df = df_backup.loc[result_df.index]  
                    result_df['search_word'] = word
                    results.append(result_df)

            if results:
                df = pd.concat(results, ignore_index=True)
            else:
                df = pd.DataFrame()

            #if letters_only:
            #    df = df_backup.loc[df.index]

            if save_to:
                print(f"The following query was executed for each word in search_word: {search_word} : ")

        _save_to(df, 'dict_search', save_to)

        return df

    def search_dictionary(self,save_to:str=False, search_word = None,search_cols={'Data Product':True,'Table':True,'Column':True,'Definition':True}, letters_only:bool=False,extact_match:bool=False, data_product = None, table = None):
    
        """
        Search for a term in a column/variable dictionary and save results to a file.

        Args:
        - `self`: Implicit reference to the instance.
        - `save_to` (str, optional): Format to save results. If False, results are not saved (default is False).
        - `search_word` (str, optional): Search term. If None, no term is searched.
        - `search_cols` (dict, optional): Dictionary indicating which columns to search. Columns are 'Data Product', 'Table', 'Column', and 'Definition' with default value as True for each.
        - `letters_only` (bool, optional): If True, search only for alphabetic characters in the search term (default is False).
        - `exact_match` (bool, optional): If True, search for an exact match of the search term. Otherwise, search for partial matches (default is False).
        - `data_product` (str, optional): Specific data product to filter results by. If None, no filtering by data product (default is None).
        - `table` (str, optional): Specific table to filter results by. If None, no filtering by table (default is None).

        Returns:
        - pandas.DataFrame: A DataFrame containing the search results. If no results are found, an empty DataFrame is returned.

        Notes:
        - If `data_product` is provided and does not match any records, a message is printed and an empty DataFrame is returned.
        - If `table` is provided and does not match any records, it attempts to perform a case-insensitive partial match search.
        - If `search_word` is provided and no matches are found, a message is printed indicating no results were found.
        - If `letters_only` is True, the search term is processed to include only alphabetic characters before searching.
        - If `save_to` is specified, the query results are saved in the format specified.
        """

        if self._tables_available is None and self._tables_backup is None:
                self.tables_available(save_to=False)

        if data_product is None and self.set_data_product is not None:
            data_product = self.set_data_product

            if table is None and self.set_table is not None:
                table = self.set_table

        if self._table_dictionary is None:
            self._table_dictionary = _table_dictionary()        
        df = self._table_dictionary
        df = df[df['Data Product'].isin(self._tables_backup['Data Product'].drop_duplicates())]

        if data_product is not None:
            df_product = df.query(f"`Data Product` == '{data_product}'")
            if df_product.empty:
                print("No such Data Product was found. Please set right data product")
                return df_product
            else:
                df = df_product
            search_cols['Data Product'] = False
        if table is not None:
            df_table = df.query(f"`Table` == '{table}'")
            if df_table.empty:   
                df_table = df.query(f"`Table`.str.contains('{table}', case=False, na=False,regex=False)")
                if df_table.empty:
                    print("No such Table was found. Please set right table")
                    return df_table
            search_cols['Table'] = False
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
                
            search_conditions = " | ".join(base_string.format(col=col) for col, include in search_cols.items() if include)
            final_string = search_conditions.format(search_word=search_word)

            df = df.query(final_string)

            if df.empty:
                base_string = "'{col}'"
                search_conditions = " , ".join(base_string.format(col=col) for col, include in search_cols.items() if include)
                print("No such 'search word' was detected across columns: " + search_conditions)
                return df

            if letters_only:
                df = df_backup.loc[df.index]

            if save_to:
                print(f"The folloiwng query was executed:" + final_string)

        _save_to(df,'dict_search',save_to)

        return df    

    def orbis_to_moodys(self,file):

        def _load_orbis_file(file):
            df = pd.read_excel(file, sheet_name='Results')

            # Get the headings (column names) from the DataFrame
            headings = df.columns.tolist()

            # Process headings to keep only the first line if they contain multiple lines
            processed_headings = [heading.split('\n')[0] for heading in headings]

            # Keep only unique headings
            unique_headings = list(set(processed_headings)) 
            unique_headings.remove('Unnamed: 0')
            return unique_headings
        
        def sort_by(df):
            # Sort by 'Data Product'
            df_sorted = df.sort_values(by='Data Product')

            # Count unique headings for each 'Data Product'
            grouped = df_sorted.groupby('Data Product')['heading'].nunique().reset_index()
            grouped.columns = ['Data Product', 'unique_headings']

            # Sort 'Data Product' based on the number of unique headings in descending order
            sorted_products = grouped.sort_values(by='unique_headings', ascending=False)['Data Product']

            # Reorder the original DataFrame based on the sorted 'Data Product'
            df_reordered = pd.concat(
                [df_sorted[df_sorted['Data Product'] == product] for product in sorted_products],
                ignore_index=True
            )
            return df_reordered

        headings = _load_orbis_file(file)
        headings_processed = [_letters_only_regex(heading) for heading in headings]

        df = _table_dictionary()
        df['letters_only'] = df['Column'].apply(_letters_only_regex)

        found = []
        not_found  = []
        for heading, heading_processed in zip(headings,headings_processed):
            df_sel = df.query(f"`letters_only` == '{heading_processed}'")

            if df_sel.empty:
                not_found.append(heading)
            else:
                df_sel = df_sel.copy()  # Avoid SettingWithCopyWarning
                df_sel['heading'] = heading 
                found.append(df_sel)
        
        # Concatenate all found DataFrames if needed
        if found:
            found = pd.concat(found, ignore_index=True)
            found = sort_by(found)
        else:
            found = pd.DataFrame()

        return found, not_found

    def tables_available(self,save_to:str=False,reset:bool=False):
        """
        Retrieve available SFTP data products and tables and save them to a file.

        Input Variables:
        - `self`: Implicit reference to the instance.
        - `save_to` (str, optional): Format to save results (default is CSV).
        - `reset` (bool, optional): Reset flag to force refreshing data products and tables.

        Returns:
        - Pandas DataFrame with the available SFTP data products and tables.
        """

        if self._tables_available is None and self._tables_backup is None:
            self._tables_available,to_delete = self._table_overview()
            self._tables_backup = self._tables_available 

            if self.hostname == "s-f2112b8b980e44f9a.server.transfer.eu-west-1.amazonaws.com" and len(to_delete) > 0:
                print("------------------  DELETING OLD EXPORTS FROM SFTP")
                self._remove_exports(to_delete)

        elif reset:
            self._tables_available = self._tables_backup

        _save_to(self._tables_available,'tables_available',save_to)
   
        return self._tables_available
    
    def search_country_codes(self,search_word = None,search_cols={'Country':True,'Code':True}):        
        """
        Search for country codes matching a search term.

        Input Variables:
        - `self`: Implicit reference to the instance.
        - `search_word` (str, optional): Term to search for country codes.
        - `search_cols` (dict, optional): Dictionary indicating columns to search (default is {'Country':True,'Code':True}).

        Returns:
        - Pandas Dataframe of country codes matching the search term
        """
        
        df = _country_codes()
        if search_word is not None:
  
            base_string = "`{col}`.str.contains('{{search_word}}', case=False, na=False,regex=False)"
            search_conditions = " | ".join(base_string.format(col=col) for col, include in search_cols.items() if include)
            final_string = search_conditions.format(search_word=search_word)

            df = df.query(final_string)

            if df.empty:
                base_string = "'{col}'"
                search_conditions = " , ".join(base_string.format(col=col) for col, include in search_cols.items() if include)
                print("No such 'search word' was detected across columns: " + search_conditions)
                return df
            else:
                print(f"The folloiwng query was executed:" + final_string)

        return df    

    def process_one(self,save_to=False,files = None,n_rows:int=1000):
        """
        Retrieve a sample of data from a table and save it to a file.

        Input Variables:
        - `self`: Implicit reference to the instance.
        - `files` (list, optional): List of files to process. Defaults to `self.remote_files`.
        - `save_to` (str, optional): Format to save sample data (default is CSV).
        - `n_rows` (int, optional): Number of rows to retrieve (default is 1000).

        Returns:
        - Pandas Dateframe with output
        """

        if files is None:
            if self._set_data_product is None or self._set_table is None:
                self.select_data()
            files = [self.remote_files[0]]
        elif isinstance(files,int):
            files = [files]    

        df, files = self.process_all(files = files,num_workers=len(files))

        if df is None and files is not None:
            dfs = []
            for file in files:
                df  = _load_table(file)
                dfs.append(df)
            df = pd.concat(dfs, ignore_index=True)
            df = df.head(n_rows)
            _save_to(df,'process_one',save_to) 
        elif not df.empty and files is not None:
            df = df.head(n_rows)
            print(f"Results have been saved to '{files}'")
        elif df.empty:  
            print("No rows were retained")  
        return df
    
    def process_all(self, files:list = None,destination:str = None, num_workers:int = -1, select_cols: list = None , date_query = None, bvd_query = None, query = None, query_args:list = None,pool_method = None):
        """
        Read and process files into a DataFrame with optional filtering and parallel processing.

        This method reads multiple files into Pandas DataFrames, optionally selecting specific columns and
        applying filters, either sequentially or in parallel.

        Input Variables:
        - `self`: Implicit reference to the instance.
        - `files` (list, optional): List of files to process. Defaults to `self.remote_files`.
        - `destination` (str, optional): Destination path for processed files.
        - `num_workers` (int, optional): Number of workers for parallel processing. Default is -1 (auto-determined).
        - `select_cols` (list, optional): Columns to select from files. Default is `self._select_cols`.
        - `date_query`: (optional): Date query for filtering data. Default is `self.time_period`.
        - `bvd_query`: (optional): BVD query for filtering data. Default is `self._bvd_list[2]`.
        - `query` (str, optional): Query for additional filtering of data.
        - `query_args` (list, optional): Arguments for the query.

        Returns:
        - `dfs`: List of Pandas DataFrames with selected columns and filtered data.
        - `file_names`: List of file names processed.

        Notes:
        - If `select_cols` is provided, it is validated against expected formats.
        - Uses parallel processing if `num_workers` is greater than 1.
        - Handles file concatenation and deletion based on instance attributes (`concat_files`, `delete_files`).
        - Prints current working directory if `self.delete_files` is `True`.

        Raises:
        - `ValueError`: If validation of arguments (`files`, `destination`, `flag`) fails.
        """

        files = files or self.remote_files
        date_query = date_query or self.time_period
        bvd_query = bvd_query or self._bvd_list[2]
        query = query or self.query
        query_args = query_args or self.query_args
        select_cols = select_cols or self._select_cols

        # To handle executing when download_all() have not finished!
        if self._download_finished is False and all(file in self._remote_files for file in files): 
            start_time = time.time()
            timeout = 5
            files_not_ready =  not all(file in self.local_files for file in files) 
            while files_not_ready:
                time.sleep(0.1)
                files_not_ready =  not all(file in self.local_files for file in files)
                if time.time() - start_time >= timeout:
                    print(f"Files have not finished downloading within the timeout period of {timeout} seconds.")
                    return None, None

            self._download_finished =True 

        if select_cols is not None:
            select_cols = _check_list_format(select_cols,self._bvd_list[1],self._time_period[2])

        try:
            flag =  any([select_cols, query, all(date_query),bvd_query]) 
            files, destination = self._check_args(files,destination,flag)
        except ValueError as e:
            print(e)
            return None
        
        if isinstance(num_workers, (int, float, complex)):
            num_workers = int(num_workers) 
        else: 
            num_workers = -1
        
        if num_workers < 1:
            num_workers =int(psutil.virtual_memory().total/ (1024 ** 3)/12)

        # Read multithreaded
        if num_workers != 1 and len(files) > 1:
            def batch_processing():
                def batch_list(input_list, batch_size):
                    """Splits the input list into batches of a given size."""
                    batches = []
                    for i in range(0, len(input_list), batch_size):
                        batches.append(input_list[i:i + batch_size])
                    return batches

                batches = batch_list(files,num_workers)

                lists = []

                print(f'Processing {len(files)} files in Parallel')
              
                for index, batch in enumerate(batches,start=1):
                    print(f"Processing Batch {index} of {len(batches)}")
                    print(f"------ First file: '{batch[0]}'")  
                    print(f"------ Last file : '{batch[-1]}'")               
                    params_list = [(file, destination, select_cols, date_query, bvd_query, query, query_args) for file in batch]
                    list_batch = _run_parallel(fnc=self._process_parallel,params_list=params_list,n_total=len(batch),num_workers=num_workers,pool_method=pool_method ,msg='Processing')
                    lists.extend(list_batch)

                   
                file_names = [elem[1] for elem in lists]
                file_names = [file_name[0] for file_name in file_names if file_name is not None]
                    
                dfs =  [elem[0] for elem in lists]
                dfs = [df for df in dfs if df is not None]

                flags =  [elem[2] for elem in lists]

                return dfs, file_names, flags

            dfs, file_names, flags = batch_processing()
        
        else: # Read Sequential
            print(f'Processing  {len(files)} files in sequence')
            dfs, file_names, flags = self._process_sequential(files, destination, select_cols, date_query, bvd_query, query, query_args,num_workers)
        
        flag =  all(flags) 

        if (not self.concat_files and not flag) or len(dfs) == 0:
                self.dfs = None
        elif self.concat_files and not flag:
            
            # Concatenate and save
            self.dfs, file_names = _save_chunks(dfs=dfs,file_name=destination,output_format=self.output_format,file_size=self.file_size_mb, num_workers=num_workers)
    
        return self.dfs, file_names
  
    def download_all(self,num_workers = None):
        
        if hasattr(os, 'fork'):
            pool_method = 'fork'
        else:
            print("Function only works on Unix systems right now")
            return
         
        if self._set_data_product is None or self._set_table is None:
            self.select_data()
        
        if num_workers is None:
            num_workers= int(cpu_count() - 2)

        if isinstance(num_workers, (int, float, complex))and num_workers != 1:
            num_workers= int(num_workers)

        _, _ = self._check_args(self._remote_files)

        print("Downloading all files")
        process = Process(target=self.process_all, kwargs={'num_workers': num_workers, 'pool_method': pool_method})
        process.start()

        self._download_finished = False 

    def specify_data_product(self):
        print("Not working yet!!")

    def get_column_names(self,save_to:str=False, files = None):
        """
        Retrieve column names from a DataFrame or dictionary and save them to a file.

        Input Variables:
        - `self`: Implicit reference to the instance.
        - `save_to` (str, optional): Format to save results (default is CSV).
        - `files` (list, optional): List of files to retrieve column names from.

        Returns:
        - List of column names or None if no valid source is provided.
        """
        
        def from_dictionary(self):
            if self.set_table is not None: 
                df = self.search_dictionary(save_to=False)
                column_names = df['Column'].to_list()
                return column_names
            else: 
                return None
        def from_files(self,files):
            if files is None and self.remote_files is None:   
                raise ValueError("No files were added")
            elif files is None and self.remote_files is not None:
                files = self.remote_files   
            
            try:
                file,_ = self._check_args([files[0]])
                file, _ = self._get_file(file[0])
                parquet_file = pq.ParquetFile(file)
                # Get the column names
                column_names = parquet_file.schema.names
                return column_names
            except ValueError as e:
                print(e)
                return None

        if files is not None:
            column_names = from_files(self,files)
        else:       
            column_names = from_dictionary(self)    
        
        if column_names is not None:
            df = pd.DataFrame({'Column_Names': column_names})
            _save_to(df,'column_names',save_to)

        return column_names
     
    def _table_overview(self,product_overview = None):
        
        print('Retrieving Data Product overview from SFTP..wait a moment')
        
        if product_overview is None:
            product_overview =_table_names()

        with self.connect() as sftp:
            product_paths = sftp.listdir()
            newest_exports = []
            time_stamp = []
            repeating = []
            data_products = []
            to_delete = []
      
            for product_path in product_paths:
                sel_product = product_overview.loc[product_overview['Top-level Directory'] == product_path, 'Data Product']
                if len(sel_product) == 0:
                    data_products.append(None)
                else:    
                    data_products.append(sel_product.values[0])
                
                tnfs_folder = product_path + '/tnfs'
                export_paths =  sftp.listdir(product_path)

                if not sftp.exists(tnfs_folder):
                    path = product_path + '/' + export_paths[0]
                    newest_exports.append(path)
                    time_stamp.append(None)
                    repeating.append('One-off')
                    continue

                # Get all .tnfs files in the 'tnfs' folder
                tnfs_files = [f for f in sftp.listdir(tnfs_folder) if f.endswith('.tnf')]
                if not tnfs_files:
                    print(tnfs_files)
                    print('Error')
                    
                # Initialize variables to keep track of the newest .tnfs file
                newest_tnfs_file = None
                newest_mtime = float('-inf')

                # Determine the newest .tnfs file
                for tnfs_file in tnfs_files:
                    tnfs_file_path  = tnfs_folder + '/' + tnfs_file
                    file_attributes = sftp.stat(tnfs_file_path)
                    mtime = file_attributes.st_mtime
                    if mtime > newest_mtime:
                        newest_mtime = mtime
                        if newest_tnfs_file is not None:
                            sftp.remove(newest_tnfs_file)
                        newest_tnfs_file = tnfs_file_path
                    else:
                        sftp.remove(tnfs_file_path)

                if newest_tnfs_file:
            
                    time_stamp.append(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(newest_mtime)))
                    if self._local_path is not None:
                        local_file = self._local_path + '/' +  "temp.tnf"
                    else: 
                        local_file = "temp.tnf"
                    sftp.get(newest_tnfs_file,  local_file)

                    # Read the contents of the newest .tnfs file
                    with open( local_file , 'r') as f:
                        tnfs_data = json.load(f)
                        newest_export = product_path + '/' + tnfs_data.get('DataFolder')
                        newest_exports.append(newest_export)
                        repeating.append('Repeating')
                
                    os.remove(local_file)

                    for export_path in export_paths:
                        export_path = product_path + '/' +  export_path 
                        if export_path != newest_export and export_path != tnfs_folder:
                            to_delete.append(export_path)

            # Create a DataFrame from the lists
            df = pd.DataFrame({'Data Product': data_products,'Top-level Directory': product_paths,'Newest Export': newest_exports,'Timestamp': time_stamp,'Repeating': repeating})

            data = []

            for _ , row  in df.iterrows():                
                data_product = row['Data Product']
                timestamp = row['Timestamp']
                main_directory = row['Top-level Directory']
                export = row['Newest Export']
                repeating = row['Repeating']
                tables = sftp.listdir(export)

                full_paths  = [export + '/' + table for table in tables]
                full_paths = [os.path.dirname(full_path) if full_path.endswith('.csv') else full_path for full_path in full_paths]
                              
                if pd.isna(data_product):
                    data_product, tables = _table_match(tables)

                # Append a dictionary with export, timestamp, and modified_list to the list
                for full_path, table in zip(full_paths,tables):

                    data.append({'Data Product':data_product,
                                 #'Table': os.path.basename(full_path),
                                 'Table': table,
                                 'Base Directory': full_path,
                                 'Timestamp': timestamp,
                                 'Repeating':repeating,
                                 'Export': export,
                                 'Top-level Directory':main_directory})

            # Create a DataFrame from the list of dictionaries
            df = pd.DataFrame(data)
            
            return  df, to_delete

    def _remove_exports(self,to_delete = None,num_workers = None):
            
        if to_delete is None:
            _, to_delete = self._table_overview()

        if len(to_delete) == 0:
            return
 
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
        lists = _run_parallel(fnc=self._recursive_collect,params_list=to_delete,n_total=len(to_delete),msg = 'Collecting files to delete')
        file_paths = [item for sublist in lists for item in sublist]

        # Define worker
        if num_workers is None:
            num_workers= int(cpu_count() - 2)

        if isinstance(num_workers, (int, float, complex))and num_workers != 1:
            num_workers = int(num_workers)
        
        # Batch files to delete
        batches = batch_list(file_paths,num_workers)

        # Deleting files
        _run_parallel(fnc=self._delete_files,params_list=batches,n_total=len(batches),msg = 'Deleting files')

        # Deleting empty folders
        _run_parallel(fnc=self._delete_folders,params_list=to_delete,n_total=len(to_delete),msg='Deleting folders')
   
    def _delete_folder(self,folder_path:str=None):
        def recursive_delete(sftp, path,extensions: tuple = (".parquet", ".csv",".orc",".avro")):
            for file_attr in sftp.listdir_attr(path):
                full_path = path + '/' + file_attr.filename

                # Check if the file ends with any of the specified extensions
                if full_path.endswith(extensions):
                    sftp.remove(full_path)
                elif sftp.isdir(full_path):
                    recursive_delete(sftp, full_path)
                else:
                    sftp.remove(full_path)

        with self.connect() as sftp:
            try:
                recursive_delete(sftp, folder_path)
                print(f"Folder {folder_path} deleted successfully")
            except FileNotFoundError:
                print(f"Folder {folder_path} not found")
            except Exception as e:
                print(f"Failed to delete folder {folder_path}: {e}")

    def _recursive_collect(self, path,extensions: tuple = (".parquet", ".csv",".orc",".avro")):
            file_paths = []
            with self.connect() as sftp:
                for file_attr in sftp.listdir_attr(path):
                    full_path = path + '/' + file_attr.filename

                    # Check if the file ends with any of the specified extensions
                    if full_path.endswith(extensions):
                        file_paths.append(full_path)
                    elif sftp.isdir(full_path):
                        subfolder_paths = self._recursive_collect(full_path,extensions)
                        file_paths.extend(subfolder_paths)
                    else:
                        file_paths.append(full_path)

            return file_paths
    
    def _delete_files(self,files):
            with self.connect() as sftp:
                for file in files:
                   sftp.remove(file)  

    def _delete_folders(self,folder_path:str=None):
        
        def recursive_delete(sftp, path):
            for file_attr in sftp.listdir_attr(path):
                full_path = path + '/' + file_attr.filename
                sftp.remove(full_path)   

        with self.connect() as sftp:
            try:
                recursive_delete(sftp, folder_path)
                print(f"Folder {folder_path} deleted successfully")
            except FileNotFoundError:
                print(f"Folder {folder_path} not found")
            except Exception as e:
                print(f"Failed to delete folder {folder_path}: {e}")

    def _get_attr(self, inputs_args):
        file, path, mode = inputs_args
        data = {'file': [],
                'size': [],
                'time_stamp': []}
        if mode == "local":
            data['file'].append(file)
            data['size'].append(os.path.getsize(path + "/" + file))
        else:
            with self.connect() as sftp:
                file_attributes = sftp.stat(path + "/" + file)
                data['file'].append(file)
                data['size'].append(file_attributes.st_size)
                data['time_stamp'].append(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(file_attributes.st_mtime)))

        return pd.DataFrame(data)

    def _file_attr(self,mode:str=None,num_workers:int = -1):
           
        file_attributes = pd.DataFrame()
        if mode == "local":
            path = self._local_path
            files = self._local_files
        else:
            path = self._remote_path
            files = self._remote_files

        if path is not None:
            print(f"----------getting {mode} file attributes")

            # Read multithreaded
            if isinstance(num_workers, (int, float, complex))and num_workers != 1:
                num_workers = int(num_workers) 
                params_list =  [(file, path, mode) for file in files]
                dfs = _run_parallel(fnc=self._get_attr,params_list=params_list,n_total=len(files),num_workers=num_workers,msg='Processing')
                file_attributes = pd.concat(dfs, ignore_index=True)
            else:
                print('------ Processing files in sequence')
                for file in files:
                    file_attributes =  pd.concat([file_attributes, self._get_attr((file, path, mode))])
        else:
            print(f"{mode} path is not defined.")
            
        file_attributes.reset_index(drop=True, inplace=True)
        file_attributes.to_csv(self._local_path + "/file_attributes.csv")
        return file_attributes

    def _check_path(self,path,mode=None):
        files = []
        if path is not None:
            if mode == "local" or mode is None:
                if os.path.exists(path):
                    if os.path.isdir(path):
                        files = os.listdir(path)
                    elif os.path.isfile(path):
                        files = [os.path.basename(path)]
                        path  = os.path.dirname(path)
                else:
                    if mode is None:
                        mode = "remote"
                    else:
                        os.makedirs(path)
                        print(f"Folder '{path}' created.")

            if mode=="remote":
                sftp = self.connect()
                if sftp.exists(path):
                    files = sftp.listdir(path)
                    if not files:
                        files = [os.path.basename(path)]
                        path  = os.path.dirname(path)
                else:
                    print(f"Remote path is invalid:'{path}'")
                    path = None

            if len(files) > 1 and any(file.endswith('.csv') for file in files):
                if self._set_table is not None:
                    # Find the file that matches the match_string without the .csv suffix
                    files = [next((file for file in files if os.path.splitext(file)[0] == self._set_table), None)]
                else:
                    print( "Please set Data Product ('.set_data_product') and ('.set_table') before setting Remote Path containg .csv")

        else:
            files = []
        return files,path

    def _check_files(self,value):
        if isinstance(value, list) and all(isinstance(item, str) for item in value):
            return value
        else:
            raise ValueError("file list must be a list of strings")

    def _get_file(self,file:str):
        
        def _file_exist(file:str):
            base_path = os.getcwd()
            base_path = base_path.replace("\\", "/")

            if not file.startswith(base_path):
                file = os.path.join(base_path,file)
    
            if len(file) > self._max_path_length:
                raise ValueError(f"file path is longer ({len(file)}) than the max path length of the OS {self._max_path_length}. Set '.local_path' closer to root. The file is : '{file}'") 

            if os.path.exists(file):
                self.delete_files = False
                flag = True
            else:
                file = self._local_path + "/" + os.path.basename(file)
                flag = False
                if not file.startswith(base_path):
                    file = base_path + "/" + file

            if len(file) > self._max_path_length:
                raise ValueError(f"file path is longer ({len(file)}) than the max path length of the OS {self._max_path_length}. Set '.local_path' closer to root. The file is : '{file}'")     

            return file, flag

        local_file,flag = _file_exist(file) 

        if not os.path.exists(local_file):
            try:
                with self.connect() as sftp: 
                    remote_file =  self.remote_path + "/" + os.path.basename(file)
                    sftp.get(remote_file, local_file)
                    file_attributes = sftp.stat(remote_file)
                    time_stamp = file_attributes.st_mtime
                    os.utime(local_file, (time_stamp, time_stamp))
            except Exception as e:
                raise ValueError(f"Error reading remote file: {e}")
        
        return local_file, flag

    def _curate_file(self,flag:bool,file:str,destination:str,local_file:str,select_cols:list, date_query:list=[None,None,None,"remove"], bvd_query:str = None, query = None, query_args:list = None,num_workers:int = -1):
        df = None 
        file_name = None
        if any([select_cols, query, all(date_query),bvd_query]) or flag: 
            
            file_extension = file.lower().split('.')[-1]

            if file_extension in ['csv']:
                df = _load_csv_table(file = local_file, 
                                    select_cols = select_cols, 
                                    date_query = date_query, 
                                    bvd_query = bvd_query, 
                                    query = query, 
                                    query_args = query_args,
                                    num_workers = num_workers
                                    )
            else:
                df = _load_table(file = local_file, 
                                    select_cols = select_cols, 
                                    date_query = date_query, 
                                    bvd_query = bvd_query, 
                                    query = query, 
                                    query_args = query_args
                                    )

            if (df is not None and self.concat_files is False and self.output_format is not None) and not flag:
                file_name, _ = os.path.splitext(destination + "/" + file)
                file_name = _save_files(df,file_name,self.output_format)
                df = None

            if self.delete_files and not flag:
                try: 
                    os.remove(local_file)
                except:
                    raise ValueError(f"Error deleting local file: {local_file}")
        else:
            file_name = local_file  

        return df, file_name
         
    def _process_sequential(self, files:list, destination:str=None, select_cols:list = None, date_query:list=[None,None,None,"remove"], bvd_query:str = None, query = None, query_args:list = None,num_workers:int = -1):
        dfs = []
        file_names = []
        flags   = []
        total_files = len(files)
        for i, file in enumerate(files, start=1):
            if total_files > 1:
                print(f"{i} of {total_files} files")   
            try:
                local_file, flag = self._get_file(file)
                df , file_name = self._curate_file(flag = flag,
                                                    file = file,
                                                    destination = destination,
                                                    local_file = local_file,
                                                    select_cols = select_cols,
                                                    date_query = date_query,
                                                    bvd_query = bvd_query,
                                                    query = query,
                                                    query_args = query_args,
                                                    num_workers = num_workers
                                                    )
                flags.append(flag)
                
                if df is not None:
                    dfs.append(df)
                else:
                    file_names.append(file_name)
            except ValueError as e:
                print(e)
        
        return dfs,file_names,flags

    def _process_parallel(self, inputs_args:list):      
        file, destination, select_cols, date_query, bvd_query, query, query_args = inputs_args
        local_file, flag = self._get_file(file)
        df, file_name = self._curate_file(flag = flag,
                                                file = file,
                                                destination = destination,
                                                local_file = local_file,
                                                select_cols = select_cols,
                                                date_query = date_query,
                                                bvd_query = bvd_query,
                                                query = query,
                                                query_args = query_args
                                                )

        return [df, file_name,flag]

    def _check_args(self,files:list,destination = None,flag:bool = False):
        
        def _detect_files(files):
            def format_timestamp(timestamp: str) -> str:
                formatted_timestamp = timestamp.replace(' ', '_').replace(':', '-')
                return formatted_timestamp

            if isinstance(files,str):
                files = [files]
            elif isinstance(files,list) and len(files) == 0:
                raise ValueError("'files' is a empty list") 
            elif not isinstance(files,list):
                raise ValueError("'files' should be str or list formats") 
            
            existing_files = [file for file in files if os.path.exists(file)]
            missing_files = [file for file in files if not os.path.exists(file)]

            if not existing_files:
                if not self.local_files and not self.remote_files:
                    raise ValueError("No local or remote files detected") 

                if self._local_path is None and self._remote_path is not None:

                    if self._time_stamp:
                        self.local_path = "Data Products/" + self.set_data_product +'_exported '+ format_timestamp(self._time_stamp) + '/' + self.set_table
                    else:
                        self.local_path = "Data Products/" + self.set_data_product +'_one-off'+ '/' + self.set_table
                    
                missing_files = [file for file in files if file not in self._remote_files and file not in self.local_files]
                existing_files = [file for file in files if file in self._remote_files or file in self.local_files] 
            
            if not existing_files:
                raise ValueError('Requested files cannot be found locally or remotely') 

            return existing_files, missing_files

        files,missing_files = _detect_files(files)

        if missing_files:
            print("Missing files:")
            for file in missing_files:
                print(file)

        if destination is None and flag:    
            current_time = datetime.now()
            timestamp_str = current_time.strftime("%y%m%d%H%M")

            if self._remote_path is not None:
                suffix= os.path.basename(self._remote_path)
            else: 
                suffix= os.path.basename(self._local_path)

            destination = f"{timestamp_str}_{suffix}"

            base_path = os.getcwd()
            base_path = base_path.replace("\\", "/")

            destination = base_path + "/" + destination
            
        if self.concat_files is False and destination is not None:
            if not os.path.exists(destination):
                os.makedirs(destination)
        elif self.concat_files is True and destination is not None:
            parent_directory = os.path.dirname(destination)

            if parent_directory and not os.path.exists(parent_directory):
                os.makedirs(parent_directory)

        return files, destination   

    def _table_search(self, search_word):

        if self._tables_available is None and self._tables_backup is None:
           self.tables_available(save_to=False) 
        
        filtered_df = self._tables_available.query(f"`Data Product`.str.contains('{search_word}', case=False, na=False,regex=False) | `Table`.str.contains('{search_word}', case=False, na=False,regex=False)")
        return filtered_df

# Select_data Class
class _SelectData:
    def __init__(self, df):
        self.df = df
        self.selected_product = None
        self.selected_table = None

        # Create the first dropdown menu
        self.product_dropdown = widgets.Dropdown(
            options=self.df['Data Product'].unique(),
            description='Data Product:',
            disabled=False,
        )

        # Create the second dropdown menu placeholder
        self.table_dropdown = widgets.Dropdown(
            options=[],
            description='Table:',
            disabled=True,
        )

        # Create the button and set its initial state to disabled
        self.ok_button = widgets.Button(
            description='OK',
            disabled=True,
        )

        # Create the Cancel button
        self.cancel_button = widgets.Button(
            description='Cancel',
            disabled=False,
        )

        # Observe changes in both dropdown selections and button click
        self.product_dropdown.observe(self._observe_product_change, names='value')
        self.table_dropdown.observe(self._observe_table_change, names='value')
        self.ok_button.on_click(self._ok_button_click)
        self.cancel_button.on_click(self._cancel_button_click)

    async def _product_change(self, change):
        if change['type'] == 'change' and change['name'] == 'value':
            self.selected_product = change.new
            filtered_tables = self.df[self.df['Data Product'] == self.selected_product]['Table'].unique()
            self.table_dropdown.options = filtered_tables.tolist()  # Ensure options are converted to list
            self.table_dropdown.disabled = False  # Enable the table dropdown
            self.ok_button.disabled = True  # Disable the button until a table is selected

    async def _table_change(self, change):
        if change['type'] == 'change' and change['name'] == 'value':
            self.selected_table = change.new
            self.ok_button.disabled = False  # Enable the button once a table is selected

    def _observe_product_change(self, change):
        asyncio.ensure_future(self._product_change(change))

    def _observe_table_change(self, change):
        asyncio.ensure_future(self._table_change(change))

    def _ok_button_click(self, b):
        self.ok_button.disabled = True  # Disable the button again after it's clicked
        self.cancel_button.disabled = True  # Disable the Cancel button
        self.product_dropdown.disabled = True 
        self.table_dropdown.disabled = True 

    def _cancel_button_click(self, b):
        self.selected_product = None  # Set selected value to None
        self.selected_table = None 
        self.ok_button.disabled = True  # Disable the OK button
        self.cancel_button.disabled = True  # Disable the Cancel button
        self.product_dropdown.disabled = True 
        self.table_dropdown.disabled = True 


    async def display_widgets(self):
        display(self.product_dropdown, self.table_dropdown, self.ok_button, self.cancel_button)

        while not self.cancel_button.disabled:
        #while self.selected_product is None or self.selected_table is None or not self.ok_button.disabled:
            await asyncio.sleep(0.1)

        return self.selected_product, self.selected_table

class _SelectList:
    def __init__(self, values, col_name: str):
        self.selected_value = values[0]

        # Create the first dropdown menu
        self.list_dropdown = widgets.Dropdown(
            options=values,
            description=f"{col_name} :",
            disabled=False,
        )

        # Create the OK button and set its initial state to disabled
        self.ok_button = widgets.Button(
            description='OK',
            disabled=False,
        )

        # Create the Cancel button
        self.cancel_button = widgets.Button(
            description='Cancel',
            disabled=False,
        )

        # Observe changes in dropdown selection and button clicks
        self.list_dropdown.observe(self._observe_list_change, names='value')
        self.ok_button.on_click(self._ok_button_click)
        self.cancel_button.on_click(self._cancel_button_click)

    async def _list_change(self, change):
        if change['type'] == 'change' and change['name'] == 'value':
            self.selected_value = change.new
            self.ok_button.disabled = False  # Enable the OK button when a value is selected

    def _observe_list_change(self, change):
        asyncio.ensure_future(self._list_change(change))

    def _ok_button_click(self, b):
        self.ok_button.disabled = True  # Disable the OK button after it's clicked
        self.cancel_button.disabled = True
        self.list_dropdown.disabled = True  

    def _cancel_button_click(self, b):
        self.selected_value = None  # Set selected value to None
        self.ok_button.disabled = True  # Disable the OK button
        self.cancel_button.disabled = True  # Disable the Cancel button
        self.list_dropdown.disabled = True  

    async def display_widgets(self):
        display(self.list_dropdown, self.ok_button, self.cancel_button)

        while not self.cancel_button.disabled:
            await asyncio.sleep(0.1)

        return self.selected_value

class _SelectMultiple:
    def __init__(self, values, col_name: str):
        self.selected_list = []
        nrows = 20 if len(values) > 20 else len(values)
        # Create the multiple select widget
        self.list_select = widgets.SelectMultiple(
            options=values,
            description=f"{col_name} :",
            disabled=False,
            rows = nrows,
            layout=widgets.Layout(width='2000px')  # Adjust the width as needed
        )

        # Create the OK button and set its initial state to enabled
        self.ok_button = widgets.Button(
            description='OK',
            disabled=False,
        )

        # Create the Cancel button
        self.cancel_button = widgets.Button(
            description='Cancel',
            disabled=False,
        )

        # Observe changes in selection and button clicks
        self.list_select.observe(self._observe_list_change, names='value')
        self.ok_button.on_click(self._ok_button_click)
        self.cancel_button.on_click(self._cancel_button_click)

    async def _list_change(self, change):
        if change['type'] == 'change' and change['name'] == 'value':
            self.selected_list = list(change.new)
            self.ok_button.disabled = False  # Enable the OK button when a value is selected

    def _observe_list_change(self, change):
        asyncio.ensure_future(self._list_change(change))

    def _ok_button_click(self, b):
        self.ok_button.disabled = True  # Disable the OK button after it's clicked
        self.cancel_button.disabled = True
        self.list_select.disabled = True

    def _cancel_button_click(self, b):
        self.selected_list = None  # Set selected list to None
        self.ok_button.disabled = True  # Disable the OK button
        self.cancel_button.disabled = True  # Disable the Cancel button
        self.list_select.disabled = True

    async def display_widgets(self):
        display(self.ok_button, self.cancel_button, self.list_select)

        while not self.cancel_button.disabled:
            await asyncio.sleep(0.1)

        return self.selected_list

class _Multi_dropdown:
    def __init__(self, values, col_names, title):
        # Check if values is a list of lists or a single list
        if isinstance(values[0], list):
            self.is_list_of_lists = True
            self.values = values
        else:
            self.is_list_of_lists = False
            self.values = [values]
        
        # Check if col_names is a list and its length matches values
        if not isinstance(col_names, list):
            raise ValueError("col_names must be a list of strings.")
        if len(col_names) != len(self.values):
            raise ValueError("Length of col_names must match the number of dropdowns.")

        # Check title
        if not isinstance(title, str):
            raise ValueError("Title must be a string.")
        self.title = title

        # Initialize dropdown widgets
        self.dropdown_widgets = []
        self.selected_values = []

        # Create dropdowns based on values and col_names
        for i, sublist in enumerate(self.values):
            description = widgets.Label(value=f"{col_names[i]} :")
            dropdown = widgets.Dropdown(
                options=sublist,
                value=sublist[0],  # Set default value to the first item in the list
                disabled=False,
            )
            # Arrange description and dropdown horizontally
            hbox = widgets.HBox([description, dropdown])
            self.dropdown_widgets.append((hbox, dropdown))  # Store hbox and dropdown separately
            self.selected_values.append(dropdown.value)
            dropdown.observe(self._observe_list_change, names='value')
        
        # Create OK and Cancel buttons
        self.ok_button = widgets.Button(
            description='OK',
            disabled=False,
        )
        self.cancel_button = widgets.Button(
            description='Cancel',
            disabled=False,
        )
        
        # Observe button clicks
        self.ok_button.on_click(self._ok_button_click)
        self.cancel_button.on_click(self._cancel_button_click)
        
        # Display the widgets
        self._display_widgets()

    def _display_widgets(self):
        # Create the title label
        title_label = widgets.Label(value=self.title)
        
        # Display title, dropdowns, and buttons
        display(title_label)
        display(widgets.VBox([hbox for hbox, dropdown in self.dropdown_widgets]))
        display(self.ok_button, self.cancel_button)
    
    def _observe_list_change(self, change):
        # Find the index of the changed dropdown
        for i, (hbox, dropdown) in enumerate(self.dropdown_widgets):
            if dropdown is change.owner:
                self.selected_values[i] = change.new
                break
    
    def _ok_button_click(self, b):
        self.ok_button.disabled = True
        self.cancel_button.disabled = True
        for hbox, dropdown in self.dropdown_widgets:
            dropdown.disabled = True
 
    def _cancel_button_click(self, b):
        self.selected_values = None
        self.ok_button.disabled = True
        self.cancel_button.disabled = True

        for hbox, dropdown in self.dropdown_widgets:
            dropdown.disabled = True

def _select_list(class_type,values, col_name: str,fnc=None, n_args = None): 
    
    async def f(class_type,values,col_name, fnc, n_args):
        if class_type == 'dropdown':
            Select_obj = _SelectList(values, col_name)
        else:
            Select_obj = _SelectMultiple(values, col_name)

        selected_value = await Select_obj.display_widgets()

        if fnc and n_args:
            fnc(selected_value, *n_args) 

    asyncio.ensure_future(f(class_type,values,col_name,fnc,n_args))

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

def _select_bvd(selected_value, bvd_list,select_cols, search_type):
    if selected_value is not None: 
        bvd_list[1]  = selected_value
        bvd_list[2] = _construct_query(bvd_list[1],bvd_list[0],search_type)
        if select_cols is not None:
                select_cols = _check_list_format(select_cols,bvd_list[1])

        print(f"The following bvd query has been created: {bvd_list[2]}")

def _select_date(selected_value, time_period,select_cols):
    if selected_value is not None: 
        time_period[2]  = selected_value
        if select_cols  is not None:
                select_cols = _check_list_format(select_cols,time_period[2])
        print(f"The following Period will be selected: {time_period}")

# Dependency functions
def _create_workers(num_workers:int = -1,n_total:int=None,pool_method = None  ,query = None):

    if num_workers < 1:
            num_workers =int(psutil.virtual_memory().total/ (1024 ** 3)/12)
    
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
    file_names = None
    if dfs:
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
            num_workers = int(num_workers) 
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

# Function to read a specific chunk of the CSV file
def _read_csv_chunk(params):
    file, chunk_idx, chunk_size, select_cols, date_query, bvd_query, query, query_args = params

    try:
        if select_cols is None:
            df =  pd.read_csv(file, skiprows=chunk_idx * chunk_size + 1,nrows=chunk_size, header=None if chunk_idx != 0 else 'infer')
        else:
            df =  pd.read_csv(file, usecols = select_cols ,skiprows=chunk_idx * chunk_size + 1,nrows=chunk_size, header=None if chunk_idx != 0 else 'infer')
    except Exception as e:
            raise ValueError(f"Error while reading chunk: {e}") 

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

    if num_workers < 1:
        num_workers =int(psutil.virtual_memory().total/ (1024 ** 3)/12)

    # Step 1: Determine the total number of rows using subprocess
    num_lines = int(subprocess.check_output(f"wc -l {file}", shell=True).split()[0]) - 1

    # Step 2: Calculate the chunk size to create 64 chunks
    chunk_size = num_lines // num_workers

    # Step 3: Prepare the params_list
    params_list = [(file, i, chunk_size, select_cols, date_query, bvd_query, query, query_args) for i in range(num_workers)]

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
        #df[date_col] = pd.to_datetime(df[date_col], format='%d-%m-%Y')
    except ValueError as e:
        print(f"{e}")
        return df 
    
    date_filter = (df[date_col].dt.year >= start_year) & (df[date_col].dt.year <= end_year)

    if date_filter.any():  
        df = df.loc[date_filter]
    else:
        df = pd.DataFrame() 

    # Add back the NaN rows
    if nan_action == 'keep':
        if not nan_rows.empty:
            df = pd.concat([df, nan_rows], sort=False)
            df = df.sort_index()
            #df = pd.concat([df, nan_rows]).sort_index()
   
    return df
    
# Data Related functions
def _read_excel(file_name):
    with pkg_resources.open_binary('moodys_datahub.data', file_name) as f:
        return pd.read_excel(f)

def _table_names(file_name:str=None):
    
    if file_name is None:
        df = _read_excel('data_products.xlsx')
    else:
        if not os.path.exists(file_name):
            raise ValueError("moody's datahub data product file was not detected")
        df = pd.read_excel(file_name)

    df = df[['Data Product', 'Top-level Directory']]
    df = df.drop_duplicates()
    return df

def _table_match(tables, file_name: str = None):
    # Step 1: Determine if any table name ends with .csv
    contains_csv = any(table.endswith('.csv') for table in tables)
    
    # Step 2: If .csv is found, remove the .csv suffix from each table name for matching purposes
    if contains_csv:
        tables = [table[:-4] if table.endswith('.csv') else table for table in tables]

    # Step 3: Read the data products Excel file
    if file_name is None:
        df = _read_excel('data_products.xlsx')
    else:
        if not os.path.exists(file_name):
            raise ValueError("Moody's datahub data product file was not detected")
        df = pd.read_excel(file_name)
    
    # Step 4: Perform the matching
    result = df.groupby('Data Product')['Table'].apply(lambda x: all(table in tables for table in x.values))

    # Step 5: Get the "Data Product" values where all tables are found within the list of strings
    matched_groups = result[result].index.tolist()

    if len(matched_groups) > 0:
        
        #data_product = matched_groups[0]
        data_product = f"Mutliple_Options: {matched_groups}"
        print("It was not possible to determine the 'data product' for all exports. Run 'self.specify_data_product()' to correct")

        # Add "[.csv]" only if the original tables list contained .csv suffixes
        #if contains_csv:
        #    data_product = f"[.csv] {data_product}"
    else:
        data_product = "Unknown"

    return data_product, tables

def _table_dictionary(file_name:str=None):
    if file_name is None:
        df = _read_excel('data_dict.xlsx')
    else:
        if not os.path.exists(file_name):
            raise ValueError("moody's datahub data dictionary file was not detected")
        df = pd.read_excel(file_name)
    return df

def _country_codes(file_name:str=None):
    if file_name is None:
        df = _read_excel('country_codes.xlsx')
    else:
        if not os.path.exists(file_name):
            raise ValueError("moody's datahub country codes  file was not detected")
        df = pd.read_excel(file_name)
    return df

def _table_dates(file_name:str=None):
    if file_name is None:
        df = _read_excel('date_cols.xlsx')
    else:
        if not os.path.exists(file_name):
            raise ValueError("moody's datahub table date columns file was not detected")
        df = pd.read_excel(file_name)
    return df
 
def _letters_only_regex(text):
    """Converts the title to lowercase and removes non-alphanumeric characters."""
    if isinstance(text,str):
        return re.sub(r'[^a-zA-Z0-9]', '', text.lower())
    else:
        return text