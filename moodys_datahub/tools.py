import sys
import shutil
import time
import importlib
import subprocess
import os
import re
import psutil
from datetime import datetime
from multiprocessing import cpu_count, Process
import importlib.resources as pkg_resources
import copy
import ast

# Check and install required libraries
required_libraries = ['pandas', 'pysftp','pyarrow','fastparquet','openpyxl','asyncio'] 
for lib in required_libraries:
    try:
        importlib.import_module(lib)
    except ImportError:
        print(f"Installing {lib}...")
        subprocess.check_call([sys.executable, '-m', 'pip', 'install', lib])
subprocess.run(['pip', 'install', '-U', 'ipywidgets'])

import pandas as pd
import pyarrow.parquet as pq 
import pysftp
import numpy as np
import asyncio
from math import ceil

from sftp_utils import _run_parallel,_save_files,_save_chunks,_load_table,_load_csv_table,_save_to,_check_list_format,_construct_query,_letters_only_regex,fuzzy_query,_bvd_changes_ray
from sftp_widgets import _SelectData,_SelectList,_SelectMultiple,_Multi_dropdown,_SelectOptions,_CustomQuestion, _select_list,_select_bvd,_select_date,_select_product
from sftp_data import _table_dictionary,_country_codes,_table_dates
from sftp_connection import Sftp_Connection
from sftp_selection import Sftp_Selection


class Sftp_Process(Sftp_Selection):
    
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
                column, _, _ = check_bvd_format([column], df_bvd)
                if column:
                    bvd_list.extend(column)
                    bvd_list = list(set(bvd_list))

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
                return df_matches, True, non_matching_items
            else:
                non_matching_items = [item for item in bvd_list if not pattern.match(item)]
                return regex_matches, False , non_matching_items

        def set_bvd_list(bvd_list):
            df =self.search_country_codes()

            if (self._bvd_list[1] is not None and self._select_cols is not None) and self._bvd_list[1] in self._select_cols:
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

            return bvd_list,search_word,search_type,non_matching_items 

        def set_bvd_col(search_word):

            if search_word is None:
                bvd_col = self.search_dictionary()
            else:
                bvd_col = self.search_dictionary(search_word = search_word,search_cols={'Data Product':False,'Table':False,'Column':True,'Definition':False})

            if bvd_col.empty:
                raise ValueError("No 'bvd' columns were found for this table")

            bvd_col = bvd_col['Column'].unique().tolist()
            
            if len(bvd_col) > 1:
                if isinstance(search_word, str) and search_word in bvd_col:
                    self._bvd_list[1] = search_word
                else:
                    return bvd_col
            else:    
                self._bvd_list[1]  = bvd_col[0]

            return False

        async def f_bvd_prompt(bvd_list ,non_matching_items):
    
            question = _CustomQuestion(f"The following elements does not seem to match bvd format: {non_matching_items}",['keep', 'remove','cancel'])
            answer = await question.display_widgets()
            
            if answer == 'keep':
                print(f"The following bvd_id_numbers were kept:{non_matching_items}")
                self._bvd_list[0] = bvd_list + non_matching_items

            elif answer == 'cancel':
                print("Adding the bvd list has been canceled")
                return
            else:
                print(f"The following bvd_id_numbers were removed:{non_matching_items}")
                self._bvd_list[0] = bvd_list

            if self._set_data_product is None or self._set_table is None:
                self.select_data()
            
            bvd_col = set_bvd_col(search_word)
            if bvd_col:
                _select_list('_SelectMultiple',bvd_col,'Columns:','Select "bvd" Columns to filtrate',_select_bvd,[self._bvd_list,self._select_cols, search_type])
                return

            self._bvd_list[2] = _construct_query(self._bvd_list[1],self._bvd_list[0],search_type)
        
            if self._select_cols is not None:
                self._select_cols = _check_list_format(self._select_cols,self._bvd_list[1],self._time_period[2])
    
        self._bvd_list = [None,None,None]
        
        if bvd_list is not None:
            bvd_list,search_word,search_type,non_matching_items  = set_bvd_list(bvd_list)

            if len(non_matching_items) > 0:    
                asyncio.ensure_future(f_bvd_prompt(bvd_list ,non_matching_items))
                return

            self._bvd_list[0]  = bvd_list

            if self._set_data_product is None or self._set_table is None:
                self.select_data()
            
            bvd_col = set_bvd_col(search_word)
            if bvd_col:
                _select_list('_SelectMultiple',bvd_col,'Columns:','Select "bvd" Columns to filtrate',_select_bvd,[self._bvd_list,self._select_cols, search_type])
                return

            self._bvd_list[2] = _construct_query(self._bvd_list[1],self._bvd_list[0],search_type)
        
        if self._select_cols is not None:
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
            if (self._time_period[2] is not None and self._select_cols is not None) and self._time_period[2] in self._select_cols:
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
                _select_list('_SelectList',date_col,'Columns:','Select "date" Column to filtrate',_select_date,[self._time_period,self._select_cols])
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

# Defining Sftp Class
class Sftp(Sftp_Connection,Sftp_Selection,Sftp_Process): 
    """
    A class to manage SFTP connections and file operations for data transfer.
    """
    def __init__(self, hostname:str = None, username:str = None, port:int = 22, privatekey:str = None, data_product_template:str = None, local_repo:str = None):
        
        """Constructor Method
        
       Constructor Parameters:
        - `hostname` (str, optional): Hostname of the SFTP server (default is CBS SFTP server).
        - `username` (str, optional): Username for authentication (default is CBS SFTP server).
        - `port` (int, optional): Port number for the SFTP connection (default is 22).
        - `privatekey` (str, optional): Path to the private key file for authentication (required for SFTP access).
        - `data_product_template` (str, optional): Template for managing data products during SFTP operations.
        - `local_repo` (str, optional): Path to a folder containing previously downloaded data products.

        Object Attributes:
        - `connection` (pysftp.Connection or None): Represents the current SFTP connection, initially set to `None`.
        - `hostname` (str): Hostname for the SFTP server.
        - `username` (str): Username for SFTP authentication.
        - `privatekey` (str or None): Path to the private key file for SFTP authentication.
        - `port` (int): Port number for SFTP connection (default is 22).
        
        File Handling Attributes:
        - `output_format` (list of str): Supported output formats for files (default is ['.csv']).
        - `file_size_mb` (int): Maximum file size in MB before splitting files (default is 500 MB).
        - `delete_files` (bool): Flag indicating whether to delete processed files (default is `False`).
        - `concat_files` (bool): Flag indicating whether to concatenate processed files (default is `True`).
        - `query` (str, function, or None): Query string or function for filtering data (default is `None`).
        - `query_args` (list or None): List of arguments for the query string or function (default is `None`).
        - `dfs` (DataFrame or None): Stores concatenated DataFrames if concatenation is enabled.
        """


        # Initialize mixins
        Sftp_Connection.__init__(self)
        Sftp_Selection.__init__(self)

       
        self.privatekey: str = privatekey
        self.port: int = port

        # Try connecting to CBS servers
        if privatekey and all([hostname, username]) is False: 
            usernames = ["D2vdz8elTWKyuOcC2kMSnw","aN54UkFxQPCOIEtmr0FmAQ"]   
            for username in usernames:
                self.hostname: str = "s-f2112b8b980e44f9a.server.transfer.eu-west-1.amazonaws.com"
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

        _,to_delete = self.tables_available(product_overview = data_product_template)

        self._server_clean_up(to_delete)
    
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

    def define_options(self):
        """
        Asynchronously define and set file operation options using interactive widgets.

        This method allows the user to configure file operation settings such as whether to delete files after processing, 
        concatenate files, specify output format, and define the maximum file size. These options are displayed as interactive 
        widgets, and the user can select their preferred values. Once the options are selected, the instance variables 
        are updated with the new configurations.

        Workflow:
        - A dictionary `config` is initialized with the current values of key file operation settings (`delete_files`, 
        `concat_files`, `output_format`, `file_size_mb`).
        - An instance of `_SelectOptions` is created with this configuration, displaying the interactive widgets.
        - The user's selections are awaited asynchronously using the `await` keyword, ensuring non-blocking behavior.
        - Once the user makes selections, the corresponding instance variables (`delete_files`, `concat_files`, `output_format`, 
        `file_size_mb`) are updated with the new values.
        - A summary of the selected options is printed for confirmation.

        Internal Async Function (`f`):
        - The internal function `f` manages the asynchronous behavior, ensuring that the user can interact with the widget 
        without blocking the main thread.
        - After the user selects the options, the configuration is validated and applied to the class attributes.
        
        Notes:
        - This method uses `asyncio.ensure_future` to execute the async function `f` concurrently, without blocking other tasks.
        - The `config` dictionary is updated with the new options chosen by the user.
        - If no changes are made by the user, the original configuration remains.

        Example:
            ```python
            # Launch the options configuration process
            self.define_options()
            ```

        Expected Outputs:
        - Updates the instance variables based on user input:
            - `self.delete_files`: Whether to delete files after processing.
            - `self.concat_files`: Whether to concatenate files.
            - `self.output_format`: The output format of processed files (e.g., `.csv`, `.parquet`).
            - `self.file_size_mb`: Maximum file size (in MB) before splitting output files.
        
        - Prints the selected options:
            - Delete Files: True/False
            - Concatenate Files: True/False
            - Output Format: List of formats (e.g., `['.csv']`)
            - Output File Size: File size in MB (e.g., `500 MB`)

        Raises:
        - `ValueError`: If the selected options are invalid or conflict with other settings.

        Example Output:
            The following options were selected:
            Delete Files: True
            Concatenate Files: False
            Output Format: ['.csv']
            Output File Size: 500 MB
        """
        async def f(self):
            if self.output_format is None:
                self.output_format = ['.csv'] 

            config = {
            'delete_files': self.delete_files,
            'concat_files': self.concat_files,
            'output_format': self.output_format,
            'file_size_mb': self.file_size_mb}

            Options_obj = _SelectOptions(config)

            config = await Options_obj.display_widgets()

            if config:
                self.delete_files = config['delete_files']
                self.concat_files = config['concat_files']
                self.file_size_mb = config['file_size_mb']
                self.output_format = config['output_format']

                if len(self.output_format) == 1 and self.output_format[0] is None:
                    self.output_format  = None
                elif len(self.output_format) > 1:
                    self.output_format  = [x for x in self.output_format  if x is not None]


                print("The following options were selected:")
                print(f"Delete Files: {self.delete_files}")
                print(f"Concatenate Files: {self.output_format}")
                print(f"Output File Size: {self.file_size_mb } MB")
        
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
            
            Select_obj = _SelectMultiple(combined,'Columns:',"Select Table Columns")
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
        Create a deep copy of the current instance and initialize its defaults.

        This method creates a deep copy of the instance, calls the 
        `_object_defaults()` method to set default values, and then 
        invokes the `select_data()` method to prepare the copied object 
        for use.
        """
        SFTP = copy.deepcopy(self)
        SFTP._object_defaults()
        SFTP.select_data()
   
        return SFTP

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
    
    def orbis_to_moodys(self,file):

        """
        Match headings from an Orbis output file to headings in Moody's DataHub.

        This method reads headings from an Orbis output file and matches them to  headings 
        in Moody's DataHub. The function returns a DataFrame with matched headings and a list of headings 
        not found in Moody's DataHub.

        Input Variables:
        - `file` (str): Path to the Orbis output file.

        Returns:
        - tuple: A tuple where:
        - The first element is a DataFrame containing matched headings.
        - The second element is a list of headings that were not found.

        Notes:
        - Headings from the Orbis file are processed to remove any extra lines and to ensure uniqueness.
        - The DataFrame is sorted based on the number of unique headings for each 'Data Product'.
        - If no headings are found, an empty DataFrame is returned.

        Example:
            matched_df, not_found_list = self.orbis_to_moodys('orbis_output.xlsx')
        """

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
        Retrieve a sample of data from a table and optionally save it to a file.

        This method retrieves a sample of data from a specified table or file. If `files` is not provided, it uses
        the default file from `self.remote_files`. It processes the files, retrieves the specified number of rows, 
        and saves the result to the specified format if `save_to` is provided.

        Input Variables:
        - `save_to` (str, optional): Format to save the sample data (default is 'CSV'). Other formats may be supported based on implementation.
        - `files` (list, optional): List of files to process. Defaults to `self.remote_files`. If an integer is provided, it is treated as a file identifier.
        - `n_rows` (int, optional): Number of rows to retrieve from the data (default is 1000).

        Returns:
        - Pandas DataFrame: DataFrame containing the sample of the processed data.

        Notes:
        - If `files` is None, the method will use the first file in `self.remote_files` and may trigger `select_data` if data product or table is not set.
        - The method processes all specified files or retrieves data from them if needed.
        - Results are saved to the file format specified by `save_to` if provided.

        Example:
            df = self.process_one(save_to='parquet', n_rows=500)
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
            if n_rows > 0:
                df = df.head(n_rows)

            _save_to(df,'process_one',save_to) 
        elif not df.empty and files is not None:
            if n_rows > 0:
                df = df.head(n_rows)
            print(f"Results have been saved to '{files}'")
        elif df.empty:  
            print("No rows were retained")  
        return df
    
    def process_all(self, files:list = None,destination:str = None, num_workers:int = -1, select_cols: list = None , date_query = None, bvd_query = None, query = None, query_args:list = None,pool_method = None):
        """
        Read and process multiple files into DataFrames with optional filtering and parallel processing.

        This method reads multiple files into Pandas DataFrames, with options for selecting specific columns, 
        applying filters, and performing parallel processing. It can handle file processing sequentially or 
        in parallel, depending on the number of workers specified.

        Input Variables:
        - `files` (list, optional): List of files to process. Defaults to `self.remote_files`.
        - `destination` (str, optional): Path to save processed files.
        - `num_workers` (int, optional): Number of workers for parallel processing. Default is -1 (auto-determined).
        - `select_cols` (list, optional): Columns to select from files. Defaults to `self._select_cols`.
        - `date_query`: (optional): Date query for filtering data. Defaults to `self.time_period`.
        - `bvd_query`: (optional): BVD query for filtering data. Defaults to `self._bvd_list[2]`.
        - `query` (str, optional): Additional query for filtering data.
        - `query_args` (list, optional): Arguments for the query.
        - `pool_method` (optional): Method for parallel processing (e.g., 'fork', 'threading').

        Returns:
        - `dfs`: List of Pandas DataFrames with selected columns and filtered data.
        - `file_names`: List of file names processed.

        Notes:
        - If `files` is `None`, the method will use `self.remote_files`.
        - If `num_workers` is less than 1, it will be set automatically based on available system memory.
        - Uses parallel processing if `num_workers` is greater than 1; otherwise, processes files sequentially.
        - Handles file concatenation and deletion based on instance attributes (`concat_files`, `delete_files`).
        - If `self.delete_files` is `True`, the method will print the current working directory.

        Raises:
        - `ValueError`: If validation of arguments (`files`, `destination`, etc.) fails.

        Example:
            dfs, file_names = self.process_all(files=['file1.csv', 'file2.csv'], destination='/processed', num_workers=4,
                                            select_cols=['col1', 'col2'], date_query='2023-01-01', query='col1 > 0')
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
            flag =  any([select_cols, query, all(date_query),bvd_query]) and self.output_format
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
        """
        Initiates downloading of all remote files using parallel processing.

        This method starts a new process to download files based on the selected data product and table. 
        It uses parallel processing if `num_workers` is specified, defaulting to `cpu_count() - 2` if not. 
        The process is managed using the `fork` method, which is supported only on Unix systems.

        Input Variables:
        - `num_workers` (int, optional): Number of workers for parallel processing. Defaults to `cpu_count() - 2`.

        Notes:
        - This method requires the `fork` method for parallel processing, which is supported only on Unix systems.
        - If `self._set_data_product` or `self._set_table` is `None`, the method will call `select_data()` to initialize them.
        - Sets `self.delete_files` to `False` before starting the download process.
        - Starts a new process using `multiprocessing.Process` to run the `process_all` method for downloading.
        - Sets `self._download_finished` to `False` when starting the process, indicating that the download is in progress.

        Example:
            self.download_all(num_workers=4)
        """
        
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

        self._download_finished = None 
        self.delete_files = False

        print("Downloading all files")
        process = Process(target=self.process_all, kwargs={'num_workers': num_workers, 'pool_method': pool_method})
        process.start()

        self._download_finished = False 

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
     
    def search_company_names(self,names:list, num_workers:int = -1,cut_off: int = 90.1, company_suffixes: list = None):

        """
        Search for company names and find the best matches based on a fuzzy query.

        This method performs a search for company names using fuzzy matching 
        techniques, leveraging concurrent processing to improve performance. 
        It processes the provided names against a dataset of firmographic data 
        and returns the best matches based on the specified cut-off score.

        Parameters:
        names : list
            A list of company names to search for.
            
        num_workers : int, optional
            The number of worker processes to use for concurrent operations.
            If set to -1 (default), it will use the maximum available workers
            minus two to avoid overloading the system.

        cut_off : float, optional
            The cut-off score for considering a match as valid. Default is 90.1.

        company_suffixes : list, optional
            A list of valid company suffixes to consider when searching for names. 


        Returns:
        pandas.DataFrame
            A DataFrame containing the best matches for the searched company names,
            including associated scores and other relevant information.

        Examples:
        results = obj.search_company_names(['Example Inc', 'Sample Ltd'], num_workers=4)
         """
    
        # Determine the number of workers if not specified
        if not num_workers or num_workers < 0:
            num_workers = max(1, cpu_count() - 2)

        SFTP = copy.deepcopy(self)
        SFTP._object_defaults()

        SFTP.set_data_product = "Firmographics (Monthly)"
        SFTP.set_table = "bvd_id_and_name"
        SFTP._select_cols = ['bvd_id_number', 'name']
        SFTP.output_format = None
        SFTP.query = fuzzy_query
        SFTP.query_args = [names,'name','bvd_id_number',cut_off,company_suffixes,1]
        df,_ = SFTP.process_all(num_workers=num_workers)

        # Finder de bedste matches på tværs af "file parts"
        max_scores = df.groupby('Search_string', as_index=False)['Score'].max()
        best_matches = pd.merge(df, max_scores, on=['Search_string', 'Score'])

        # Keep only unique rows
        best_matches = best_matches.drop_duplicates()
        best_matches.reset_index(drop=True)

        # save to csv
        current_time = datetime.now()
        timestamp_str = current_time.strftime("%y%m%d%H%M")
        best_matches.to_csv(f"{timestamp_str}_company_name_search.csv")

        return best_matches

    def batch_bvd_search(self,products:str = "products.xlsx",bvd_numbers:str = "bvd_numbers.txt"):

        def check_file_exists(base_name,extension = '.csv' ,max_attempts=100):
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
                with pkg_resources.open_binary('moodys_datahub.data', 'products.xlsx') as src, open('products.xlsx', 'wb') as target_file:
                    shutil.copyfileobj(src, target_file)
                    files.append(target_file)
            if not os.path.exists(bvd_numbers):
                with pkg_resources.open_binary('moodys_datahub.data', 'bvd_numbers.txt') as src, open('bvd_numbers.txt', 'wb') as target_file:
                    shutil.copyfileobj(src, target_file)
                    files.append(target_file)
            print(f"The following input templates have been create: {files}. Please fill out and re-run the function")
            return
            
        df = pd.read_excel(products)
            # Convert columns A, B, and C to lists
        data_products = df['Data Product'].tolist()
        tables = df['Table'].tolist()
        columns = df['Column'].tolist()
        to_runs = df['Run'].tolist()

        df = pd.read_csv(bvd_numbers, header=None)
        bvd_numbers = df[0].tolist()

        SFTP = copy.deepcopy(self)
        SFTP._object_defaults()

        in_complete = []
        # Loop through both lists together
        n = 0
        for data_product, table,column,to_run in zip(data_products, tables, columns,to_runs):
            if to_run:
                n = n+1
                file_name = f"{n}_{data_product}_{table}"
                
                existing_file = check_file_exists(file_name)
                
                if existing_file:
                    continue 
                
                print(f"{n} : {data_product} : {table}")
                SFTP.set_data_product = data_product
                SFTP.set_table = table
                
                if SFTP._set_table is not None:
                    available_cols = SFTP.get_column_names()
                    if isinstance(column, str) and ',' in column:  # Check if it's a string and contains a comma  
                        column = [word.strip() for word in column.split(',')]
                        SFTP.query = ' | '.join(f"{col} in {bvd_numbers}" for col in column)
                    else:
                        SFTP.query=f"{column} in {bvd_numbers}"
                        column = [column]

                    if all(col in available_cols for col in column):
                        SFTP.process_all(destination = file_name)
                    else:
                        in_complete.append([n,data_product,table,available_cols])
                else:
                    in_complete.append([n,data_product,table,'Not found'])

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
                "oü"
            ]
            return company_suffixes 

    def search_bvd_changes(self,bvd_list:list, num_workers:int = -1):
        """
        Search for changes in BvD IDs based on the provided list.

        This method retrieves changes in BvD IDs by processing the provided 
        list of BvD IDs. It utilizes concurrent processing for efficiency 
        and returns the new IDs, the newest IDs, and a filtered DataFrame 
        containing relevant change information.

        Parameters:
        bvd_list : list
            A list of BvD IDs to check for changes.

        num_workers : int, optional
            The number of worker processes to use for concurrent operations. 
            If set to -1 (default), it will use the maximum available workers 
            minus two to avoid overloading the system.

        Returns:
        tuple
            A tuple containing:
                - new_ids: A list of newly identified BvD IDs.
                - newest_ids: A list of the most recent BvD IDs.
                - filtered_df: A DataFrame with relevant change information.

        Examples:
        new_ids, newest_ids, changes_df = obj.search_bvd_changes(['BVD123', 'BVD456'])
        """

             # Determine the number of workers if not specified
        if not num_workers or num_workers < 0:
            num_workers = max(1, cpu_count() - 2)
        
        SFTP = copy.deepcopy(self)
        SFTP._object_defaults()

        SFTP.set_data_product = "BvD ID Changes"
        SFTP.set_table = "bvd_id_changes_full"
        SFTP._select_cols = ['old_id', 'new_id', 'change_date']
        SFTP.output_format = None
        df,_ = SFTP.process_all(num_workers=num_workers)

        new_ids, newest_ids,filtered_df = _bvd_changes_ray(bvd_list, df,num_workers)
 
        return  new_ids, newest_ids,filtered_df

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
                #file = os.path.normpath(os.path.join(self._local_path,os.path.basename(file)))
                flag = False
                if not file.startswith(base_path):
                    file = base_path + "/" + file
                    #file  = os.path.normpath(os.path.join(base_path,file))

            if len(file) > self._max_path_length:
                raise ValueError(f"file path is longer ({len(file)}) than the max path length of the OS {self._max_path_length}. Set '.local_path' closer to root. The file is : '{file}'")     

            return file, flag

        local_file,flag = _file_exist(file) 

        if not os.path.exists(local_file):
            try:
                with self._connect() as sftp: 
                    remote_file =  self.remote_path + "/" + os.path.basename(file)
                    #remote_file = os.path.normpath(os.path.join(self.remote_path,os.path.basename(file)))
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
                #file_name, _ = os.path.splitext(os.path.normpath(os.path.join(destination,file)))
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
                        self.local_path = "Data Products" + "/" + self.set_data_product +'_exported '+ format_timestamp(self._time_stamp) + "/" + self.set_table
                        #self.local_path = os.path.normpath(os.path.join("Data Products",(self.set_data_product +'_exported '+ format_timestamp(self._time_stamp)),self.set_table))
                    else:
                        self.local_path = "Data Products" + "/" + self.set_data_product + "/" + self.set_table
                        #self.local_path = os.path.normpath(os.path.join("Data Products",self.set_data_product,self.set_table))
                    
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
            #destination = os.path.normpath(os.path.join(base_path,destination))
        
            
        if self.concat_files is False and destination is not None:
            if not os.path.exists(destination):
                os.makedirs(destination)
        elif self.concat_files is True and destination is not None:
            parent_directory = os.path.dirname(destination)

            if parent_directory and not os.path.exists(parent_directory):
                os.makedirs(parent_directory)

        return files, destination   

    def _table_search(self, search_word):
     
        filtered_df = self._tables_available.query(f"`Data Product`.str.contains('{search_word}', case=False, na=False,regex=False) | `Table`.str.contains('{search_word}', case=False, na=False,regex=False)")
        return filtered_df

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

