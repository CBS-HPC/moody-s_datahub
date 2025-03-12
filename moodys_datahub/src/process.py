import sys
import time
import importlib
import subprocess
import os
import re
import psutil
from datetime import datetime
from multiprocessing import cpu_count, Process

# Check and install required libraries
required_libraries = ['pandas','fastparquet','openpyxl','asyncio'] 
for lib in required_libraries:
    try:
        importlib.import_module(lib)
    except ImportError:
        print(f"Installing {lib}...")
        subprocess.check_call([sys.executable, '-m', 'pip', 'install', lib])
subprocess.run(['pip', 'install', '-U', 'ipywidgets'])

import pandas as pd
import numpy as np
import asyncio
from math import ceil

sys.path.append('src')
from .utils import _run_parallel,_save_files_pd,_save_chunks,_load_pd,_load_csv_table,_save_to,_check_list_format,_construct_query,_letters_only_regex,_load_pl,_save_files_pl
from .widgets import _SelectMultiple,_SelectOptions,_CustomQuestion, _select_list,_select_bvd,_select_date
from .load_data import _table_dictionary,_country_codes,_table_dates
from .selection import _Selection

class _Process(_Selection):
    
    def __init__(self):
        # Initialize mixins
        _Selection.__init__(self)

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

    def search_dictionary(self, save_to:str=False, search_word = None, search_cols={'Data Product':True,'Table':True,'Column':True,'Definition':True}, letters_only:bool=False, extact_match:bool=False, data_product = None, table = None):
    
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

    def table_dates(self, save_to:str=False, data_product = None, table = None):
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
    
    def search_country_codes(self, search_word = None, search_cols={'Country':True,'Code':True}):        
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

    def process_one(self, save_to=False, files = None, n_rows:int=1000):
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
                df  = _load_pd(file)
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
    
    def process_all(self, files:list = None, destination:str = None, num_workers:int = -1, n_batches:int = None, select_cols: list = None, date_query = None, bvd_query = None, query = None, query_args:list = None, pool_method = None):
        """
        Read and process multiple files into DataFrames with optional filtering and parallel processing.

        This method reads multiple files into Pandas DataFrames, with options for selecting specific columns, 
        applying filters, and performing parallel processing. It can handle file processing sequentially or 
        in parallel, depending on the number of workers specified.

        Input Variables:
        - `files` (list, optional): List of files to process. Defaults to `self.remote_files`.
        - `destination` (str, optional): Path to save processed files.
        - `num_workers` (int, optional): Number of workers for parallel processing. Default is -1 (auto-determined).
        - `n_batches` (int, optional): Number of batches of files being process in parallel. Default is None (batch size will be equal to num_workers).
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
        def batch_processing(n_batches:int = None):
            
            def batch_list(input_list, batch_size):
                """Splits the input list into batches of a given size."""
                batches = []
                for i in range(0, len(input_list), batch_size):
                    batches.append(input_list[i:i + batch_size])
                return batches

            if n_batches is not None and isinstance(n_batches, (int, float)):
                batch_size = len(files) // n_batches
                batches = batch_list(files,batch_size)
            else:
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

        files = files or self.remote_files
        date_query = date_query or self.time_period
        bvd_query = bvd_query or self._bvd_list[2]
        query = query or self.query
        query_args = query_args or self.query_args
        select_cols = select_cols or self._select_cols
        
        # To handle executing when download_all() have not finished!
        if not self._check_download(files):
            return None, None

        select_cols,files, destination, flag = self._validate_args(files = files, destination = destination, select_cols = select_cols, date_query = date_query, bvd_query = bvd_query, query = query)
        if not flag:
            return None
    
        # Set num_workers
        num_workers = set_workers(num_workers,int(psutil.virtual_memory().total/ (1024 ** 3)/12))

        # Read multithreaded
        if num_workers != 1 and len(files) > 1:
            dfs, file_names, flags = batch_processing(n_batches)
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
  
    def polars_all(self, files:list = None,destination:str = None, num_workers:int = -1, select_cols: list = None , date_query = None, bvd_query = None, query = None, query_args:list = None):
        """
        Read and process multiple files into DataFrames with optional filtering and parallel processing.

        This method reads multiple files into Pandas DataFrames, with options for selecting specific columns, 
        applying filters, and performing parallel processing. It can handle file processing sequentially or 
        in parallel, depending on the number of workers specified.

        Input Variables:
        - `files` (list, optional): List of files to process. Defaults to `self.remote_files`.
        - `destination` (str, optional): Path to save processed files.
        - `num_workers` (int, optional): Number of workers for parallel processing. Default is -1 (auto-determined).
        - `n_batches` (int, optional): Number of batches of files being process in parallel. Default is None (batch size will be equal to num_workers).
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
        bvd_query = bvd_query or [self._bvd_list[0],self._bvd_list[1]]
        query = query or self.query
        query_args = query_args or self.query_args
        select_cols = select_cols or self._select_cols
        
        self.concat_files = True

        # To handle executing when download_all() have not finished!
        if not self._check_download(files):
            return None, None
        
        select_cols,files, destination, flag = self._validate_args(files = files, destination = destination, select_cols = select_cols, date_query = date_query, bvd_query = bvd_query, query = query)
        if not flag:
            return None

        print(f'Processing  {len(files)} files using polars')
        dfs = self._process_polars(files, destination, select_cols, date_query, bvd_query, query, query_args)
        
        # Set num_workers  
        num_workers = set_workers(num_workers,int(cpu_count() - 2))

        # Concatenate and save
        self.dfs, file_names = _save_chunks(dfs=dfs,file_name=destination,output_format=self.output_format,file_size=self.file_size_mb, num_workers=num_workers)

        return self.dfs, file_names
    
    def download_all(self,files:list = None, num_workers:int = None, async_mode:bool=True):
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
        
        files = files or self.remote_files

        if hasattr(os, 'fork'):
            pool_method = 'fork'
        else:
            print("Function only works on Unix systems right now")
            return
         
        if self._set_data_product is None or self._set_table is None:
            self.select_data()
        
        # Set num_workers  
        num_workers = set_workers(num_workers,int(cpu_count() - 2))

        _, _ = self._check_args(files)

        self._download_finished = None 
        self.delete_files = False
        
        missing_files = [file for file in files if not os.path.exists(self._file_exist(file)[0])]
       

        if missing_files:
            print(f"Downloading {len(missing_files)} of {len(files)} files ")

            current_value = self.concat_files

            self.concat_files = False
            if async_mode:
                #process = Process(target=self.process_all, kwargs={'files':missing_files,'num_workers': num_workers, 'pool_method': pool_method,'n_batches':1})
                process = Process(target=_run_parallel, kwargs={'fnc':self._get_file,'params_list':[(file) for file in missing_files],'n_total':len(missing_files),'num_workers':num_workers,'pool_method':pool_method ,'msg':'Downloading'})
                process.start()
                self.concat_files = current_value 
                self._download_finished = False
            else:
                #self.process_all(files = missing_files, num_workers = num_workers, pool_method = pool_method, n_batches =  1,select_cols=None)

                _run_parallel(fnc=self._get_file,params_list=[(file) for file in missing_files],n_total=len(missing_files),num_workers=num_workers,pool_method=pool_method ,msg='Downloading')

            self.concat_files = current_value
        else: 
            print("Are files are already downloaded")
    
    def _file_exist(self,file:str):
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

    def _get_file(self,file:str):
        local_file,flag = self._file_exist(file) 

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

    def _curate_file(self,flag:bool,destination:str,local_file:str,select_cols:list, date_query:list=[None,None,None,"remove"], bvd_query:str = None, query = None, query_args:list = None,num_workers:int = -1):
        df = None 
        file_name = None
        if any([select_cols, query, all(date_query),bvd_query]) or flag:  
            file_extension = local_file.lower().split('.')[-1]
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
                df = _load_pd(file = local_file, 
                                    select_cols = select_cols, 
                                    date_query = date_query, 
                                    bvd_query = bvd_query, 
                                    query = query, 
                                    query_args = query_args
                                    )

            if (df is not None and self.concat_files is False and self.output_format is not None) and not flag:
  
                file_name, _ = os.path.splitext(destination + "/" + os.path.basename(local_file))
                file_name = _save_files_pd(df,file_name,self.output_format)
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
                df, file_name = self._curate_file(flag = flag,
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
    
    def _process_polars(self, files:list=None, destination:str=None, select_cols:list = None, date_query:list=[None,None,None,"remove"], bvd_query:str = None, query = None, query_args:list = None,num_workers:int = -1):
        
        files = files or self.remote_files

        self.download_all(files = files, num_workers=num_workers, async_mode=False)
    
        print("### Start processing files with polars")

        local_files = [self._file_exist(file)[0] for file in files if os.path.exists(self._file_exist(file)[0])]

        df = _load_pl(file_list=local_files,
                    select_cols = select_cols,
                    date_query = date_query,
                    bvd_query = bvd_query,
                    query = query,
                    query_args = query_args
                    )

        return df

    def _process_parallel(self, inputs_args:list):      
        file, destination, select_cols, date_query, bvd_query, query, query_args = inputs_args
        local_file, flag = self._get_file(file)
        df, file_name = self._curate_file(flag = flag,
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

    def _check_download(self,files):
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
                    return False
                
            self._download_finished =True 
        return True

    def _validate_args(self, files:list = None, destination:str = None, select_cols: list = None, date_query = None, bvd_query = None, query = None):
        
        if select_cols is not None:
            select_cols = _check_list_format(select_cols,self._bvd_list[1],self._time_period[2])
        
        try:
            flag =  any([select_cols, query, all(date_query),bvd_query]) and self.output_format
            files, destination = self._check_args(files,destination,flag)
            return select_cols, files, destination, True
        except ValueError as e:
            print(e)
            return select_cols, None, None, False

def set_workers(num_workers,default_value:int):

    if isinstance(num_workers, (int, float, complex))and num_workers != 1:
        num_workers= int(num_workers)
    else: 
        num_workers = -1
    
    if num_workers < 1:
        num_workers= default_value

    return num_workers