import sys
import shutil
import importlib
import subprocess
import os
from datetime import datetime
from multiprocessing import cpu_count
import importlib.resources as pkg_resources
import copy

# Check and install required libraries
required_libraries = ['pandas','pyarrow','fastparquet','openpyxl'] 
for lib in required_libraries:
    try:
        importlib.import_module(lib)
    except ImportError:
        print(f"Installing {lib}...")
        subprocess.check_call([sys.executable, '-m', 'pip', 'install', lib])
subprocess.run(['pip', 'install', '-U', 'ipywidgets'])

import pandas as pd
import pyarrow.parquet as pq 

from .src.utils import _save_to,_letters_only_regex,fuzzy_query,_bvd_changes_ray
from .src.load_data import _table_dictionary
from .src.process import _Process

# Defining Sftp Class
class Sftp(_Process): 
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
        _Process.__init__(self)

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