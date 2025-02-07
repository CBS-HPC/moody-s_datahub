import sys
import time
import json
import importlib
import subprocess
import os
import re
from datetime import datetime
from multiprocessing import  cpu_count
import ast

# Check and install required libraries
required_libraries = ['pandas', 'pysftp','fastparquet','openpyxl','asyncio'] 
for lib in required_libraries:
    try:
        importlib.import_module(lib)
    except ImportError:
        print(f"Installing {lib}...")
        subprocess.check_call([sys.executable, '-m', 'pip', 'install', lib])
subprocess.run(['pip', 'install', '-U', 'ipywidgets'])

import pandas as pd
import pysftp
import asyncio
from math import ceil
from sftp_utils import _run_parallel,_save_to,_check_list_format
from sftp_widgets import _CustomQuestion,_Multi_dropdown
from sftp_data import _table_names,_table_match

# Defining Sftp Class

class Sftp_Connection:
    def __init__(self):
        self.connection: object = None
        self.privatekey: str = None
        self.port: int = None
        self._cnopts = pysftp.CnOpts()
        self._cnopts.hostkeys = None
        self.hostname: str = None
        self.username: str = None
        self._local_repo: str = None

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
        
        self._tables_available = None
        self._tables_backup = None
        self._table_dictionary = None
        self._table_dates = None
        self.output_format: list =  ['.csv'] 
        self.file_size_mb:int = 500
        self.delete_files: bool = False
        self.concat_files: bool = True

    def tables_available(self,product_overview = None,save_to:str=False,reset:bool=False):
        """
        Retrieve and optionally save available SFTP data products and tables.

        This method fetches the available data products and tables from the SFTP server. It can optionally 
        save the results to a file in the specified format and reset the data if needed.

        Input Variables:
        - `product_overview` (str, optional): Overview of the data products to filter. Defaults to None.
        - `save_to` (str, optional): Format to save the results (e.g., 'csv', 'xlsx'). Defaults to 'csv'.
        - `reset` (bool, optional): Flag to force refresh and reset the data products and tables. Defaults to False.

        Returns:
        - Pandas DataFrame: DataFrame containing the available SFTP data products and tables.

        Notes:
        - If `reset` is `True`, the method will reset `_tables_available` to `_tables_backup`.
        - Old exports may be deleted from the SFTP server based on conditions (e.g., large CPU count).
        - The results are saved using the `_save_to` function.

        Example:
            df = self.tables_available(product_overview='overview.xlsx', save_to='csv', reset=True)
        """
        to_delete = []
        if self._tables_available is None and self._tables_backup is None:
            self._tables_available,to_delete = self._table_overview(product_overview = product_overview)
            self._tables_backup = self._tables_available.copy()

        elif reset:
            self._tables_available = self._tables_backup.copy()
      
        
        # Specify unknown data product exports
        self._specify_data_products()

        _save_to(self._tables_available,'tables_available',save_to)
   
        return self._tables_available.copy(), to_delete
    
    def _connect(self):
        """
        Establish an SFTP connection.

        Input Variables:
        - `self`: Implicit reference to the instance.

        Returns:
        - SFTP connection object.
        """
        sftp = pysftp.Connection(host=self.hostname , username=self.username ,port = self.port ,private_key=self.privatekey, cnopts=self._cnopts)
        return sftp

    def _table_overview(self,product_overview = None):

        def check_sftp(product_overview,sftp):
            product_paths = sftp.listdir()
            newest_exports = []
            time_stamp = []
            data_products = []
            to_delete = []
    
            for product_path in product_paths:
                sel_product = product_overview.loc[product_overview['Top-level Directory'] == product_path, 'Data Product']
                if len(sel_product) == 0:
                    data_products.append(None)
                else:    
                    data_products.append(sel_product.values[0])
                
                tnfs_folder = product_path + "/" + "tnfs"
                #tnfs_folder = os.path.normpath(os.path.join(product_path ,"tnfs"))

                export_paths =  sftp.listdir(product_path)

                if not sftp.exists(tnfs_folder):
                    path = product_path + "/" + export_paths[0]
                    #path = os.path.normpath(os.path.join(product_path ,export_paths[0]))
                    newest_exports.append(path)
                    time_stamp.append(None)
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
                    tnfs_file_path  = tnfs_folder + "/" + tnfs_file
                    #tnfs_file_path = os.path.normpath(os.path.join(tnfs_folder,tnfs_file))
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
                        local_file = self._local_path + "/" +  "temp.tnf"
                        #local_file = os.path.normpath(os.path.join(self._local_path,"temp.tnf"))
                    else: 
                        local_file = "temp.tnf"
                    sftp.get(newest_tnfs_file,  local_file)

                    # Read the contents of the newest .tnfs file
                    with open( local_file , 'r') as f:
                        tnfs_data = json.load(f)
                        newest_export = product_path + "/" + tnfs_data.get('DataFolder')
                        #newest_export = os.path.normpath(os.path.join(product_path,tnfs_data.get('DataFolder')))
                        newest_exports.append(newest_export)
                
                    os.remove(local_file)

                    for export_path in export_paths:
                        export_path = product_path + "/" +  export_path 
                        #export_path = os.path.normpath(os.path.join(product_path,export_path))
                        if export_path != newest_export and export_path != tnfs_folder:
                            to_delete.append(export_path)

            # Create a DataFrame from the lists
            df = pd.DataFrame({'Data Product': data_products,'Top-level Directory': product_paths,'Newest Export': newest_exports,'Timestamp': time_stamp})

            return df, to_delete

        def check_local(local_path: str = None):
            product_paths = os.listdir(local_path)
            newest_exports = []
            time_stamps = []
            data_products = []
            for product_path in product_paths:

                newest_exports.append(local_path + "/" + product_path) # FIX ME
                #newest_exports.append(os.path.normpath(os.path.join(local_path ,product_path)))
               
                filename = os.path.basename(product_path)  # Extract the last part of the path
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
                    timestamp = timestamp.strftime('%Y-%m-%d %H:%M:%S')
                except ValueError:
                    timestamp = None  # Handle cases where format doesn't match
                time_stamps.append(timestamp)

            # Create a DataFrame from the lists
            df = pd.DataFrame({'Data Product': data_products,'Top-level Directory': product_paths,'Newest Export': newest_exports,'Timestamp': time_stamps})

            return df, []
        
        def compile_table(df,sftp = None):
            data = []

            for _ , row  in df.iterrows():                
                data_product = row['Data Product']
                timestamp = row['Timestamp']
                main_directory = row['Top-level Directory']
                export = row['Newest Export']
                
                if sftp:
                    tables = sftp.listdir(export)
                    full_paths  = [export + "/" + table for table in tables]
                    #full_paths  = [os.path.normpath(os.path.join(export,table)) for table in tables]

                else:
                    tables = os.listdir(export)
                    full_paths  = [os.path.normpath(os.path.join(export ,table)) for table in tables]

                full_paths = [os.path.dirname(full_path) if full_path.endswith('.csv') else full_path for full_path in full_paths]
                tables = [table[:-4] if table.endswith(".csv") else table for table in tables ]
                            
                if pd.isna(data_product):
                    data_product, tables = _table_match(tables)

                # Append a dictionary with export, timestamp, and modified_list to the list
                for full_path, table in zip(full_paths,tables):

                    data.append({'Data Product':data_product,
                                'Table': table,
                                'Base Directory': full_path,
                                'Timestamp': timestamp,
                                'Export': export,
                                'Top-level Directory':main_directory})
            
            # Create a DataFrame from the list of dictionaries
            df = pd.DataFrame(data)

            return df
 
        if not self._local_repo:
            print('Retrieving Data Product overview from SFTP..wait a moment')
            product_overview =_table_names(file_name = product_overview) 
            with self._connect() as sftp:
                df, to_delete = check_sftp(product_overview,sftp) 
                df = compile_table(df,sftp)              
        else:
            print(f'Retrieving Data Product overview from {self._local_repo}')
            df, to_delete = check_local(self._local_repo)

            df = compile_table(df) 
  
        return  df, to_delete

    def _server_clean_up(self,to_delete):

        async def f(self,to_delete):
                question = _CustomQuestion("Please help maintain the server by deleting old exports? It may take a few minutes",['ok', 'no'])
                result = await question.display_widgets()
            
                if result == 'ok':
                    print("------------------  DELETING OLD EXPORTS FROM SFTP")
                    self._remove_exports(to_delete)

        if to_delete is None:
            _, to_delete = self._table_overview()

        to_delete = _check_list_format(to_delete)

        if len(to_delete) == 0:
            return 
        elif self.hostname == "s-f2112b8b980e44f9a.server.transfer.eu-west-1.amazonaws.com" and self.username in ["D2vdz8elTWKyuOcC2kMSnw","aN54UkFxQPCOIEtmr0FmAQ"] and int(cpu_count()) >= 32:
            asyncio.ensure_future(f(self,to_delete))

    def _remove_exports(self,to_delete = None,num_workers = None):

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

        if isinstance(num_workers, (int, float, complex)) and num_workers != 1:
            num_workers = int(num_workers)
        
        # Batch files to delete
        batches = batch_list(file_paths,num_workers)

        # Deleting files
        _run_parallel(fnc=self._delete_files,params_list=batches,n_total=len(batches),msg = 'Deleting files')

        # Deleting empty folders
        _run_parallel(fnc=self._delete_folders,params_list=to_delete,n_total=len(to_delete),msg='Deleting folders')
   
    def _recursive_collect(self, path,extensions: tuple = (".parquet", ".csv",".orc",".avro")):
            file_paths = []
            with self._connect() as sftp:
                for file_attr in sftp.listdir_attr(path):
                    full_path = path + "/" + file_attr.filename
                    #full_path = os.path.normpath(os.path.join(path,file_attr.filename))

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
            with self._connect() as sftp:
                for file in files:
                   sftp.remove(file)  

    def _delete_folders(self,folder_path:str=None):
        
        def recursive_delete(sftp, path):
            for file_attr in sftp.listdir_attr(path):
                full_path = path + "/" + file_attr.filename
                #full_path = os.path.normpath(os.path.join(path,file_attr.filename))
                sftp.remove(full_path)   

        def recursive_delete_not_used(sftp, path,extensions: tuple = (".parquet", ".csv",".orc",".avro")):
            for file_attr in sftp.listdir_attr(path):
                full_path = path + "/" + file_attr.filename
                #full_path = os.path.normpath(os.path.join(path,file_attr.filename))

                # Check if the file ends with any of the specified extensions
                if full_path.endswith(extensions):
                    sftp.remove(full_path)
                elif sftp.isdir(full_path):
                    recursive_delete(sftp, full_path)
                else:
                    sftp.remove(full_path)        

        with self._connect() as sftp:
            try:
                recursive_delete(sftp, folder_path)
                print(f"Folder {folder_path} deleted successfully")
            except FileNotFoundError:
                print(f"Folder {folder_path} not found")
            except Exception as e:
                print(f"Failed to delete folder {folder_path}: {e}")
    
    def _specify_data_products(self):

        def extract_options(row):
            if "Mutliple_Options: " in row:
                # Extract the substring starting after "Mutliple_Options: "
                list_str = row.split("Mutliple_Options: ")[1]
                # Convert the extracted substring to a Python list using ast.literal_eval
                try:
                    options_list = ast.literal_eval(list_str)
                    return options_list
                except (SyntaxError, ValueError):
                    print(f"Error parsing list from row: {row}")
                    return None
            else:
                return None

        # Function to check if a row contains "Mutliple_Options: "  
        def contains_multiple_options(row):
            return "Mutliple_Options: " in row

        async def f(self,df,df_multiple):    
                    selected_values = None
                
                    # Keep only columns '1' and '2'
                    df_multiple = df_multiple[['Data Product','Top-level Directory']]

                    # Remove duplicate rows based on columns '1' and '2'
                    df_multiple = df_multiple.drop_duplicates()

                    # Apply the function to the column '1' and store the results in a new column 'Options_List'
                    df_multiple['Options_List'] = df_multiple['Data Product'].apply(extract_options)

                    Select_obj = _Multi_dropdown(df_multiple['Options_List'].to_list(), df_multiple['Top-level Directory'].to_list(), "Specify 'Data Product' for unknown exports:")

                    selected_values = await Select_obj.display_widgets()
                            

                    if selected_values:

                        df_multiple['Data Product'] = selected_values

                        # Create a mapping from df1
                        mapping = pd.Series(df_multiple['Data Product'].values, index=df_multiple['Top-level Directory']).to_dict()

                        # Update df2['2'] based on the mapping
                        df['Data Product'] = df['Top-level Directory'].map(mapping).combine_first(df['Data Product'])

                        self._tables_available = df 
                        self._tables_backup = df 

                        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                        filename = f"{timestamp}_data_products.csv"
                        df = df[['Data Product','Table','Top-level Directory']]
                        df.to_csv(filename)
                        print(f"Data Product template has been saved to: {filename}")
 
        df = self._tables_available.copy()

        # Filter rows where column '1' contains "Mutliple_Options: "
        df_multiple = df[df['Data Product'].apply(contains_multiple_options)].copy()

        if not df_multiple.empty:
            asyncio.ensure_future(f(self,df,df_multiple))
  
    def _object_defaults(self):
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
        self._set_data_product:str = None
        self._time_stamp:str = None
        self._set_table:str = None
        self._download_finished = None