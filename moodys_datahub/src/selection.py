import sys
import time
import importlib
import subprocess
import os

# Check and install required libraries
required_libraries = ['asyncio'] 
for lib in required_libraries:
    try:
        importlib.import_module(lib)
    except ImportError:
        print(f"Installing {lib}...")
        subprocess.check_call([sys.executable, '-m', 'pip', 'install', lib])
subprocess.run(['pip', 'install', '-U', 'ipywidgets'])

import asyncio

sys.path.append('src')
from .widgets import _SelectData,_select_list,_select_product
from .connection import _Connection

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
            self._object_defaults()

        elif path is not self.remote_path:
            self._local_files  = []
            self._local_path   = None

            if self._local_repo:
                self._remote_files, self._remote_path = self._check_path(path)
            else:
                self._remote_files, self._remote_path = self._check_path(path,"remote")
     
            if self._remote_path:
                if len(self._tables_available) > 1:
                    df = self._tables_available.query(f"`Base Directory` == '{self._remote_path}'") # FIX ME !! Investigating this !!!  get stuck on this if df len == 1
                else:
                    df = self._tables_available
      
                if df.empty:
                    df = self._tables_available.query(f"`Export` == '{self._remote_path}'")
                    self._set_table = None
                else:     
                    if self._set_data_product not in df['Data Product'].values:    
                        self._set_data_product = df['Data Product'].iloc[0]
                        self._tables_available = df 
                    
                    if self._set_table not in df['Table'].values:
                        self._set_table = df['Table'].iloc[0]
 
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

        if (product is None) or (product is not self._set_data_product):
            self._tables_available = self._tables_backup.copy()

        if product is None:
            self._object_defaults()

        if product is not self._set_data_product:
            
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
                self._object_defaults()
                self._tables_available = df
                self._set_data_product = product
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

        if table is None:
            self._object_defaults()
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
                self._object_defaults()
                self._set_table = table
                self._set_data_product = df['Data Product'].iloc[0]
                self._time_stamp = df['Timestamp'].iloc[0]
                self.remote_path = df['Base Directory'].iloc[0]

    def select_data(self):  
        """
        Asynchronously select and set the data product and table using interactive widgets.

        This method facilitates user interaction for selecting a data product and table from a backup of available tables.
        It leverages asynchronous widgets, allowing the user to make selections and automatically updating instance attributes
        for the selected data product and table. The selections are applied to `self.set_data_product` and `self.set_table`, 
        which can be used in subsequent file operations.

        Workflow:
        - An instance of the `_SelectData` class is created, using `_tables_backup` to populate the widget options.
        - Users select a data product and table through the interactive widget.
        - The method validates the selected options and updates the instance's `set_data_product` and `set_table` attributes.
        - If multiple data products match the selection, a list of options is displayed to the user for further refinement.
        - The selected data product and table are printed to confirm the operation.

        Internal Async Function (`f`):
        - The method contains an internal asynchronous function `f` that handles the widget display and data selection.
        - It uses the `await` keyword to ensure non-blocking behavior while the user interacts with the widget.
        - The selected values are processed and validated before being assigned to the instance variables.
        
        Notes:
        - This method uses `asyncio.ensure_future` to ensure that the asynchronous function `f` runs concurrently without blocking other operations.
        - If multiple matches for the selected data product are found, the user is prompted to further specify the data product.
        - The method uses a copy of `_tables_backup` to preserve the original data structure during the filtering process.

        Example:
            ```python
            # Trigger the data selection process
            self.select_data()
            ```
        Raises:
        - `ValueError`: If no valid data product or table is selected after interaction.
        
        Expected Outputs:
        - Updates `self.set_data_product` and `self.set_table` based on user selections.
        - Prints confirmation of the selected data product and table.
        """
           
        async def f(self):
            Select_obj = _SelectData(self._tables_backup,'Select Data Product and Table')
            selected_product, selected_table = await Select_obj.display_widgets()
            df = self._tables_backup.copy()
            df = df[['Data Product','Table','Base Directory','Top-level Directory']].query(f"`Data Product` == '{selected_product}' & `Table` == '{selected_table}'").drop_duplicates()
            if len(df) > 1:
                options = df['Top-level Directory'].tolist()
                product = df['Data Product'].drop_duplicates().tolist()
                msg = f"Multiple data products match '{product[0]}'. Please set right data product:" 
                self._set_table = selected_table
                self._set_data_product = selected_product
                _select_list('_SelectList',options,f"'{product[0]}':",msg,_select_product,[df,self])
            elif len(df) == 1:
                self.set_data_product = selected_product
                self.set_table = selected_table
                print(f"{self.set_data_product} was set as Data Product")
                print(f"{self.set_table} was set as Table")

        self._download_finished = None 

        asyncio.ensure_future(f(self))

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
                sftp = self._connect()
                if sftp.exists(path):
                    files = sftp.listdir(path)
                    if not files:
                        files = [os.path.basename(path)]
                        path  = os.path.dirname(path)
                else:
                    print(f"Remote path is invalid:'{path}'")
                    path = None

            if len(files) > 1 and any(file.endswith('.csv') for file in files):
                if self._set_table:
                    # Find the file that matches the match_string without the .csv suffix
                    files = [next((file for file in files if os.path.splitext(file)[0] == self._set_table), None)]

            if mode=="remote" and not self._time_stamp:
                file_attributes = sftp.stat(path + "/" + files[0])
                #file_attributes = sftp.stat(os.path.normpath(os.path.join(path,files[0])))
                self._time_stamp = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(file_attributes.st_mtime))
        else:
            files = []
        return files,path      
  
    def _check_files(self,value):
        if isinstance(value, list) and all(isinstance(item, str) for item in value):
            return value
        else:
            raise ValueError("file list must be a list of strings")
