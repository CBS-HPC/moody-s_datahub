import sys
import importlib
import subprocess
import os
import importlib.resources as pkg_resources

# Check and install required libraries
required_libraries = ['pandas'] 
for lib in required_libraries:
    try:
        importlib.import_module(lib)
    except ImportError:
        print(f"Installing {lib}...")
        subprocess.check_call([sys.executable, '-m', 'pip', 'install', lib])
subprocess.run(['pip', 'install', '-U', 'ipywidgets'])

import pandas as pd


# Data Related functions
def _read_excel(file_name):
    with pkg_resources.open_binary('moodys_datahub.data', file_name) as f:
        return pd.read_excel(f)

def _table_names(file_name = None):
    
    if file_name is None:
        df = _read_excel('data_products.xlsx')
    elif os.path.isfile(file_name):
        read_functions = {
                    'csv': pd.read_csv,
                    'xlsx': pd.read_excel
                    }
        file_extension = file_name.lower().split('.')[-1]

        if file_extension not in read_functions:
            raise ValueError(f"Unsupported file format: {file_extension}")

        read_function = read_functions[file_extension]

        df = read_function(file_name)
         
    else:
        raise ValueError("moody's datahub data product file was not detected")
    
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
        #print("It was not possible to determine the 'data product' for all exports. Run 'self.specify_data_product()' to correct")

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
