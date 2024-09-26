import sys
import importlib
import subprocess
import re
from multiprocessing import Pool, cpu_count

# Check and install required libraries
required_libraries = ['pandas','rapidfuzz'] 
for lib in required_libraries:
    try:
        importlib.import_module(lib)
    except ImportError:
        print(f"Installing {lib}...")
        subprocess.check_call([sys.executable, '-m', 'pip', 'install', lib])
subprocess.run(['pip', 'install', '-U', 'ipywidgets'])

from rapidfuzz import process
import pandas as pd

from IPython.display import display


# Common Wrangling Functions
def year_distribution(df=None):
    """
    Display the distribution of years in the data.

    Input Variables:
    - `self`: Implicit reference to the instance.

    Returns:
    - None
    """

    if df is None:
        print('No Dataframe (df) was detected')
        return
    
    columns_to_check = ['closing_date', 'information_date']
    date_col = next((col for col in columns_to_check if col in df.columns), None)
    
    if not date_col:
        print('No valid date columns found')
        return

    # Convert the date column to datetime
    df[date_col] = pd.to_datetime(df[date_col], format='%d-%m-%Y')
    
    # Create a new column extracting the year
    df['year'] = df[date_col].dt.year

    year_counts = df['year'].value_counts().reset_index()
    year_counts.columns = ['Year', 'Frequency']

    # Sort by year
    year_counts = year_counts.sort_values(by='Year')

    # Calculate percentage
    year_counts['Percentage'] = (year_counts['Frequency'] / year_counts['Frequency'].sum()) * 100

    # Calculate total row
    total_row = pd.DataFrame({'Year': ['Total'], 'Frequency': [year_counts['Frequency'].sum()],'Percentage':[year_counts['Percentage'].sum()]})

    # Concatenate total row to the DataFrame
    year_counts = pd.concat([year_counts, total_row])

    # Display the table
    print(year_counts)

def national_identifer(obj,national_ids:list=None,num_workers:int=-1):

    new_obj = obj.copy_obj(obj)
    new_obj.set_data_product = 'Key Financials (Monthly)'
    new_obj.set_table = 'key_financials_eur'

    query_args = national_ids
    query_str =f"national_id_number in {query_args}"
    select_cols = ['bvd_id_number','national_id_number']

    # Execute
    df = new_obj.process_all(num_workers = num_workers,select_cols = select_cols , query = query_str,query_args=query_args) 

    return df

def _fuzzy_worker(args):
    input_string, choices, cut_off, df, match_column, return_column= args
        
    match_obj = process.extractOne(input_string, choices, score_cutoff=cut_off)

    if match_obj is not None:
        match = match_obj[0]
        score = match_obj[1]
        index = [match == choice for choice in choices]
        match_value = df[index][match_column].iloc[0]
        return_value = df[index][return_column].iloc[0]
        return (input_string, match, score, match_value, return_value)
    else:
        return (input_string, None, None, None, None)

def fuzzy_match(input_strings:list, df: pd.DataFrame, num_workers:int = 1, match_column:str =None , return_column:str = None, cut_off:int = 50 , remove_str:list = None):
    """
    Perform fuzzy string matching with a list of input strings against a specific column in a DataFrame.

    Parameters:
    - input_strings (list): A list of strings for fuzzy matching.
    - df (pandas.DataFrame): The DataFrame containing the target columns.
    - match_column (str): The name of the column to match against.
    - return_column (str): The name of the column from which to return the matching value.
    - remove_str (list): A list of substrings to remove from the choices.

    Returns:
    - pandas.DataFrame: A DataFrame containing the original data along with the best match, its score, and the matching value from another column.
    """

    def remove_substrings(choices, substrings):
        for substring in substrings:
            choices = [choice.replace(substring.lower(), '') for choice in choices]
        return choices

    matches = []

    choices = [choice.lower() for choice in df[match_column].tolist()]
    input_strings = [input.lower() for input in input_strings]

    if remove_str is not None:
        choices = remove_substrings(choices, remove_str)

    if isinstance(num_workers, (int, float, complex))and num_workers != 1:
        num_workers = int(num_workers) 
        if num_workers < 0:
            num_workers = int(cpu_count() - 2)
        args_list = [(input_string, choices, cut_off, df, match_column, return_column) for input_string in input_strings]
        pool = Pool(processes=num_workers)
        matches = pool.map(_fuzzy_worker, args_list)
        pool.close()
        pool.join()
    else:    
        for input_string in input_strings:
            args_list = (input_string, choices, cut_off, df, match_column, return_column)
            input_string, match, score, match_value, return_value = _fuzzy_worker(args_list)
            matches.append((input_string, match, score, match_value, return_value))

    result_df = pd.DataFrame(matches, columns=['Search_string', 'BestMatch', 'Score', match_column, return_column])
    
    return result_df


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


   

    """Converts the title to lowercase and removes non-alphanumeric characters."""
    if isinstance(text,str):
        return re.sub(r'[^a-zA-Z0-9]', '', text.lower())
    else:
        return text