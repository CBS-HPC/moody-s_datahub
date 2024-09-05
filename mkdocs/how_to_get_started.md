## Download 

The pip wheel can be manually downloaded using the link below: 

https://github.com/CBS-HPC/moody-s_datahub/blob/main/dist/moodys_datahub-0.0.1-py3-none-any.whl


Or directly to the working folder by running the line below:


```python
!curl -s -L -o moodys_datahub-0.0.1-py3-none-any.whl https://github.com/CBS-HPC/moody-s_datahub/blob/main/dist/moodys_datahub-0.0.1-py3-none-any.whl 
```

## Installation

Install the package "orbis-0.0.1-py3-none-any.whl" or "orbis-0.0.1.tar.gz" using pip:



```python
pip install moodys_datahub-0.0.1-py3-none-any.whl
```

## Usage


```python
from moodys_datahub.tools import *
```

### Connect to SFTP server using the provided SSH key file (.pem)


```python
# Connects to default CBS SFTP server
SFTP = Sftp(privatekey="user_provided-ssh-key.pem")

# Connects to custom SFTP server
SFTP = Sftp(hostname = "example.com", username = "username", port = 22,privatekey="user_provided-ssh-key.pem") 
```

### Select Data Product and Table

Run the function below to select "Data Product" and "Table" that are available on the SFTP.


```python
SFTP.select_data()
```


    ---------------------------------------------------------------------------

    NameError                                 Traceback (most recent call last)

    Cell In[2], line 1
    ----> 1 SFTP.select_data()
    

    NameError: name 'SFTP' is not defined


### Overview of Remote Files

The "Data Product" and "Table" has been selected the associated files on the SFTP server are listed as shown below:


```python
SFTP.remote_files
```

### Column Selection

Select which columns (variables) to keep 


```python
SFTP.select_columns()
```

### BVD Filter

Set a "bvd_id" filter. This can be provided in different ways as seen below as a python list of in .txt [Link] or .xlsx[Link] format. When setting the .bvd_list the user will be prompted to select one or more "bvd" related columns.

It can perform an extract search based on full bvd_id numbers or based on the country code that is the two starting letter of the bvd_id numbers.


```python
# Text file
SFTP.bvd_list = 'bvd_numbers.txt'

# Excel file - Will search through columns for relevant bvd formats
SFTP.bvd_list = 'bvd_numbers.xlsx'

# Country filter
SFTP.bvd_list = ['US','DK','CN']

# bvd number lists
SFTP.bvd_list = ['DK28505116','SE5567031702','SE5565475489','NO934382404','SE5566674205','DK55828415']
```

### Time Period Filter

A time periode filter can be set as seen below. Subsequently the user will be prompted to select a "date" column. 

Not all table have suitable "date" columns for which time periode filtration is not possible. 


```python
SFTP.time_period = [1998,2005]
```

### Create Filters using the SFTP.query() method

With the SFTP.query() method the user can create more custom filters.The method utilises pandas.query() method. A few examples are shown below:


```python
# Example 1: 
SFTP.query ="total_assets > 1000000000"

# Example 2
query_args = ['CN9360885371','CN9360885372','CN9360885373']
SFTP.query=f"bvd_id_number in {query_args}"

# Example 3
query_args = 'DK'
SFTP.query = f"bvd_id_number.str.startswith('{query_args}', na=False)"

# Example 4
bvd_numbers = ['CN9360885371','CN9360885372','CN9360885373']
country_code = 'CN'
SFTP.query =f"bvd_id_number in {bvd_numbers} | (total_assets > 1000000000 & bvd_id_number.str.startswith('{country_code}', na=False))"
```

### Create Filters using custom functions

It is also possible to defined SFTP.queryprovide a custom functionWith the pandas.query() method the user can create more custom filters. Below are show four examples of how to setup a query string. 


```python
bvd_id_numbers = ['CN9360885371','CN9360885372','CN9360885373']
column_filter = ['bvd_id_number','fixed_assets','original_currency','total_assets']  # Example column filter

def bvd_filter(df,bvd_id_numbers,column_filter,specific_value,specific_col):

     # Check if specific_col is a column in the DataFrame
    if specific_col is not None and specific_col not in df.columns:
        raise ValueError(f"{specific_col} is not a column in the DataFrame.")

    if specific_value is not None:
                df = df[df[specific_col] > specific_value]

    if bvd_id_numbers:
        if isinstance(bvd_id_numbers, list):
            row_filter = df['bvd_id_number'].isin(bvd_id_numbers)
        elif isinstance(bvd_id_numbers, str):
            row_filter  = df['bvd_id_number'].str.startswith(bvd_id_numbers)
        else:
            raise ValueError("bvd_id_numbers must be a list or a string")
                
        if row_filter.any():
            df = df.loc[row_filter]
        else: 
           df = None 

    if df is not None and column_filter:
        df = df[column_filter]

    return df

SFTP.query = bvd_filter
SFTP.query_args = [bvd_id_numbers,column_filter,1000000000,'total_assets']

```

### Test the selected Filters

Before running the selected filters on all files (SFTP.remote_files) is can be a good idea to test it on a single sub-file using the function below. 

**It should be noted that the sub-file that is used below will not contain rows that a relevant for the defined filters.**


```python
df_sample = SFTP.process_one()

df_sample = SFTP.process_one(save_to = 'csv',files = SFTP.remote_files[0], n_rows = 2000)
```

### Download all files before "Batch Processing"

If working on a slower connection it may be benificial to start downloading all remote files before processing them.

When the downloading process has been started "SFTP._download_finished" will change from a "None" to "False and then "True" upon download completion.

The function is executed asyncionsly and the user can proceed working in the jupyter notebook while it is running.



```python
SFTP.download_all()

# Define the number of workers/processors that should be utilsed. 

SFTP.download_all(num_workers = 12)
```

### Batch Process for on all files 

All files can be processed using the function below. 

- If "SFTP._download_finished" is "None" the function also download the files. 

- If "SFTP._download_finished" is "False" it will wait upon the download process has been completed and "SFTP._download_finished" is set to "True". 


```python

# If no input arguments are provided it will utilise the filters that has beeen defined in the selection above.
results = SFTP.process_all()

# Input arguments can also be set manually as shown below:  
results = SFTP.process_all(files = SFTP.remote_files, 
                            destination = "file_name",  
                            num_workers = 12, 
                            select_cols = ['bvd_id_number','fixed_assets','original_currency','total_assets'],
                            date_query = None,
                            bvd_query = None,
                            query = bvd_filter, 
                            query_args = [bvd_id_numbers,column_filter,1000000000,'total_assets']
                            )

```

### Search in Data Dictionary for variables/columns

It is possible to search in the "Data Dictionary" for variables, keywords or topic. The "Data Dictionary" will be filtrated according to "Data Product" and "Table" selection.


```python
df_search = SFTP.search_dictionary(save_to = 'xlsx', search_word = 'total_asset')

df_search = SFTP.search_dictionary(save_to = 'xlsx',
                                    search_word = 'subsidiaries',
                                    search_cols= { 'Data Product': False,'Table': False,'Column': True,'Definition': False },
                                    letters_only = True,
                                    extact_match = False,
                                    data_product = None,
                                    table = None,  
                                    )
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>Unnamed: 0</th>
      <th>Data Product</th>
      <th>Table</th>
      <th>Column</th>
      <th>Definition</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>1546</th>
      <td>1547</td>
      <td>Key Ownership (Monthly)</td>
      <td>basic_shareholder_info</td>
      <td>no_of_recorded_subsidiaries</td>
      <td>This field indicates the number of subsidiarie...</td>
    </tr>
  </tbody>
</table>
</div>



### Search for country codes

The function below can be used to find the "bvd_id" country codes for specific nations.


```python
# Search for country codes by country name
SFTP.search_country_codes(search_word='congo')

# Define columns to search in:

SFTP.search_country_codes(search_word='DK', search_cols = { 'Country': False,'Code': True })
```

### Create a new SFTP Object

The "SFTP.copy_obj()" function can be used to create a new SFTP object in order to process another "Data Product"/"Table.

- SFTP.select_data() will be prompted automatically 
- Other filters will be reset.


```python
SFTP_2 = SFTP.copy_obj()
```


    Dropdown(description='Data Product:', options=('Listed Financials (Monthly)', 'Interim Financials (Monthly)', …



    Dropdown(description='Table:', disabled=True, options=(), value=None)



    Button(description='OK', disabled=True, style=ButtonStyle())



    Button(description='Cancel', style=ButtonStyle())


### Other Options


```python
SFTP.delete_files = False # True/False: To delete the downloaded files post curation (to prevent very large amounts of data being stored locally)
SFTP.concat_files = True # True/False: To concatenate the curated data product sub files into a single output file.
SFTP.output_format =  [".xlsx",".csv",".parquet"] # [".csv","xlsx",".parquet",".pickle",".dta"] # Defining output formats.
SFTP.file_size_mb = 100 # Cut-off size for when to split output files into multiple files.
```

### Other functions


```python
search_dict = SFTP.search_dictionary(save_to='xlsx', search_word='total_asset')

# Create a new SFTP Object
SFTP_2 = SFTP.copy_obj()

# Search for country codes
SFTP.search_country_codes(search_word='congo')
```

    The folloiwng query was executed:`Column`.str.contains('total_asset', case=False, na=False,regex=False) | `Definition`.str.contains('total_asset', case=False, na=False,regex=False)
    Results have been saved to 'orbis_dict_search_20240704_092243.xlsx'
    



# Matching bvd_numbers from Identifiers dataset


```python
SFTP.delete_files = False # True/False: To delete the downloaded files post curation (to prevent very large amounts of data being stored locally)
SFTP.concat_files = True # True/False: To concatenate the curated data product sub files into a single output file.
SFTP.output_format =  [".csv"] # Defining output formats.
SFTP.file_size_mb = 300 # Cut-off size for when to split output files into multiple files.
```

## company name -> bvd_numbers (using fuzzy match algoritm)


```python
SFTP.remote_path = "IvdS14LwRxucymVszEBE3Q/unscheduled/giin"
SFTP.local_path = "company_names" 
df_names = SFTP.process_all(SFTP.remote_files,destination='company_name',select_cols= ['bvd_id_number','name_as_in_the_fatca_register_']) # Only loading the columns defined in filter_col
```

    Remote path is valid:'IvdS14LwRxucymVszEBE3Q/unscheduled/giin'
    Folder 'company_names' created.
    


```python
# Example usage:
input_strings = ["Bank of America", "AXA2", "JPMorgan Chase"]
extended_list = input_strings * 100
match_col = 'name_as_in_the_fatca_register_'
return_col = 'bvd_id_number'

str_remove = ["GMBH"," - Branch","CO.","LTD","Ltd","Limited", "GMBH","A/S"]

result_df = fuzzy_match(input_strings=extended_list,df=df_names,num_workers=-1, match_column=match_col, return_column=return_col,cut_off=50,remove_str=str_remove)

result_df.head()
```

## national id numbers -> bvd_numbers 


```python
SFTP.local_path = "Identifiers"
SFTP.remote_path = "IvdS14LwRxucymVszEBE3Q/unscheduled/identifiers"

# Below is just to collect 1000 random 'national_id_numbers'
SFTP.output_format =  None # Defining output formats.
df_id_number = SFTP.process_all(files =SFTP.remote_files[0:4],num_workers=-1,destination='identifiers',select_cols= ['bvd_id_number','national_id_number'])
national_id_number = random_sample = df_id_number['national_id_number'].sample(n=1000, random_state=42)

```

    Folder 'Identifiers' already exists.
    Remote path is valid:'IvdS14LwRxucymVszEBE3Q/unscheduled/identifiers'
    


```python
SFTP.output_format =  [".csv"] # Defining output formats.

# Define query statement
query_args = list(national_id_number.dropna())
query_str =f"national_id_number in {query_args}"

# Execute
query_id_numbers = SFTP.process_all(SFTP.remote_files[0:4],num_workers = -1,destination ="query_2",select_cols = ['bvd_id_number','national_id_number'],query = query_str,query_args=query_args) 

# Sanity check 
query_id_numbers['national_id_number'].isin(query_args).all()
```




    True


