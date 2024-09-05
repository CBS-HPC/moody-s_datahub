## Download 

https://github.com/CBS-HPC/moody-s_datahub/blob/main/dist/moodys_datahub-0.0.1-py3-none-any.whl

### On Windows


```python
import subprocess

url = "https://github.com/CBS-HPC/moody-s_datahub/blob/main/dist/moodys_datahub-0.0.1-py3-none-any.whl"
output_file = "moodys_datahub-0.0.1-py3-none-any.whl"

subprocess.run([
    "powershell", 
    "-Command", 
    f"Invoke-WebRequest -Uri {url} -OutFile {output_file}"
])
```

### On Linux


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

### Connect to SFTP server using SSH key.


```python
# Connects to default CBS SFTP server

SFTP = Sftp(privatekey="user_provided-ssh-key.pem")

# Connects to custom SFTP server
SFTP = Sftp(hostname = "example.com", username = "username", port = 22,privatekey="user_provided-ssh-key.pem") 
```

### Select Data Product and Table


```python
SFTP.select_data()
```

    Retrieving Data Product overview from SFTP..wait a moment
    


    ---------------------------------------------------------------------------

    SSHException                              Traceback (most recent call last)

    Cell In[3], line 1
    ----> 1 SFTP.select_data()
    

    File c:\Users\kgp.lib\Anaconda3\envs\orbis\Lib\site-packages\orbis\tools.py:567, in Sftp.select_data(self)
        564     print(f"{self.set_table} was set as Table")
        566 if self._tables_available is None and self._tables_backup is None:
    --> 567         self.tables_available(save_to=False)
        569 asyncio.ensure_future(f(self))
    

    File c:\Users\kgp.lib\Anaconda3\envs\orbis\Lib\site-packages\orbis\tools.py:819, in Sftp.tables_available(self, save_to, reset)
        806 """
        807 Retrieve available SFTP data products and tables and save them to a file.
        808 
       (...)
        815 - Pandas DataFrame with the available SFTP data products and tables.
        816 """
        818 if self._tables_available is None and self._tables_backup is None:
    --> 819    self._tables_available = self._table_overview()
        820    self._tables_backup = self._tables_available 
        821 elif reset:
    

    File c:\Users\kgp.lib\Anaconda3\envs\orbis\Lib\site-packages\orbis\tools.py:1011, in Sftp._table_overview(self, product_overview)
       1008 if product_overview is None:
       1009     product_overview =_table_names()
    -> 1011 with self.connect() as sftp:
       1012     exports = sftp.listdir()
       1013     newest_exports = []
    

    File c:\Users\kgp.lib\Anaconda3\envs\orbis\Lib\site-packages\orbis\tools.py:553, in Sftp.connect(self)
        543 def connect(self):
        544     """
        545     Establish an SFTP connection.
        546 
       (...)
        551     - SFTP connection object.
        552     """
    --> 553     sftp = pysftp.Connection(host=self.hostname , username=self.username ,port = self.port ,private_key=self.privatekey, cnopts=self._cnopts)
        554     return sftp
    

    File c:\Users\kgp.lib\Anaconda3\envs\orbis\Lib\site-packages\pysftp\__init__.py:140, in Connection.__init__(self, host, username, private_key, password, port, private_key_pass, ciphers, log, cnopts, default_path)
        138 # Begin the SSH transport.
        139 self._transport = None
    --> 140 self._start_transport(host, port)
        141 self._transport.use_compression(self._cnopts.compression)
        142 self._set_authentication(password, private_key, private_key_pass)
    

    File c:\Users\kgp.lib\Anaconda3\envs\orbis\Lib\site-packages\pysftp\__init__.py:176, in Connection._start_transport(self, host, port)
        174 '''start the transport and set the ciphers if specified.'''
        175 try:
    --> 176     self._transport = paramiko.Transport((host, port))
        177     # Set security ciphers if set
        178     if self._cnopts.ciphers is not None:
    

    File c:\Users\kgp.lib\Anaconda3\envs\orbis\Lib\site-packages\paramiko\transport.py:448, in Transport.__init__(self, sock, default_window_size, default_max_packet_size, gss_kex, gss_deleg_creds, disabled_algorithms, server_sig_algs)
        446                 break
        447     else:
    --> 448         raise SSHException(
        449             "Unable to connect to {}: {}".format(hostname, reason)
        450         )
        451 # okay, normal socket-ish flow here...
        452 threading.Thread.__init__(self)
    

    SSHException: Unable to connect to example.com: [WinError 10060] Et forsøg på at oprette forbindelse mislykkedes, fordi den part, der havde oprettet forbindelse, ikke svarede korrekt efter en periode, eller en oprettet forbindelse blev afbrudt, fordi værten ikke svarede


### BVD Filter


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


```python
SFTP.time_period = [1998,2005]
```

### Column Filter


```python
SFTP.select_columns()
```

### Test the selected Filters


```python
test_sample = SFTP.process_one(save_to='csv')
```

### Batch Process for on all files 


```python
results = SFTP.process_all()
```

### Search in Data Dictionary for variables/columns


```python
search_dict = SFTP.search_dictionary(save_to='xlsx', search_word='total_asset')

search_dict = SFTP.search_dictionary(search_word='subsidiaries',letters_only=True,search_cols= { 'Data Product': False,'Table': False,'Column': True,'Definition': False })
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



### Create a new SFTP Object


```python
SFTP_2 = SFTP.copy_obj()
```


    Dropdown(description='Data Product:', options=('Listed Financials (Monthly)', 'Interim Financials (Monthly)', …



    Dropdown(description='Table:', disabled=True, options=(), value=None)



    Button(description='OK', disabled=True, style=ButtonStyle())



    Button(description='Cancel', style=ButtonStyle())


### Create Filters using the pandas.query() method


```python

# Example 1
SFTP.query ="total_assets > 1000000000"
query_1 = SFTP.process_all() 

# Example 2
query_args = ['CN9360885371','CN9360885372','CN9360885373']
SFTP.query=f"bvd_id_number in {query_args}"
query_2 = SFTP.process_all() 

# Example 3
query_args = 'DK'
SFTP.query = f"bvd_id_number.str.startswith('{query_args}', na=False)"
query_3 = SFTP.process_all() 

# Example 4
bvd_numbers = ['CN9360885371','CN9360885372','CN9360885373']
country_code = 'CN'
SFTP.query =f"bvd_id_number in {bvd_numbers} | (total_assets > 1000000000 & bvd_id_number.str.startswith('{country_code}', na=False))"
query_4 = SFTP.process_all() 
```

### Create Filters using custom functions


```python
bvd_id_numbers = ['CN9360885371','CN9360885372','CN9360885373']
column_filter = ['bvd_id_number','fixed_assets','original_currency','total_assets']  # Example column filter

def bvd_filter(df,bvd_id_numbers=None,column_filter=None,specific_value=None,specific_col=None):

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
query_5 = SFTP.process_all() 
```

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


