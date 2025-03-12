import sys
import importlib
import subprocess

# Check and install required libraries
required_libraries = ['asyncio'] 
for lib in required_libraries:
    try:
        importlib.import_module(lib)
    except ImportError:
        print(f"Installing {lib}...")
        subprocess.check_call([sys.executable, '-m', 'pip', 'install', lib])
subprocess.run(['pip', 'install', '-U', 'ipywidgets'])

import ipywidgets as widgets
from IPython.display import display
import asyncio

sys.path.append('src')
from .utils import _construct_query, _check_list_format

class _SelectData:
    def __init__(self, df, title="Select Data"):
        self.df = df

        # Create the title widget
        self.title = widgets.HTML(value=f"<h2>{title}</h2>")

        # Set an initial value for product dropdown (first product by default)
        self.selected_product = self.df['Data Product'].unique()[0]
         # Create the second dropdown menu, prepopulate based on initial product
        filtered_tables = self.df[self.df['Data Product'] == self.selected_product]['Table'].unique()
        self.selected_table = filtered_tables[0]

        # Create the first dropdown menu
        self.product_dropdown = widgets.Dropdown(
            options=self.df['Data Product'].unique(),
            value=self.selected_product,  # Set the initial value
            description='Data Product:',
            disabled=False,
        )

        # Create the second dropdown menu placeholder
        self.table_dropdown = widgets.Dropdown(
            options=filtered_tables.tolist(),  # Populate based on initial product
            value = self.selected_table,
            description='Table:',
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

        # Observe changes in both dropdown selections and button click
        self.product_dropdown.observe(self._observe_product_change, names='value')
        self.table_dropdown.observe(self._observe_table_change, names='value')
        self.ok_button.on_click(self._ok_button_click)
        self.cancel_button.on_click(self._cancel_button_click)

    async def _product_change(self, change):
        self.selected_product = self.product_dropdown.value
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
        # Display the title and widgets arranged horizontally
        display(self.title)
        display(widgets.HBox([self.product_dropdown]))
        display(widgets.HBox([self.table_dropdown]))
        display(widgets.HBox([self.ok_button, self.cancel_button]))

        # Wait for user interaction to complete
        while not self.cancel_button.disabled:
            await asyncio.sleep(0.1)

        return self.selected_product, self.selected_table
class _SelectList:
    def __init__(self, values, col_name: str, title="Select an Option"):
        self.selected_value = values[0]

        # Create the title widget
        self.title = widgets.HTML(value=f"<h2>{title}</h2>")

        # Create the dropdown menu
        self.list_dropdown = widgets.Dropdown(
            options=values,
            description=f"{col_name} :",
            disabled=False,
        )

        # Create the OK button
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
        # Display the title and widgets arranged horizontally
        display(self.title)
        display(widgets.HBox([self.list_dropdown]))
        display(widgets.HBox([self.ok_button, self.cancel_button]))

        while not self.cancel_button.disabled:
            await asyncio.sleep(0.1)

        return self.selected_value
class _SelectMultiple:
    def __init__(self, values, col_name: str, title="Select Multiple Options"):
        self.selected_list = []
        nrows = 20 if len(values) > 20 else len(values)
        
        # Create the title widget
        self.title = widgets.HTML(value=f"<h2>{title}</h2>")
        
        # Create the multiple select widget
        self.list_select = widgets.SelectMultiple(
            options=values,
            description=f"{col_name} :",
            disabled=False,
            rows=nrows,
            layout=widgets.Layout(width='2000px')  # Adjust the width as needed
        )

        # Create the OK button
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
        # Display the title and arrange widgets horizontally
        display(self.title)
        display(widgets.HBox([self.list_select]))
        display(widgets.HBox([self.ok_button, self.cancel_button]))

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
        # Create the title widget
        self.title = widgets.HTML(value=f"<h2>{title}</h2>")

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
        
    async def _list_change(self, change):
        # Handle asynchronous dropdown updates
        if change['type'] == 'change' and change['name'] == 'value':
            for i, (hbox, dropdown) in enumerate(self.dropdown_widgets):
                if dropdown is change.owner:
                    self.selected_values[i] = change.new
                    self.ok_button.disabled = False  # Enable OK button on change
                    break

    def _observe_list_change(self, change):
        asyncio.ensure_future(self._list_change(change))

    def _ok_button_click(self, b):
        self.ok_button.disabled = True  # Disable OK button after it's clicked
        self.cancel_button.disabled = True
        for hbox, dropdown in self.dropdown_widgets:
            dropdown.disabled = True

    def _cancel_button_click(self, b):
        self.selected_values = None  # Reset selected values
        self.ok_button.disabled = True  # Disable OK button
        self.cancel_button.disabled = True
        for hbox, dropdown in self.dropdown_widgets:
            dropdown.disabled = True

    async def display_widgets(self):
        # Display all widgets asynchronously
        display(self.title)
        display(widgets.VBox([hbox for hbox, dropdown in self.dropdown_widgets]))
        display(self.ok_button, self.cancel_button)

        while not self.cancel_button.disabled:
            await asyncio.sleep(0.1)

        return self.selected_values
class _SelectOptions:
    def __init__(self,config = None):
        # Initialize default configuration

        if config:
            self.config  = config
        else:    
            self.config = {
                'delete_files': False,
                'concat_files': True,
                'output_format': [".csv"],
                'file_size_mb': 500,
            }

        # Title for delete_files
        self.delete_files_title = widgets.HTML(value="<h3>Delete Files After Processing (To Prevent Large Storage Consumption - 'False' is recommeded):</h3>")
        # Dropdown for delete_files
        self.delete_files_dropdown = widgets.Dropdown(
            options=[True, False],
            value=self.config['delete_files'],  # Use default value
            description='Delete Files:',
            disabled=False,
        )

        # Title for concat_files
        self.concat_files_title = widgets.HTML(value="<h3>Concatenate Sub-Files into a Single Output File ('True' is Recommeded):</h3>")
        # Dropdown for concat_files
        self.concat_files_dropdown = widgets.Dropdown(
            options=[True, False],
            value=self.config['concat_files'],  # Use default value
            description='Concat Files:',
            disabled=False,
        )

        # Title for output_format
        self.output_format_title = widgets.HTML(value="<h3>Select Output File Formats (More than one can be selected - '.xlsx' is not recommeded):</h3>")
        # Multi-select dropdown for output_format
        self.output_format_multiselect = widgets.SelectMultiple(
            options=[".csv", ".xlsx", ".parquet", ".pickle", None],
            #options=[".csv", ".xlsx", ".parquet", ".pickle", ".dta"],
            value=self.config['output_format'],  # Use default value
            description='Formats:',
            disabled=False,
        )

        # Title for file_size_mb
        self.file_size_title = widgets.HTML(value="<h3>File Size Cutoff (MB) Before Splitting into Multiple Output files (Only an approxiate):</h3>")
        # Input field for file_size_mb
        self.file_size_input = widgets.FloatText(
            value=self.config['file_size_mb'],  # Use default value
            description='File Size (MB):',
            disabled=False,
        )

        # Create the OK button
        self.ok_button = widgets.Button(
            description='OK',
            disabled=False,
        )

        # Create the Cancel button
        self.cancel_button = widgets.Button(
            description='Cancel',
            disabled=False,
        )

        # Observe changes in dropdown selections and button clicks
        self.ok_button.on_click(self._ok_button_click)
        self.cancel_button.on_click(self._cancel_button_click)

    def _ok_button_click(self, b):
        self.ok_button.disabled = True  # Disable the OK button after it's clicked
        self.cancel_button.disabled = True
        self.delete_files_dropdown.disabled = True
        self.concat_files_dropdown.disabled = True
        self.output_format_multiselect.disabled = True
        self.file_size_input.disabled = True

        # Store the current configuration based on user input
        self.config = {
            'delete_files': self.delete_files_dropdown.value,
            'concat_files': self.concat_files_dropdown.value,
            'output_format': list(self.output_format_multiselect.value),
            'file_size_mb': self.file_size_input.value,
        }

    def _cancel_button_click(self, b):
        # On cancel, keep the default config (already initialized in __init__)
        self.ok_button.disabled = True  # Disable the OK button
        self.cancel_button.disabled = True  # Disable the Cancel button
        self.delete_files_dropdown.disabled = True
        self.concat_files_dropdown.disabled = True
        self.output_format_multiselect.disabled = True
        self.file_size_input.disabled = True

    async def display_widgets(self):

        spacer = widgets.Box(
            children=[widgets.Label(value="")],  # Add a label with empty text for spacing
            layout=widgets.Layout(height='20px')  # Adjust height for desired spacing
            )
    

        # Display the titles, widgets, and buttons
        display(widgets.VBox([
            self.delete_files_title,
            self.delete_files_dropdown,
            self.concat_files_title,
            self.concat_files_dropdown,
            self.output_format_title,
            self.output_format_multiselect,
            self.file_size_title,
            self.file_size_input,
            spacer,  # Add spacing between input fields and buttons
            widgets.HBox([self.ok_button, self.cancel_button]),
        ]))

        while not self.cancel_button.disabled:
            await asyncio.sleep(0.1)

        return self.config
class _CustomQuestion:
    def __init__(self, question, buttons):
        self.question = question
        self.result_future = asyncio.get_event_loop().create_future()

        # Create a list of buttons based on the input
        self.buttons = [widgets.Button(description=btn_name) for btn_name in buttons]

        # Assign click handlers dynamically
        for button in self.buttons:
            button.on_click(self.on_button_clicked)

    def display_widgets(self):
        # Display the question and buttons
        display(widgets.Label(value=self.question))
        for button in self.buttons:
            display(button)
        return self.result_future  # Return the future for awaiting the result

    def on_button_clicked(self, b):
        # Disable all buttons and set the result to the clicked button's description
        self._disable_buttons()
        self.result_future.set_result(b.description)

    def _disable_buttons(self):
        # Disable all buttons after a selection
        for button in self.buttons:
            button.disabled = True

def _select_list(class_type,values, col_name: str,title:str,fnc=None, n_args = None): 
    
    async def f(class_type,values,col_name,title, fnc, n_args):
        if class_type == '_SelectList':
            Select_obj = _SelectList(values, col_name,title)
        elif class_type == '_SelectMultiple':
            Select_obj = _SelectMultiple(values, col_name,title)

        selected_value = await Select_obj.display_widgets()

        if fnc and n_args:
            fnc(selected_value, *n_args) 

    asyncio.ensure_future(f(class_type,values,col_name,title,fnc,n_args))

def _select_bvd(selected_value, bvd_list,select_cols, search_type):
    if selected_value is not None: 
        bvd_list[1]  = selected_value
        bvd_list[2] = _construct_query(bvd_list[1],bvd_list[0],search_type)
        if select_cols is not None:
                select_cols = _check_list_format(select_cols,bvd_list[1])
        print(f"{len(bvd_list[0])} unique bvd_id numbers were detected")
        print(f"The following bvd query has been created: {bvd_list[2]}")

def _select_date(selected_value, time_period,select_cols):
    if selected_value is not None: 
        time_period[2]  = selected_value
        if select_cols  is not None:
                select_cols = _check_list_format(select_cols,time_period[2])
        print(f"The following Period will be selected: {time_period}")

def _select_product(selected_value,df,obj):
    if selected_value is not None:
        df = df.query(f"`Top-level Directory` == '{selected_value}'")
        if not df.empty: 
            obj.remote_path = df['Base Directory'].iloc[0]
            print(f"{obj.set_data_product} was set as Data Product")
            print(f"{obj.set_table} was set as Table")
            
        else: 
            obj.remote_path = None
            obj._set_table = None
            obj._set_data_product = None
    else:
        obj.remote_path = None
        obj._set_table = None
        obj._set_data_product = None
    