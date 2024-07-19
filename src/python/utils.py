import json 
import os
import pandas as pd

def load_config(config_path: str) -> dict:
    """Opens a json configuration file and returns the result as a python dict object.

    Args:
        config_path (str): The relative path to the configuration file.

    Returns:
        dict: A python dict with the contents of the configuration file. 
    """
    try:
        with open(config_path, 'r') as config_file:
            return json.load(config_file)
    except Exception as e:
         raise ValueError(f"Couldn't load configuration file located at {config_path}. {e}")
    
def get_out_file_path(base_dir: str, path: str) -> str:
    """Gets the absolute path of the output file. If the default is present in the configs then the path is 
    dynamically built using the current working directory of the app. Otherwise it is assumed that the user passed
    a valid path to save the output to.  

    Args:
        base_dir (str): The current working directory of the program.
        path (str): The path contained in the configs file. 

    Returns:
        str: An absolute path to the output file.
    """
    if path == "data/out/transactions.csv":
        return os.path.join(base_dir, 'data/out/transactions.csv')
    else:
        if is_valid_csv_file_path(path):
             return path
        else:
             raise ValueError(f"The filepath {path} is not valid. Either the directory doesn't exist or the file is not of type CSV.")

def is_valid_csv_file_path(self, file_path: str) -> bool:
        """
        Check if the directory of the given file path is valid and if the file has a .csv extension.

        Args:
            file_path (str): The file path to be checked.

        Returns:
            bool: True if the directory is valid and the file has a .csv extension, False otherwise.
        """
        directory = os.path.dirname(file_path)
        
        return os.path.exists(directory)

def enforce_dataframe_schema(df, schema):
    """
    Enforce the given schema on the DataFrame by checking and casting the column types.

    Parameters:
    - df (pd.DataFrame): The DataFrame to enforce the schema on.
    - schema (dict): A dictionary where keys are column names and values are the expected data types.

    Returns:
    - pd.DataFrame: The DataFrame with enforced schema.
    """
    # Check for missing columns
    missing_columns = [col for col in schema.keys() if col not in df.columns]
    if missing_columns:
        raise ValueError(f"Transactions dataframe has missing columns. Missing columns: {missing_columns}")

    # Check for extra columns
    extra_columns = [col for col in df.columns if col not in schema.keys()]
    if extra_columns:
        raise ValueError(f"Transactions dataframe has extra columns. Extra columns: {extra_columns}")

    # Check for correct data types
    for col, dtype in schema.items():
        if col in df.columns:
            current_dtype = str(df[col].dtype)
            if dtype == 'datetime64[ns]':
                df[col] = pd.to_datetime(df[col], errors='coerce')            
            elif dtype == 'float64':
                df[col] = df[col].astype('float64')
            elif dtype == 'int64':
                df[col] = df[col].astype('int64')
            elif dtype == 'string':
                df[col] = df[col].astype('string')
            elif dtype == 'object':
                df[col] = df[col].astype('object')
            else:
                raise ValueError(f"Unsupported data type: {dtype}")

            # Validate the casting
            if str(df[col].dtype) != dtype:
                raise ValueError(f"Column {col} has incorrect data type. Expected {dtype}, got {df[col].dtype}.")

    return df