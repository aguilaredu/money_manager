import json 
import pandas as pd

def load_config(config_path: str) -> dict:
    """Opens a json configuration file and returns the result as a python dict object.

    Args:
        config_path (str): The relative path to the configuration file.

    Returns:
        dict: A python dict with the contents of the configuration file. 
    """
    with open(config_path, 'r') as config_file:
        return json.load(config_file)