from utils import load_config, get_out_file_path
from pandas import DataFrame, read_csv, read_excel
import os, shutil

class InputTransactions:
    def __init__(self, base_dir: str, clear_input_dir = False) -> None:
        configs_dir = os.path.join(base_dir, 'configs')
        accounts_attributes_path = os.path.join(configs_dir, 'accounts.json') 
        configs_path = os.path.join(configs_dir, 'configs.json') 
        configs = load_config(configs_path)
        
        self.account_attributes = load_config(accounts_attributes_path)
        self.in_folder_path = os.path.join(base_dir, configs["path_configs"]["in_folder_path"])
        self.out_file_path = get_out_file_path(base_dir, configs["path_configs"]["out_file_path"])
        self.classified_dataframes = {}
        self.existing_transactions = None
        self.clear_input_dir = clear_input_dir

    def clear_input_directory(self) -> None:
        if self.clear_input_dir:
            print("Input directory files will be deleted.")
            for filename in os.listdir(self.in_folder_path):
                file_path = os.path.join(self.in_folder_path, filename)
                try:
                    if os.path.isfile(file_path) or os.path.islink(file_path):
                        os.unlink(file_path)
                    elif os.path.isdir(file_path):
                        shutil.rmtree(file_path)
                except Exception as e:
                    print('Failed to delete %s. Reason: %s' % (file_path, e))
        else:
            print("Input directory was not cleaned. Either clean manually or set clean_input_dir parameter to 'True'")

    def get_classified_dataframes(self):
        self.read_and_classify_bank_statements()
        return self.classified_dataframes

    def read_file(self, file_path: str) -> DataFrame:
        try:
            if file_path.endswith(('.xls', '.xlsx')):
                return read_excel(file_path, skiprows=7) # Hardcoded skiprows to santander files
            elif file_path.endswith('.csv'):
                return read_csv(file_path)
            else:
                raise ValueError("Unsupported file format")
        except Exception as e:
            raise ValueError(f"Error reading {file_path}: {e}")
        
    def validate_account_configs(self, account_name: str) -> dict:
        if not(account_name in self.account_attributes):
            raise KeyError(f"The account {account_name} cannot be found in the accounts.json config.")

    def read_and_classify_bank_statements(self)-> dict:
        dataframes_dict = {}

        # Iterate through all files in the directory
        for filename in os.listdir(self.in_folder_path):
            filepath = os.path.join(self.in_folder_path, filename)
            
            # Check if it is a file and has a .csv extension
            if os.path.isfile(filepath) and (filename.endswith('.csv') or filename.endswith('.xls')):
                # Read the CSV file and obtain its columns as a tuple
                transactions = self.read_file(filepath)
                # Get the name of the account
                account_name = self.get_account_name(filename)
                # Get bank name
                bank_name = self.account_attributes[account_name]['bank']

                # Validate the account attributes of each file
                self.validate_account_configs(account_name)
                if bank_name not in self.classified_dataframes:
                    self.classified_dataframes[bank_name] = {}
                self.classified_dataframes[bank_name][account_name] = transactions
        

    def get_account_name(self, filename):
        account_name = filename.split('.', 1)[0]
        account_name = str.strip(account_name.split('(', 1)[0])
        return account_name

    
            
