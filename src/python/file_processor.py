from python.bac_transformer import TransformationsBac
from pandas import DataFrame, read_csv
import os

class FileProcessor:
    def __init__(self, account_configs: str, in_folder_path: str) -> None:
        self.transfomer = TransformationsBac 
        self.account_configs = account_configs
        self.in_folder_path = in_folder_path
        self.classified_files = {}

    def read_csv(self, file_path: str) -> pd.DataFrame:
        try:
            return read_csv(file_path)
        except Exception as e:
            print(f"Error reading {file_path}: {e}")
            return DataFrame()
        
    def get_account_attributes(self, account_name: str) -> dict:

        try:
            account_currency = self.account_configs["account_currencies"][account_name]
            account_type = self.account_configs['account_types'][account_name]
            account_bank = self.account_configs['account_banks'][account_name]

            attributes = {
                'name': account_name,
                'currency': account_currency,
                'type': account_type,
                'bank': account_bank
            }
        except Exception as e:
            print(f"Error getting attributes for acccount {account_name}: {e}")

    def read_and_classify_bank_statements(self, in_folder_path: str)-> dict:
        dataframes_dict = {}

        # Iterate through all files in the directory
        for filename in os.listdir(in_folder_path):
            filepath = os.path.join(in_folder_path, filename)
            
            # Check if it is a file and has a .csv extension
            if os.path.isfile(filepath) and filename.endswith('.csv'):
            # Read the CSV file and obtain its columns as a tuple
                transactions = self.read_csv(filepath)
                # Get the characters before the first occurrence of "(" or "." from the filename to get the account name
                account_name = filename.split('.', 1)[0]
                account_name = str.strip(account_name.split('(', 1)[0])

                # Get the attributets of the account from the configs
                account_attributes = self.get_account_attributes(account_name)
                
                if account_attributes["account_bank"] in dataframes_dict:
                    dataframes_dict["account_bank"].append(transactions)
                else:
                    dataframes_dict["account_bank"] = [transactions]
        
        return dataframes_dict

