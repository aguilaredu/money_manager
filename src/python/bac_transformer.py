from pandas import DataFrame, read_csv, to_datetime, to_numeric, concat, merge
from pandas_utils import (drop_null_or_empty_rows, remove_unwanted_chars,
                          replace_empty_string_with_nan, clean_column_values,
                          fill_missing_exchange_rates)
import numpy as np
import os
from utils import load_config
from dataframe_hasher import DataFrameHasher
class BacTransformer:
    def __init__(self, df_dict: dict, base_dir: str) -> None:
        """Initialize TransformationsBac with account configurations and folder path.

        Args:
            account_configs (dict): Dictionary containing account configurations.
            in_folder_path (str): Path to the input folder.
        """
        configs_dir = os.path.join(base_dir, 'configs')
        accounts_attributes_path = os.path.join(configs_dir, 'accounts.json') 
        transaction_structure_path = os.path.join(configs_dir, 'transaction_structure.json')
        
        self.account_attributes = load_config(accounts_attributes_path)
        self.processed_files = []
        self.df_dict = df_dict
        self.processed_transactions = DataFrame()        
        self.transaction_structure = load_config(transaction_structure_path)


    def clean_credit_card_statement(self, df: DataFrame, account_name: str):
        """Clean and transform a credit card statement CSV file.

        Args:
            csv_path (str): Path to the CSV file containing credit card statement data.

        Returns:
            DataFrame: Cleaned and transformed DataFrame containing financial transactions.
        """

        # Dictionary that contains column renames 
        rename_dict = {
            'Fecha': 'date',
            'Monto lempiras': 'monto_lempiras',
            'Monto dólares': 'monto_dolares',
            'Concepto': 'description'
        }
        
        # Intake the DataFrame from a path and rename columns 
        raw_df: DataFrame = df.rename(columns=rename_dict)

        # Apply transformation functions 
        clean_df: DataFrame = (raw_df
            .pipe(drop_null_or_empty_rows, col_index = 0)
            .pipe(remove_unwanted_chars, columns = ["monto_lempiras", "monto_dolares"])
            .pipe(replace_empty_string_with_nan, columns = ["monto_lempiras", "monto_dolares"])
            .pipe(clean_column_values)
            .assign(currency=lambda df: np.where(df["monto_lempiras"].isna(), "USD", "HNL"))
            .assign(date=lambda df: to_datetime(df['date'], format='%d/%m/%Y'))
            .assign(amount=lambda df: df['monto_lempiras'].fillna(df['monto_dolares']))
            .assign(amount=lambda df: to_numeric(df['amount'], errors='coerce'))
            .assign(amount=lambda df: df['amount'].astype(float).round(2))
            .assign(amount=lambda df: df['amount'] * -1.0)
            .assign(tran_type=lambda df: 'Expense')
            .assign(exchange_rate=lambda df: np.nan)
            .assign(account_name=lambda df: account_name)
            .drop(['monto_lempiras', 'monto_dolares'], axis=1))

        return clean_df 

    def clean_savings_account_statement(self, account_name: str, df: DataFrame) -> DataFrame:
        """
        Clean and transform a savings account statement CSV file.

        Args:
            csv_path (str): Path to the CSV file containing savings account statement data.

        Returns:
            DataFrame: Cleaned and transformed DataFrame containing financial transactions.
        """

        # Dictionary to rename columns 
        rename_dict = {
            'Fecha': 'date',
            'Descripción': 'description',
            'Débitos': 'debits',
            'Créditos': 'credits'
        }
        
        # Intake the DataFrame from a path and rename columns 
        raw_df: DataFrame = df.rename(columns=rename_dict)

        # Get the file name and based on this assign a currency. The mapping is in configs.json 
        currency = self.account_attributes[account_name]['currency']

        # Apply transformation functions 
        clean_df: DataFrame = (raw_df
            .pipe(drop_null_or_empty_rows, col_index = 0)
            .pipe(remove_unwanted_chars, columns = ["debits", "credits"])
            .pipe(replace_empty_string_with_nan, columns = ["debits", "credits"])
            .pipe(clean_column_values)
            .assign(date=lambda df: to_datetime(df['date'], format='%d/%m/%Y'))
            .assign(debits=lambda df: to_numeric(df['debits'], errors='coerce'))
            .assign(credits=lambda df: to_numeric(df['credits'], errors='coerce'))
            .assign(debits=lambda df: df['debits'].astype(float).round(2))
            .assign(credits=lambda df: df['credits'].astype(float).round(2))
            .assign(debits=lambda df: df['debits'] * -1.0)
            .assign(amount=lambda df: np.where((df['debits'] == 0.00) | (df['debits'].isnull()), df['credits'], df['debits']))
            .assign(tran_type=lambda df: np.where(df['amount'] < 0, 'Expense', 'Income'))
            .assign(exchange_rate=lambda df: np.nan)
            .assign(currency=lambda df: currency)
            .assign(account_name=lambda df: account_name)
            .drop(['Referencia', 'debits', 'credits', 'Balance'], axis=1))

        return clean_df
        
    def get_account_type(self, account_name: str) -> str:
        """
        Get the type for the account based on the CSV file name.

        Args:
            csv_path (str): Path to the CSV file containing account data.

        Returns:
            str: The type associated with the account.

        Raises:
            KeyError: If the account type is not defined in the configurations.
        """
        # Get the file name and based on this assign a type. The mapping is in configs.json 
        try:
            type = self.account_attributes[account_name]['type']
            return type
        except KeyError:
            raise KeyError(f'The type for the account {account_name} is not defined. You can define the account type in accounts.json in the configs folder.')
    
    def get_clean_statements(self):
        """Clean savings and credit card statements for a given path list. The correct function is applied to the specific account name
        which is defined in configs.json. 
        """

        cleaned_dataframes = []
        for account_name in self.df_dict.keys():
            transaction_df = self.df_dict[account_name]
            account_type = self.get_account_type(account_name) # Change to account name

            # Apply the correct transformation based on the account type
            if account_type == 'credit_card':
                clean_statement = self.clean_credit_card_statement(transaction_df, account_name)
            elif account_type == 'savings':
                clean_statement = self.clean_savings_account_statement(account_name, transaction_df)
            else:
                raise KeyError(f'The account type {account_type} is not supported. Supported values for account type are {{credit_card, savings}}.')
            
            # Append the clean statement to the cleaned dataframes list 
            cleaned_dataframes.append(clean_statement)

        # Add hash to each row
        hashed_df = DataFrameHasher(concat(cleaned_dataframes), self.transaction_structure['cols_to_hash'], 'id').get_hashed_df()

        # Concatenate all the dataframes into a single one 
        self.processed_transactions = hashed_df

        return self.processed_transactions