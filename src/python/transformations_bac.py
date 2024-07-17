from pandas import DataFrame, read_csv, to_datetime, to_numeric, concat
from pandas_utils import drop_null_or_empty_rows, remove_unwanted_chars, replace_empty_string_with_nan
import numpy as np
import os

class TransformationsBac:
    def __init__(self, account_configs: dict, in_folder_path: str) -> None:
        """Initialize TransformationsBac with account configurations and folder path.

        Args:
            account_configs (dict): Dictionary containing account configurations.
            in_folder_path (str): Path to the input folder.
        """
        self.account_currencies = account_configs['account_currencies'] # Hardcoded in the configs.json file
        self.account_types = account_configs['account_types'] # Hardcoded in the configs.json file
        self.in_folder_path = in_folder_path
        self.processed_files = []

    def clean_credit_card_statement(self, csv_path: str):
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
        raw_df: DataFrame = read_csv(csv_path).rename(columns=rename_dict)

        # Apply transformation functions 
        clean_df: DataFrame = (raw_df
            .pipe(drop_null_or_empty_rows, col_index = 0)
            .pipe(remove_unwanted_chars, columns = ["monto_lempiras", "monto_dolares"])
            .pipe(replace_empty_string_with_nan, columns = ["monto_lempiras", "monto_dolares"])
            .assign(currency=lambda df: np.where(df["monto_lempiras"].isna(), "USD", "HNL"))
            .assign(date=lambda df: to_datetime(df['date'], format='%d/%m/%Y'))
            .assign(amount=lambda df: df['monto_lempiras'].fillna(df['monto_dolares']))
            .assign(amount=lambda df: to_numeric(df['amount'], errors='coerce'))
            .assign(amount=lambda df: df['amount'].astype(float).round(2))
            .assign(amount=lambda df: df['amount'] * -1.0)
            .assign(tran_type=lambda df: 'Expense')
            .drop(['monto_lempiras', 'monto_dolares'], axis=1))

        return clean_df 

    def clean_savings_account_statement(self, csv_path: str) -> DataFrame:
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
        raw_df: DataFrame = read_csv(csv_path).rename(columns=rename_dict)

        # Get the file name and based on this assign a currency. The mapping is in configs.json 
        currency = self.get_account_currency(csv_path)

        # Apply transformation functions 
        clean_df: DataFrame = (raw_df
            .pipe(drop_null_or_empty_rows, col_index = 0)
            .pipe(remove_unwanted_chars, columns = ["debits", "credits"])
            .pipe(replace_empty_string_with_nan, columns = ["debits", "credits"])
            .assign(date=lambda df: to_datetime(df['date'], format='%d/%m/%Y'))
            .assign(debits=lambda df: to_numeric(df['debits'], errors='coerce'))
            .assign(credits=lambda df: to_numeric(df['credits'], errors='coerce'))
            .assign(debits=lambda df: df['debits'].astype(float).round(2))
            .assign(credits=lambda df: df['credits'].astype(float).round(2))
            .assign(debits=lambda df: df['debits'] * -1.0)
            .assign(amount=lambda df: np.where((df['debits'] == 0.00) | (df['debits'].isnull()), df['credits'], df['debits']))
            .assign(tran_type=lambda df: np.where(df['amount'] < 0, 'Expense', 'Income'))
            .assign(currency=lambda df: currency)
            .drop(['Referencia', 'debits', 'credits', 'Balance'], axis=1))

        return clean_df

    def get_account_currency(self, csv_path: str) -> str:
        """
        Get the currency for the account based on the CSV file name.

        Args:
            csv_path (str): Path to the CSV file containing account data.

        Returns:
            str: The currency associated with the account.

        Raises:
            KeyError: If the account currency is not defined in the configurations.
        """
        # Get the file name and based on this assign a currency. The mapping is in configs.json 
        filename = os.path.splitext(os.path.basename(csv_path))[0]
        try:
            currency = self.account_currencies[filename]
            return currency
        except KeyError:
            raise KeyError(f'The currency for the account {filename} is not defined. You can define the account currency in configs.json.')
        
    def get_account_type(self, csv_path: str) -> str:
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
        filename = os.path.splitext(os.path.basename(csv_path))[0]
        try:
            type = self.account_types[filename]
            return type
        except KeyError:
            raise KeyError(f'The type for the account {filename} is not defined. You can define the account type in configs.json.')
    
    # TODO: Not implemented yet
    def clean_bac_statements(self, df_list: list):
        """Clean savings and credit card statements for a given path list. The correct function is applied to the specific account name
        which is defined in configs.json. 
        """

        cleaned_dataframes = []
        for dataframe in df_list:
            account_name = 'get_account_name' # Get the account name from a column, not filename, somehow
            account_type = self.get_account_type(account_name) # Change to account name

            # Apply the correct transformation based on the account type
            if account_type == 'credit_card':
                clean_statement = self.clean_credit_card_statement(dataframe)
            elif account_type == 'savings':
                clean_statement = self.clean_savings_account_statement(dataframe)
            else:
                raise KeyError(f'The account type {account_type} is not supported. Supported values for account type are {{credit_card, savings}}.')
            
            # Append the clean statement to the cleaned dataframes list 
            cleaned_dataframes.append(clean_statement)

        # Concatenate all the dataframes into a single one 
        consolidated_statements = concat(cleaned_dataframes)

        return consolidated_statements