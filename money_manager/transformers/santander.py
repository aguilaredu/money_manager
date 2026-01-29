import os

import numpy as np
from pandas import DataFrame, to_datetime, to_numeric

from money_manager.models.statement import Statement
from money_manager.utils import utils as ut
from money_manager.utils.dataframe_hasher import DataFrameHasher
from money_manager.utils.pandas_utils import (
    clean_column_values,
    drop_null_or_empty_rows,
    remove_unwanted_chars,
    replace_empty_string_with_nan,
)


class SantanderTransformer:
    def __init__(self, base_dir: str) -> None:
        """Initialize SantanderTransformer with account configurations and folder path.

        Args:
            base_dir (str): Path to the project base directory.
        """
        configs_dir = os.path.join(base_dir, "configs")
        accounts_attributes_path = os.path.join(configs_dir, "accounts.json")
        transaction_structure_path = os.path.join(
            configs_dir, "transaction_structure.json"
        )

        self.account_attributes: dict[str, dict[str, str]] = ut.load_config(
            accounts_attributes_path
        )
        self.processed_transactions: DataFrame = DataFrame()
        self.transaction_structure = ut.load_config(transaction_structure_path)

    def clean(self, statement: Statement) -> Statement | None:
        clean_stmt_data: DataFrame | None = None
        stmt_data = statement.data
        acc_name = statement.account_name
        acc_type = statement.account_type
        filename = statement.filename
        bank_name = statement.bank_name
        filepath = statement.filepath
        currency = statement.currency

        # Clean either credit_card or savings statement
        print(f"Cleaning {filename} | {bank_name}-{acc_type} | ", end="")
        if acc_type == "credit_card":
            clean_stmt_data = self.clean_cc(stmt_data, acc_name)
        elif acc_type == "savings":
            clean_stmt_data = self.clean_savings(stmt_data, acc_name)
        else:
            print("account type not supported")
            return None

        # Hash the dataframe`
        hashed_data = DataFrameHasher(
            clean_stmt_data, self.transaction_structure["cols_to_hash"], "id"
        ).get_hashed_df()

        # Return the new statement object with the cleaned data
        clean_stmt = Statement(
            hashed_data,
            filepath,
            filename,
            acc_name,
            bank_name,
            currency,
            acc_type,
        )

        print(f"success, parsed {clean_stmt_data.shape[0]} rows")

        return clean_stmt

    def clean_cc(self, df: DataFrame, acc_name: str) -> DataFrame:
        """Clean and transform a credit card statement CSV file.

        Args:
            df (DataFrame): DataFrame containing credit card statement data.
            account_name (str): Name of the account.

        Returns:
            DataFrame: Cleaned and transformed DataFrame.
        """
        # Dictionary to rename columns
        rename_dict = {
            "FECHA OPERACIÓN": "date",
            "CONCEPTO": "description",
            "IMPORTE EUR": "amount",
        }

        # Intake the DataFrame from a path and rename columns
        df.columns = df.iloc[6]
        raw_df: DataFrame = (
            df.iloc[7:].reset_index(drop=True).rename(columns=rename_dict)
        )
        # Get the file name and based on this assign a currency. The mapping is in configs.json
        currency = self.account_attributes[acc_name]["currency"]
        # print(raw_df.iloc[6:].pipe(lambda d: d.rename(columns=d.iloc[0])))
        # Apply transformation functions
        clean_df: DataFrame = (
            raw_df.pipe(drop_null_or_empty_rows, col_index=0)
            .pipe(remove_unwanted_chars, columns=["amount"])
            .pipe(replace_empty_string_with_nan, columns=["amount"])
            .pipe(clean_column_values)
            .assign(
                date=lambda df: to_datetime(
                    df["date"], format="%d/%m/%Y", errors="coerce"
                )
            )
            .loc[lambda df: df["date"].notna()]
            .assign(amount=lambda df: to_numeric(df["amount"], errors="coerce"))
            .assign(amount=lambda df: df["amount"].astype(float).round(2))
            .assign(
                tran_type=lambda df: np.where(df["amount"] < 0, "Expense", "Transfer")
            )
            .assign(currency=lambda df: currency)
            .assign(account_name=lambda df: acc_name)
        )

        return clean_df

    def clean_savings(
        self,
        df: DataFrame,
        acc_name: str,
    ) -> DataFrame:
        """
        Clean and transform a savings account statement CSV file.

        Args:
            df (DataFrame): DataFrame containing savings account statement data.
            acc_name (str): Name of the account.

        Returns:
            DataFrame: Cleaned and transformed DataFrame containing financial transactions.
        """
        # Dictionary to rename columns
        rename_dict = {
            "FECHA OPERACIÓN": "date",
            "CONCEPTO": "description",
            "IMPORTE EUR": "amount",
        }

        # Intake the DataFrame from a path and rename columns
        df.columns = df.iloc[6]
        raw_df: DataFrame = (
            df.iloc[7:].reset_index(drop=True).rename(columns=rename_dict)
        )
        # Get the file name and based on this assign a currency. The mapping is in configs.json
        currency = self.account_attributes[acc_name]["currency"]
        # print(raw_df.iloc[6:].pipe(lambda d: d.rename(columns=d.iloc[0])))
        # Apply transformation functions
        clean_df: DataFrame = (
            raw_df.pipe(drop_null_or_empty_rows, col_index=0)
            .pipe(remove_unwanted_chars, columns=["amount"])
            .pipe(replace_empty_string_with_nan, columns=["amount"])
            .pipe(clean_column_values)
            .assign(
                date=lambda df: to_datetime(
                    df["date"], format="%d/%m/%Y", errors="coerce"
                )
            )
            .loc[lambda df: df["date"].notna()]
            .assign(amount=lambda df: to_numeric(df["amount"], errors="coerce"))
            .assign(amount=lambda df: df["amount"].astype(float).round(2))
            .assign(
                tran_type=lambda df: np.where(df["amount"] < 0, "Expense", "Income")
            )
            .assign(currency=lambda df: currency)
            .assign(account_name=lambda df: acc_name)
            .drop(["FECHA VALOR", "SALDO"], axis=1)
        )

        return clean_df

    def get_account_type(self, account_name: str) -> str:
        """
        Get the type for the account based on the CSV file name.

        Args:
            account_name (str): Name of the account.

        Returns:
            str: The type associated with the account.

        Raises:
            KeyError: If the account type is not defined in the configurations.
        """
        # Get the file name and based on this assign a type. The mapping is in configs.json
        try:
            type = self.account_attributes[account_name]["type"]
            return type
        except KeyError:
            raise KeyError(
                f"The type for the account {account_name} is not defined. You can define the account type in accounts.json in the configs folder."
            )
