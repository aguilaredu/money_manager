import os

import numpy as np
from pandas import DataFrame, to_datetime, to_numeric

from money_manager.models.statement import Statement

from ..utils import pandas_utils as pu
from ..utils import utils as ut
from ..utils.dataframe_hasher import DataFrameHasher


class BacTransformer:
    def __init__(self, base_dir: str) -> None:
        """Initialize TransformationsBac with account configurations and folder path.

        Args:
            account_configs (dict): Dictionary containing account configurations.
            in_folder_path (str): Path to the input folder.
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

        # Hash the dataframe
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

    def clean_cc(self, df: DataFrame, account_name: str) -> DataFrame:
        """Clean and transform a credit card statement CSV file.

        Args:
            csv_path (str): Path to the CSV file containing credit card statement data.

        Returns:
            DataFrame: Cleaned and transformed DataFrame containing financial transactions.
        """

        # Dictionary that contains column renames
        rename_dict = {
            "Fecha": "date",
            "Monto lempiras": "monto_lempiras",
            "Monto dólares": "monto_dolares",
            "Concepto": "description",
        }

        # Intake the DataFrame from a path and rename columns
        raw_df: DataFrame = df.rename(columns=rename_dict)

        # Apply transformation functions
        clean_df: DataFrame = (
            raw_df.pipe(pu.drop_null_or_empty_rows, col_index=0)
            .pipe(pu.remove_unwanted_chars, columns=["monto_lempiras", "monto_dolares"])
            .pipe(
                pu.replace_empty_string_with_nan,
                columns=["monto_lempiras", "monto_dolares"],
            )
            .pipe(pu.clean_column_values)
            .assign(
                currency=lambda df: np.where(df["monto_lempiras"].isna(), "USD", "HNL")
            )
            .assign(date=lambda df: to_datetime(df["date"], format="%d/%m/%Y"))
            .loc[lambda df: df["date"].notna()]
            .assign(amount=lambda df: df["monto_lempiras"].fillna(df["monto_dolares"]))
            .assign(amount=lambda df: to_numeric(df["amount"], errors="coerce"))
            .assign(amount=lambda df: df["amount"].astype(float).round(2))
            .assign(amount=lambda df: df["amount"] * -1.0)
            .assign(tran_type=lambda df: "Expense")
            .assign(account_name=lambda df: account_name)
            .drop(["monto_lempiras", "monto_dolares"], axis=1)
        )

        # Hash the dataframe
        hashed_df = DataFrameHasher(
            clean_df, self.transaction_structure["cols_to_hash"], "id"
        ).get_hashed_df()

        return hashed_df

    def clean_savings(
        self,
        df: DataFrame,
        acc_name: str,
    ) -> DataFrame:
        """
        Clean and transform a savings account statement CSV file.

        Args:
            csv_path (str): Path to the CSV file containing savings account statement data.

        Returns:
            DataFrame: Cleaned and transformed DataFrame containing financial transactions.
        """

        # Dictionary to rename columns
        rename_dict = {
            "Fecha": "date",
            "Descripción": "description",
            "Débitos": "debits",
            "Créditos": "credits",
        }

        # Intake the DataFrame from a path and rename columns
        raw_df: DataFrame = df.rename(columns=rename_dict)

        # Get the file name and based on this assign a currency. The mapping is in configs.json
        currency = self.account_attributes[acc_name]["currency"]

        # Apply transformation functions
        clean_df: DataFrame = (
            raw_df.pipe(pu.drop_null_or_empty_rows, col_index=0)
            .pipe(pu.remove_unwanted_chars, columns=["debits", "credits"])
            .pipe(pu.replace_empty_string_with_nan, columns=["debits", "credits"])
            .pipe(pu.clean_column_values)
            .assign(date=lambda df: to_datetime(df["date"], format="%d/%m/%Y"))
            .loc[lambda df: df["date"].notna()]
            .assign(debits=lambda df: to_numeric(df["debits"], errors="coerce"))
            .assign(credits=lambda df: to_numeric(df["credits"], errors="coerce"))
            .assign(debits=lambda df: df["debits"].astype(float).round(2))
            .assign(credits=lambda df: df["credits"].astype(float).round(2))
            .assign(debits=lambda df: df["debits"] * -1.0)
            .assign(
                amount=lambda df: np.where(
                    (df["debits"] == 0.00) | (df["debits"].isnull()),
                    df["credits"],
                    df["debits"],
                )
            )
            .assign(
                tran_type=lambda df: np.where(df["amount"] < 0, "Expense", "Income")
            )
            .assign(currency=lambda df: currency)
            .assign(account_name=lambda df: acc_name)
            .drop(
                ["Referencia", "debits", "credits", "Balance", "Account Name"], axis=1
            )
        )

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
            type = self.account_attributes[account_name]["type"]
            return type
        except KeyError:
            raise KeyError(
                f"The type for the account {account_name} is not defined. You can define the account type in accounts.json in the configs folder."
            )
