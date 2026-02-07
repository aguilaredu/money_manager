import os
from sqlite3.dbapi2 import DataError

import numpy as np
from pandas import DataFrame, to_datetime, to_numeric

import money_manager.utils.pandas_utils as pu
from money_manager.models.statement import Statement
from money_manager.utils import utils as ut
from money_manager.utils.dataframe_hasher import DataFrameHasher


class FicohsaTransformer:
    def __init__(self, base_dir: str) -> None:
        """Initialize FicohsaTransformer with account configurations and folder path.

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

        # Clean savings statement
        print(f"Cleaning {filename} | {bank_name}-{acc_type} | ", end="")
        if acc_type == "savings":
            clean_stmt_data = self.clean_savings(stmt_data, acc_name)
        else:
            print("account type not supported")
            return None

        if clean_stmt_data is None:
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
            False,
        )

        print(f"success, parsed {clean_stmt_data.shape[0]} rows")

        return clean_stmt

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
        rename_dict = {
            "Fecha": "date",
            "Descripción": "description",
            "Débito": "debits",
            "Crédito": "credits",
            "Balance": "balance",
            "N° Cheque": "check_num",
        }

        try:
            header_row_index = self._get_header_index(df, "Fecha")
        except ValueError as e:
            print(f"Could not find header row: {e}")
            # return DataFrame(columns=["date", "account_name", "description", "amount"])

        # 5th row (0 indexed) contains the headers
        df.columns = df.iloc[header_row_index]

        # Drop header and rows above it, then rename columns
        raw_df = (
            df.drop(df.index[0 : header_row_index + 1])
            .reset_index(drop=True)
            .rename(columns=rename_dict)
        )

        # Get the file name and based on this assign a currency. The mapping is in configs.json
        currency = self.account_attributes[acc_name]["currency"]

        # Apply transformation functions
        clean_df: DataFrame = (
            raw_df.pipe(pu.drop_null_or_empty_rows, col_index=0)
            .pipe(pu.remove_unwanted_chars, columns=["debits", "credits"])
            .pipe(pu.replace_empty_string_with_nan, columns=["debits", "credits"])
            .pipe(pu.clean_column_values)
            .assign(
                date=lambda df: to_datetime(
                    df["date"], format="%d/%m/%Y", errors="coerce"
                )
            )
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
        )
        final_stmt: DataFrame = clean_df[
            ["date", "account_name", "description", "amount", "tran_type", "currency"]
        ]
        return final_stmt

    def _get_header_index(self, df: DataFrame, keyword: str) -> int:
        """Find the row index of the header by searching for a keyword in the first column."""
        for i, row in df.iterrows():
            if str(row.iloc[0]) == keyword:
                return i
        raise ValueError(f"Header keyword '{keyword}' not found in the first column.")

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
