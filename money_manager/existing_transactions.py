import os
from os import path

from pandas import DataFrame, read_csv

from money_manager.utils.dataframe_hasher import DataFrameHasher
from money_manager.utils.exceptions import FileReadError
from money_manager.utils.pandas_utils import replace_empty_string_with_nan
from money_manager.utils.utils import enforce_dataframe_schema, load_config


class Ledger:
    """The existing transaction processor
    will validate the file path
    will read or create the transactions from the path
    will validate the and clean the transactions that already are there
    will create an empty file if the transactions are not there.
    """

    def __init__(self, base_dir: str) -> None:
        configs_dir = os.path.join(base_dir, "configs")
        transaction_structure_path = os.path.join(
            configs_dir, "transaction_structure.json"
        )
        configs_path = os.path.join(configs_dir, "configs.json")
        configs = load_config(configs_path)
        self.output_path = configs["path_configs"]["out_file_path"]
        self.base_dir = base_dir
        self.existing_transactions: DataFrame = DataFrame()
        self.transaction_structure = load_config(transaction_structure_path)

    def get(self) -> DataFrame:
        self.build_output_path()

        if path.exists(self.output_path):
            self.read_existing_transactions()
            self.clean_dataframe()
            self.hash_rows()
        else:
            self.create_transactions_file()

        print(f"Read ledger with {self.existing_transactions.shape[0]} rows")
        return self.existing_transactions

    def hash_rows(self):
        dataframe_hasher = DataFrameHasher(
            self.existing_transactions, self.transaction_structure["cols_to_hash"], "id"
        )
        self.existing_transactions = dataframe_hasher.get_hashed_df()

    def clean_dataframe(self):
        self.validate_transaction_df()
        columns = list(self.transaction_structure["structure"].keys())
        self.existing_transactions = replace_empty_string_with_nan(
            df=self.existing_transactions, columns=columns
        )
        self.hash_rows()

    def validate_transaction_df(self):
        """Enforce transaction file structure. Important because a different datatype inference can cause
        the entries to be rehashed to a different hash. For example date vs datetime are different objects therefore
        have different hashes.
        """
        if not (self.existing_transactions is None):
            self.existing_transactions = enforce_dataframe_schema(
                self.existing_transactions, self.transaction_structure["structure"]
            )

    def validate_output_dir(self):
        directory = os.path.dirname(self.output_path)
        if not (path.exists(directory)):
            raise ValueError(f"The directory does not exist. Directory: {directory}")

    def create_transactions_file(self):
        try:
            with open(self.output_path, "w") as my_empty_csv:
                pass
        except Exception as e:
            print(f"Error creating output file. Path: {self.output_path}. {e}")

    def read_existing_transactions(self):
        try:
            self.existing_transactions = read_csv(self.output_path)
        except Exception as e:
            raise FileReadError(f"Couldn't read transaction file.", self.output_path)

    def build_output_path(self) -> None:
        """Takes in the file path and:
        if the file path is the default builds the file path and returns it.
        if the file path is not the default one checks if the directory is valid
            if not - error
            else return the full path
        if the path is valid then don't modify the filepath read from the configs.
        """
        if self.output_path == "data/out/transactions.csv":
            self.output_path = os.path.join(self.base_dir, "data/out/transactions.csv")
        else:
            self.validate_output_dir()  # Validate and raise exception if not valid.

    def get_output_path(self):
        return self.output_path
