import os
from difflib import SequenceMatcher

from pandas import DataFrame

from money_manager.utils.dataframe_hasher import DataFrameHasher
from money_manager.utils.utils import load_config


class Merger:
    def __init__(
        self,
        base_dir: str,
        existing_transactions: DataFrame,
        new_transactions: DataFrame,
        match_threshold: float = 0.80,
    ) -> None:
        self.ledger: DataFrame = existing_transactions
        self.new_transactions: DataFrame = new_transactions
        self.match_threshold: float = match_threshold
        self.updated_existing_transactions: DataFrame = DataFrame()

        # Required to hash rows consistently
        configs_dir = os.path.join(base_dir, "configs")
        transaction_structure_path = os.path.join(
            configs_dir, "transaction_structure.json"
        )
        self.transaction_structure = load_config(transaction_structure_path)

    def get_clean_ledger(self):
        self.remove_duplicates()
        self.hash_rows()
        return self.updated_existing_transactions

    def hash_rows(self):
        dataframe_hasher = DataFrameHasher(
            self.updated_existing_transactions,
            self.transaction_structure["cols_to_hash"],
            "id",
        )
        self.updated_existing_transactions = dataframe_hasher.get_hashed_df()

    def calculate_similarity(self, a: str, b: str):
        return SequenceMatcher(None, a, b).ratio()

    def remove_duplicates(self):
        # Make a copy to avoid modifying the original dataframe
        ledger_c = self.ledger.copy()
        threshold = self.match_threshold

        # Keep track of how many rows are merged
        merged_rows = 0
        for _, new_row in self.new_transactions.iterrows():
            # Step 1: Filter existing_transactions by date, amount, and account_name
            subset_df: DataFrame = self.ledger[
                (self.ledger["date"] == new_row["date"])
                & (self.ledger["amount"] == new_row["amount"])
                & (self.ledger["account_name"] == new_row["account_name"])
                & (self.ledger["description"] != new_row["description"])
            ]

            # Step 2: If no matching rows, go to the next iteration
            if subset_df.empty:
                continue

            # Step 3: Calculate similarity between descriptions
            subset_df["similarity_percentage"] = subset_df["description"].apply(
                lambda desc: self.calculate_similarity(desc, new_row["description"])
            )

            # Step 4: Filter for similarities above the threshold
            filtered_df = subset_df[subset_df["similarity_percentage"] >= threshold]

            # Step 5: If no similar rows, go to the next iteration
            if filtered_df.empty:
                continue

            merged_rows += filtered_df.shape[0]

            # Update the existing_transaction dataframe
            ledger_c.loc[filtered_df.index, "description"] = new_row["description"]

        print(f"merged {merged_rows} rows", end=" | ")
        self.updated_existing_transactions = ledger_c
