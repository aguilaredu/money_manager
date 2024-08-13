from difflib import SequenceMatcher
import os
from utils import load_config
from dataframe_hasher import DataFrameHasher

class DuplicateRemover():
    def __init__(self, base_dir, existing_transactions, new_transactions, match_threshold = 0.80) -> None:
        self.existing_transactions = existing_transactions
        self.new_transactions = new_transactions
        self.match_threshold = match_threshold
        self.updated_existing_transactions = None

        # Required to hash rows consistently
        configs_dir = os.path.join(base_dir, 'configs')
        transaction_structure_path = os.path.join(configs_dir, 'transaction_structure.json')
        self.transaction_structure = load_config(transaction_structure_path)

    def get_updated_existing_transactions(self):
        self.remove_duplicates()
        self.hash_rows()
        return self.updated_existing_transactions

    def hash_rows(self):
        dataframe_hasher = DataFrameHasher(self.updated_existing_transactions, self.transaction_structure['cols_to_hash'], 'id')
        self.updated_existing_transactions = dataframe_hasher.get_hashed_df()

    def calculate_similarity(self, a, b):
        return SequenceMatcher(None, a, b).ratio()

    def remove_duplicates(self):        
        updated_transactions = self.existing_transactions.copy()
        for _, new_row in self.new_transactions.iterrows():
            # Step 1: Filter existing_transactions by date, amount, and account_name
            subset_df = self.existing_transactions[
                (self.existing_transactions['date'] == new_row['date']) &
                (self.existing_transactions['amount'] == new_row['amount']) &
                (self.existing_transactions['account_name'] == new_row['account_name']) &
                (self.existing_transactions['description'] != new_row['description'])
            ]
            
            # Step 2: If no matching rows, go to the next iteration
            if subset_df.empty:
                continue

            # Step 3: Calculate similarity between descriptions
            subset_df['similarity_percentage'] = subset_df['description'].apply(
                lambda desc: self.calculate_similarity(desc, new_row['description'])
            )
            
            # Step 4: Filter for similarities above the threshold
            filtered_df = subset_df[subset_df['similarity_percentage'] >= self.match_threshold]

            # Step 5: If no similar rows, go to the next iteration
            if filtered_df.empty:
                continue

            # Step 6: If more than one similar row, print a warning and continue
            if len(filtered_df) > 1:
                print(f"Warning: Multiple matching transactions found for {new_row['description']}.")
                continue

            # Step 7: Update the existing_transaction dataframe
            old_row = filtered_df.iloc[0]
            print(f"Updating transaction:\nOld: {old_row.to_dict()}\nNew: {new_row.to_dict()}")

            # Update the existing_transaction dataframe
            updated_transactions.loc[filtered_df.index, 'description'] = new_row['description']

        self.updated_existing_transactions = updated_transactions