from input_transactions import InputTransactions
from existing_transactions import ExistingTransactions
from transformer_bac import BacTransformer
from transformer_santander import SantanderTransformer
from transformer_revolut import RevolutTransformer
from duplicate_remover import DuplicateRemover
import pandas as pd

class Processor():
    def __init__(self, base_dir) -> None:
        self.base_dir = base_dir
        self.existing_transactions = pd.DataFrame()
        self.new_transactions = pd.DataFrame()
        self.processed_data = pd.DataFrame()

    def get_processed_data(self):
        self.process_data()
        return self.processed_data
    
    def process_data(self):
        # Read the existing transactions
        self.existing_transactions = ExistingTransactions(self.base_dir).get_existing_transactions()
        self.apply_transformation_to_inputs()
        self.remove_duplicates()
        self.join_existing_with_new_transactions()

    def remove_duplicates(self):
        duplicate_remover = DuplicateRemover(self.base_dir, self.existing_transactions, self.new_transactions)
        self.existing_transactions = duplicate_remover.get_updated_existing_transactions()
    
    def apply_transformation_to_inputs(self):
        input_transactions = InputTransactions(self.base_dir).get_classified_dataframes()
        dataframes = []
        bank_transformation_mapping = {
            "BAC": BacTransformer,
            "SANTANDER": SantanderTransformer,
            "REVOLUT": RevolutTransformer
        }

        for bank in input_transactions:
            bank_transactions = input_transactions[bank]
            dataframes.append(bank_transformation_mapping[bank](bank_transactions, self.base_dir).get_clean_statements())

        self.new_transactions = pd.concat(dataframes)


    def join_existing_with_new_transactions(self):
        
        # Perform a left join to identify new records since the bank statement exports can contain records already in the existing
        # transactions. Add a placeholder column with a constant value to perform the join without pandas
        # adding _x and _y to the output column names because both dataframes have the same column names.
        self.existing_transactions['placeholder'] = 1
        new_valid_transactions = self.new_transactions\
                .merge(self.existing_transactions[['placeholder']], left_index=True, right_index=True, how='left')\
                .query('placeholder.isnull()')\
                .sort_values(by=['date'], ascending=False)\
                .drop(['placeholder'], axis=1)
        
        self.existing_transactions.drop(['placeholder'], axis=1, inplace=True)
        if new_valid_transactions.empty:
            self.processed_data = self.existing_transactions
        else:
            self.processed_data = pd.concat([self.existing_transactions, new_valid_transactions])
