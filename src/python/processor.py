from input_transactions import InputTransactions
from existing_transactions import ExistingTransactions
from bac_transformer import BacTransformer
from exchange_rate import ExchangeRates
from pandas_utils import fill_missing_exchange_rates
from duplicate_remover import DuplicateRemover
import pandas as pd

class Processor():
    def __init__(self, base_dir) -> None:
        self.base_dir = base_dir
        self.existing_transactions = pd.DataFrame()
        self.new_transactions = pd.DataFrame()
        self.processed_data = pd.DataFrame()
        self.exchange_rate_df = None

    def get_processed_data(self):
        self.process_data()
        return self.processed_data
    
    def process_data(self):
        # Read the existing transactions
        self.existing_transactions = ExistingTransactions(self.base_dir).get_existing_transactions()
        self.apply_transformation_to_inputs()
        self.remove_duplicates()
        self.join_existing_with_new_transactions()
        self.apply_exchange_rates()

    def remove_duplicates(self):
        duplicate_remover = DuplicateRemover(self.existing_transactions, self.new_transactions)
        duplicate_remover.remove_duplicates()
        self.existing_transactions = duplicate_remover.updated_existing_transactions
        self.new_transactions = duplicate_remover.updated_new_transactions
    
    def apply_exchange_rates(self):
        self.get_exchange_rate_df()
        self.processed_data = fill_missing_exchange_rates(self.processed_data, 'date', self.exchange_rate_df)

    def apply_transformation_to_inputs(self):
        input_transactions = InputTransactions(self.base_dir).get_classified_dataframes()
        dataframes = []
        bank_transformation_mapping = {
            "BAC": BacTransformer
        }

        for bank in input_transactions:
            bank_transactions = input_transactions[bank]
            dataframes.append(bank_transformation_mapping[bank](bank_transactions, self.base_dir).get_clean_bac_statements())

        self.new_transactions = pd.concat(dataframes)

    def get_exchange_rate_df(self):
        try:
            print("Getting exchange rate information from https://www.alphavantage.co.")
            self.exchange_rate_df = ExchangeRates(self.base_dir).get_exchange_rate_dataframe()
        except Exception as e:
            print(f"Could not fetch exchange rates. Continuing execution. Error: {e}")

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
        self.processed_data = pd.concat([self.existing_transactions, new_valid_transactions])