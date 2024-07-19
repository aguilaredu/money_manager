import os
from processor import Processor
import pandas as pd
from existing_transactions import ExistingTransactions
def main():
    # Get the directory of the configs file 
    script_dir = os.path.dirname(os.path.abspath(__file__))
    base_dir = os.path.dirname(os.path.dirname(script_dir))
    configs_dir = os.path.join(base_dir, 'configs')

    # Test transformations class
    processor = Processor(base_dir=base_dir)
    transactions = processor.get_processed_data()
    
    existing_transactions = ExistingTransactions(base_dir)
    existing_transactions.build_output_path()
    transactions.to_csv(existing_transactions.get_output_path())



if __name__ == "__main__":
    main()