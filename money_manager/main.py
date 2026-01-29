import os

from money_manager.existing_transactions import Ledger
from money_manager.input_transactions import Inputs
from money_manager.processor import Processor


def main():
    print("Starting file processing..")

    # Get the directory of the configs file
    script_dir = os.path.dirname(os.path.abspath(__file__))
    base_dir = os.path.dirname(script_dir)

    # Process data
    processor = Processor(base_dir=base_dir)
    processor.process()
    ledger = processor.get_ledger()

    existing_transactions = Ledger(base_dir)
    existing_transactions.build_output_path()
    ledger.to_csv(existing_transactions.get_output_path(), encoding="utf_8_sig")

    # Delete input files
    input_transactions = Inputs(base_dir, clear_input_dir=False)
    input_transactions.clear_input_directory()

    print(f"Success, ledger has {ledger.shape[0]} rows")


if __name__ == "__main__":
    main()
