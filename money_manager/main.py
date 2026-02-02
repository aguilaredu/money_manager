import os

from money_manager.existing_transactions import Ledger
from money_manager.input_transactions import Inputs
from money_manager.processor import Processor


def main():
    print("Starting file processing..")

    # Get the directory of the configs file
    script_dir = os.path.dirname(os.path.abspath(__file__))
    base_dir = os.path.dirname(script_dir)
    out_path = os.path.join(base_dir, "data/out/transactions.csv")

    # Process data
    processor = Processor(base_dir=base_dir, delete_inputs=True)
    processor.process()
    ledger = processor.get_ledger()

    # Save
    ledger.to_csv(out_path, encoding="utf_8_sig", index=False)

    print(f"Success, ledger has {ledger.shape[0]} rows")


if __name__ == "__main__":
    main()
