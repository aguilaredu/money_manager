from pandas import DataFrame, concat

from money_manager.existing_transactions import Ledger
from money_manager.input_transactions import Inputs
from money_manager.models.statement import Statement
from money_manager.utils.merger import Merger
from money_manager.utils.utils import delete_inputs, get_tran_cols


class Processor:
    def __init__(self, base_dir: str, delete_inputs: bool = False) -> None:
        self.base_dir: str = base_dir
        self.ledger: DataFrame = DataFrame()
        self.cleaned_statements: list[Statement] = []
        self.delete_inputs: bool = delete_inputs

    def get_ledger(self):
        return self.ledger

    def process(self) -> None:
        # Read the existing ledger file or create a new one
        ledger = Ledger(self.base_dir).get()

        # Read the new files and get the cleaned statements
        inputs: Inputs = Inputs(self.base_dir)
        inputs.process()
        clean_statements: list[Statement] = inputs.get_statements()

        # Merge each input with the ledger considering non-exact duplicates
        for stmt in clean_statements:
            stmt_data = stmt.data
            filename = stmt.filename

            print(f"Merging {filename}", end=" | ")

            # This merges considering non-exact matches since the bank can slightly
            # change the transaction description throughout the lifetime of the statement
            merger = Merger(self.base_dir, ledger, stmt_data)
            ledger = merger.get_clean_ledger()
            ledger = self.concat_statement(ledger, stmt_data)
            stmt.merged = True

        tran_cols = get_tran_cols()
        self.ledger = ledger.sort_values(
            by=["date", "account_name"], ascending=True
        ).reset_index()[tran_cols]

        # Delete the input files
        if self.delete_inputs:
            delete_inputs(clean_statements)

    def concat_statement(self, ledger: DataFrame, stmt: DataFrame):
        # Perform a left join to identify new records since the bank statement exports can contain records already in the existing
        # transactions. Add a placeholder column with a constant value to perform the join without pandas
        # adding _x and _y to the output column names because both dataframes have the same column names.

        # Create a copy of the ledger
        ledger_c = ledger.copy()
        ledger_c["placeholder"] = 1
        valid_stmt = (
            stmt.merge(
                ledger_c[["placeholder"]],
                left_index=True,
                right_index=True,
                how="left",
            )
            .query("placeholder.isnull()")
            .sort_values(by=["date"], ascending=False)
            .drop(["placeholder"], axis=1)
        )

        # Drop the helper column and concatenate
        ledger_c.drop(["placeholder"], axis=1, inplace=True)
        concatenated: DataFrame = concat([ledger_c, valid_stmt])

        print(f"added {valid_stmt.shape[0]} rows")

        return concatenated
