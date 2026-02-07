import os
import re
import shutil

from pandas import (
    DataFrame,
    read_csv,
    read_excel,  # pyright: ignore [ reportUnknownVariableType]
)

from money_manager.models.statement import Statement
from money_manager.transformers.bac import BacTransformer
from money_manager.transformers.ficohsa import FicohsaTransformer
from money_manager.transformers.revolut import RevolutTransformer
from money_manager.transformers.santander import SantanderTransformer
from money_manager.utils.utils import get_out_file_path, load_config


class Inputs:
    def __init__(self, base_dir: str, clear_input_dir: bool = False) -> None:
        configs_dir = os.path.join(base_dir, "configs")
        accounts_attributes_path = os.path.join(configs_dir, "accounts.json")
        configs_path = os.path.join(configs_dir, "configs.json")
        configs = load_config(configs_path)

        self.base_dir: str = base_dir
        self.acc_atts: dict[str, dict[str, str]] = load_config(accounts_attributes_path)
        self.in_folder_path: str = os.path.join(
            base_dir, configs["path_configs"]["in_folder_path"]
        )
        self.out_file_path: str = get_out_file_path(
            base_dir, configs["path_configs"]["out_file_path"]
        )
        self.raw_statements: list[Statement] = []
        self.cleaned_statements: list[Statement] = []
        self.existing_transactions: DataFrame | None = None
        self.clear_input_dir: bool = clear_input_dir

    def clear_input_directory(self) -> None:
        if self.clear_input_dir:
            print("Input directory files will be deleted.")
            for filename in os.listdir(self.in_folder_path):
                file_path = os.path.join(self.in_folder_path, filename)
                try:
                    if os.path.isfile(file_path) or os.path.islink(file_path):
                        os.unlink(file_path)
                    elif os.path.isdir(file_path):
                        shutil.rmtree(file_path)
                except Exception as e:
                    print("Failed to delete %s. Reason: %s" % (file_path, e))
        else:
            print(
                "Input directory was not cleaned. Either clean manually or set clean_input_dir parameter to 'True'"
            )

    def process(self):
        self.__process_files()
        self.clean_statements()

    def get_statements(self):
        return self.cleaned_statements

    def clean_statements(self) -> None:
        transformers = {
            "BAC": BacTransformer(self.base_dir),
            "SANTANDER": SantanderTransformer(self.base_dir),
            "REVOLUT": RevolutTransformer(self.base_dir),
            "FICOHSA": FicohsaTransformer(self.base_dir),
        }

        for statement in self.raw_statements:
            if statement.bank_name in transformers:
                clean_stmt = transformers[statement.bank_name].clean(statement)
                if clean_stmt is not None:
                    self.cleaned_statements.append(clean_stmt)
            else:
                print(f"No transformer found for bank: {statement.bank_name}")

    def read_file(self, file_path: str) -> DataFrame | None:
        print(f"Reading {os.path.basename(file_path)}", end=" | ")
        try:
            if file_path.endswith((".xls", ".xlsx")):
                df = read_excel(file_path)
                print("read success", end=" | ")
                return df
            elif file_path.endswith(".csv"):
                df = read_csv(file_path, encoding="cp1252")
                print("read success", end=" | ")
                return df
            else:
                print("read failure")
                return None
        except Exception as e:
            print(f"failure {e}")
            return None

    def validate_account_configs(self, account_name: str):
        if account_name not in self.acc_atts:
            raise KeyError(
                f"The account {account_name} cannot be found in the accounts.json config."
            )

    def __process_files(self):
        # Iterate through all files in the directory
        for filename in os.listdir(self.in_folder_path):
            # Attempt to read the file
            filepath = os.path.join(self.in_folder_path, filename)
            stmt_data: DataFrame | None = self.read_file(filepath)

            # Skip if the file couldn't be read
            if stmt_data is None:
                continue

            # Default values
            matches_found = 0  # To keep track if more than 1 match is found
            acc_name = ""
            acc_currency = ""
            acc_bank = ""
            acc_type = ""

            # Attempt to identify the file based on its contents and the id_pattern
            # We try all accounts to see if we can match more than 1 account
            # If more than 1 account is matched then the id process yields ambiguous results
            for curr_acc_name in self.acc_atts.keys():
                id_pattern: str = self.acc_atts[curr_acc_name]["id_pattern"]
                found_match: bool = self.identify_statement(stmt_data, id_pattern, 400)

                # If a match is found then break out of the loop
                if found_match:
                    matches_found += 1
                    acc_name = curr_acc_name
                    acc_currency = self.acc_atts[curr_acc_name]["currency"]
                    acc_bank = self.acc_atts[curr_acc_name]["bank"]
                    acc_type = self.acc_atts[curr_acc_name]["type"]

            # If no match was found then we print a message and we skip the file
            if matches_found == 0:
                print("no match, skipping")
                continue

            # If more than 1 match was found then we also skip the file
            if matches_found > 1:
                print(">1 match found, skipping")
                continue

            # If a match is found then we store the dataframe
            print(acc_name)
            stmt = Statement(
                stmt_data,
                filepath,
                filename,
                acc_name,
                acc_bank,
                acc_currency,
                acc_type,
                False,
            )
            self.raw_statements.append(stmt)

    def get_account_name(self, filename: str) -> str:
        account_name = filename.split(".", 1)[0]
        account_name = str.strip(account_name.split("(", 1)[0])
        return account_name

    def identify_statement(
        self, df: DataFrame, id_pattern: str, num_chars: int
    ) -> bool:
        try:
            # print(df.head())
            content = str.upper(df.to_string()[:num_chars].replace(" ", ""))
            # print(content)
            if re.search(id_pattern, content):
                return True
        except Exception:
            return False
        return False
