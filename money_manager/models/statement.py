from dataclasses import dataclass

from pandas import DataFrame


@dataclass
class Statement:
    data: DataFrame
    filepath: str
    filename: str
    account_name: str
    bank_name: str
    currency: str
    account_type: str
    merged: bool
