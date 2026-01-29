from datetime import datetime
from typing import ClassVar

import polars as pl
from pydantic import BaseModel


class Transaction(BaseModel):
    # 1. Define the structure
    id: str
    date: datetime
    account_name: str
    description: str
    category: str = ""
    notes: str = ""
    currency: str
    amount: float
    tran_type: str

    COLS_TO_HASH: ClassVar[list[str]] = [
        "date",
        "description",
        "amount",
        "account_name",
    ]

    @classmethod
    def get_polars_schema(cls) -> dict[str, pl.DataType]:
        """Helper to get Polars dtypes for Step 7 and 9."""
        return {
            "id": pl.String(),
            "date": pl.Datetime(),
            "account_name": pl.String(),
            "description": pl.String(),
            "category": pl.String(),
            "notes": pl.String(),
            "currency": pl.String(),
            "amount": pl.Float64(),
            "tran_type": pl.String(),
        }
