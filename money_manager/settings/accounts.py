import re
from enum import Enum

from pydantic import BaseModel


class BankName(str, Enum):
    BAC = "BAC"
    SANTANDER = "SANTANDER"
    REVOLUT = "REVOLUT"


class AccountType(str, Enum):
    SAVINGS = "savings"
    CREDIT_CARD = "credit_card"


class Currency(str, Enum):
    HNL = "HNL"
    USD = "USD"
    EUR = "EUR"
    MIXED = "MIXED"


class AccountDetail(BaseModel):
    currency: Currency
    bank: BankName
    type: AccountType
    id_pattern: str

    def is_match(self, file_content: str) -> bool:
        """Step 4: Detect account number from file contents."""
        return bool(re.search(self.id_pattern, file_content))


# Add more accounts as needed, the json was unnecessary and cumbersome
ACCOUNTS: dict[str, AccountDetail] = {
    "BAC ECONOMIA": AccountDetail(
        currency=Currency.MIXED,
        bank=BankName.BAC,
        type=AccountType.CREDIT_CARD,
        id_pattern=r"BAC ECONOMIA",
    ),
    "BAC USD 911": AccountDetail(
        currency=Currency.USD,
        bank=BankName.BAC,
        type=AccountType.SAVINGS,
        id_pattern=r"BAC USD 911",
    ),
    "BAC USD 021": AccountDetail(
        currency=Currency.USD,
        bank=BankName.BAC,
        type=AccountType.SAVINGS,
        id_pattern=r"BAC USD 021",
    ),
    "BAC HNL 271": AccountDetail(
        currency=Currency.HNL,
        bank=BankName.BAC,
        type=AccountType.SAVINGS,
        id_pattern=r"BAC HNL 271",
    ),
    "BAC IAP 471": AccountDetail(
        currency=Currency.USD,
        bank=BankName.BAC,
        type=AccountType.SAVINGS,
        id_pattern=r"BAC IAP 471",
    ),
    "BAC IAP 491": AccountDetail(
        currency=Currency.USD,
        bank=BankName.BAC,
        type=AccountType.SAVINGS,
        id_pattern=r"BAC IAP 491",
    ),
    "BAC HNL 971": AccountDetail(
        currency=Currency.HNL,
        bank=BankName.BAC,
        type=AccountType.SAVINGS,
        id_pattern=r"BAC HNL 971",
    ),
    "SANTANDER": AccountDetail(
        currency=Currency.EUR,
        bank=BankName.SANTANDER,
        type=AccountType.SAVINGS,
        id_pattern=r"SANTANDER",
    ),
    "REVOLUT": AccountDetail(
        currency=Currency.EUR,
        bank=BankName.REVOLUT,
        type=AccountType.SAVINGS,
        id_pattern=r"REVOLUT",
    ),
}
