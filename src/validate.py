import pandas as pd
from schema import CANONICAL_COLUMNS


REQUIRED_COLUMNS = [
    "event_id",
    "payment_type",
    "customer_id",
    "amount",
    "currency",
    "event_timestamp",
    "status",
]


def validate_columns(df: pd.DataFrame) -> None:
    missing = [col for col in CANONICAL_COLUMNS if col not in df.columns]
    if missing:
        raise ValueError(f"Missing canonical columns: {missing}")


def validate_required_values(df: pd.DataFrame) -> None:
    for col in REQUIRED_COLUMNS:
        if df[col].isnull().any():
            raise ValueError(f"Null values found in required column: {col}")


def validate_amounts(df: pd.DataFrame) -> None:
    if (pd.to_numeric(df["amount"], errors="coerce") <= 0).any():
        raise ValueError("Amounts must be greater than 0")


def validate_no_duplicate_event_ids(df: pd.DataFrame) -> None:
    if df["event_id"].duplicated().any():
        raise ValueError("Duplicate event_id values found")


def run_validations(df: pd.DataFrame) -> None:
    validate_columns(df)
    validate_required_values(df)
    validate_amounts(df)
    validate_no_duplicate_event_ids(df)