from pathlib import Path
import pandas as pd


RAW_DIR = Path("data/raw")


def read_cards() -> pd.DataFrame:
    return pd.read_csv(RAW_DIR / "cards.csv")


def read_transfers() -> pd.DataFrame:
    return pd.read_csv(RAW_DIR / "transfers.csv")


def read_bill_payments() -> pd.DataFrame:
    return pd.read_csv(RAW_DIR / "bill_payments.csv")