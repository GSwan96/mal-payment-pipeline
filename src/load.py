from pathlib import Path
import pandas as pd


PROCESSED_DIR = Path("data/processed")
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)


def write_outputs(df: pd.DataFrame) -> None:
    df.to_csv(PROCESSED_DIR / "unified_payments.csv", index=False)
    df.to_parquet(PROCESSED_DIR / "unified_payments.parquet", index=False)