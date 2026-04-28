import pandas as pd

from extract import read_cards, read_transfers, read_bill_payments
from transform import transform_cards, transform_transfers, transform_bill_payments
from validate import run_validations
from load import write_outputs


def create_v1_example(df: pd.DataFrame) -> pd.DataFrame:
    v1 = df.copy()
    v1["contract_version"] = "v1"
    v1 = v1.drop(columns=["description"])
    return v1


def main() -> None:
    cards = transform_cards(read_cards())
    transfers = transform_transfers(read_transfers())
    bills = transform_bill_payments(read_bill_payments())

    unified = pd.concat([cards, transfers, bills], ignore_index=True)

    run_validations(unified)
    write_outputs(unified)

    v1_example = create_v1_example(unified)
    v1_example.to_csv("data/processed/unified_payments_v1_example.csv", index=False)

    print("Pipeline completed successfully.")
    print(f"Unified rows: {len(unified)}")
    print("Outputs written to data/processed/")


if __name__ == "__main__":
    main()