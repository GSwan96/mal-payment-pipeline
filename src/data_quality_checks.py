from pathlib import Path
from datetime import datetime, timezone
import pandas as pd


RAW_DIR = Path("data/raw")
MONITORING_DIR = Path("monitoring")
MONITORING_DIR.mkdir(parents=True, exist_ok=True)


SOURCE_CONFIG = {
    "cards": {
        "file": "cards.csv",
        "timestamp_col": "txn_timestamp",
        "id_col": "card_txn_id",
        "required_cols": [
            "card_txn_id",
            "card_id",
            "customer_id",
            "merchant_id",
            "amount",
            "currency",
            "txn_timestamp",
            "status",
        ],
        "expected_row_count": 5,
    },
    "transfers": {
        "file": "transfers.csv",
        "timestamp_col": "created_at",
        "id_col": "transfer_id",
        "required_cols": [
            "transfer_id",
            "from_account_id",
            "to_account_id",
            "customer_id",
            "beneficiary_id",
            "transfer_amount",
            "ccy",
            "created_at",
            "transfer_status",
        ],
        "expected_row_count": 5,
    },
    "bill_payments": {
        "file": "bill_payments.csv",
        "timestamp_col": "paid_at",
        "id_col": "bill_payment_id",
        "required_cols": [
            "bill_payment_id",
            "account_id",
            "customer_id",
            "biller_code",
            "bill_reference",
            "payment_amount",
            "currency_code",
            "paid_at",
            "payment_status",
        ],
        "expected_row_count": 5,
    },
}


def read_source_csv(filename: str) -> pd.DataFrame:
    return pd.read_csv(RAW_DIR / filename)


def schema_compliance_rate(df: pd.DataFrame, required_cols: list[str]) -> float:
    present = sum(1 for col in required_cols if col in df.columns)
    return round((present / len(required_cols)) * 100, 2)


def freshness_hours(df: pd.DataFrame, timestamp_col: str) -> float:
    max_ts = pd.to_datetime(df[timestamp_col]).max()
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    lag = now - max_ts.to_pydatetime()
    return round(lag.total_seconds() / 3600, 2)


def null_rate(df: pd.DataFrame, required_cols: list[str]) -> float:
    total_cells = len(df) * len(required_cols)
    if total_cells == 0:
        return 0.0
    nulls = df[required_cols].isnull().sum().sum()
    return round((nulls / total_cells) * 100, 2)


def duplicate_count(df: pd.DataFrame, id_col: str) -> int:
    return int(df[id_col].duplicated().sum())


def volume_anomaly_flag(df: pd.DataFrame, expected_row_count: int) -> str:
    current = len(df)
    if current < expected_row_count * 0.8:
        return "YES"
    return "NO"


def build_report() -> pd.DataFrame:
    rows = []

    for source_name, config in SOURCE_CONFIG.items():
        df = read_source_csv(config["file"])

        row = {
            "source_system": source_name,
            "row_count": len(df),
            "expected_row_count": config["expected_row_count"],
            "schema_compliance_rate_pct": schema_compliance_rate(df, config["required_cols"]),
            "freshness_lag_hours": freshness_hours(df, config["timestamp_col"]),
            "null_rate_pct": null_rate(df, config["required_cols"]),
            "duplicate_event_count": duplicate_count(df, config["id_col"]),
            "volume_anomaly_flag": volume_anomaly_flag(df, config["expected_row_count"]),
        }
        rows.append(row)

    return pd.DataFrame(rows)


def write_markdown_summary(report_df: pd.DataFrame) -> None:
    lines = []
    lines.append("# Data Quality Monitoring Summary")
    lines.append("")
    lines.append("This report tracks schema compliance, freshness, null rates, duplicate events, and volume anomalies across the three payment source systems.")
    lines.append("")
    lines.append("| Source | Row Count | Schema Compliance % | Freshness Lag (hrs) | Null Rate % | Duplicate Events | Volume Anomaly |")
    lines.append("|--------|-----------|---------------------|---------------------|-------------|------------------|----------------|")

    for _, row in report_df.iterrows():
        lines.append(
            f"| {row['source_system']} | {row['row_count']} | {row['schema_compliance_rate_pct']} | "
            f"{row['freshness_lag_hours']} | {row['null_rate_pct']} | {row['duplicate_event_count']} | "
            f"{row['volume_anomaly_flag']} |"
        )

    lines.append("")
    lines.append("## Monitoring Logic")
    lines.append("")
    lines.append("- Schema compliance is based on required-column presence by source.")
    lines.append("- Freshness lag is calculated from the latest event timestamp in each source file.")
    lines.append("- Null rate is calculated across required columns only.")
    lines.append("- Duplicate events are counted using the source event identifier.")
    lines.append("- Volume anomaly is flagged when current volume falls below 80% of the expected baseline.")

    output_path = MONITORING_DIR / "data_quality_summary.md"
    output_path.write_text("\n".join(lines), encoding="utf-8")


def main() -> None:
    report_df = build_report()
    report_df.to_csv(MONITORING_DIR / "data_quality_report.csv", index=False)
    write_markdown_summary(report_df)

    print("Data quality monitoring report created.")
    print(report_df.to_string(index=False))


if __name__ == "__main__":
    main()