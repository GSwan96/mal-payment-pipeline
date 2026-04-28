from pathlib import Path
import pandas as pd

from data_quality_checks import (
    RAW_DIR,
    MONITORING_DIR,
    SOURCE_CONFIG,
    schema_compliance_rate,
    freshness_hours,
    null_rate,
    duplicate_count,
    volume_anomaly_flag,
)


def load_sources() -> dict[str, pd.DataFrame]:
    return {
        source_name: pd.read_csv(RAW_DIR / config["file"])
        for source_name, config in SOURCE_CONFIG.items()
    }


def inject_anomalies(sources: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    demo_sources = {name: df.copy() for name, df in sources.items()}

    # Cards: inject a duplicate event
    cards = demo_sources["cards"].copy()
    cards = pd.concat([cards, cards.iloc[[0]]], ignore_index=True)
    demo_sources["cards"] = cards

    # Transfers: inject nulls in required fields
    transfers = demo_sources["transfers"].copy()
    transfers.loc[0, "beneficiary_id"] = None
    transfers.loc[1, "customer_id"] = None
    demo_sources["transfers"] = transfers

    # Bill payments: reduce volume and make data stale
    bills = demo_sources["bill_payments"].copy().iloc[:3].copy()
    bills.loc[bills.index[-1], "paid_at"] = "2026-04-20T08:00:00"
    demo_sources["bill_payments"] = bills

    return demo_sources


def build_demo_report() -> pd.DataFrame:
    anomaly_notes = {
        "cards": "Injected duplicate card_txn_id to demonstrate duplicate detection.",
        "transfers": "Injected nulls into required fields to demonstrate null-rate monitoring.",
        "bill_payments": "Reduced row count from 5 to 3 and backdated latest timestamp to demonstrate volume anomaly and stale data.",
    }

    clean_sources = load_sources()
    demo_sources = inject_anomalies(clean_sources)

    rows = []

    for source_name, config in SOURCE_CONFIG.items():
        df = demo_sources[source_name]

        row = {
            "source_system": source_name,
            "row_count": len(df),
            "expected_row_count": config["expected_row_count"],
            "schema_compliance_rate_pct": schema_compliance_rate(df, config["required_cols"]),
            "freshness_lag_hours": freshness_hours(df, config["timestamp_col"]),
            "null_rate_pct": null_rate(df, config["required_cols"]),
            "duplicate_event_count": duplicate_count(df, config["id_col"]),
            "volume_anomaly_flag": volume_anomaly_flag(df, config["expected_row_count"]),
            "anomaly_note": anomaly_notes[source_name],
        }
        rows.append(row)

    return pd.DataFrame(rows)


def write_demo_summary(report_df: pd.DataFrame) -> None:
    lines = []
    lines.append("# Data Quality Monitoring Demo - Injected Anomalies")
    lines.append("")
    lines.append("This demo intentionally injects anomalies into clean source data to show how the monitoring layer surfaces common data quality issues.")
    lines.append("")
    lines.append("| Source | Row Count | Schema Compliance % | Freshness Lag (hrs) | Null Rate % | Duplicate Events | Volume Anomaly | Injected Scenario |")
    lines.append("|--------|-----------|---------------------|---------------------|-------------|------------------|----------------|-------------------|")

    for _, row in report_df.iterrows():
        lines.append(
            f"| {row['source_system']} | {row['row_count']} | {row['schema_compliance_rate_pct']} | "
            f"{row['freshness_lag_hours']} | {row['null_rate_pct']} | {row['duplicate_event_count']} | "
            f"{row['volume_anomaly_flag']} | {row['anomaly_note']} |"
        )

    lines.append("")
    lines.append("## Injected anomalies")
    lines.append("")
    lines.append("- Cards: duplicate event ID")
    lines.append("- Transfers: nulls in required fields")
    lines.append("- Bill Payments: reduced volume and stale latest timestamp")

    output_path = MONITORING_DIR / "data_quality_demo_summary.md"
    output_path.write_text("\n".join(lines), encoding="utf-8")


def main() -> None:
    report_df = build_demo_report()
    report_df.to_csv(MONITORING_DIR / "data_quality_demo_report.csv", index=False)
    write_demo_summary(report_df)

    print("Demo anomaly report created.")
    print(report_df.to_string(index=False))


if __name__ == "__main__":
    main()