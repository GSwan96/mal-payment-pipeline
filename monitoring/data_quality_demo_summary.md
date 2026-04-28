# Data Quality Monitoring Demo - Injected Anomalies

This demo intentionally injects anomalies into clean source data to show how the monitoring layer surfaces common data quality issues.

| Source | Row Count | Schema Compliance % | Freshness Lag (hrs) | Null Rate % | Duplicate Events | Volume Anomaly | Injected Scenario |
|--------|-----------|---------------------|---------------------|-------------|------------------|----------------|-------------------|
| cards | 6 | 100.0 | 29.68 | 0.0 | 1 | NO | Injected duplicate card_txn_id to demonstrate duplicate detection. |
| transfers | 5 | 100.0 | 28.84 | 4.44 | 0 | NO | Injected nulls into required fields to demonstrate null-rate monitoring. |
| bill_payments | 3 | 100.0 | 34.68 | 0.0 | 0 | YES | Reduced row count from 5 to 3 and backdated latest timestamp to demonstrate volume anomaly and stale data. |

## Injected anomalies

- Cards: duplicate event ID
- Transfers: nulls in required fields
- Bill Payments: reduced volume and stale latest timestamp