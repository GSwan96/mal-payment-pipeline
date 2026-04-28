# Data Quality Monitoring Summary

This report tracks schema compliance, freshness, null rates, duplicate events, and volume anomalies across the three payment source systems.

| Source | Row Count | Schema Compliance % | Freshness Lag (hrs) | Null Rate % | Duplicate Events | Volume Anomaly |
|--------|-----------|---------------------|---------------------|-------------|------------------|----------------|
| cards | 5 | 100.0 | 29.44 | 0.0 | 0 | NO |
| transfers | 5 | 100.0 | 28.6 | 0.0 | 0 | NO |
| bill_payments | 5 | 100.0 | 27.52 | 0.0 | 0 | NO |

## Monitoring Logic

- Schema compliance is based on required-column presence by source.
- Freshness lag is calculated from the latest event timestamp in each source file.
- Null rate is calculated across required columns only.
- Duplicate events are counted using the source event identifier.
- Volume anomaly is flagged when current volume falls below 80% of the expected baseline.