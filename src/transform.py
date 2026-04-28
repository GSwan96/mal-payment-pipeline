import json
import pandas as pd


def transform_cards(df: pd.DataFrame) -> pd.DataFrame:
    out = pd.DataFrame({
        "event_id": df["card_txn_id"],
        "contract_version": "v2",
        "payment_type": "card",
        "source_system": "cards",
        "source_record_id": df["card_txn_id"],
        "customer_id": df["customer_id"],
        "account_id": df["card_id"],
        "counterparty_id": df["merchant_id"],
        "amount": df["amount"],
        "currency": df["currency"],
        "event_timestamp": df["txn_timestamp"],
        "status": df["status"],
        "payment_method": "card",
        "reference": df["auth_code"],
        "description": df["merchant_category"],
        "processing_date": pd.to_datetime(df["txn_timestamp"]).dt.date.astype(str),
        "product_attributes": df.apply(
            lambda row: json.dumps({
                "card_network": row["card_network"],
                "merchant_category": row["merchant_category"],
                "auth_code": row["auth_code"],
            }),
            axis=1,
        ),
    })
    return out


def transform_transfers(df: pd.DataFrame) -> pd.DataFrame:
    out = pd.DataFrame({
        "event_id": df["transfer_id"],
        "contract_version": "v2",
        "payment_type": "transfer",
        "source_system": "transfers",
        "source_record_id": df["transfer_id"],
        "customer_id": df["customer_id"],
        "account_id": df["from_account_id"],
        "counterparty_id": df["beneficiary_id"],
        "amount": df["transfer_amount"],
        "currency": df["ccy"],
        "event_timestamp": df["created_at"],
        "status": df["transfer_status"],
        "payment_method": df["channel"],
        "reference": df["to_account_id"],
        "description": df["transfer_type"],
        "processing_date": pd.to_datetime(df["created_at"]).dt.date.astype(str),
        "product_attributes": df.apply(
            lambda row: json.dumps({
                "to_account_id": row["to_account_id"],
                "transfer_type": row["transfer_type"],
                "channel": row["channel"],
            }),
            axis=1,
        ),
        })
    return out


def transform_bill_payments(df: pd.DataFrame) -> pd.DataFrame:
    out = pd.DataFrame({
        "event_id": df["bill_payment_id"],
        "contract_version": "v2",
        "payment_type": "bill_payment",
        "source_system": "bill_payments",
        "source_record_id": df["bill_payment_id"],
        "customer_id": df["customer_id"],
        "account_id": df["account_id"],
        "counterparty_id": df["biller_code"],
        "amount": df["payment_amount"],
        "currency": df["currency_code"],
        "event_timestamp": df["paid_at"],
        "status": df["payment_status"],
        "payment_method": df["payment_method"],
        "reference": df["bill_reference"],
        "description": df["service_type"],
        "processing_date": pd.to_datetime(df["paid_at"]).dt.date.astype(str),
        "product_attributes": df.apply(
            lambda row: json.dumps({
                "biller_code": row["biller_code"],
                "bill_reference": row["bill_reference"],
                "service_type": row["service_type"],
            }),
            axis=1,
        ),
    })
    return out