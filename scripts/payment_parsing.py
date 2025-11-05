import sys
import json
import pandas as pd
from datetime import datetime, timezone
import io
import logging
import os
import polars as pl
import math
# --- NiFi Entry Point ---

VODACOM_PAYMENT_COLUMNS = [
    "SerialNo", "EventTimeStamp", "SubscriberID", "MSISDN", "SubscriberType", "TransactionId", 
    "OperationType", "Amount", "PaymentMode", "PPSEntName", "POSEntName", "PreviousPPSBalance", 
    "CurrentPPSBalance", "PreviousPOSBalance", "CurrentPOSBalance", "BonusEntId", "LoanFlag", 
    "LoanRepaymentAmount", "SuppressWorkOrder", "OCS_resultCode", "OCS_resultMessage", "BSS_resultCode", 
    "BSS_resultMessage", "Direction", "CorrelationID", "PrimaryOfferID", "CustomerType", 
    "BonusOfferSubscriptionID", "BonusValidity", "ThirdPartyNumber", "LocationInformation", 
    "VoucherSequenceNo", "VoucherBatchNo", "CurrentUserState", "FirstActiveDate", 
    "AdjustAccountIndicator", "AdjustAccountBatchNumber", "AdjustAccountNotificationIndicator", 
    "AdjustAccountSource", "AdjustAccountAdditionalInfo"
]
SUBSCRIBER_TYPE = {
    "1": "Prepaid",
    "2": "Postpaid",
    "3": "Prepaid",
    "prepaid": "Prepaid",
    "postpaid": "Postpaid",
    "hybrid": "Prepaid"
}

OPERATION_TYPE = {
    "0": "Payment",
    "1": "VoucherRecharge",
    "2": "Bonus",
    "3": "Loan",
    "4": "M2U_Transfer"
}

CREDIT_TYPE = {
    "0": "Credit",
    "1": "Debit"
}

def safe_float(val, default=0.0):
    try:
        if val is None:
            return default
        try:
            if pd.isna(val):
                return default
        except Exception:
            pass
        s = str(val).replace('"', '').strip()
        if s == "":
            return default
        return float(s)
    except Exception:
        return default

def parse_amount(key, value):
    v = safe_float(value, 0.0)
    if v == 0.0:
        return { key: 0.0 }
    else:
        return { key: v / 10000, key+"Currency": "cents" }

def to_iso_timestamp_from_epoch_ms(val):
    try:
        if val is None:
            return None
        if pd.isna(val):
            return None
        s = str(val).strip()
        if s == "":
            return None
        # accept seconds or milliseconds
        iv = int(float(s))
        if iv > 1e12:  # already in micro/nano? fallback
            return None
        if iv > 1e11:  # milliseconds
            return datetime.fromtimestamp(iv / 1000, tz=timezone.utc).isoformat()
        else:
            return datetime.fromtimestamp(iv, tz=timezone.utc).isoformat()
    except Exception:
        return None

def parse_date(val):
    try:
        if val is None:
            return None
        if pd.isna(val):
            return None
        s = str(val).strip()
        if s == "":
            return None
        return datetime.strptime(s, "%Y%m%d%H%M%S").isoformat() if len(s) == 14 else None
    except Exception:
        return None

def map_vodacom_payment_columns(df, filename: str) -> pl.DataFrame:
    output = []

    # accept pandas or polars inputs
    if hasattr(df, "to_dict"):
        records = df.to_dict(orient="records")
    elif hasattr(df, "to_dicts"):
        records = df.to_dicts()
    else:
        records = list(df)

    for row in records:
        row = dict(row)

        subscriberType = row.get("SubscriberType", None)
        row["SubscriberType"] = SUBSCRIBER_TYPE.get(str(subscriberType), "Unknown") if subscriberType is not None else "Unknown"

        operationType = row.get("OperationType", None)
        row["OperationType"] = OPERATION_TYPE.get(str(operationType), "Unknown") if operationType is not None else "Unknown"

        Amount = parse_amount("Amount", row.pop("Amount", None))
        row = {**row, **Amount}

        for fld in ("PreviousPPSBalance","CurrentPPSBalance","PreviousPOSBalance","CurrentPOSBalance","LoanRepaymentAmount"):
            parsed = parse_amount(fld, row.pop(fld, None))
            row = {**row, **parsed}

        # parse possible timestamp fields
        row["FirstActiveDate"] = parse_date(row.get("FirstActiveDate"))
        row["EventTimeStamp"] = parse_date(row.get("EventTimeStamp"))

        # support epoch ms timestamps if provided
        if not row.get("EventTimeStamp"):
            evt_epoch = to_iso_timestamp_from_epoch_ms(row.get("RecordOpeningTime"))
            if evt_epoch:
                row["EventTimeStamp"] = evt_epoch

        Direction = row.get("Direction", None)
        row["CreditType"] = CREDIT_TYPE.get(str(Direction), "Unknown") if Direction is not None else "Unknown"

        row['RecordType'] = 'Voucher'
        row['Operator'] = 'Vodacom'
        row['FileName'] = filename
        row['DateParsed'] = datetime.now().isoformat()

        # Convert datetime values to ISO 8601 strings (already strings from helpers)
        for key, value in list(row.items()):
            if isinstance(value, datetime):
                row[key] = value.isoformat()

        # Remove None and empty string values
        row = {k: v for k, v in row.items() if v not in [None, ""]}

        output.append(row)

    return pl.DataFrame(output)

def parse_date_format(val,format):
    try:
        if val is None:
            return None
        if pd.isna(val):
            return None
        return datetime.strptime(str(val), format).isoformat() if val else None
    except Exception:
        return None

def main():
    # setup_logging(log_path)

    # Read CSV from stdin
    raw_data = sys.stdin.read()
    # Load into Pandas DataFrame
    df = pd.read_csv(
        io.StringIO(raw_data),
        sep="|",
        header=None,
        names=VODACOM_PAYMENT_COLUMNS
    )

    # Transform
    filename = sys.argv[1] if len(sys.argv) > 1 else "nifi_input"
    records = map_vodacom_payment_columns(df, filename)

    # If map_vodacom_payment_columns returns a Polars DataFrame, convert to list of dicts
    if isinstance(records, pl.DataFrame):
        records = records.to_dicts()
    records.pop(0)

    def sanitize_value(v):
        if v is None:
            return None
        if isinstance(v, datetime):
            return v.isoformat()
        try:
            if isinstance(v, float) and math.isnan(v):
                return None
        except Exception:
            pass
        try:
            if hasattr(v, "item"):
                return v.item()
        except Exception:
            pass
        return v

    sanitized = [{k: sanitize_value(v) for k, v in (r or {}).items()} for r in records]

    # Emit a ndjson response
    for record in sanitized:
        print(json.dumps(record, ensure_ascii=False))

if __name__ == "__main__":
    main()
