import sys
import json
import pandas as pd
from datetime import datetime, timezone
import io
import logging
import os
import polars as pl
import math

VODACOM_RETURN_COLUMNS = [
    "SerialNo", "EventTimeStamp", "BrandID", "SubCOSID", "MSISDN", "TriggerMSISDN", "ReturnMode", "ReturnAmount", "AccountLeft", "AftAccountLeft", "BorrowValuesBf", "BorrowValuesAf", "ETUGracePeriod", "GracePeriod", "ETUFraudState", "AccountType1", "ChargeAmount1", "CurrentAcctAmount1", "AccountType2", "ChargeAmount2", "CurrentAcctAmount2", "AccountType3", "ChargeAmount3", "CurrentAcctAmount3", "AccountType4", "ChargeAmount4", "CurrentAcctAmount4", "AccountType5", "ChargeAmount5", "CurrentAcctAmount5", "PrimaryOfferID", "CommissionRate", "SubscriberID", "LoanType"
]

VODACOM_RETURN_MODE = {
    "1": "recharge",
    "2": "transfer",
    "3": "forcible loan payment"
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
        return { key: v / 10000, key + "Currency": "cents" }

def to_iso_timestamp_from_epoch_ms(val):
    try:
        if val is None:
            return None
        try:
            if pd.isna(val):
                return None
        except Exception:
            pass
        s = str(val).strip()
        if s == "":
            return None
        iv = int(float(s))
        # treat > 1e11 as milliseconds
        if iv > 1e11:
            return datetime.fromtimestamp(iv / 1000, tz=timezone.utc).isoformat()
        else:
            return datetime.fromtimestamp(iv, tz=timezone.utc).isoformat()
    except Exception:
        return None

def parse_date(val):
    try:
        if val is None:
            return None
        try:
            if pd.isna(val):
                return None
        except Exception:
            pass
        s = str(val).strip()
        if s == "":
            return None
        # yyyymmddHHMMSS
        if len(s) == 14 and s.isdigit():
            return datetime.strptime(s, "%Y%m%d%H%M%S").isoformat()
        # epoch ms or seconds
        return to_iso_timestamp_from_epoch_ms(s)
    except Exception:
        return None

def map_vodacom_return_columns(df: pl.DataFrame, filename: str) -> pl.DataFrame:
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

        row_dict = {}
        row_dict['RecordType'] = 'Return'
        row_dict['Operator'] = 'Vodacom'
        row_dict['FileName'] = filename
        row_dict['DateParsed'] = datetime.now().isoformat()

        row_dict["EventTimeStamp"] = parse_date(row.get("EventTimeStamp"))
        row_dict["ReturnMode"] = VODACOM_RETURN_MODE.get(str(row.get("ReturnMode", "")).strip(), "Unknown")

        # amounts
        row_dict.update(parse_amount("ReturnAmount", row.pop("ReturnAmount", None)))
        row_dict.update(parse_amount("AccountLeft", row.pop("AccountLeft", None)))
        row_dict.update(parse_amount("AftAccountLeft", row.pop("AftAccountLeft", None)))
        row_dict.update(parse_amount("BorrowValuesBf", row.pop("BorrowValuesBf", None)))
        row_dict.update(parse_amount("BorrowValuesAf", row.pop("BorrowValuesAf", None)))

        # charge amounts 1..5
        for i in range(1, 6):
            tmpName = f"ChargeAmount{i}"
            row_dict.update(parse_amount(tmpName, row.pop(tmpName, None)))

        # commission: use parsed ReturnAmount (already divided)
        commission_rate = safe_float(row.get("CommissionRate", None), None)
        try:
            return_amt = float(row_dict.get("ReturnAmount", 0.0))
        except Exception:
            return_amt = 0.0
        if commission_rate is not None:
            try:
                comm = return_amt * (commission_rate / 100.0)
                row_dict['CommssionAmount'] = round(comm, 2) if comm else 0.0
            except Exception:
                row_dict['CommssionAmount'] = 0.0

        # preserve some identifiers
        for keep in ("SerialNo", "MSISDN", "TriggerMSISDN", "PrimaryOfferID", "SubscriberID", "LoanType", "BrandID", "SubCOSID"):
            if keep in row and row.get(keep) not in [None, ""]:
                row_dict[keep] = row.get(keep)

        # Convert any remaining datetime objects to ISO 8601
        for key, value in list(row_dict.items()):
            if isinstance(value, datetime):
                row_dict[key] = value.isoformat()

        # Remove None and empty string values
        row_dict = {k: v for k, v in row_dict.items() if v not in [None, ""]}
        output.append(row_dict)

    return pl.DataFrame(output)

def main():
    # Read CSV from stdin
    raw_data = sys.stdin.read()
    # Load into Pandas DataFrame
    df = pd.read_csv(
        io.StringIO(raw_data),
        sep="|",
        header=None,
        names=VODACOM_RETURN_COLUMNS,
        dtype=str
    )

    # normalize NA -> empty string
    df = df.fillna("")

    filename = sys.argv[1] if len(sys.argv) > 1 else "nifi_input"
    records = map_vodacom_return_columns(df, filename)

    # If Polars DataFrame returned, convert to list of dicts
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
        # keep empty strings as None
        if isinstance(v, str) and v == "":
            return None
        return v

    sanitized = [{k: sanitize_value(v) for k, v in (r or {}).items()} for r in (records or [])]

    # Emit a ndjson response
    for record in sanitized:
        print(json.dumps(record, ensure_ascii=False))

if __name__ == "__main__":
    main()