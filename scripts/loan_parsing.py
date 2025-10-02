import sys
import json
import pandas as pd
from datetime import datetime, timezone
import io
import logging
import os
import polars as pl
import math





VODACOM_LOAN_COLUMNS = [
    "SerialNo", "SubscriberNo", "ServiceNumber", "SubscriberKey", "SubCOS", "LoanFlag", "LoanTime", "ToSubAcctType", "LoanAmount", "EventType", "OldBalance", "NewBalance", "OldLoanSubAcctBal", "NewLoanSubAcctBal", "DiameterSessionID", "SubscriberIDType", "AccountID", "CustomerKey", "CustomerCode", "ResultCode", "PrimaryOfferID", "CommissionRate", "ICAP_ID", "LoanType", "CorrelationID"
]

LOAN_FLAG = {
    "0": "Loan",
    "1": "Loan Payment"
}
LOAN_TYPE = {
    "2101": "call",
    "2201": "SMS",
    "2401": "recharge",
    "2601": "rental charging",
    "2802": "GPRS",
    "2901": "balance query",
    "3001": "one-off fee deduction",
    "3011": "RBT charging",
    "3101": "account transfer",
    "3106": "account adjustment",
    "3601": "MMS",
    "3901": "content charging",
    "9999": "others"
}

LOAN_SUBS_ID_TYPE = {
    "0": "END_USER_E164",
    "2": "END_USER_SIP_URI"
}
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
        return datetime.strptime(s, "%Y%m%d%H%M%S").isoformat() if s and len(s) == 14 else None
    except Exception:
        return None

def parse_amount(key, value):
    # safe float conversion, treat NaN/empty as zero
    try:
        if value is None:
            return {key: 0.0}
        try:
            if pd.isna(value):
                return {key: 0.0}
        except Exception:
            pass
        val_str = str(value).replace('"', '').strip()
        if val_str == "":
            return {key: 0.0}
        v = float(val_str)
    except Exception:
        return {key: 0.0}

    if v == 0.0:
        return {key: 0.0}
    else:
        return {key: v / 10000, key + "Currency": "cents"}

def safe_float(val, default=0.0):
    try:
        if val is None:
            return default
        try:
            if pd.isna(val):
                return default
        except Exception:
            pass
        s = str(val).strip()
        if s == "":
            return default
        return float(s)
    except Exception:
        return default

def map_vodacom_loan_columns(df: pl.DataFrame, filename: str) -> pl.DataFrame:
    output = []

    # support both pandas and polars DataFrame inputs
    if hasattr(df, "to_dict"):
        records = df.to_dict(orient="records")
    elif hasattr(df, "to_dicts"):
        records = df.to_dicts()
    else:
        records = list(df)

    for row in records:
        row = dict(row)  # ensure mutable dict copy

        LoanFlag = row.pop("LoanFlag", None)
        row["LoanFlag"] = LOAN_FLAG.get(str(LoanFlag), "Unknown") if LoanFlag is not None else "Unknown"

        row["EventType"] = LOAN_TYPE.get(str(row.get("EventType", None)), "Unknown")
        row["SubscriberIDType"] = LOAN_SUBS_ID_TYPE.get(str(row.get("SubscriberIDType", None)), "Unknown")

        row['RecordType'] = 'Loan'
        row['Operator'] = 'Vodacom'
        row['FileName'] = filename
        row['DateParsed'] = datetime.now().isoformat()

        # compute commission safely
        LoanAmount_raw = row.get("LoanAmount", None)
        CommissionRate_raw = row.get("CommissionRate", None)
        loan_amount_val = safe_float(LoanAmount_raw, 0.0)
        commission_rate_val = safe_float(CommissionRate_raw, 0.0)
        if loan_amount_val:
            commission = loan_amount_val * (commission_rate_val / 100) if commission_rate_val else 0.0
            row['CommssionAmount'] = round(commission, 2) if commission else 0.0
        else:
            row['CommssionAmount'] = 0.0

        row["LoanTime"] = parse_date(row.get("LoanTime"))

        LoanAmount = parse_amount("LoanAmount", row.pop("LoanAmount", None))
        row = {**row, **LoanAmount}
        OldBalance = parse_amount("OldBalance", row.pop("OldBalance", None))
        row = {**row, **OldBalance}
        NewBalance = parse_amount("NewBalance", row.pop("NewBalance", None))
        row = {**row, **NewBalance}
        OldLoanSubAcctBal = parse_amount("OldLoanSubAcctBal", row.pop("OldLoanSubAcctBal", None))
        row = {**row, **OldLoanSubAcctBal}
        NewLoanSubAcctBal = parse_amount("NewLoanSubAcctBal", row.pop("NewLoanSubAcctBal", None))
        row = {**row, **NewLoanSubAcctBal}
        CommssionAmount = parse_amount("CommssionAmount", row.pop("CommssionAmount", None))
        row = {**row, **CommssionAmount}

        # Convert remaining datetime objects to ISO strings
        for key, value in row.items():
            if isinstance(value, datetime):
                row[key] = value.isoformat()

        # Remove None and empty string values
        row = {k: v for k, v in row.items() if v not in [None, ""]}
        output.append(row)

    return pl.DataFrame(output)

VODACOM_RETURN_MODE = {
    "1": "recharge",
    "2": "transfer",
    "3": "forcible loan payment"
}




# --- NiFi Entry Point ---
def main():
    # Read CSV from stdin
    raw_data = sys.stdin.read()
    # Load into Pandas DataFrame
    df = pd.read_csv(
        io.StringIO(raw_data),
        sep="|",
        header=None,
        names=VODACOM_LOAN_COLUMNS
    )

    # Transform
    filename = sys.argv[1] if len(sys.argv) > 1 else "nifi_input"
    records = map_vodacom_loan_columns(df, filename)

    # If map_vodacom_loan_columns returns a Polars DataFrame, convert to list of dicts
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