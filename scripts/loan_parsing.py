import sys
import json
import pandas as pd
from datetime import datetime, timezone
import io
import logging
import os
import polars as pl





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
        return datetime.strptime(str(val), "%Y%m%d%H%M%S").isoformat() if val and len(str(val)) == 14 else None
    except Exception:
        return None

def parse_amount(key,value):
    value = float(value) if value else 0.0
    if value == 0.0:
        return {
            key: 0.0
        }
    else:
        return {
            key: value / 10000,
            key+"Currency": "cents"
        }

def map_vodacom_loan_columns(df: pl.DataFrame, filename: str) -> pl.DataFrame:
    output = []

    for row in df.to_dicts():
        LoanFlag = row.pop("LoanFlag", None)
        row["LoanFlag"] = LOAN_FLAG.get(LoanFlag, "Unknown") if LoanFlag else "Unknown"

        row["EventType"] = LOAN_TYPE.get(row.get("EventType", None), "Unknown")
        row["SubscriberIDType"] = LOAN_SUBS_ID_TYPE.get(row.get("SubscriberIDType", None), "Unknown")


        row['RecordType'] = 'Loan'
        row['Operator'] = 'Vodacom'
        row['FileName'] = filename
        row['DateParsed'] = datetime.now().isoformat()
        LoanAmount = row.get("LoanAmount", None)
        CommissionRate = row.get("CommissionRate", None)
        if LoanAmount is not None:
            Commission = float(LoanAmount) * (float(CommissionRate) / 100) if CommissionRate else 0.0
            row['CommssionAmount'] = round(Commission, 2) if Commission else 0.0
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
    # setup_logging(log_path)

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

    # Output records as JSON lines to stdout
    for record in records.to_dicts():
        print(json.dumps(record))

if __name__ == "__main__":
    main()