import sys
import json
import pandas as pd
from datetime import datetime, timezone
import io
import logging
import os
import polars as pl

VODACOM_RETURN_COLUMNS = [
    "SerialNo", "EventTimeStamp", "BrandID", "SubCOSID", "MSISDN", "TriggerMSISDN", "ReturnMode", "ReturnAmount", "AccountLeft", "AftAccountLeft", "BorrowValuesBf", "BorrowValuesAf", "ETUGracePeriod", "GracePeriod", "ETUFraudState", "AccountType1", "ChargeAmount1", "CurrentAcctAmount1", "AccountType2", "ChargeAmount2", "CurrentAcctAmount2", "AccountType3", "ChargeAmount3", "CurrentAcctAmount3", "AccountType4", "ChargeAmount4", "CurrentAcctAmount4", "AccountType5", "ChargeAmount5", "CurrentAcctAmount5", "PrimaryOfferID", "CommissionRate", "SubscriberID", "LoanType"
]


VODACOM_RETURN_MODE = {
    "1": "recharge",
    "2": "transfer",
    "3": "forcible loan payment"
}

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

def parse_date(val):
    try:
        return datetime.strptime(str(val), "%Y%m%d%H%M%S").isoformat() if val and len(str(val)) == 14 else None
    except Exception:
        return None

def map_vodacom_return_columns(df: pl.DataFrame, filename: str) -> pl.DataFrame:
    output = []


    for row in df.to_dicts():
        row_dict = dict(row)

        row_dict['RecordType'] = 'Return'
        row_dict['Operator'] = 'Vodacom'
        row_dict['FileName'] = filename
        row_dict['DateParsed'] = datetime.now().isoformat()

        row_dict["EventTimeStamp"] = parse_date(row.get("EventTimeStamp"))
        row_dict["ReturnMode"] = VODACOM_RETURN_MODE.get(row.get("ReturnMode", None), "Unknown")
        
        ReturnAmount = parse_amount("ReturnAmount", row.pop("ReturnAmount", None))
        row_dict = {**row_dict, **ReturnAmount}
        
        AccountLeft = parse_amount("AccountLeft", row.pop("AccountLeft", None))
        row_dict = {**row_dict, **AccountLeft}
        
        AftAccountLeft = parse_amount("AftAccountLeft", row.pop("AftAccountLeft", None))
        row_dict = {**row_dict, **AftAccountLeft}
        
        BorrowValuesBf = parse_amount("BorrowValuesBf", row.pop("BorrowValuesBf", None))
        row_dict = {**row_dict, **BorrowValuesBf}
        
        BorrowValuesAf = parse_amount("BorrowValuesAf", row.pop("BorrowValuesAf", None))
        row_dict = {**row_dict, **BorrowValuesAf}
        
        CommissionRate = row.pop("CommissionRate", None)
        if CommissionRate is not None:
            Commission = float(row_dict.get("ReturnAmount", 0)) * (float(CommissionRate) / 100)
            row_dict['CommssionAmount'] = round(Commission, 2) if Commission else 0.0
        
        
        
        for i in range(1, 6):
            tmpName = f"ChargeAmount{i}"
            ChargeAmount = parse_amount(tmpName, row.pop(tmpName, None))
            row_dict = {**row_dict, **ChargeAmount}

        # Convert any remaining datetime objects to ISO 8601
        for key, value in row_dict.items():
            if isinstance(value, datetime):
                row_dict[key] = value.isoformat()

        # Remove None and empty string values
        row_dict = {k: v for k, v in row_dict.items() if v not in [None, ""]}
        output.append(row_dict)

    return pl.DataFrame(output)


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
        names=VODACOM_RETURN_COLUMNS
    )

    # Transform
    filename = sys.argv[1] if len(sys.argv) > 1 else "nifi_input"
    records = map_vodacom_return_columns(df, filename)

    # Output records as JSON lines to stdout
    for record in records.to_dicts():
        print(json.dumps(record))

if __name__ == "__main__":
    main()