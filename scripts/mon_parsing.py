import sys
import json
import pandas as pd
from datetime import datetime, timezone
import io
import logging
import os
import polars as pl
import math

VODACOM_MON_COLUMNS = [
    "EventTimeStamp", "SubscriberID", "MSISDN", "SubscriberType", "TransactionID", "PrimaryOfferID", "CustomerType", "OperationType", "Amount", "ProductId", "PPSEntName", "POSEntName", "PreviousPPSBalance", "CurrentPPSBalance", "PreviousPOSBalance", "CurrentPOSBalance", "CycleBeginTime", "CycleEndTime", "ElapseCycles", "Direction", "CorrelationID", "RatingGroup", "ServiceID", "OpenetOfferID", "AccountType1", "BalanceImpactAccountType1", "BalanceID1", "CurrAcctAmount1", "ExpiryTimeOfAccountType1", "AccountType2", "BalanceImpactAccountType2", "BalanceID2", "CurrAcctAmount2", "ExpiryTimeOfAccountType2", "AccountType3", "BalanceImpactAccountType3", "BalanceID3", "CurrAcctAmount3", "ExpiryTimeOfAccountType3", "AccountType4", "BalanceImpactAccountType4", "BalanceID4", "CurrAcctAmount4", "ExpiryTimeOfAccountType4", "AccountType5", "BalanceImpactAccountType5", "BalanceID5", "CurrAcctAmount5", "ExpiryTimeOfAccountType5", "AccountType6", "BalanceImpactAccountType6", "BalanceID6", "CurrAcctAmount6", "ExpiryTimeOfAccountType6", "AccountType7", "BalanceImpactAccountType7", "BalanceID7", "CurrAcctAmount7", "ExpiryTimeOfAccountType7", "AccountType8", "BalanceImpactAccountType8", "BalanceID8", "CurrAcctAmount8", "ExpiryTimeOfAccountType8", "AccountType9", "BalanceImpactAccountType9", "BalanceID9", "CurrAcctAmount9", "ExpiryTimeOfAccountType9", "AccountType10", "BalanceImpactAccountType10", "BalanceID10", "CurrAcctAmount10", "ExpiryTimeOfAccountType10", "ThirdPartyNumber", "LocationInformation", "HuaweiOfferID", "AdjustAccountIndicator", "AdjustAccountBatchNumber", "AdjustAccountNotificationIndicator", "AdjustAccountSource", "AdjustAccountAdditionalInfo"
]

MON_OPERATION_TYPE = {
    "0": "Primary offer renewal admin fee",
    "1": "Primary offer renewal service fee",
    "2": "Primary offer one time prorate",
    "3": "Optional offer purchase fee (one time)",
    "4": "Optional offer renewal fee",
    "5": "buy offer for others",
    "6": "CUG addition fee",
    "7": "CUG renewal fee"
}

MGR_CHANNEL = {
    "1": "EVC",
    "2": "M.Money",
    "5": "VDC APP",
    "6": "ICAP",
    "7": "Collab",
    "8": "USSD",
    "9": "IPG",
    "10": "CVM"
}
BSS_SUBSCRIBER_TYPE = {
    "0": "Prepaid",
    "1": "Postpaid",
    "2": "Prepaid"   
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
        if s == "":
            return None
        return datetime.strptime(s, "%Y%m%d%H%M%S").isoformat() if len(s) == 14 else None
    except Exception:
        return None
    
def parse_amount(key, value):
    try:
        if value is None:
            return {key: 0.0}
        try:
            if pd.isna(value):
                return {key: 0.0}
        except Exception:
            pass
        s = str(value).replace('"', '').strip()
        if s == "":
            return {key: 0.0}
        v = float(s)
    except Exception:
        return {key: 0.0}
    if v == 0.0:
        return {key: 0.0}
    else:
        return {key: v / 10000, key + "Currency": "cents"}

def map_vodacom_mon_columns(df: pl.DataFrame, filename: str) -> pl.DataFrame:
    output = []

    # support both pandas and polars DataFrame inputs
    if hasattr(df, "to_dict"):
        records = df.to_dict(orient="records")
    elif hasattr(df, "to_dicts"):
        records = df.to_dicts()
    else:
        records = list(df)

    for row in records:
        row = dict(row)

        operationType = row.pop("OperationType", None)
        row["OperationType"] = MON_OPERATION_TYPE.get(operationType, "Unknown") if operationType else "Unknown"

        # map Channel properly
        row["Channel"] = MGR_CHANNEL.get(row.get("Channel", None), "Unknown")

        row["SubscriberType"] = BSS_SUBSCRIBER_TYPE.get(row.get("SubscriberType", None), "Unknown")

        row['RecordType'] = 'MON'
        row['Operator'] = 'Vodacom'
        row['FileName'] = filename
        row['DateParsed'] = datetime.now().isoformat()

        row["EventTimeStamp"] = parse_date(row.get("EventTimeStamp"))
        row["CycleBeginTime"] = parse_date(row.get("CycleBeginTime"))
        row["CycleEndTime"] = parse_date(row.get("CycleEndTime"))
        
        Amount = parse_amount("Amount", row.pop("Amount", None))
        row = {**row, **Amount}
        
        PreviousPPSBalance = parse_amount("PreviousPPSBalance", row.pop("PreviousPPSBalance", None))
        row = {**row, **PreviousPPSBalance}
        
        CurrentPPSBalance = parse_amount("CurrentPPSBalance", row.pop("CurrentPPSBalance", None))
        row = {**row, **CurrentPPSBalance}
        
        PreviousPOSBalance = parse_amount("PreviousPOSBalance", row.pop("PreviousPOSBalance", None))
        row = {**row, **PreviousPOSBalance}
        
        CurrentPOSBalance = parse_amount("CurrentPOSBalance", row.pop("CurrentPOSBalance", None))
        row = {**row, **CurrentPOSBalance}
        
        for i in range(1,11):
            tmpName = f"CurrAcctAmount{i}"
            tmp = parse_amount(tmpName, row.pop(tmpName, None))
            row = {**row, **tmp}

        for i in range(1, 11):
            key = f"ExpiryTimeOfAccountType{i}"
            if key in row:
                row[key] = parse_date(row.get(key))

        for i in range(1, 23):
            row.pop(f"RESERVED{i}", None)

        # Convert remaining datetime objects to ISO strings
        for key, value in list(row.items()):
            if isinstance(value, datetime):
                row[key] = value.isoformat()

        row = {k: v for k, v in row.items() if v not in [None, ""]}
        output.append(row)

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
        names=VODACOM_MON_COLUMNS
    )

    # Transform
    filename = sys.argv[1] if len(sys.argv) > 1 else "nifi_input"
    records = map_vodacom_mon_columns(df, filename)

    # If map_vodacom_mon_columns returns a Polars DataFrame, convert to list of dicts
    if isinstance(records, pl.DataFrame):
        records = records.to_dicts()

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