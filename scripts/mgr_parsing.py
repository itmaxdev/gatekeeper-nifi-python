import sys
import json
import pandas as pd
from datetime import datetime, timezone
import io
import logging
import os
import polars as pl
import math


VODACOM_MGR_COLUMNS = [
    "SerialNo", "Channel", "EventTimeStamp", "SubscriberID", "ChargingPartyNumber", "MSISDN", "OperationID", 
    "OperationType", "TransferAmount", "FeeAmount", "PrimaryOffer", "TotalCharge", "CustomerType", 
    "NewPrimaryOffer", "SubscriberType", "CycleDay", "SubscriberBeginTime", "SubscriberEndTime", 
    "ResultCode", "ChargeFromPrepaid", "PrepaidBalance", "ChargeFromPostpaid", "PostpaidBalance", 
    "BalanceType", "BalanceDeducted", "CurrentBalance", "NewPassword", "OldPassword", "NewMSISDN", 
    "Transferee", "IMSI", "MaxActiveDays", "MemberGroup", "GroupOperationType", "GroupType", 
    "MemberType", "MemberNo", "LastActiveDate", "FirstActiveDate", "LastRechargeTime", "LangType",
    "SubscriberStates", "SubscriberOffers", "TransferorAcctChgList", "TransfereeAcctChgList", 
    "TransfereeAcctValidity", "AccountType", "SMSLangType", "NewSMSLangType", "NewLangType", "AdjustAccountAmount", 
    "AdditionalInfo", "CorrelationID", "HostName", "Direction", "OperatorID", "LocationInformation", 
    "AdjustAccountIndicator", "AdjustAccountBatchNumber", "AdjustAccountNotificationIndicator", 
    "AdjustAccountSource", "AdjustAccountAdditionalInfo", "OptionalOfferId", "RESERVED1", "RESERVED2", "RESERVED3", 
    "RESERVED4", "RESERVED5", "RESERVED6", "RESERVED7", "RESERVED8", "RESERVED9", "RESERVED10", "RESERVED11", 
    "RESERVED12", "RESERVED13", "RESERVED14", "RESERVED15", "RESERVED16", "RESERVED17", "RESERVED18", "RESERVED19", 
    "RESERVED20", "RESERVED21", "RESERVED22"

]

MGR_OPERATION_TYPE = {
    "0": "Cancel",
    "1": "Enable",
    "4": "Add",
    "5": "Modify",
    "6": "Delete",
    "7": "Query",
    "8": "Transfer-Out",
    "9": "Transfer-In",
    "15": "One-Time Charge",
    "30": "Reset FN Number",
    "999": "Not Specified"
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

MGR_OPERATION_ID = {
    "4052537": "Manage subscriber group members",
    "4052133": "Query account balance",
    "4052125": "Transfer free resources",
    "4052112": "Change main product",
    "4052101": "Transfer account balance",
    "4052106": "Modify subscriber language",
    "4052100": "Change account balance",
    "4050022": "Change subscriber password",
    "4050018": "Unsubscribe to product",
    "4050009": "Change MSISDN",
    "4050001": "Subscribe to product for individual subscriber"
}

MGR_LANG_TYPE = {
    "1": "French",
    "2": "English",
    "3": "Kikongo",
    "4": "Swahili",
    "5": "Lingala",
    "6": "Tshiluba"
}

BSS_SUBSCRIBER_TYPE = {
    "0": "Prepaid",
    "1": "Postpaid",
    "2": "Prepaid"   
}

CREDIT_TYPE = {
    "0": "Credit",
    "1": "Debit"
}


def parse_amount(key,value):
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
        return {key: v / 10000, key+"Currency": "cents"}
        
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

def map_vodacom_mgr_columns(df: pl.DataFrame, filename: str) -> pl.DataFrame:
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
        row["OperationType"] = MGR_OPERATION_TYPE.get(operationType, "Unknown") if operationType else "Unknown"

        row["Channel"] = MGR_CHANNEL.get(row.get("Channel", None), "Unknown")

        orerationId = row.pop("OperationID", None)
        row['Operation'] = MGR_OPERATION_ID.get(orerationId, "Unknown") if orerationId else "Unknown"

        row["SubscriberType"] = BSS_SUBSCRIBER_TYPE.get(row.get("SubscriberType", None), "Unknown")
        row['LangType'] = MGR_LANG_TYPE.get(row.get("LangType", None), "Unknown")
        row['SMSLangType'] = MGR_LANG_TYPE.get(row.get("SMSLangType", None), "Unknown")
        row['NewSMSLangType'] = MGR_LANG_TYPE.get(row.get("NewSMSLangType", None), "Unknown")
        row['NewLangType'] = MGR_LANG_TYPE.get(row.get("NewLangType", None), "Unknown")

        row['CreditType'] = CREDIT_TYPE.get(row.get("Direction", None), "Unknown")

        row['RecordType'] = 'MGR'
        row['Operator'] = 'Vodacom'
        row['FileName'] = filename
        row['DateParsed'] = datetime.now().isoformat()
        
        TransferAmount = parse_amount("TransferAmount", row.pop("TransferAmount", None))
        row = {**row, **TransferAmount}
        
        FeeAmount = parse_amount("FeeAmount", row.pop("FeeAmount", None))
        row = {**row, **FeeAmount}
        
        TotalCharge = parse_amount("TotalCharge", row.pop("TotalCharge", None))
        row = {**row, **TotalCharge}
        
        PrepaidBalance = parse_amount("PrepaidBalance", row.pop("PrepaidBalance", None))
        row = {**row, **PrepaidBalance}
        
        ChargeFromPostpaid = parse_amount("ChargeFromPostpaid", row.pop("ChargeFromPostpaid", None))
        row = {**row, **ChargeFromPostpaid}
        
        PostpaidBalance = parse_amount("PostpaidBalance", row.pop("PostpaidBalance", None))
        row = {**row, **PostpaidBalance}
        
        BalanceDeducted = parse_amount("BalanceDeducted", row.pop("BalanceDeducted", None))
        row = {**row, **BalanceDeducted}
        
        CurrentBalance = parse_amount("CurrentBalance", row.pop("CurrentBalance", None))
        row = {**row, **CurrentBalance}
        
        AdjustAccountAmount = parse_amount("AdjustAccountAmount", row.pop("AdjustAccountAmount", None))
        row = {**row, **AdjustAccountAmount}

        row["EventTimeStamp"] = parse_date(row.get("EventTimeStamp"))
        row["SubscriberBeginTime"] = parse_date(row.get("SubscriberBeginTime"))
        row["SubscriberEndTime"] = parse_date(row.get("SubscriberEndTime"))
        row["LastActiveDate"] = parse_date(row.get("LastActiveDate"))
        row["FirstActiveDate"] = parse_date(row.get("FirstActiveDate"))
        row["LastRechargeTime"] = parse_date(row.get("LastRechargeTime"))

        # Remove RESERVED1 to RESERVED22
        for i in range(1, 23):
            row.pop(f"RESERVED{i}", None)

        # Remove None and empty string values
        row = {k: v for k, v in row.items() if v not in [None, ""]}

        output.append(row)

    return pl.DataFrame(output, schema_overrides={k: pl.Utf8 for k in output[0].keys()} if output else None)




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
        names=VODACOM_MGR_COLUMNS
    )

    # Transform
    filename = sys.argv[1] if len(sys.argv) > 1 else "nifi_input"
    records = map_vodacom_mgr_columns(df, filename)

    # If map_vodacom_mgr_columns returns a Polars DataFrame, convert to list of dicts
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
