import sys
import json
import pandas as pd
from datetime import datetime, timezone
import io
import logging
import os
import polars as pl


VODACOM_CLRCDR_COLUMNS = [
    "SerialNo", "EventTimestamp", "SubscriptionID", "PrimaryOffer", "CustomerType", "OptionalOfferID", "SubscriberType", "CycleDay", "SubscriberID", " CurrUserState", "ResultCode", "PrepaidBalance", "PostpaidBalance", "Loan", " NonEligibleOffers", "LoanAmnt", "LoanState", "AccountType1", "BalanceImpactAccountType1", "CleanedAmount1", "ExpiryTimeOfAccountType1", "AccountType2", "BalanceImpactAccountType2", "CleanedAmount2", "ExpiryTimeOfAccountType2", "AccountType3", "BalanceImpactAccountType3", "CleanedAmount3", "ExpiryTimeOfAccountType3", "AccountType4", "BalanceImpactAccountType4", "CleanedAmount4", "ExpiryTimeOfAccountType4", "AccountType5", "BalanceImpactAccountType5", "CleanedAmount5", "ExpiryTimeOfAccountType5", "AccountType6", "BalanceImpactAccountType6", "CleanedAmount6", "ExpiryTimeOfAccountType6", "AccountType7", "BalanceImpactAccountType7", "CleanedAmount7", "ExpiryTimeOfAccountType7", "AccountType8", "BalanceImpactAccountType8", "CleanedAmount8", "ExpiryTimeOfAccountType8", "AccountType9", "BalanceImpactAccountType9", "CleanedAmount9", "ExpiryTimeOfAccountType9", "AccountType10", "BalanceImpactAccountType10", "CleanedAmount10", "ExpiryTimeOfAccountType10", "CorrelationID", "OpenetOfferID", "HuaweiOfferID", "MSISDN", "RESERVED1", "RESERVED2", "RESERVED3"
]

BSS_SUBSCRIBER_TYPE = {
    "0": "Prepaid",
    "1": "Postpaid",
    "2": "Prepaid"   
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


def map_vodacom_clrcdr_columns(df: pl.DataFrame, filename: str) -> pl.DataFrame:
    output = []


    for row in df.to_dicts():
        row_dict = dict(row)

        row_dict['RecordType'] = 'CLRCDR'
        row_dict['Operator'] = 'Vodacom'
        row_dict['FileName'] = filename
        row_dict['DateParsed'] = datetime.now().isoformat()
        
        row_dict["EventTimestamp"] = parse_date(row.get("EventTimestamp"))
        row_dict["SubscriberType"] = BSS_SUBSCRIBER_TYPE.get(row.get("SubscriberType", None), "Unknown")
        PrepaidBalance = parse_amount("PrepaidBalance", row.pop("PrepaidBalance", None))
        row_dict = {**row_dict, **PrepaidBalance}
        PostpaidBalance = parse_amount("PostpaidBalance", row.pop("PostpaidBalance", None))
        row_dict = {**row_dict, **PostpaidBalance}
        Loan = parse_amount("Loan", row.pop("Loan", None))
        row_dict = {**row_dict, **Loan}
        LoanAmnt = parse_amount("LoanAmnt", row.pop("LoanAmnt", None))
        row_dict = {**row_dict, **LoanAmnt}
        
        row_dict.pop("SubscriptionID")
        row_dict.pop("CycleDay")
        row_dict.pop("CorrelationID")
        
        for i in range(1, 11):
            expiryTime = f"ExpiryTimeOfAccountType{i}"
            tmp = f"CleanedAmount{i}"
            accountType = f"AccountType{i}"
            if row_dict.get(accountType) not in ['BI_CASH_PREPAID','BI_CASH_DISABLE_PAYG_BLOCK']:
                tmpAmount = parse_amount(tmp, row.pop(tmp, None))
                row_dict = {**row_dict, **tmpAmount}
            row_dict[expiryTime] = parse_date(row.pop(expiryTime, None))



        # Convert any remaining datetime objects to ISO 8601
        for key, value in row_dict.items():
            if isinstance(value, datetime):
                row_dict[key] = value.isoformat()

        # Remove None and empty string values
        row_dict = {k: v for k, v in row_dict.items() if v not in [None, "", 0.0]}
        output.append(row_dict)

    return pl.DataFrame(output)


# --- NiFi Entry Point ---``
def main():
    # setup_logging(log_path)

    # Read CSV from stdin
    raw_data = sys.stdin.read()
    # Load into Pandas DataFrame
    df = pd.read_csv(
        io.StringIO(raw_data),
        sep="|",
        header=None,
        names=VODACOM_CLRCDR_COLUMNS
    )

    # Transform
    filename = sys.argv[1] if len(sys.argv) > 1 else "nifi_input"
    records = map_vodacom_clrcdr_columns(df, filename)

    # Output records as JSON lines to stdout
    for record in records.to_dicts():
        print(json.dumps(record))

if __name__ == "__main__":
    main()