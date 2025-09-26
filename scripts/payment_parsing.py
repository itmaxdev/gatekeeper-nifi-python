import sys
import json
import pandas as pd
from datetime import datetime, timezone
import io
import logging
import os
import polars as pl
# --- NiFi Entry Point ---

VODACOM_PAYMENT_COLUMNS = [
    "RecordType", "OfferID", "GroupID", "SubscriptionID", "BalanceID", 
    "OfferEntitlementName", "MSISDN", "IMSI", "SubscriberID", "SubscriberType", 
    "ChargeType", "RecordOpeningTime", "RecordClosingTime", "SessionID", 
    "SGSNIPAddress", "MCCMNC", "GGSNAddress", "GGSNChargingID", "APN", 
    "RatingGroup", "ServiceID", "RATType", "UserLocationInformation", 
    "MSTimezone", "IsRoaming", "Zone", "CCTime", "CCTotalOctets", 
    "CCInputOctets", "CCOutputOctets", "TerminationCause", "CounterFinalValue", 
    "RatedCashValuePreTax", "RatedCashValuePostTax", "UserEquipmentInfoType", 
    "UserEquipmentInfoValue", "BalanceImpactName", "RequestNumber", "IMEI", 
    "OtherPartyAddress", "Classification", "DirectionType", "ZoneCalled", 
    "CCServiceSpecificUnits", "RequestType", "OfferName", "ResultCode", 
    "VLRID", "GLID", "CarrierID", "RoamingCountry", "AccumulatedInBundle", 
    "AccumulatedOutBundle", "AccumulatedNotApplied", "AccumulatedUsageBalance", 
    "AccumulatedChargeBalance", "RatedCashValue", "ChunkSize", "ChunkNumber", 
    "TaxCode",
    # Reserved columns (61 to 89)
    *[f"ReservedColumn{i}" for i in range(61, 90)],
    # Remaining columns (90 to 123)
    "GrantedCcTime", "GrantedCcTotalOctets", "GrantedCcInputOctets", 
    "GrantedCcOutputOctets", "GrantedCcServiceSpecificUnits", "RawUsedCcTime", 
    "RawUsedCcTotalOctets", "RawUsedCcInputOctets", "RawUsedCcOutputOctets", 
    "RawUsedCcServiceSpecificUnits", "HuaweiOfferID", "HuaweiOfferName", 
    "PrimaryOfferID", "Channel", "EntitlementUnitType", "UserSessionID", 
    "VASCategoryID", "VASContentID", "VASSPID", "VASServiceID", "VASCDRINFO1", 
    "VASCDRINFO2", "VASCDRINFO3", "VASCDRINFO4", "VASCDRINFO5", "VASCDRINFO6", 
    "CustomerType", "ISUPLocationNumber", "RecordOpenningTimeHr", 
    "RecordClosingTimeHr", "RequestedPartyAddress", "GroupOwnerMSISDN", 
    "CUGID", "UsedUnits"
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

def map_vodacom_payment_columns(df: pl.DataFrame, filename: str) -> pl.DataFrame:
    # Convert Polars DataFrame to list of dicts for row-wise processing
    output = []
    for row in df.to_dicts():
        subscriberType = row.pop("SubscriberType", None)
        row["SubscriberType"] = SUBSCRIBER_TYPE.get(subscriberType, "Unknown") if subscriberType else "Unknown"

        operationType = row.pop("OperationType", None)
        row["OperationType"] = OPERATION_TYPE.get(operationType, "Unknown") if operationType else "Unknown"

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
        
        LoanRepaymentAmount = parse_amount("LoanRepaymentAmount", row.pop("LoanRepaymentAmount", None))
        row = {**row, **LoanRepaymentAmount}
        
        row["FirstActiveDate"] = parse_date(row.get("FirstActiveDate"))

        row["EventTimeStamp"] = parse_date(row.get("EventTimeStamp"))

        Direction = row.pop("Direction", None)
        row["CreditType"] = CREDIT_TYPE.get(Direction, "Unknown") if Direction else "Unknown"

        row['RecordType'] = 'Voucher'
        row['Operator'] = 'Vodacom'
        row['FileName'] = filename
        row['DateParsed'] = datetime.now().isoformat()

        # Convert datetime values to ISO 8601 strings
        for key, value in row.items():
            if isinstance(value, datetime):
                row[key] = value.isoformat()

        # Remove None and empty string values
        row = {k: v for k, v in row.items() if v not in [None, ""]}
        output.append(row)

    return pl.DataFrame(output)

def parse_date(val):
    try:
        return datetime.strptime(str(val), "%Y%m%d%H%M%S").isoformat() if val and len(str(val)) == 14 else None
    except Exception:
        return None

def parse_date_format(val,format):
    try:
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

    # Output records as JSON lines to stdout
    for record in records.to_dicts():
        print(json.dumps(record))

if __name__ == "__main__":
    main()
