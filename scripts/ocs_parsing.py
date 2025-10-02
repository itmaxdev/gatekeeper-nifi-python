#!/usr/bin/env python3
import sys
import json
import pandas as pd
from datetime import datetime, timezone
import io
import logging
import os
import polars as pl
import math

# --- Logging Setup ---

VODACOM_CDR_COLUMNS = [
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


# --- Static Mappings (example placeholders, replace with your actual dicts) ---
RECORD_TYPE = {
    "1": "Data",
    "2": "Voice",
    "3": "SMS",
    "4": "MMS",
    "5": "VAS"
}
SUBSCRIBER_TYPE = {
    "1": "Prepaid",
    "2": "Postpaid",
    "3": "Prepaid",
    "prepaid": "Prepaid",
    "postpaid": "Postpaid",
    "hybrid": "Prepaid"
}
VAS_CATEGORY_ID = {
    "1": "RBT Service fee",
    "2": "Song fee",
    "3": "Musicbox fee",
    "4": "PLUS Service fee",
    "5": "RBT Monthly Package fee",
    "6": "Monthly rental for calling RBTs",
    "7": "Fee for setting an RBT",
    "8": "One-off charge for service description"
}
VAS_CONTENT_ID = {
    "1": "RBT",
    "2": "MRBT"
}
CDR_SERVICE_ID = {
    "4": "MO",
    "6": "MT"
}

# --- Helpers ---
def to_str(val):
    # Treat None and any pandas/NumPy NA as empty string
    if val is None:
        return ""
    try:
        if pd.isna(val):
            return ""
    except Exception:
        pass
    return str(val)

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
        return {
            key: v / 10000,
            key + "Currency": "cents"
        }

# --- Core Mapping Function ---
def map_vodacom_cdr_columns(df, filename):
    output = []
    for idx, row in df.iterrows():
        classification = str(row.get("Classification", "") or "")
        TotalChargeRaw = row.get("RatedCashValuePostTax") or row.get("ReservedColumn88") or ""
        # safe int parsing for timestamps
        def safe_int(v):
            try:
                if v is None:
                    return 0
                try:
                    if pd.isna(v):
                        return 0
                except Exception:
                    pass
                return int(float(str(v).strip()))
            except Exception:
                return 0

        RecordOpeningTime = safe_int(row.get("RecordOpeningTime"))
        RecordClosingTime = safe_int(row.get("RecordClosingTime"))

        TotalCharge = parse_amount("TotalCharge", TotalChargeRaw)

        row_dict = {
            "RecordType": to_str(RECORD_TYPE.get(row.get("RecordType"), "Unknown")),
            "OfferID": to_str(row.get("OfferID")),
            "BalanceID": to_str(row.get("BalanceID")),
            "MSISDN": to_str(row.get("MSISDN")),
            "IMSI": to_str(row.get("IMSI")),
            "SubscriberTypeCode": to_str(row.get("SubscriberType")),
            "SubscriberType": to_str(SUBSCRIBER_TYPE.get(row.get("SubscriberType"), "Unknown")),
            "IsCharged": to_str(row.get("ChargeType") == "0"),
            "SGSNIPAddress": to_str(row.get("SGSNIPAddress")),
            "GGSNAddress": to_str(row.get("GGSNAddress")),
            "GGSNChargingID": to_str(row.get("GGSNChargingID")),
            "RecordOpeningTime": to_str(datetime.fromtimestamp(RecordOpeningTime, tz=timezone.utc).isoformat()) if RecordOpeningTime else "",
            "RecordClosingTime": to_str(datetime.fromtimestamp(RecordClosingTime, tz=timezone.utc).isoformat()) if RecordClosingTime else "",
            "IsRoaming": to_str(row.get("IsRoaming") == "Y"),
            "DataVolumeGPRSTotal": to_str(row.get("CCTotalOctets")),
            "DataVolumeGPRSDownlink": to_str(row.get("CCInputOctets")),
            "DataVolumeGPRSUplink": to_str(row.get("CCOutputOctets")),
            "TerminationCause": to_str(row.get("TerminationCause")),
            "IMEI": to_str(row.get("IMEI")),
            "DirectionType": to_str(row.get("DirectionType")),
            "OfferName": to_str(row.get("OfferName")),
            "RoamingCountry": to_str(row.get("RoamingCountry")),
            "RatedCashValue": to_str(row.get("RatedCashValue")),
            "HuaweiOfferID": to_str(row.get("HuaweiOfferID")),
            "HuaweiOfferName": to_str(row.get("HuaweiOfferName")),
            "PrimaryOfferID": to_str(row.get("PrimaryOfferID")),
            "VASCategoryID": to_str(VAS_CATEGORY_ID.get(row.get("VASCategoryID"))),
            "Direction": to_str(VAS_CONTENT_ID.get(row.get("VASContentID"))),
            "VASSPID": to_str(row.get("VASSPID")),
            "ServiceID": to_str(CDR_SERVICE_ID.get(row.get("ServiceID"), "")),
            "VASServiceID": to_str(row.get("VASServiceID")),
            "VASCDRINFO1": to_str(row.get("VASCDRINFO1")),
            "Duration": to_str(row.get("CCTime", "0")),
            "RequestedPartyAddress": to_str(row.get("RequestedPartyAddress")),
            "GroupOwnerMSISDN": to_str(row.get("GroupOwnerMSISDN")),
            "CUGID": to_str(row.get("CUGID")),
            "UsedUnits": to_str(row.get("UsedUnits")),
            "Operator": "Vodacom",
            "FileName": to_str(filename),
            "DateParsed": datetime.now().isoformat()
        }

        # Merge charge info
        row_dict.update(TotalCharge)

        # UserLocationInformation breakdown
        userLocation = row.get("UserLocationInformation")
        if userLocation:
            for item in str(userLocation).split(":"):
                if "CGI_MCCMNC" in item and "=" in item:
                    row_dict["CGI_MCCMNC"] = to_str(item.split("=", 1)[1])
                elif "CGI_LAC" in item and "=" in item:
                    row_dict["LocationAreaCode"] = to_str(item.split("=", 1)[1])
                elif "CGI_CI" in item and "=" in item:
                    row_dict["CellId"] = to_str(item.split("=", 1)[1])
                elif "TYPE" in item and "=" in item:
                    row_dict["LocationType"] = to_str(item.split("=", 1)[1])

        # Classification flags
        if classification == "ONNET":
            row_dict["IsOnNet"] = to_str(True)
        elif classification == "OFFNET":
            row_dict["IsOnNet"] = to_str(False)
        elif "SPEC" in classification:
            row_dict["IsSpecial"] = to_str(True)
        elif "MT_NATIONAL" in classification:
            row_dict["IsInternational"] = to_str(True)

        output.append(row_dict)

    return output

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
        names=VODACOM_CDR_COLUMNS,
        dtype=str
    )
    # Ensure all NaN/NA values become empty strings
    df = df.fillna("")

    # Transform
    filename = sys.argv[1] if len(sys.argv) > 1 else "nifi_input"
    records = map_vodacom_cdr_columns(df, filename)

    # If records is a Polars DataFrame convert (defensive)
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
        # keep empty string as None to produce cleaner JSON objects
        if isinstance(v, str) and v == "":
            return None
        return v

    sanitized = [{k: sanitize_value(v) for k, v in (r or {}).items()} for r in records]

    # Emit a ndjson response
    for record in sanitized:
        print(json.dumps(record, ensure_ascii=False))

if __name__ == "__main__":
    main()
