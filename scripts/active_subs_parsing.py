import sys
import json
import pandas as pd
from datetime import datetime, timezone
import io
import logging
import os
import polars as pl
import math

VODACOM_ACTIVE_SUBS_COLUMNS = [
    "DOC_ID", "ENTRY_ID", "OBJECT_ID", "PARENT_ID", "BAN", "CARRIER_ID", "CREATION_DATE", "CURRENT_STATE", "CYCLE_DAY", "EXPIRY_DATE", "FIRST_NAME", "IMEI", "IMSI", "LAST_NAME", "LIFECYCLE_ID", "LOCALE", "MSISDN", "OWNER", "PREFERRED_NOTIFICATION_CHANNEL", "PREVIOUS_CYCLE_DAY", "PREVIOUS_STATE", "PRIVILEGE_PROFILE", "SUBSCRIBER_ID", "TITLE", "TRANSITION_DATE", "TYPE", "ONE_TIME_REDIRECTION_FLAG", "TRACE"
]






def map_vodacom_active_subs_columns(df: pl.DataFrame, filename: str) -> pl.DataFrame:
    output = []

    # support both pandas and polars DataFrame inputs
    if hasattr(df, "to_dict"):
        records = df.to_dict(orient="records")
    elif hasattr(df, "to_dicts"):
        records = df.to_dicts()
    else:
        # fallback: try iterating
        records = list(df)

    for row in records:
        row_dict = dict(row)

        row_dict['RecordType'] = 'ActiveSubscriber'
        row_dict['Operator'] = 'Vodacom'
        row_dict['FileName'] = filename
        row_dict['DateParsed'] = datetime.now().isoformat()
        
        def safe_timestamp(val):
            # guard against None, empty strings and NaN
            if val is None:
                return None
            try:
                if pd.isna(val):
                    return None
            except Exception:
                pass
            val_str = str(val).replace('"', '').strip()
            if val_str == "":
                return None
            try:
                return datetime.fromtimestamp(int(val_str) / 1000).isoformat()
            except Exception:
                return None

        if row.get('EXPIRY_DATE') is not None and row.get('EXPIRY_DATE') != "":
            row_dict["EXPIRY_DATE"] = safe_timestamp(row.get('EXPIRY_DATE'))
        if row.get('TRANSITION_DATE') is not None and row.get('TRANSITION_DATE') != "":
            row_dict["TRANSITION_DATE"] = safe_timestamp(row.get('TRANSITION_DATE'))
        if row.get('CREATION_DATE') is not None and row.get('CREATION_DATE') != "":
            row_dict["CREATION_DATE"] = safe_timestamp(row.get('CREATION_DATE'))

        row_dict.pop("ENTRY_ID", None)
        row_dict.pop("OBJECT_ID", None)
        row_dict.pop("BAN", None)
        row_dict.pop("CARRIER_ID", None)
        row_dict.pop("LIFECYCLE_ID", None)
        row_dict.pop("PREFERRED_NOTIFICATION_CHANNEL", None)
        row_dict.pop("TRACE", None)
        row_dict.pop("TRANSACTIONS", None)
        row_dict.pop("MINUTES", None)
        row_dict.pop("CHARGE", None)
        row_dict.pop("CHARGE_CURRENCY", None)

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
        names=VODACOM_ACTIVE_SUBS_COLUMNS
    )

    # Transform
    filename = sys.argv[1] if len(sys.argv) > 1 else "nifi_input"
    records = map_vodacom_active_subs_columns(df, filename)

    # If map_vodacom_active_subs_columns returns a Polars DataFrame, convert to list of dicts
    if isinstance(records, pl.DataFrame):
        records = records.to_dicts()

    def sanitize_value(v):
        # None stays None
        if v is None:
            return None
        # datetimes -> isoformat
        if isinstance(v, datetime):
            return v.isoformat()
        # NaN -> None
        try:
            if isinstance(v, float) and math.isnan(v):
                return None
        except Exception:
            pass
        # numpy / polars scalar -> native Python
        try:
            if hasattr(v, "item"):
                return v.item()
        except Exception:
            pass
        return v

    sanitized = [{k: sanitize_value(v) for k, v in (r or {}).items()} for r in records]

    # Emit a single JSON array (valid JSON for EvaluateJsonPath)
    print(json.dumps(sanitized, ensure_ascii=False))

if __name__ == "__main__":
    main()