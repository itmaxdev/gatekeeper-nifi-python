import sys
import json
import pandas as pd
from datetime import datetime, timezone
import io
import logging
import os
import polars as pl
import math

VODACOM_WHOLESALE_COLUMNS = [
    "DATE", "DIRECTION", "TYPE", "TRANSACTION_TYPE", "OPERATOR_CARRIER", "TRANSACTIONS", "MINUTES", "CHARGE", "DEST_COUNTRY"
]


def parse_date_format(val, format):
    try:
        if val is None:
            return None
        # guard against NaN or empty strings
        if pd.isna(val) or (isinstance(val, str) and val.strip() == ""):
            return None
        return datetime.strptime(str(val), format).isoformat()
    except Exception:
        return None


def safe_int(val, default=0):
    try:
        if val is None or pd.isna(val) or (isinstance(val, str) and val.strip() == ""):
            return default
        # convert via float to handle values like "0.0"
        return int(float(val))
    except Exception:
        return default


def safe_float(val, default=0.0):
    try:
        if val is None or pd.isna(val) or (isinstance(val, str) and val.strip() == ""):
            return default
        return float(val)
    except Exception:
        return default


def map_vodacom_wholesale_columns(df: pd.DataFrame, filename: str) -> pl.DataFrame:
    output = []

    for row in df.to_dict(orient="records"):
        row_dict = dict(row)

        row_dict['RecordType'] = 'Wholesale'
        row_dict['Operator'] = 'Vodacom'
        row_dict['FileName'] = filename
        row_dict['DateParsed'] = datetime.now().isoformat()
        row_dict["DATE"] = parse_date_format(row.get("DATE"), "%Y/%m/%d")
        row_dict["TRANSACTIONS"] = safe_int(row.get("TRANSACTIONS", 0), 0)
        row_dict["MINUTES"] = safe_float(row.get("MINUTES", 0), 0.0)
        row_dict["CHARGE"] = safe_float(row.get("CHARGE", 0), 0.0)
        row_dict["CHARGE_CURRENCY"] = "USD"

        # Convert any remaining datetime objects to ISO 8601
        for key, value in row_dict.items():
            if isinstance(value, datetime):
                row_dict[key] = value.isoformat()

        row_dict = {k: v for k, v in row_dict.items() if v not in [None, ""]}
        output.append(row_dict)

    return pl.DataFrame(output)





# --- NiFi Entry Point ---
def main():
    # Read CSV from stdin
    raw_data = sys.stdin.read()

    # Load into Pandas DataFrame
    df = pd.read_csv(
        io.StringIO(raw_data),
        sep="|",
        header=None,
        names=VODACOM_WHOLESALE_COLUMNS
    )

    filename = sys.argv[1] if len(sys.argv) > 1 else "nifi_input"
    records = map_vodacom_wholesale_columns(df, filename)

    # If map_vodacom_wholesale_columns returns a Polars DataFrame, convert to list of dicts
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