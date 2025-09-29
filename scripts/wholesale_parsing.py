import sys
import json
import pandas as pd
from datetime import datetime, timezone
import io
import logging
import os
import polars as pl

VODACOM_WHOLESALE_COLUMNS = [
    "DATE", "DIRECTION", "TYPE", "TRANSACTION_TYPE", "OPERATOR_CARRIER", "TRANSACTIONS", "MINUTES", "CHARGE", "DEST_COUNTRY"
]


def parse_date_format(val,format):
    try:
        return datetime.strptime(str(val), format).isoformat() if val else None
    except Exception:
        return None


def map_vodacom_wholesale_columns(df: pl.DataFrame, filename: str) -> pl.DataFrame:
    output = []


    for row in df.to_dicts():
        row_dict = dict(row)

        row_dict['RecordType'] = 'Wholesale'
        row_dict['Operator'] = 'Vodacom'
        row_dict['FileName'] = filename
        row_dict['DateParsed'] = datetime.now().isoformat()
        row_dict["DATE"] = parse_date_format(row.get("DATE"), "%Y/%m/%d")
        row_dict["TRANSACTIONS"] = int(row.get("TRANSACTIONS", 0)) if row.get("TRANSACTIONS") else 0
        row_dict["MINUTES"] = float(row.get("MINUTES", 0)) if row.get("MINUTES") else 0.0
        row_dict["CHARGE"] = float(row.get("CHARGE", 0)) if row.get("CHARGE") else 0.0
        row_dict["CHARGE_CURRENCY"] = "USD"

        # row_dict["EventTimeStamp"] = parse_date(row.get("EventTimeStamp"))
        # row_dict["ReturnMode"] = VODACOM_RETURN_MODE.get(row.get("ReturnMode", None), "Unknown")

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
        names=VODACOM_WHOLESALE_COLUMNS
    )

    # Transform
    filename = sys.argv[1] if len(sys.argv) > 1 else "nifi_input"
    records = map_vodacom_wholesale_columns(df, filename)

    # Output records as JSON lines to stdout
    for record in records.to_dicts():
        print(json.dumps(record))

if __name__ == "__main__":
    main()