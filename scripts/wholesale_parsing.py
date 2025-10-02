import sys
import json
import polars as pl
from datetime import datetime
import io
import math

VODACOM_WHOLESALE_COLUMNS = [
    "DATE_", "DIRECTION", "TYPE", "TRANSACTION_TYPE", "OPERATOR_CARRIER",
    "TRANSACTIONS", "MINUTES", "CHARGE", "DEST_COUNTRY"
]

def parse_date_format(val: str, fmt: str):
    if val is None:
        return None
    if isinstance(val, str) and val.strip() == "":
        return None
    try:
        return datetime.strptime(val, fmt).isoformat()
    except Exception:
        return None

def safe_int(val, default=0):
    try:
        if val is None or (isinstance(val, str) and val.strip() == ""):
            return default
        return int(float(val))
    except Exception:
        return default

def safe_float(val, default=0.0):
    try:
        if val is None or (isinstance(val, str) and val.strip() == ""):
            return default
        return float(val)
    except Exception:
        return default

def map_vodacom_wholesale_columns(df: pl.DataFrame, filename: str) -> pl.DataFrame:
    # add derived columns using polars expressions
    df = df.with_columns([
        pl.lit("Wholesale").alias("RecordType"),
        pl.lit("Vodacom").alias("Operator"),
        pl.lit(filename).alias("FileName"),
        pl.lit(datetime.now().isoformat()).alias("DateParsed"),
        pl.col("DATE").map_elements(lambda x: parse_date_format(x, "%Y/%m/%d"), return_dtype=pl.Utf8),
        pl.col("TRANSACTIONS").map_elements(lambda x: safe_float(x), return_dtype=pl.Float64),
        pl.col("MINUTES").map_elements(lambda x: safe_float(x), return_dtype=pl.Float64),
        pl.col("CHARGE").map_elements(lambda x: safe_float(x), return_dtype=pl.Float64),
        pl.lit("USD").alias("CHARGE_CURRENCY"),
    ])
    return df

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
        if hasattr(v, "item"):  # polars scalars
            return v.item()
    except Exception:
        pass
    return v

# --- NiFi Entry Point ---
def main():
    raw_data = sys.stdin.read()

    df = pl.read_csv(
        io.StringIO(raw_data),
        separator=',',
        has_header=False,
        skip_rows=1,
        try_parse_dates=False,
        schema_overrides={col: pl.Utf8 for col in VODACOM_WHOLESALE_COLUMNS},
        ignore_errors=True,
        truncate_ragged_lines=True
    )

    # handle extra cols → DEST_COUNTRY
    if df.width > len(VODACOM_WHOLESALE_COLUMNS):
        extra_cols = df.columns[len(VODACOM_WHOLESALE_COLUMNS)-1:]
        df = df.with_columns([
            pl.concat_str([pl.col(c) for c in extra_cols], separator=",").alias("DEST_COUNTRY")
        ])
        df = df.drop(extra_cols[:-1])

    df.columns = VODACOM_WHOLESALE_COLUMNS
    if "DATE_" in df.columns:
        df = df.rename({"DATE_": "DATE"})

    filename = sys.argv[1] if len(sys.argv) > 1 else "nifi_input"
    df = map_vodacom_wholesale_columns(df, filename)

    # convert to dicts for JSON output
    records = df.to_dicts()

    for record in records:
        sanitized = {k: sanitize_value(v) for k, v in record.items()}
        print(json.dumps(sanitized, ensure_ascii=False))

if __name__ == "__main__":
    main()
