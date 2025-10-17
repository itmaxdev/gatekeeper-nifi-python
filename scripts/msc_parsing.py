import sys
import traceback
from asn1.msc_compiler import *


def decode_raw_msc(raw_data, filename):
    decoded = decode_msc_file(raw_data)
    cleaned = clean_nested(decoded)
    prefix_to_remove = ["18", "19", "110", "180", "0"]
    normalized_records = normalize_msc_file(cleaned, filename, "Vodacom", prefix_to_remove)
    with open(f"output/{filename}_msc_normalized.txt", "w") as out_file:
        for record in normalized_records:
            out_file.write(str(record) + "\n")
    # Process the decoded data as needed
    # print(f"Decoded MSC data from {filename}: {normalized_records}")
    # sys.exit(1)

def main():
    raw_data = sys.stdin.buffer.read()  # read bytes, not text
    filename = sys.argv[1] if len(sys.argv) > 1 else "nifi_input"
    decode_raw_msc(raw_data, filename)

if __name__ == "__main__":
    main()