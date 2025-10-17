import sys
import traceback
from asn1.tap_compiler import clean_nested
from asn1.tap_compiler import decode_tap_file, normalize_tap_file

def main():
    raw_data = sys.stdin.buffer.read()  # read bytes, not text
    filename = sys.argv[1] if len(sys.argv) > 1 else "nifi_input"
    
    try:
        # print(response)
        response = decode_tap_file(raw_data)
        clear_response = clean_nested(response)
        normalized_records = normalize_tap_file(clear_response, filename, "Vodacom")
        if normalized_records:
            for record in normalized_records:
                print(record)
        else:
            print("No records found", file=sys.stderr)
            # sys.exit(1)
    except Exception as e:
        print(f"Error decoding: {e}", file=sys.stderr)
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
