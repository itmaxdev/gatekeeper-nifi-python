import sys
import traceback
from asn1.sgsn_compiler import *
import bisect
import json


def make_json_serializable(obj):
    """Recursively convert all bytes objects to hex strings for JSON serialization."""
    if isinstance(obj, dict):
        return {k: make_json_serializable(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [make_json_serializable(item) for item in obj]
    elif isinstance(obj, tuple):
        return tuple(make_json_serializable(item) for item in obj)
    elif isinstance(obj, (bytes, bytearray)):
        return '0x' + obj.hex()
    else:
        return obj


def decode_raw_sgsn(raw_data, filename):
    try:
        # The input file may contain multiple concatenated BER records.
        # Iterate over each BER element and decode them individually.

        records = []
        # print(f"Decoding file: {filename}")
        missing_data = fetch_missing_fields(raw_data, filename)
        # print(f"Missing data: {len(missing_data['extracted_data'])}")
        # missing_extracted_data = missing_data.get('extracted_data')
        offset = 0
        # for mid in missing_extracted_data:
        
        # Pre-index proximity-matched entries by context to avoid scanning
        # the entire list for each record. Each entry list is converted to
        # two parallel arrays: sorted offsets and corresponding data dicts.
        indices = {}
        try:
            for ctx_name, lst in missing_data.items():
                if not isinstance(lst, list):
                    continue
                arr = [(entry.get('offset'), entry.get('data') or {}) for entry in lst if entry.get('offset') is not None]
                if not arr:
                    indices[ctx_name] = ([], [])
                    continue
                arr.sort(key=lambda x: x[0])
                offsets = [o for o, d in arr]
                datas = [d for o, d in arr]
                indices[ctx_name] = (offsets, datas)
        except Exception:
            # Defensive: if indexing fails, fall back to raw lists (original behavior)
            indices = {}

        # Helper that merges nearby context entries using bisect on the
        # precomputed offsets array. This avoids scanning entire lists per record.
        def merge_ctx_index(ctx_name, prev_offset, mapped_record, key_map=None, max_distance=1024):
            tup = indices.get(ctx_name)
            if not tup:
                # Fall back to original list scanning if we don't have an index
                lst = missing_data.get(ctx_name, [])
                for entry in lst:
                    off = entry.get('offset')
                    if off is None:
                        continue
                    if abs(off - prev_offset) <= max_distance:
                        data = entry.get('data') or {}
                        if key_map:
                            for src, dst in key_map.items():
                                if src in data and dst not in mapped_record:
                                    mapped_record[dst] = data[src]
                        else:
                            for k, v in data.items():
                                if k not in mapped_record:
                                    mapped_record[k] = v
                return

            offsets, datas = tup
            if not offsets:
                return
            lo = bisect.bisect_left(offsets, prev_offset - max_distance)
            hi = bisect.bisect_right(offsets, prev_offset + max_distance)
            for i in range(lo, hi):
                data = datas[i] or {}
                if key_map:
                    for src, dst in key_map.items():
                        if src in data and dst not in mapped_record:
                            mapped_record[dst] = data[src]
                else:
                    for k, v in data.items():
                        if k not in mapped_record:
                            mapped_record[k] = v

        raw_records = list(iter_ber_elements(raw_data))
        for chunk in raw_records:
            # print(f"Decoding BER element of length {len(chunk)} bytes")
            # print(f"Offset: {offset}")
            prev_offset = offset
            offset += len(chunk)
            # print(f"Record {record_count}: Offset {prev_offset} - {offset} (length {len(chunk)})")
            decoded = decode_sgsn_file(chunk)
            # print(decoded)
            # print('--------------------------------')
            pretty = pretty_decode(decoded)
            # print(pretty)
            mapped_record = map_sgsn_record(pretty, filename)
            # Merge in any values found by the direct binary parser when
            # asn1tools didn't decode them (servedMSISDN, LAC/CID, timestamps,
            # etc). The direct parser returns lists like 'context_27_found'
            # containing dicts of {'offset': n, 'data': {...}} where offsets
            # are file-relative. We compare against this chunk's start offset
            # (prev_offset) and copy nearby values.
            try:
                if missing_data:
                    # servedMSISDN (context_27), servedIMSI (context_3), timestamps (context_16)
                    merge_ctx_index('context_27_found', prev_offset, mapped_record)
                    merge_ctx_index('context_3_found', prev_offset, mapped_record)
                    merge_ctx_index('context_16_found', prev_offset, mapped_record)
                    # location / cell lists
                    merge_ctx_index('context_8_found', prev_offset, mapped_record, key_map={'locationAreaCode': 'locationAreaCode', 'locationAreaCode_first2bytes': 'locationAreaCode_first2bytes'}, max_distance=1024)
                    merge_ctx_index('context_9_found', prev_offset, mapped_record, key_map={'cellIdentifier': 'cellIdentifier', 'cellIdentifier_first2bytes': 'cellIdentifier_first2bytes'}, max_distance=1024)
                    # recordSequenceNumber / localSequenceNumber
                    merge_ctx_index('context_21_found', prev_offset, mapped_record)
            except Exception:
                # non-fatal merging errors should not stop processing
                pass

            # print(mapped_record)
            # exit(0)

            records.append(mapped_record)
        if not records:
            # fallback: try decoding whole blob as single record
            # print("fallback: try decoding whole blob as single record")
            decoded = decode_sgsn_file(raw_data)
            pretty = pretty_decode(decoded)
            mapped_record = map_sgsn_record(pretty)
            records.append(mapped_record)
        for record in records:
            serializable_record = make_json_serializable(record)
            print(json.dumps(serializable_record, ensure_ascii=False))
        # clear_response = clean_nested(response)
        # normalized_records = normalize_sgsn_file(clear_response, filename, "Vodacom")
        # if normalized_records:
        #     for record in normalized_records:
        #         print(record)
        # else:
        #     print("No records found", file=sys.stderr)
        #     # sys.exit(1)
    except Exception as e:
        print(f"Error decoding: {e}", file=sys.stderr)
        traceback.print_exc()
        sys.exit(1)

def main():
    raw_data = sys.stdin.buffer.read()  # read bytes, not text
    # iter through all files in input_path
    filename = sys.argv[1] if len(sys.argv) > 1 else "nifi_input"

    decode_raw_sgsn(raw_data, filename)

if __name__ == "__main__":
    main()
