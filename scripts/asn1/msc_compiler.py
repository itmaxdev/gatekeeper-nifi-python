import asn1tools
import json
import argparse
import os
import sys
from datetime import datetime, timezone, timedelta
import traceback




try:
    # Use the tap_cdr_schema.asn from the schema folder
    sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
    schema_path = os.path.join(
        os.path.dirname(os.path.dirname(__file__)), "schema", "msc_cdr_schema.asn"
    )
    msc_compiler = asn1tools.compile_files(schema_path, "ber")
except Exception as e:
    print(f"Error compiling msc_cdr_schema.asn: {e}")
    print("Please ensure the schema file is correct and contains valid ASN.1 definitions.")
    exit()

def clean_nested(data):
    # Handle dict
    if isinstance(data, dict):
        return {k: clean_nested(v) for k, v in data.items()}
    
    # Handle list
    elif isinstance(data, list):
        return [clean_nested(item) for item in data]
    
    # Handle tuple that looks like (key, value)
    elif isinstance(data, tuple) and len(data) == 2 and isinstance(data[0], str):
        return {data[0]: clean_nested(data[1])}
    
    # Handle bytes
    elif isinstance(data, (bytes, bytearray)):
        try:
            return data.decode("utf-8")
        except:
            return data.hex()  # fallback
    
    # Primitive
    else:
        return data

def get_ber_tlv_length(data):
    """
    Reads the length of a BER-TLV encoded record.
    Handles short-form, long-form, and indefinite-form lengths.
    """
    if len(data) < 2:
        raise ValueError("Data too short to determine TLV length.")
    
    length_octet = data[1]
    
    if (length_octet & 0x80) == 0:
        # Short form: length is in the second byte
        return 2 + length_octet
        
    # Long form
    num_length_octets = length_octet & 0x7f
    if num_length_octets == 0:
        # Indefinite form, not supported for this use case
        raise ValueError("Indefinite-form BER length not supported.")
    
    if (len(data) < 2 + num_length_octets):
        raise ValueError("Data too short for long-form BER length.")
        
    value_length = int.from_bytes(data[2:2 + num_length_octets], 'big')
    return 2 + num_length_octets + value_length

def decode_msc_file(raw_data):
        # Strategy 1: Try to decode as a complete file structure.
    try:
        decoded_data = msc_compiler.decode("CallEventDataFile", raw_data)
        # print("Successfully decoded file as a single CallEventDataFile.")
        # The structure is different, so we need to extract the records
        all_records = decoded_data.get("callEventRecords", [])
    except Exception:
        # Strategy 2: If the first fails, try decoding concatenated records.
        print("Could not decode as CallEventDataFile. Attempting to decode as a stream of records...")
        remaining_data = raw_data
        record_count = 0
        
        skipped_group_call_count = 0
        last_error_type = None
        
        while remaining_data:
            try:
                record_len = get_ber_tlv_length(remaining_data)
                if record_len == 0:
                    break
                
                record_bytes = remaining_data[:record_len]
                
                # Use 'CallEventRecord' which is a CHOICE of different MSC record types
                decoded_record = msc_compiler.decode("CallEventRecord", record_bytes)
                all_records.append(decoded_record)
                
                remaining_data = remaining_data[record_len:]
                record_count += 1
                
                # Reset consecutive error tracking on successful decode
                if skipped_group_call_count > 0:
                    # print(f"Successfully resumed processing after skipping {skipped_group_call_count} GroupCallRecord errors")
                    skipped_group_call_count = 0
                    last_error_type = None
                    
            except Exception as e:
                error_msg = str(e)
                
                # For any GroupCallRecord decoding errors, try to continue with next record
                if "groupCallRecord" in error_msg:
                    skipped_group_call_count += 1
                    current_error_type = "IMSI" if "subscriberIMSI" in error_msg else "subBillType"
                    
                    # Only print detailed warnings for the first few errors and error type changes
                    if skipped_group_call_count <= 3 or current_error_type != last_error_type:
                        # print(f"Warning: Skipping record {record_count + 1} due to GroupCallRecord {current_error_type} error")
                        last_error_type = current_error_type
                    elif skipped_group_call_count % 50 == 0:  # Print progress every 50 skipped records
                        # print(f"... skipped {skipped_group_call_count} GroupCallRecord errors so far ...")
                        pass
                    
                    try:
                        # Skip this problematic record and continue
                        record_len = get_ber_tlv_length(remaining_data)
                        if record_len > 0:
                            remaining_data = remaining_data[record_len:]
                            record_count += 1  # Increment the counter for the skipped record
                            continue
                    except Exception as skip_error:
                        print(f"Failed to skip GroupCallRecord: {skip_error}")
                        pass
                else:
                    # For non-GroupCallRecord errors, print full warning
                    # print(f"Warning: Skipping record {record_count + 1} due to decoding error: {e}")
                    pass
                
                # For other decoding errors, try to skip and continue
                try:
                    record_len = get_ber_tlv_length(remaining_data)
                    if record_len > 0:
                        remaining_data = remaining_data[record_len:]
                        record_count += 1
                        # print(f"Skipped problematic record, continuing...")
                        continue
                except:
                    pass
                
                # If we can't recover, stop processing
                print(f"Stopping at record {record_count + 1} due to unrecoverable decoding error")
                break
        
        # Final summary of skipped GroupCallRecords
        if skipped_group_call_count > 0:
            # print(f"Total GroupCallRecord errors encountered and skipped: {skipped_group_call_count}")
            
            # If ALL records failed and they were all GroupCallRecord errors, suggest wrong file type
            if len(all_records) == 0 and skipped_group_call_count > 100:
                print(f"\n⚠️  IMPORTANT: All {skipped_group_call_count} records failed with GroupCallRecord parsing errors.")
                print("This suggests the file may not contain MSC CDR data in the expected format.")
                print("Consider:")
                print("  • Verify this is the correct MSC file")
                print("  • Check if this file contains SGSN data (use process_sgsn.ps1)")
                print("  • Verify the ASN.1 schema matches your MSC vendor format")
    if all_records:
        # print(f"Successfully decoded {len(all_records)} records.")
        return all_records

def decode_bcd_variant(data, swap_nibbles=False):
    """
    Decodes BCD or T-BCD data from bytes or a hex string.
    - If swap_nibbles is True, it performs T-BCD decoding (swaps nibbles).
    - Otherwise, it performs standard BCD decoding.
    """
    if not isinstance(data, (bytes, str)):
        return data
    if isinstance(data, str):
        try:
            data = bytes.fromhex(data)
        except (ValueError, TypeError):
            data = data.encode('latin-1')

    digits = []
    for byte in data:
        lsb = byte & 0x0F
        msb = (byte >> 4) & 0x0F
        
        first_nibble = lsb if swap_nibbles else msb
        second_nibble = msb if swap_nibbles else lsb

        if first_nibble == 0x0F: break
        digits.append(str(first_nibble))
        
        if second_nibble == 0x0F: break
        digits.append(str(second_nibble))
        
    return ''.join(digits)

def decode_tbcd(data):
    """Decodes T-BCD data by swapping nibbles."""
    return decode_bcd_variant(data, swap_nibbles=True)

def decode_bcd(bcd_data):
    """Decodes standard BCD data."""
    return decode_bcd_variant(bcd_data, swap_nibbles=False)    


def decode_address(b, is_tbcd=False, prefixes_to_remove=None):
    """Decodes a BCD or T-BCD address, removes prefixes, and formats it."""
    decoded = decode_tbcd(b) if is_tbcd else decode_bcd(b)
    if decoded:
        if prefixes_to_remove:
            for prefix in prefixes_to_remove:
                if decoded.startswith(prefix):
                    decoded = decoded[len(prefix):]
                    break
    return decoded

def decode_hex_or_int(value):
    """Converts short byte strings to integers, otherwise to hex.

    This function is defensive about input types:
    - If given an int, return it.
    - If given bytes and length <= 4, convert to int.
    - If given a string, try to interpret as a hex string (e.g. '0A1B') or
      encode to bytes using utf-8 with replacement and then fall back to hex.
    - For unknown types, return as-is.
    """
    # If it's already an int, return it
    if isinstance(value, int):
        return value

    # If bytes-like
    if isinstance(value, (bytes, bytearray)):
        if len(value) <= 4:
            return int.from_bytes(bytes(value), byteorder='big')
        return decode_hex_string(bytes(value))

    # If it's a string, it might be a hex string or unicode representation
    if isinstance(value, str):
        val = value.strip()
        # Common case: hex string without 0x, or with 0x
        try:
            # Remove optional 0x and spaces
            candidate = val[2:] if val.lower().startswith('0x') else val
            candidate = candidate.replace(' ', '')
            # If it looks like hex (even length and hex chars), decode
            if len(candidate) > 0 and all(c in '0123456789abcdefABCDEF' for c in candidate) and len(candidate) % 2 == 0:
                raw = bytes.fromhex(candidate)
                if len(raw) <= 4:
                    return int.from_bytes(raw, byteorder='big')
                return decode_hex_string(raw)
        except Exception:
            pass

        # As a last resort, encode to utf-8 with replacement and handle as bytes
        try:
            raw = val.encode('utf-8', errors='replace')
            if len(raw) <= 4:
                return int.from_bytes(raw, byteorder='big')
            return decode_hex_string(raw)
        except Exception:
            return val

    # Otherwise, return value unchanged
    return value

def decode_hex_string(b):
    """Converts bytes to a formatted hex string, filtering out null-like 'ffff' patterns."""
    if b is None:
        return None
    # If given a string, try to interpret as hex text first (e.g., '0A1B')
    if isinstance(b, str):
        s = b.strip()
        try:
            candidate = s[2:] if s.lower().startswith('0x') else s
            candidate = candidate.replace(' ', '')
            if len(candidate) > 0 and all(c in '0123456789abcdefABCDEF' for c in candidate) and len(candidate) % 2 == 0:
                b = bytes.fromhex(candidate)
            else:
                # Fall back to utf-8 bytes
                b = s.encode('utf-8', errors='replace')
        except Exception:
            b = s.encode('utf-8', errors='replace')

    if isinstance(b, (bytes, bytearray)):
        # Check for the null-like marker before converting
        if b'\xff\xff' in b:
            return None
        hex_str = bytes(b).hex().upper()

        # Format longer hex strings for better readability
        if len(hex_str) >= 8:  # 4+ bytes, group every 4 chars
            return ''.join(hex_str[i:i+4] for i in range(0, len(hex_str), 4))
        elif len(hex_str) >= 4:  # 2+ bytes, group every 2 chars
            return ''.join(hex_str[i:i+2] for i in range(0, len(hex_str), 2))

        return hex_str

    return b

TELESERVICE_MAP = { 
    17: "telephony (speech)", 16: "all teleservices", 18: "shortMessageMT", 
    19: "shortMessageMO", 34: "Depersonalisation Short Message" 
}

def decode_basic_service(service_dict):
    """Decodes the basicService field to include both code and description."""
    # exit()
    if service_dict is None:
        return None

    
    for kind,code in service_dict.items():
        if isinstance(code, str):
            code_int = code.encode('latin-1')[0]
        elif isinstance(code, bytes):
            code_int = int.from_bytes(code, 'big')
        else:
            code_int = code
            
        description = TELESERVICE_MAP.get(code_int, "unknown")
        return f"{description}, Code: {code_int}"

def parse_timestamp(raw_bytes):
    """Parses a BCD timestamp with timezone info into an ISO 8601 string."""
    if isinstance(raw_bytes, str):
        raw_bytes = raw_bytes.encode('latin-1')
    elif not isinstance(raw_bytes, bytes):
        return raw_bytes

    try:
        bcd_str = decode_bcd(raw_bytes)
        if len(bcd_str) >= 12:
            dt = datetime.strptime(bcd_str[:12], '%y%m%d%H%M%S')
            # Timezone decoding based on 3GPP TS 32.005
            if len(raw_bytes) >= 7:
                tz_byte = raw_bytes[6]
                tz_sign = -1 if (tz_byte & 0b00001000) else 1
                tz_bcd = decode_bcd(bytes([tz_byte & 0b11110111]))
                if len(tz_bcd) >= 2:
                    tz_hours = int(tz_bcd[0])
                    tz_minutes_tens = int(tz_bcd[1])
                    tz_offset_minutes = (tz_hours * 60) + (tz_minutes_tens * 10)
                    dt = dt.replace(tzinfo=timezone(timedelta(minutes=tz_sign * tz_offset_minutes)))
            return dt.isoformat()
    except (ValueError, IndexError) as e:
        print(f"Warning: Could not parse timestamp '{raw_bytes.hex()}'. Error: {e}")
        pass
    return ''.join(f'{b:02x}' for b in raw_bytes)


def transform_record(record, filename, config):
    """Transforms a single raw CDR record into the client-specified format."""
    all_transformed = []
    for call_type, record_data in record.items():

        # --- Step 1: Decode all possible fields into a flat dictionary with camelCase keys ---
        # Handle GroupCallRecord which uses different field names
        if call_type == "groupCallRecord":
            imsi_field = decode_tbcd(record_data.get("subscriberIMSI"))
            subscriber_number = decode_address(record_data.get("subscriberNumber"), is_tbcd=True, prefixes_to_remove=config.get("prefixes_to_remove", []))
        else:
            imsi_field = decode_tbcd(record_data.get("servedIMSI"))
            subscriber_number = decode_address(record_data.get("servedMSISDN"), is_tbcd=True, prefixes_to_remove=config.get("prefixes_to_remove", []))

        decoded_data = {
            "recordType": call_type.replace('Record', ''),
            "servedIMSI": imsi_field,
            "servedIMEI": decode_tbcd(record_data.get("servedIMEI")),
            "servedMSISDN": subscriber_number,
            "callingNumber": decode_address(record_data.get("callingNumber"), is_tbcd=True, prefixes_to_remove=config.get("prefixes_to_remove", [])),
            "calledNumber": decode_address(record_data.get("calledNumber"), is_tbcd=True, prefixes_to_remove=config.get("prefixes_to_remove", [])),
            "roamingNumber": decode_address(record_data.get("roamingNumber"), is_tbcd=True, prefixes_to_remove=config.get("prefixes_to_remove", [])),
            "recordingEntity": decode_address(record_data.get("recordingEntity"), is_tbcd=True, prefixes_to_remove=config.get("prefixes_to_remove", [])),
            "locationAreaCode": decode_hex_or_int(record_data.get('location', {}).get('locationAreaCode')),
            "cellIdentifier": decode_hex_or_int(record_data.get('location', {}).get('cellIdentifier')),
            "basicService": decode_basic_service(record_data.get('basicService')),
            "seizureTime": parse_timestamp(record_data.get('seizureTime')),
            "answerTime": parse_timestamp(record_data.get('answerTime')),
            "releaseTime": parse_timestamp(record_data.get('releaseTime')),
            "callDuration": record_data.get('callDuration'),
            "callReference": decode_hex_string(record_data.get("callReference")),
            "chargeIndicator": record_data.get("additionalChgInfo", {}).get("chargeIndicator"),
            "mSCAddress": decode_address(record_data.get("mSCAddress"), is_tbcd=True, prefixes_to_remove=config.get("prefixes_to_remove", [])),
            "chargedParty": record_data.get('chargedParty'),
            "mscOutgoingCircuit": decode_hex_or_int(record_data.get("mscOutgoingCircuit")),
            "orgRNCorBSCId": decode_hex_string(record_data.get("orgRNCorBSCId")),
            "orgMSCId": decode_hex_string(record_data.get("orgMSCId")),
            "mscIncomingRoute": record_data.get("mscIncomingROUTE"),
            "mscOutgoingRoute": record_data.get("mscOutgoingROUTE"),
            "causeForTermination": decode_hex_or_int(record_data.get("causeForTermination")),
            "callerPortedFlag": record_data.get("callerPortedFlag"),
            "cugAccessIndicator": record_data.get("cugAccessIndicator"),
            "translatedNumber": decode_address(record_data.get("translatedNumber"), is_tbcd=True, prefixes_to_remove=config.get("prefixes_to_remove", [])),
            "serviceCentre": decode_address(record_data.get("serviceCentre"), is_tbcd=True, prefixes_to_remove=config.get("prefixes_to_remove", [])),
            "msClassmark": decode_hex_string(record_data.get("msClassmark")),
            "systemType": record_data.get("systemType"),
            "messageReference": decode_hex_string(record_data.get("messageReference")),
            "recordNumber": decode_hex_or_int(record_data.get("recordNumber")),
            "destinationNumber": decode_address(record_data.get("destinationNumber"), is_tbcd=True, prefixes_to_remove=config.get("prefixes_to_remove", [])),
            "deliveryTime": parse_timestamp(record_data.get("deliveryTime")),
            "originationTime": parse_timestamp(record_data.get("originationTime")),
            "ssActionTime": parse_timestamp(record_data.get("ssActionTime")),
            "callType": call_type.replace('Record', ''),
            "filename": filename,
            "parsed_time": datetime.now(timezone.utc).isoformat(),
            "operator": config.get("operator", "unknown"),
            "basicServiceType": config.get("basicServiceType", "unknown"),
            "subscriberNumber": subscriber_number  # For GroupCallRecord mapping
        }
        
        all_transformed.append(decoded_data)
    return all_transformed

def normalize_msc_file(data, filename, operator, prefixes_to_remove=None):
    # print(f"Normalizing {len(data)} CallEventRecords from {filename} for operator {operator}")
    normalized = []
    for record in data:
        transformed = transform_record(record, filename, {
            "operator": operator,
            "basicServiceType": "MSC",
            "prefixes_to_remove": prefixes_to_remove
        })
        normalized.extend(transformed)
    
    return normalized
