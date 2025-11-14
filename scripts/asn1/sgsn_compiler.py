import asn1tools
import json
import argparse
import os
import sys
from datetime import datetime, timezone
import string

from asn1.direct_binary_parser import DirectBinaryParser
from asn1.utils import decode_tbcd

def fetch_missing_fields(binary_data, filename):
    parser = DirectBinaryParser(show_tbcd_steps=False)
    
    results = parser.scan_file(filename, binary_data)

    return results
try:
    # Try with different compiler options to handle indefinite length better
    # Use the sgsn_cdr_schema.asn from the schema folder
    schema_path = os.path.join(
        os.path.dirname(os.path.dirname(__file__)), "schema", "sgsn_cdr_schema.asn"
    )
    sgsn_compiler = asn1tools.compile_files(schema_path, "ber")

except Exception as e:
    print(f"Error compiling sgsn_cdr_schema.asn: {e}")
    print("Please ensure the schema file is correct and contains valid ASN.1 definitions.")
    exit()
    

def convert_indefinite_to_definite_length(data):
    """
    Convert indefinite length BER encoding to definite length.
    Also unwraps SEQUENCE and CONTEXT wrappers to get to the actual CallEventRecord.
    This helps with ASN.1 libraries that don't handle indefinite length well.
    Uses proper BER tag-length-value parsing to handle nested structures.
    """
    # First, unwrap any SEQUENCE (0x30) and CONTEXT (0xA0) wrappers
    current_data = data
    unwrap_count = 0
    
    while (len(current_data) > 2 and 
           current_data[0] in [0x30, 0xA0] and 
           unwrap_count < 3):  # Prevent infinite loop
        
        tag = current_data[0]
        length_octet = current_data[1]
        
        # Parse length to find content
        if (length_octet & 0x80) == 0:
            # Short form
            content_start = 2
        else:
            # Long form or indefinite
            num_length_octets = length_octet & 0x7f
            if num_length_octets == 0:
                # Indefinite length - find End-of-Contents
                content_start = 2
                # We'll handle this in the main conversion below
                break
            else:
                content_start = 2 + num_length_octets
        
        # Skip to the content (unwrap this layer)
        current_data = current_data[content_start:]
        unwrap_count += 1
        
        # print(f"DEBUG: Unwrapped {tag:02x} wrapper, now have: {current_data[:10].hex()}")
    
    # Now handle the tag 80 case (CONTEXT[0] IMPLICIT)
    if len(current_data) > 0 and current_data[0] == 0x80:
        # print("DEBUG: Converting CONTEXT[0] (0x80) to sgsnPDPRecord (0xB4)")
        
        # Extract the original content
        length_octet = current_data[1]
        if (length_octet & 0x80) == 0:
            content_start = 2
            content_length = length_octet
        else:
            num_length_octets = length_octet & 0x7f
            content_start = 2 + num_length_octets
            content_length = int.from_bytes(current_data[2:content_start], 'big')
        
        original_content = current_data[content_start:content_start + content_length]
        # print(f"DEBUG: Original content: {original_content.hex()}")
        
        # Add recordType field (tag 80, length 1, value 18 = sgsnPDPRecord)
        recordtype_field = bytes([0x80, 0x01, 0x12])
        new_content = recordtype_field + original_content
        new_length = len(new_content)
        
        # Build new record with B4 tag
        new_record = bytearray([0xB4])  # sgsnPDPRecord tag
        
        # Add new length
        if new_length < 0x80:
            new_record.append(new_length)
        else:
            length_bytes = []
            temp_length = new_length
            while temp_length > 0:
                length_bytes.insert(0, temp_length & 0xFF)
                temp_length >>= 8
            new_record.append(0x80 | len(length_bytes))
            new_record.extend(length_bytes)
        
        # Add content (recordType + original content)
        new_record.extend(new_content)
        
        current_data = bytes(new_record)
        # print(f"DEBUG: After adding recordType: {current_data[:15].hex()}")
        
        # Check if this is a minimal record format
        if len(original_content) < 20:  # Too small for complete sgsnPDPRecord
            # print(f"DEBUG: Minimal record detected ({len(original_content)} bytes)")
            # print("DEBUG: This file uses proprietary CDR format, not standard ASN.1")
            # print("DEBUG: Raising special exception to skip ASN.1 for entire file")
            # Raise a special exception to indicate proprietary format
            raise ValueError("PROPRIETARY_FORMAT_DETECTED")
    
    # Now convert indefinite lengths to definite lengths
    result = bytearray()
    offset = 0
    
    def find_end_of_contents(data, start_pos, nesting_level=0):
        """Find the matching End-of-Contents for indefinite length, 
        handling nesting"""
        pos = start_pos
        current_nesting = nesting_level
        
        while pos < len(data) - 1:
            if data[pos] == 0x00 and data[pos + 1] == 0x00:
                if current_nesting == 0:
                    return pos  # Found the matching End-of-Contents
                else:
                    current_nesting -= 1  # This closes a nested indefinite length
                    pos += 2
                    continue
            
            # Check if this starts a new indefinite length structure
            if pos + 1 < len(data) and data[pos + 1] == 0x80:
                current_nesting += 1
                pos += 2
                continue
            
            # Skip over this TLV
            if pos + 1 >= len(data):
                break
                
            length_octet = data[pos + 1]
            if (length_octet & 0x80) == 0:
                # Short form
                tlv_length = 2 + length_octet
            else:
                # Long form
                num_length_octets = length_octet & 0x7f
                if num_length_octets == 0:
                    # Another indefinite length
                    pos += 2
                    continue
                    
                if pos + 2 + num_length_octets >= len(data):
                    break
                    
                value_length = int.from_bytes(
                    data[pos + 2:pos + 2 + num_length_octets], 'big'
                )
                tlv_length = 2 + num_length_octets + value_length
            
            pos += tlv_length
        
        return None  # End-of-Contents not found
    
    while offset < len(current_data):
        if offset + 1 >= len(current_data):
            result.extend(current_data[offset:])
            break
            
        tag = current_data[offset]
        length_octet = current_data[offset + 1]
        
        # Check if this is indefinite length (0x80)
        if length_octet == 0x80:
            # Find the matching End-of-Contents
            content_start = offset + 2
            content_end = find_end_of_contents(current_data, content_start, 0)
            
            if content_end is None:
                # If we can't find End-of-Contents, just copy the rest
                result.extend(current_data[offset:])
                break
                
            # Calculate definite length
            content_length = content_end - content_start
            
            # Write tag
            result.append(tag)
            
            # Write definite length
            if content_length < 0x80:
                # Short form
                result.append(content_length)
            else:
                # Long form
                length_bytes = []
                temp_length = content_length
                while temp_length > 0:
                    length_bytes.insert(0, temp_length & 0xFF)
                    temp_length >>= 8
                
                result.append(0x80 | len(length_bytes))
                result.extend(length_bytes)
            
            # Write content (recursively process in case of nested indefinite length)
            content = current_data[content_start:content_end]
            processed_content = convert_indefinite_to_definite_length(content)
            result.extend(processed_content)
            
            # Skip past the End-of-Contents
            offset = content_end + 2
        else:
            # Regular definite length - copy as is
            if (length_octet & 0x80) == 0:
                # Short form
                record_length = 2 + length_octet
            else:
                # Long form
                num_length_octets = length_octet & 0x7f
                if offset + 2 + num_length_octets > len(current_data):
                    result.extend(current_data[offset:])
                    break
                    
                value_length = int.from_bytes(
                    current_data[offset + 2:offset + 2 + num_length_octets], 'big'
                )
                record_length = 2 + num_length_octets + value_length
            
            end_pos = min(offset + record_length, len(current_data))
            result.extend(current_data[offset:end_pos])
            offset = end_pos
    
    return bytes(result)


def decode_sgsn_file(raw_data):
    try:
        decoded_data = sgsn_compiler.decode("CallEventRecord", raw_data)
        return decoded_data
    except Exception as e:
        print(f"Error decoding SGSN data: {e}")
        raise


def _tbcd_to_digits(b: bytes) -> str:
    # TBCD: low nibble is first digit, high nibble is second; 0xF is filler
    if not b:
        return ""
    digits = []
    for octet in b:
        low = octet & 0x0F
        high = (octet & 0xF0) >> 4
        for nib in (low, high):
            if nib == 0x0F:
                continue
            if nib <= 9:
                digits.append(str(nib))
            else:
                # map * # a b c to common characters when present
                if nib == 0x0A:
                    digits.append('*')
                elif nib == 0x0B:
                    digits.append('#')
                elif nib == 0x0C:
                    digits.append('a')
                elif nib == 0x0D:
                    digits.append('b')
                elif nib == 0x0E:
                    digits.append('c')
                else:
                    digits.append('?')
    return ''.join(digits)


def _ip_choice_to_str(val):
    # Recursively unwrap CHOICE tuples from asn1tools until we find an IP value
    try:
        if isinstance(val, tuple) and len(val) >= 2:
            tag = val[0]
            inner = val[1]

            # If inner is another tuple, recurse
            if isinstance(inner, tuple):
                # Common patterns: ('iPAddress', ('iPBinaryAddress', ('iPBinV4Address', b'...')))
                res = _ip_choice_to_str(inner)
                if res is not inner:
                    return res

            # Handle direct binary v4/v6 octet strings
            if tag in ('iPBinV4Address', 'iPBinaryAddress') and isinstance(inner, (bytes, bytearray)):
                if len(inner) == 4:
                    return '.'.join(str(b) for b in inner)
                if len(inner) == 16:
                    parts = [format((inner[i] << 8) | inner[i+1], 'x') for i in range(0, 16, 2)]
                    return ':'.join(parts)

            if tag == 'iPBinV6Address' and isinstance(inner, (bytes, bytearray)) and len(inner) == 16:
                parts = [format((inner[i] << 8) | inner[i+1], 'x') for i in range(0, 16, 2)]
                return ':'.join(parts)

            # Text represented addresses
            if tag in ('iPTextV4Address', 'iPTextV6Address', 'iPTextRepresentedAddress') and isinstance(inner, str):
                return inner

    except Exception:
        pass
    return val


def _try_parse_timestamp(b: bytes) -> str:
    # Best-effort parse for TimeStamp OCTET STRING (YYMMDDhhmmssS hhmm)
    if not isinstance(b, (bytes, bytearray)):
        return str(b)
    try:
        # Expecting 9 octets: YY MM DD hh mm ss S tzHH tzMM
        if len(b) == 9:
            def bcd_two_digits(byte_val):
                hi = (byte_val & 0xF0) >> 4
                lo = byte_val & 0x0F
                return f"{hi}{lo}"

            yy = bcd_two_digits(b[0])
            mm = bcd_two_digits(b[1])
            dd = bcd_two_digits(b[2])
            hh = bcd_two_digits(b[3])
            mi = bcd_two_digits(b[4])
            ss = bcd_two_digits(b[5])

            # Octet 6 is ASCII sign '+' (0x2B) or '-' (0x2D)
            sign = None
            if b[6] in (0x2B, 0x2D):
                sign = chr(b[6])
            else:
                # If not ASCII sign, try interpret as BCD nibble pair (rare)
                sign = '+'

            tz_h = bcd_two_digits(b[7])
            tz_m = bcd_two_digits(b[8])
            # Normalize two-digit year to full year (assume 20xx for 00-99)
            year = int(yy)
            if year < 100:
                year = 2000 + year
            return f"{year:04d}-{mm}-{dd} {hh}:{mi}:{ss} {sign}{tz_h}:{tz_m}"

        # fallback: show hex
    except Exception:
        pass
    return '0x' + bytes(b).hex()


def _to_hex(b: bytes) -> str:
    if isinstance(b, bytearray):
        b = bytes(b)
    if isinstance(b, (bytes, bytearray)):
        return '0x' + bytes(b).hex()
    return str(b)

def hex_to_ascii(hex_str):
    # Remove "0x" prefix if present
    if hex_str.startswith("0x"):
        hex_str = hex_str[2:]

    # Convert from hex to bytes, then decode printable ASCII characters
    bytes_data = bytes.fromhex(hex_str)
    try:
        decoded = bytes_data.decode("utf-8", errors="ignore")
    except UnicodeDecodeError:
        decoded = bytes_data.decode("latin1", errors="ignore")
        
    clean = "".join(ch for ch in decoded if ch in string.printable and ch not in "\r\n\t")

    return clean

def _two_octet_be_to_int(b: bytes):
    if isinstance(b, (bytes, bytearray)) and len(b) == 2:
        return int.from_bytes(bytes(b), 'big')
    return None


def _one_octet_to_int(b: bytes):
    if isinstance(b, (bytes, bytearray)) and len(b) == 1:
        return int(b[0])
    return None


def _charging_characteristics_to_dict(b: bytes) -> dict:
    """Interpret 2-octet chargingCharacteristics as profile index + behavior bits.

    The schema indicates a 2-octet field where:
    - Bits 0-3: Profile Index (low nibble of first octet)
    - Bits 4-15: For Behavior (remaining bits)
    This helper returns profile_index and raw behavior bits for inspection.
    """
    if not isinstance(b, (bytes, bytearray)):
        return {'raw': b}
    raw = bytes(b)
    if len(raw) != 2:
        return {'raw_hex': '0x' + raw.hex(), 'length': len(raw)}
    # profile index is bits 0-3 of the first octet -> low nibble of first octet
    profile_index = raw[0] & 0x0F
    # behavior bits are remaining 12 bits (high nibble of first octet + second octet)
    behavior = ((raw[0] & 0xF0) >> 4) << 8 | raw[1]
    return {
        'raw_hex': '0x' + raw.hex(),
        'profile_index': profile_index,
        'behavior_bits': f'0x{behavior:03x}',
        'behavior_int': behavior
    }


def _msisdn_from_bcd_bytes(b: bytes) -> str:
    # AddressString byte format described in schema: first octet includes TON/NPI etc.
    if not isinstance(b, (bytes, bytearray)):
        return str(b)
    raw = bytes(b)
    if len(raw) == 0:
        return ''
    # first octet: extension bit(8)=1, bits 7-5 nature of address, bits 4-1 numbering plan
    first = raw[0]
    # digits start from octet 2 onward (TBCD)
    tbcd = raw[1:]
    digits = _tbcd_to_digits(tbcd)
    return digits



def _pretty_value(key, val):
    # heuristics based on field names and types
    # print(f"Processing key: {key}, type: {type(val)}")
    def _parse_ber_tlv(b: bytes):
        """Parse a single BER TLV element from bytes and return a dict with tag/length/payload.

        Returns None on parse failure. This is a small, forgiving parser that handles
        single-byte tags and simple multi-byte lengths (and simple long-form tags).
        """
        if not isinstance(b, (bytes, bytearray)):
            return None
        data = bytes(b)
        L = len(data)
        if L < 2:
            return None
        i = 0
        try:
            tag = data[i]
            i += 1
            # handle long-form tag number (0x1f) - consume subsequent tag-number bytes
            if (tag & 0x1F) == 0x1F:
                # consume subsequent bytes until bit7 == 0
                while i < L and (data[i] & 0x80):
                    i += 1
                if i < L:
                    i += 1
            if i >= L:
                return None
            first_len = data[i]
            i += 1
            if first_len & 0x80:
                num_bytes = first_len & 0x7F
                # indefinite form not supported
                if num_bytes == 0 or i + num_bytes > L:
                    return None
                length = int.from_bytes(data[i:i+num_bytes], 'big')
                i += num_bytes
            else:
                length = first_len
            if i + length > L:
                return None
            payload = data[i:i+length]
            return {
                'tag': tag,
                'tag_class': (tag >> 6) & 0x03,
                'constructed': bool(tag & 0x20),
                'tag_number': (tag & 0x1F) if (tag & 0x1F) != 0x1F else 'long',
                'length': length,
                'payload': payload,
            }
        except Exception:
            return None
    if isinstance(val, dict):
        return pretty_decode(val)
    if isinstance(val, list):
        return [ _pretty_value(key, v) for v in val ]
    if isinstance(val, tuple):
        # likely an ASN.1 CHOICE; try IP conversion first
        ip = _ip_choice_to_str(val)
        if ip is not val:
            return ip
        return str(val)
    if isinstance(val, (bytes, bytearray)):
        lname = (key or '').lower()
        # IMSI/IMEI heuristics
        if 'imsi' in lname or 'imei' in lname:
            try:
                return _tbcd_to_digits(bytes(val))
            except Exception:
                return _to_hex(val)
        # Location Area Code (2 octets) -> integer only
        if 'locationarea' in lname or 'locationareacode' in lname:
            n = _two_octet_be_to_int(val)
            if n is not None:
                return n
        # Cell Identifier (2 octets) -> integer only
        if 'cell' in lname or 'cellidentifier' in lname:
            n = _two_octet_be_to_int(val)
            if n is not None:
                return n
        # Routing Area Code (1 octet) -> integer only
        if 'routingarea' in lname or 'routingarea' in (key or '').lower():
            n = _one_octet_to_int(val)
            if n is not None:
                return n
        if 'address' in lname or 'ip' in lname or 'sgsnaddress' in lname or 'ggsn' in lname:
            # try ip if length matches
            if len(val) == 4:
                return '.'.join(str(b) for b in val)
            if len(val) == 16:
                parts = [format((val[i]<<8) | val[i+1], 'x') for i in range(0, 16, 2)]
                return ':'.join(parts)
        
        if 'time' in lname or 'timestamp' in lname or 'recordopeningtime' in lname or 'changeTime' in (key or ''):
            return _try_parse_timestamp(bytes(val))
        # chargingCharacteristics is 2 octets: profile + behavior
        if 'chargingcharacteristic' in lname or 'chargingcharacteristics' in lname:
            return _charging_characteristics_to_dict(bytes(val))
        # recordSequenceNumber often contains an embedded BER TLV (tag/len/value)
        # e.g. 0x960d + ASCII payload -> tag [22] context-specific with string payload
        if 'recordsequence' in lname or 'recordsequencenumber' in lname:
            
            # if isinstance(val, (bytes, bytearray)):
            #     record_seq_hex = _to_hex(val)
            # else:
            #     record_seq_hex = str(val)
            record_seq_ascii = hex_to_ascii(val.hex())
            return record_seq_ascii
        #     parsed = _parse_ber_tlv(bytes(val))
        #     if parsed is not None:
        #         pl = parsed.get('payload')
        #         # If payload is printable ASCII, return it directly
        #         try:
        #             if pl and all(0x20 <= b <= 0x7E for b in pl):
        #                 return pl.decode('ascii')
        #         except Exception:
        #             pass
        #         # If payload is a single- or multi-octet integer, return integer
        #         try:
        #             if pl and len(pl) <= 8:
        #                 # prefer unsigned big-endian
        #                 return int.from_bytes(pl, 'big')
        #         except Exception:
        #             pass
        #         # otherwise return a small dict describing the TLV
        #         return {
        #             'tag': f"0x{parsed['tag']:02x}",
        #             'tag_class': parsed['tag_class'],
        #             'constructed': parsed['constructed'],
        #             'tag_number': parsed['tag_number'],
        #             'length': parsed['length'],
        #             'payload_hex': '0x' + parsed['payload'].hex(),
        #         }
        # servedMSISDN is an AddressString (first octet + TBCD)
        if 'servedmsisdn' in lname or 'msisdn' in lname:
            raw_msisdn = val
        
            # Check if this looks like a clean phone number (from enhanced extraction)
            # if isinstance(raw_msisdn, str) and raw_msisdn.isdigit() and len(raw_msisdn) >= 10:
            #     msisdn_value = raw_msisdn
            # elif raw_msisdn:
            #     # Fallback to TBCD decoding for binary data
            #     try:
            #         decoded_msisdn = decode_tbcd(raw_msisdn)
            #         if decoded_msisdn and decoded_msisdn.isdigit() and len(decoded_msisdn) >= 10:
            #             msisdn_value = decoded_msisdn
            #     except:
            #         pass
            return raw_msisdn
        # fallback to hex
        return _to_hex(val)
    return val


def pretty_decode(decoded: dict) -> dict:
    """Recursively post-process a decoded CallEventRecord to return human-friendly values.

    Rules applied (best-effort):
    - IMSI / IMEI: decode TBCD
    - IPBinaryAddress / IPTextRepresentedAddress: convert to dotted or colon form
    - TimeStamp-like octet strings: best-effort BCD -> readable datetime
    - bytes/bytearray: hex string fallback
    """
    # Normalize top-level CHOICE tuples that asn1tools may return, e.g.
    # ('sgsnPDPRecord', { ... })
    # print(f"Decoded top-level CHOICE:  {type(decoded)}")
    # print(decoded)
    # print('--------------------------------')
    if isinstance(decoded, tuple) and len(decoded) == 2 and isinstance(decoded[0], str):
        key, value = decoded
        return {key: _pretty_value(key, value)}

    # If a non-dict top-level object is passed, try to pretty print it directly
    if not isinstance(decoded, dict):
        return _pretty_value(None, decoded)

    out = {}
    for k, v in decoded.items():
        try:
            out[k] = _pretty_value(k, v)
        except Exception:
            out[k] = str(v)
            
    # print(out)
    # print(decoded)
    return out


def _hexstr_to_bytes_if_hex(val):
    """If val is a string of the form '0x...' return its bytes, otherwise return val."""
    if isinstance(val, str) and val.startswith('0x'):
        hexpart = val[2:]
        try:
            return bytes.fromhex(hexpart)
        except Exception:
            return val
    return val


def convert_hex_strings(obj):
    """Recursively walk a decoded structure and convert hex-strings to bytes.

    This is useful if you have a decoded output where octet strings were
    post-serialized to hex strings and you want to re-run pretty decoding.
    """
    if isinstance(obj, dict):
        return {k: convert_hex_strings(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [convert_hex_strings(v) for v in obj]
    if isinstance(obj, tuple):
        # tuple might be ('tag', value)
        if len(obj) == 2 and isinstance(obj[0], str):
            return (obj[0], convert_hex_strings(obj[1]))
        return tuple(convert_hex_strings(v) for v in obj)
    # If it's a hex string like '0x010203', convert to bytes
    return _hexstr_to_bytes_if_hex(obj)


def pretty_decode_from_rawhex(decoded_with_hex_strings: dict) -> dict:
    """Convert hex-strings back to bytes then pretty-decode the structure."""
    normalized = convert_hex_strings(decoded_with_hex_strings)
    return pretty_decode(normalized)


# if __name__ == '__main__':
#     parser = argparse.ArgumentParser(description='Decode and pretty-print SGSN ASN.1 CallEventRecord')
#     parser.add_argument('file', help='Path to binary file containing a CallEventRecord')
#     args = parser.parse_args()
#     with open(args.file, 'rb') as f:
#         raw = f.read()
#     decoded = decode_sgsn_file(raw)
#     pretty = pretty_decode(decoded)
#     print(json.dumps(pretty, indent=2))

def iter_ber_elements(data: bytes):
    i = 0
    L = len(data)
    while i < L:
        start = i
        # read tag (handle multi-byte tag numbers)
        if i >= L:
            break
        i += 1
        # if tag number is 31 (0x1f) long-form, skip subsequent tag bytes
        # (simple handling: consume until a byte with high bit 0)
        if (data[start] & 0x1F) == 0x1F:
            # consume subsequent tag-number bytes: they have bit7 set on all
            # but the last tag-number byte has bit7 == 0, so loop until that
            while i < L and (data[i] & 0x80):
                i += 1
            # consume the final tag-number byte (which has bit7 == 0) if present
            if i < L:
                i += 1

        if i >= L:
            break

        # read length
        first_len = data[i]
        i += 1
        if first_len & 0x80:
            num_bytes = first_len & 0x7F
            # indefinite form (0) not supported here
            if num_bytes == 0:
                raise ValueError("Indefinite length form not supported")
            if i + num_bytes > L:
                break
            length = int.from_bytes(data[i:i+num_bytes], 'big')
            i += num_bytes
        else:
            length = first_len

        # extract full element
        if i + length > L:
            break
        element = data[start:i+length]
        yield element
        i = i + length
        
        


def map_sgsn_record(record: dict, filename: str) -> dict:
    """Map a pretty-decoded SGSN record to a normalized flat dictionary."""
    output = {}
    if not isinstance(record, dict):
        return output
    for record_type, content in record.items():

        listOfTrafficVolumes = content.pop('listOfTrafficVolumes', [])
        output = content.copy()
        output['recordType'] = record_type
        
        chargingCharacteristics = output.pop('chargingCharacteristics', None)
        if chargingCharacteristics is not None:
            output['chargingCharacteristic'] = chargingCharacteristics.get('profile_index', None)
    
        # Flatten listOfTrafficVolumes if present
        if isinstance(listOfTrafficVolumes, list):
            dataVolumeGPRSUplink = 0
            dataVolumeGPRSDownlink = 0
            for idx, vol in enumerate(listOfTrafficVolumes):
                if isinstance(vol, dict):
                    ul = vol.get('dataVolumeGPRSUplink', 0)
                    dl = vol.get('dataVolumeGPRSDownlink', 0)
                    if isinstance(ul, int):
                        dataVolumeGPRSUplink += ul
                    if isinstance(dl, int):
                        dataVolumeGPRSDownlink += dl
                    # Optionally, include individual volume records
            output['dataVolumeGPRSUplink'] = dataVolumeGPRSUplink
            output['dataVolumeGPRSDownlink'] = dataVolumeGPRSDownlink
            
    output['operator'] = 'Vodacom'
    output['filename'] = filename
    output['parsed_time'] = datetime.now(timezone.utc).isoformat()
    return output
            