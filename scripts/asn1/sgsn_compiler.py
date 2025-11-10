import asn1tools
import json
import argparse
import os
import sys
from datetime import datetime, timezone


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
        # if 'recordsequence' in lname or 'recordsequencenumber' in lname:
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
            return _msisdn_from_bcd_bytes(bytes(val))
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
        if data[start] & 0x1F == 0x1F:
            while i < L and (data[i] & 0x80):
                i += 1
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
    output.pop('pdpType', None)
    output['operator'] = 'Vodacom'
    output['filename'] = filename
    output['parsed_time'] = datetime.now(timezone.utc).isoformat()
    return output
            