import json
from datetime import datetime, timezone, timedelta

class BytesEncoder(json.JSONEncoder):
    """A custom JSON encoder to handle bytes and bytearray objects."""
    def default(self, obj):
        if isinstance(obj, (bytes, bytearray)):
            return obj.decode('latin-1')  # Or another appropriate encoding
        return json.JSONEncoder.default(self, obj)

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

def decode_ip_v4(raw_bytes_str):
    """Decodes a raw byte string into an IPv4 address string."""
    if not isinstance(raw_bytes_str, str): return raw_bytes_str
    return ".".join(map(str, raw_bytes_str.encode('latin-1')))

def decode_ip_address(ip_data):
    """Safely decodes various IP address formats from the raw CDR."""
    if not isinstance(ip_data, list) or len(ip_data) != 2:
        return None
    
    # Direct binary address
    if ip_data[0] == "iPBinaryAddress":
        address_type, address_bytes = ip_data[1]
        if address_type == 'iPBinV4Address':
            return decode_ip_v4(address_bytes)
            
    # Nested IPAddress structure
    if ip_data[0] == "iPAddress":
        return decode_ip_address(ip_data[1])
        
    return None

def parse_3gpp_timestamp(raw_bytes):
    """Dynamically parses various timestamp formats into an ISO 8601 string."""
    if not isinstance(raw_bytes, (bytes, str)):
        return raw_bytes
    
    # Handle hex string input
    if isinstance(raw_bytes, str):
        # Check if it's already a hex string
        if (len(raw_bytes) > 20 and
                all(c in '0123456789abcdefABCDEF' for c in raw_bytes)):
            try:
                raw_bytes = bytes.fromhex(raw_bytes)
            except ValueError:
                raw_bytes = raw_bytes.encode('latin-1')
        else:
            raw_bytes = raw_bytes.encode('latin-1')

    # Try different timestamp formats dynamically
    return _try_multiple_timestamp_formats(raw_bytes)

def _try_multiple_timestamp_formats(raw_bytes):
    """Try multiple timestamp formats and return the first successful one."""
    
    # Format 1: Standard 3GPP BCD format (6-9 bytes: YYMMDDhhmmssShhmm)
    if 6 <= len(raw_bytes) <= 9:
        try:
            bcd_digits = ""
            for byte in raw_bytes[:6]:
                high_nibble = (byte >> 4) & 0x0F
                low_nibble = byte & 0x0F
                if high_nibble <= 9:
                    bcd_digits += str(high_nibble)
                if low_nibble <= 9:
                    bcd_digits += str(low_nibble)
            
            if len(bcd_digits) >= 12:
                dt = datetime.strptime(bcd_digits[:12], '%y%m%d%H%M%S')
                
                # Add timezone if available and convert to UTC
                if len(raw_bytes) >= 9:
                    tz_sign = -1 if chr(raw_bytes[6]) == '-' else 1
                    tz_h = int(f'{raw_bytes[7]:02x}')
                    tz_m = int(f'{raw_bytes[8]:02x}')
                    tz_offset = timezone(timedelta(hours=tz_h, minutes=tz_m) * tz_sign)
                    dt = dt.replace(tzinfo=tz_offset)
                    # Convert to UTC
                    dt = dt.astimezone(timezone.utc)
                else:
                    # If no timezone info, assume UTC
                    dt = dt.replace(tzinfo=timezone.utc)
                
                return dt.isoformat()
        except (ValueError, IndexError):
            pass
    
    # Format 2: Unix timestamp (4 or 8 bytes)
    if len(raw_bytes) in [4, 8]:
        try:
            if len(raw_bytes) == 4:
                timestamp = int.from_bytes(raw_bytes, 'big')
            else:
                timestamp = int.from_bytes(raw_bytes, 'big')
            
            # Check if it's a reasonable timestamp (between 2000 and 2030)
            if 946684800 <= timestamp <= 1893456000:  # 2000-2030
                dt = datetime.fromtimestamp(timestamp, tz=timezone.utc)
                return dt.isoformat()
        except (ValueError, OSError):
            pass
    
    # Format 3: ISO-like string embedded in hex
    try:
        # Try to extract readable characters from hex
        readable_chars = ""
        for byte in raw_bytes:
            if 32 <= byte <= 126:  # Printable ASCII
                readable_chars += chr(byte)
        
        # Look for date-like patterns
        if len(readable_chars) >= 10:
                        # Try common date formats
            for fmt in ['%Y-%m-%d', '%Y%m%d', '%d/%m/%Y', '%m/%d/%Y']:
                try:
                    dt = datetime.strptime(readable_chars[:10], fmt)
                    # Ensure UTC timezone
                    dt = dt.replace(tzinfo=timezone.utc)
                    return dt.isoformat()
                except ValueError:
                    continue
    except:
        pass
    
    # Format 4: Binary date format (various encodings)
    if len(raw_bytes) >= 4:
        try:
            # Try different byte interpretations
            for byte_order in ['big', 'little']:
                for offset in range(0, min(4, len(raw_bytes) - 3)):
                    try:
                        # Try as year/month/day
                        year = int.from_bytes(raw_bytes[offset:offset+2], byte_order)
                        month = raw_bytes[offset+2]
                        day = raw_bytes[offset+3]
                        
                        if 2000 <= year <= 2030 and 1 <= month <= 12 and 1 <= day <= 31:
                            dt = datetime(year, month, day, tzinfo=timezone.utc)
                            return dt.isoformat()
                    except (ValueError, IndexError):
                        continue
        except:
            pass
    
    # Format 5: If all else fails, try to extract any reasonable date from the data
    try:
        # Look for patterns that might represent dates
        hex_str = raw_bytes.hex()
        
        # Look for year patterns (2000-2030)
        for year in range(2000, 2031):
            year_hex = hex(year)[2:].zfill(4)
            if year_hex in hex_str:
                # Found a year, try to extract month/day
                year_pos = hex_str.find(year_hex)
                if year_pos + 8 <= len(hex_str):
                    month_hex = hex_str[year_pos+4:year_pos+6]
                    day_hex = hex_str[year_pos+6:year_pos+8]
                    try:
                        month = int(month_hex, 16)
                        day = int(day_hex, 16)
                        if 1 <= month <= 12 and 1 <= day <= 31:
                            dt = datetime(year, month, day, tzinfo=timezone.utc)
                            return dt.isoformat()
                    except ValueError:
                        continue
    except:
        pass
    
    # If no format works, try to extract any reasonable timestamp from the data
    # Look for patterns that might represent dates in the hex string
    try:
        hex_str = raw_bytes.hex()
        
        # Look for year patterns (2000-2030) in different positions
        for year in range(2000, 2031):
            year_hex = hex(year)[2:].zfill(4)
            if year_hex in hex_str:
                # Found a year, try to extract month/day from nearby positions
                year_pos = hex_str.find(year_hex)
                
                # Try different offsets for month/day
                for offset in [4, 6, 8]:
                    if year_pos + offset + 4 <= len(hex_str):
                        month_hex = hex_str[year_pos+offset:year_pos+offset+2]
                        day_hex = hex_str[year_pos+offset+2:year_pos+offset+4]
                        try:
                            month = int(month_hex, 16)
                            day = int(day_hex, 16)
                            if 1 <= month <= 12 and 1 <= day <= 31:
                                dt = datetime(year, month, day, tzinfo=timezone.utc)
                                return dt.isoformat()
                        except ValueError:
                            continue
    except:
        pass
    
    # If still no format works, return None instead of current time
    # This allows the calling code to handle missing timestamps appropriately
    return None

def get_nested(data, *keys, default=None):
    """Safely get a nested value from a dictionary."""
    for key in keys:
        if isinstance(data, dict) and key in data:
            data = data[key]
        else:
            return default
    return data

def to_int(value):
    """Safely convert a value to an integer."""
    if value is None:
        return None
    try:
        if isinstance(value, str):
            return int.from_bytes(value.encode('latin-1'), 'big')
        return int(value)
    except (ValueError, TypeError):
        return None

def parse_mcc_mnc_from_hex(hex_string):
    if not isinstance(hex_string, str) or len(hex_string) < 6: return hex_string
    mcc = hex_string[1] + hex_string[0] + hex_string[3]
    mnc = (hex_string[5] + hex_string[4] + hex_string[2]).replace('f', '')
    return f"MCC:{mcc}, MNC:{mnc}"

def decode_plmn_id(raw_bytes_str):
    if not isinstance(raw_bytes_str, str): return raw_bytes_str
    return parse_mcc_mnc_from_hex(raw_bytes_str.encode('latin-1').hex())

def safe_decode_with_hex(value):
    if not isinstance(value, str): return value
    hex_repr = value.encode('latin-1').hex()
    printable_repr = ''.join(c if ' ' <= c <= '~' else '.' for c in value)
    return {'text': printable_repr, 'hex': hex_repr}

def decode_tbcd(tbcd_data):
    """
    Decodes T-BCD (Telephony BCD) data by swapping nibbles,
    which is standard for fields like IMSI and MSISDN.
    """
    if not isinstance(tbcd_data, (bytes, str)):
        return tbcd_data
    if isinstance(tbcd_data, str):
        tbcd_data = tbcd_data.encode('latin-1')

    digits = []
    for byte in tbcd_data:
        lsb = byte & 0x0F
        msb = (byte >> 4) & 0x0F
        
        # Stop if filler nibble is in the low part of the byte.
        if lsb == 0x0F: break
        digits.append(str(lsb))
        
        # Stop if filler nibble is in the high part of the byte.
        if msb == 0x0F: break
        digits.append(str(msb))
        
    return ''.join(digits) 