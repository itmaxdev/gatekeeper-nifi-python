"""
Direct Binary Parser for SGSN CDR Files
This parser scans the raw .dat file directly for CONTEXT_21 and CONTEXT_27 tags
and extracts localSequenceNumber and servedMSISDN regardless of ASN.1 schema 
issues.
"""

from typing import Dict, List, Tuple, Optional, Any

class DirectBinaryParser:
    """Direct binary parser that scans for CONTEXT tags in raw binary data"""
    
    def __init__(self, show_tbcd_steps=False):
        self.context_21_locations: List[Tuple[int, bytes]] = []
        self.context_27_locations: List[Tuple[int, bytes]] = []
        self.show_tbcd_steps = show_tbcd_steps
        
    def scan_file(self, filename: str, data: bytes) -> Dict[str, Any]:
        """Scan a .dat file for CONTEXT_21 and CONTEXT_27 tags"""
        result = {
            'filename': filename,
            'context_3_found': [],
            'context_21_found': [],
            'context_27_found': [],
            'context_16_found': [],
            'extracted_data': []
        }
        
        try:
            # with open(filename, 'rb') as f:
            #     data = f.read()
            
            # Find CONTEXT_3 (servedIMSI) entries
            context_3_offsets = self._find_context_tag(data, 0x83)
            
            # print(f"Scanning {filename} ({len(data)} bytes) for CONTEXT tags...")

            for i, offset in enumerate(context_3_offsets):
                extracted = self._extract_from_context_3(data, offset)
                if extracted:
                    result['context_3_found'].append({
                        'offset': offset,
                        'data': extracted
                    })
            
            # Find CONTEXT_27 (servedMSISDN) entries
            context_27_offsets = self._find_context_tag(data, 0x9B)
            # print(f"Found {len(context_27_offsets)} CONTEXT_27 entries")
            
            for i, offset in enumerate(context_27_offsets):
                # if i % 1000 == 0 and i > 0:  # Show progress every 1000 entries
                #     print(f"Processing CONTEXT_27 entry {i}/"
                #           f"{len(context_27_offsets)}")
                extracted = self._extract_from_context_27(data, offset)
                if extracted:
                    result['context_27_found'].append({
                        'offset': offset,
                        'data': extracted
                    })

            # Find CONTEXT_21 (recordSequenceNumber) entries  
            context_21_offsets = self._find_context_tag(data, 0x95)
            # print(f"Found {len(context_21_offsets)} CONTEXT_21 entries")
            
            for i, offset in enumerate(context_21_offsets):
                # if i % 1000 == 0 and i > 0:  # Show progress every 1000 entries
                    # print(f"Processing CONTEXT_21 entry {i}/{len(context_21_offsets)}")
                extracted = self._extract_from_context_21(data, offset)
                if extracted:
                    result['context_21_found'].append({
                        'offset': offset,
                        'data': extracted
                    })
            
            # Find CONTEXT_16 (recordOpeningTime) entries
            context_16_offsets = self._find_context_tag(data, 0x90)
            
            for i, offset in enumerate(context_16_offsets):
                extracted = self._extract_timestamp_from_context_16(data, offset)
                if extracted:
                    result['context_16_found'].append({
                        'offset': offset,
                        'data': {'recordOpeningTime': extracted}
                    })
            
            # Find CONTEXT_8 (locationAreaCode) entries
            context_8_offsets = self._find_context_tag(data, 0x88)
            result['context_8_found'] = []
            for i, offset in enumerate(context_8_offsets):
                # Extract the value as hex string (or int if possible)
                pos = offset + 1
                length_byte = data[pos]
                pos += 1
                if length_byte & 0x80 == 0:
                    ctx8_length = length_byte
                else:
                    num_octets = length_byte & 0x7F
                    if pos + num_octets > len(data):
                        continue
                    ctx8_length = int.from_bytes(data[pos:pos + num_octets], 'big')
                    pos += num_octets
                if pos + ctx8_length > len(data):
                    continue
                lac_bytes = data[pos:pos + ctx8_length]
                lac_value = int.from_bytes(lac_bytes, 'big')
                
                # Create result entry with full value
                result_entry = {'offset': offset, 'locationAreaCode': lac_value}
                
                # If longer than 2 bytes, also store first 2 bytes value
                if len(lac_bytes) > 2:
                    likely_lac = int.from_bytes(lac_bytes[:2], 'big')
                    result_entry['locationAreaCode_first2bytes'] = likely_lac
                
                result['context_8_found'].append(result_entry)
            # Find CONTEXT_9 (cellIdentifier) entries
            context_9_offsets = self._find_context_tag(data, 0x89)
            result['context_9_found'] = []
            for i, offset in enumerate(context_9_offsets):
                pos = offset + 1
                length_byte = data[pos]
                pos += 1
                if length_byte & 0x80 == 0:
                    ctx9_length = length_byte
                else:
                    num_octets = length_byte & 0x7F
                    if pos + num_octets > len(data):
                        continue
                    ctx9_length = int.from_bytes(data[pos:pos + num_octets], 'big')
                    pos += num_octets
                if pos + ctx9_length > len(data):
                    continue
                cid_bytes = data[pos:pos + ctx9_length]
                cid_value = int.from_bytes(cid_bytes, 'big')
                
                # Create result entry with full value
                result_entry = {'offset': offset, 'cellIdentifier': cid_value}
                
                # If longer than 2 bytes, also store first 2 bytes value
                if len(cid_bytes) > 2:
                    likely_cell_id = int.from_bytes(cid_bytes[:2], 'big')
                    result_entry['cellIdentifier_first2bytes'] = likely_cell_id
                
                result['context_9_found'].append(result_entry)
            
            # Combine results
            for ctx21 in result['context_21_found']:
                record_data = {'offset': ctx21['offset']}
                record_data.update(ctx21['data'])

                # Look for nearby CONTEXT_3 (IMSI)
                for ctx3 in result['context_3_found']:
                    if abs(ctx21['offset'] - ctx3['offset']) < 1000:  # Within 1KB
                        record_data.update(ctx3['data'])
                        break
                
                # Look for nearby CONTEXT_27
                for ctx27 in result['context_27_found']:
                    if abs(ctx21['offset'] - ctx27['offset']) < 1000:  # Within 1KB
                        record_data.update(ctx27['data'])
                        break
                
                # Look for nearby CONTEXT_16 (timestamp)
                for ctx16 in result['context_16_found']:
                    if abs(ctx21['offset'] - ctx16['offset']) < 1000:  # Within 1KB
                        record_data.update(ctx16['data'])
                        break
                
                # Look for nearby CONTEXT_8 (locationAreaCode)
                for ctx8 in result['context_8_found']:
                    if abs(ctx21['offset'] - ctx8['offset']) < 1000:
                        record_data['locationAreaCode'] = ctx8['locationAreaCode']
                        break
                
                # Look for nearby CONTEXT_9 (cellIdentifier)
                for ctx9 in result['context_9_found']:
                    if abs(ctx21['offset'] - ctx9['offset']) < 1000:
                        record_data['cellIdentifier'] = ctx9['cellIdentifier']
                        break
                
                result['extracted_data'].append(record_data)
            
            return result
            
        except Exception as e:
            print(f"Error scanning file: {e}")
            return {'error': str(e)}
    
    def _find_context_tag(self, data: bytes, tag: int) -> List[int]:
        """Find all occurrences of a specific CONTEXT tag"""
        offsets = []
        
        for i in range(len(data) - 1):
            if data[i] == tag:
                # Verify it looks like a valid ASN.1 tag-length structure
                if i + 1 < len(data):
                    length_byte = data[i + 1]
                    
                    # Check if it's a reasonable length (not too large)
                    if length_byte < 0x80:  # Short form
                        content_length = length_byte
                    else:  # Long form
                        num_octets = length_byte & 0x7F
                        if (num_octets > 0 and num_octets <= 4 and 
                            i + 1 + num_octets < len(data)):
                            try:
                                content_length = int.from_bytes(
                                    data[i + 2:i + 2 + num_octets], 'big'
                                )
                            except (ValueError, IndexError):
                                continue
                        else:
                            continue
                    
                    # Reasonable content length check
                    if 0 < content_length < 10000:
                        offsets.append(i)
        
        return offsets
    
    def _extract_from_context_21(self, data: bytes, offset: int) -> Optional[Dict[str, Any]]:
        """Extract recordSequenceNumber from CONTEXT_21 tag with flexible parsing for variable field placements"""
        
        try:
            if offset + 10 > len(data):  # Need at least 10 bytes
                return None
            
            # Parse the CONTEXT_21 tag
            if data[offset] != 0x95:
                return None
            
            pos = offset + 1
            length = data[pos]
            pos += 1
            
            if pos + length > len(data):
                return None
            
            # Extract the content within CONTEXT_21
            content = data[pos:pos + length]
            result = {}
            
            # Try flexible parsing - scan for any integers or sequences within the content
            content_pos = 0
            found_sequence_num = None
            found_node_id = None
            
            while content_pos < len(content) - 1:
                tag = content[content_pos]
                
                # Look for INTEGER tags (0x02) anywhere in the content
                if tag == 0x02 and content_pos + 1 < len(content):
                    try:
                        int_length = content[content_pos + 1]
                        if content_pos + 2 + int_length <= len(content) and int_length <= 8:
                            int_data = content[content_pos + 2:content_pos + 2 + int_length]
                            sequence_num = int.from_bytes(int_data, 'big')
                            
                            # Use the first reasonable integer we find as sequence number
                            if sequence_num > 0 and sequence_num < 2**31 and found_sequence_num is None:
                                found_sequence_num = sequence_num
                            
                            content_pos += 2 + int_length
                        else:
                            content_pos += 1
                    except:
                        content_pos += 1
                
                # Look for OCTET STRING tags (0x04) for node ID
                elif tag == 0x04 and content_pos + 1 < len(content):
                    try:
                        str_length = content[content_pos + 1]
                        if content_pos + 2 + str_length <= len(content) and 1 <= str_length <= 20:
                            str_data = content[content_pos + 2:content_pos + 2 + str_length]
                            if found_node_id is None:
                                found_node_id = str_data.hex()
                            
                            content_pos += 2 + str_length
                        else:
                            content_pos += 1
                    except:
                        content_pos += 1
                
                # Look for context-specific tags that might contain our data
                elif tag in [0x80, 0x81, 0x82, 0xa0, 0xa1, 0xa2] and content_pos + 1 < len(content):
                    try:
                        field_length = content[content_pos + 1]
                        if content_pos + 2 + field_length <= len(content):
                            # Recursively scan inside context-specific tags
                            sub_content = content[content_pos + 2:content_pos + 2 + field_length]
                            sub_pos = 0
                            
                            while sub_pos < len(sub_content) - 1:
                                sub_tag = sub_content[sub_pos]
                                if sub_tag == 0x02 and sub_pos + 1 < len(sub_content):  # INTEGER
                                    try:
                                        sub_int_length = sub_content[sub_pos + 1]
                                        if sub_pos + 2 + sub_int_length <= len(sub_content) and sub_int_length <= 8:
                                            sub_int_data = sub_content[sub_pos + 2:sub_pos + 2 + sub_int_length]
                                            sub_sequence_num = int.from_bytes(sub_int_data, 'big')
                                            
                                            if sub_sequence_num > 0 and sub_sequence_num < 2**31 and found_sequence_num is None:
                                                found_sequence_num = sub_sequence_num
                                            
                                            sub_pos += 2 + sub_int_length
                                        else:
                                            sub_pos += 1
                                    except:
                                        sub_pos += 1
                                else:
                                    sub_pos += 1
                            
                            content_pos += 2 + field_length
                        else:
                            content_pos += 1
                    except:
                        content_pos += 1
                else:
                    content_pos += 1
            
            # Build result with found values
            if found_sequence_num is not None:
                result['localSequenceNumber'] = found_sequence_num
                result['recordSequenceNumber'] = found_sequence_num
            
            if found_node_id is not None:
                result['recordNodeId'] = found_node_id
            
            return result if result else None
            
        except Exception as e:
            return None  # Silently fail for now to avoid spam

    def _extract_from_context_3(self, data: bytes, offset: int) -> Optional[Dict[str, Any]]:
        """Extract servedIMSI from CONTEXT_3 tag"""
        
        try:
            if offset + 10 > len(data):  # Need at least 10 bytes
                return None
            
            # Parse the CONTEXT_3 tag
            if data[offset] != 0x83:
                return None
            
            pos = offset + 1
            
            # Get CONTEXT_3 length
            length_byte = data[pos]
            pos += 1
            
            if length_byte & 0x80 == 0:
                # Short form
                ctx3_length = length_byte
            else:
                # Long form
                num_octets = length_byte & 0x7F
                if pos + num_octets > len(data):
                    return None
                ctx3_length = int.from_bytes(data[pos:pos + num_octets], 'big')
                pos += num_octets
            
            # Extract the content and decode as TBCD
            if pos + ctx3_length > len(data):
                return None
            
            imsi_data = data[pos:pos + ctx3_length]
            decoded_imsi = self._decode_tbcd(imsi_data, show_steps=self.show_tbcd_steps)
            
            if decoded_imsi:
                return {'servedIMSI': decoded_imsi}
            
            return None
            
        except Exception as e:
            print(f"Error extracting from CONTEXT_3: {e}")
            return None
    
    def _extract_from_context_27(self, data: bytes, offset: int) -> Optional[Dict[str, Any]]:
        """Extract servedMSISDN from CONTEXT_27 tag"""
        
        try:
            if offset + 10 > len(data):  # Need at least 10 bytes
                return None
            
            # Parse the CONTEXT_27 tag
            if data[offset] != 0x9B:
                return None
            
            pos = offset + 1
            
            # Get CONTEXT_27 length
            length_byte = data[pos]
            pos += 1
            
            if length_byte & 0x80 == 0:
                # Short form
                ctx27_length = length_byte
            else:
                # Long form
                num_octets = length_byte & 0x7F
                if pos + num_octets > len(data):
                    return None
                ctx27_length = int.from_bytes(data[pos:pos + num_octets], 'big')
                pos += num_octets
            
            # Extract the content and decode as TBCD
            if pos + ctx27_length > len(data):
                return None
            
            msisdn_data = data[pos:pos + ctx27_length]
            # Prefer the shared utility which is stricter about TBCD padding
            try:
                from asn1.utils import decode_tbcd as _util_decode_tbcd
                decoded_msisdn = _util_decode_tbcd(msisdn_data)
            except Exception:
                # Fallback to local implementation if utility not available
                decoded_msisdn = self._decode_tbcd(msisdn_data, show_steps=self.show_tbcd_steps)

            # If the decoded value is unexpectedly long (likely because we
            # captured extra bytes), try to heuristically extract a plausible
            # MSISDN (7-16 digits). Prefer the first reasonable match.
            if isinstance(decoded_msisdn, str) and len(decoded_msisdn) > 16:
                import re
                m = re.search(r"(\d{7,16})", decoded_msisdn)
                if m:
                    decoded_msisdn = m.group(1)
                else:
                    # As a last resort, truncate to a reasonable max length
                    decoded_msisdn = decoded_msisdn[:16]

            if decoded_msisdn:
                return {'servedMSISDN': decoded_msisdn}
            
            return None
            
        except Exception as e:
            print(f"Error extracting from CONTEXT_27: {e}")
            return None
    
    def _decode_tbcd(self, data: bytes, show_steps: bool = False) -> Optional[str]:
        """
        Decode TBCD (Telephony Binary Coded Decimal) data
        TBCD standard: For each byte, the high nibble comes first, then low nibble
        """
        if not data:
            return None
            
        try:
            result = ""
            hex_bytes = [f"{b:02X}" for b in data]
            
            if show_steps:
                print(f"TBCD Decoding Steps:")
                print(f"Input bytes: {' '.join(hex_bytes)}")
                print(f"Split into digit pairs: {' '.join(hex_bytes)}")
                print(f"Flip each pair:")
            
            flipped_pairs = []
            for i, byte in enumerate(data):
                # TBCD: Swap nibbles - Low nibble first, then high nibble  
                high_nibble = (byte & 0xF0) >> 4
                low_nibble = byte & 0x0F
                
                # Skip padding (0xF)
                if low_nibble != 0xF:
                    result += str(low_nibble)
                if high_nibble != 0xF:
                    result += str(high_nibble)
                
                if show_steps:
                    original = hex_bytes[i]
                    flipped = f"{low_nibble}{high_nibble}"
                    flipped_pairs.append(flipped)
                    print(f"  {original} → {flipped}")
            
            if show_steps:
                print(f"Concatenate: {result}")
                
                # Check for common prefixes to remove
                if result.startswith("19") and len(result) > 2:
                    final_result = result[2:]
                    print(f"Remove prefix (19) → Final: {final_result}")
                    return final_result
                else:
                    print(f"Final result: {result}")
            
            return result if result else None
            
        except Exception as e:
            print(f"Error decoding TBCD: {e}")
            return None

    def _extract_timestamp_from_context_16(self, data: bytes, offset: int) -> Optional[str]:
        """Extract timestamp from CONTEXT_16 (recordOpeningTime)"""
        try:
            pos = offset
            
            # Skip the CONTEXT_16 tag
            if data[pos] != 0x90:  # CONTEXT_16
                return None
            pos += 1
            
            # Get length
            if pos >= len(data):
                return None
                
            length_octet = data[pos]
            pos += 1
            
            if (length_octet & 0x80) == 0:
                # Short form
                timestamp_length = length_octet
            else:
                # Long form
                num_octets = length_octet & 0x7f
                if pos + num_octets > len(data):
                    return None
                timestamp_length = int.from_bytes(data[pos:pos + num_octets], 'big')
                pos += num_octets
            
            # Extract timestamp data
            if pos + timestamp_length > len(data):
                return None
                
            timestamp_data = data[pos:pos + timestamp_length]
            
            # Parse timestamp dynamically (handles multiple formats)
            from asn1.utils import parse_3gpp_timestamp
            decoded_timestamp = parse_3gpp_timestamp(timestamp_data)
            
            # If the dynamic parser couldn't extract a timestamp, try to find one in nearby data
            if decoded_timestamp is None:
                # Look for timestamp patterns in a larger window around this position
                window_start = max(0, offset - 50)
                window_end = min(len(data), offset + 100)
                window_data = data[window_start:window_end]
                
                # Try to find any reasonable timestamp in this window
                decoded_timestamp = parse_3gpp_timestamp(window_data)
            
            return decoded_timestamp
            
        except Exception as e:
            print(f"Error extracting timestamp from CONTEXT_16: {e}")
            return None


def analyze_sgsn_dat_file(filename: str, show_tbcd_steps: bool = False) -> Dict[str, Any]:
    """Analyze a .dat SGSN file directly for CONTEXT tags"""
    parser = DirectBinaryParser(show_tbcd_steps=show_tbcd_steps)
    return parser.scan_file(filename)


if __name__ == "__main__":
    # Test with the BUN file that should have the structured data
    result = analyze_sgsn_dat_file("Input Folder/BUN_CG120240619001501_00224377.dat")
    
    print(f"\n{'='*60}")
    print(f"FINAL RESULTS")
    print(f"{'='*60}")
    
    if 'error' in result:
        print(f"Error: {result['error']}")
    else:
        print(f"File: {result['filename']}")
        print(f"CONTEXT_21 found: {len(result['context_21_found'])}")
        print(f"CONTEXT_27 found: {len(result['context_27_found'])}")
        print(f"Extracted records: {len(result['extracted_data'])}")
        
        for i, record in enumerate(result['extracted_data']):
            print(f"\nRecord {i+1} (offset {record['offset']}):")
            for key, value in record.items():
                if key != 'offset':
                    print(f"  {key}: {value}")
    
    print(f"\n{'='*60}")