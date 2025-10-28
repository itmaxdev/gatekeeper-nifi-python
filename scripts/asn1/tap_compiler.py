import asn1tools
import json
import argparse
import os
import sys
from datetime import datetime, timezone
import traceback
# from utils import BytesEncoder



# --- Static Mappings ---
MAPPINGS = {
    "callTypeLevel1": {
        1: "MOC",       # Mobile Originated Call
        2: "MTC",       # Mobile Terminated Call
        12: "GPRS"
    },
    "recEntityType": {
        1: "SGSN",
        2: "GGSN",
        3: "MSC",
        4: "GMLC",
        5: "SMLC"
    }
} 


try:
    # Use the tap_cdr_schema.asn from the schema folder
    sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
    schema_path = os.path.join(
        os.path.dirname(os.path.dirname(__file__)), "schema", "tap_cdr_schema.asn"
    )
    tap_compiler = asn1tools.compile_files(schema_path, "ber")
except Exception as e:
    print(f"Error compiling tap_cdr_schema.asn: {e}")
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


def get_nested(data, *keys, default=None):
    """Safely get a nested value from a dictionary or list."""
    for key in keys:
        if isinstance(data, dict):
            data = data.get(key)
        elif isinstance(data, list) and isinstance(key, int) and len(data) > key:
            data = data[key]
        else:
            return default
    return data


def get_tax_code_display_name(tax_code, tax_list):
    """
    Convert tax code to display name based on the starting index of the tax list.
    
    Args:
        tax_code: The tax code value
        tax_list: The list of tax items to determine starting index
        
    Returns:
        str: The display name for the tax code (1-indexed)
    """
    if tax_code is None or not tax_list:
        return None
    
    # Check if tax codes start from 0 or 1
    first_tax_code = tax_list[0].get('taxCode') if tax_list else None
    
    if first_tax_code == 0:
        # If first tax code is 0, convert 0-indexed to 1-indexed
        display_code = tax_code + 1
    else:
        # If first tax code is 1 or higher, use as is (already 1-indexed)
        display_code = tax_code
    
    return f"taxCode{display_code}"

def flatten_event_data(event_type, event_data, header_data):
    """
    Flattens the nested event data structure into a flat dictionary,
    matching the client's required flat format for all event types.
    """
    flat_data = {}

    # --- Common fields (IMSI, MSISDN, IMEI) ---
    chargeable_subscriber_path = []
    equipment_path = []
    start_time_path = []

    if event_type == "gprsCall":
        chargeable_subscriber_path = [
            "gprsBasicCallInformation", "gprsChargeableSubscriber", 
            "chargeableSubscriber", "simChargeableSubscriber"
        ]
        equipment_path = ["equipmentIdentifier"]
        start_time_path = [
            "gprsBasicCallInformation", "callEventStartTimeStamp", 
            "localTimeStampReadable"
        ]
    elif event_type in ["mobileOriginatedCall", "mobileTerminatedCall"]:
        chargeable_subscriber_path = ["basicCallInformation", "chargeableSubscriber", "simChargeableSubscriber"]
        equipment_path = ["equipmentIdentifier"]
        start_time_path = [
            "basicCallInformation", "callEventStartTimeStamp", 
            "localTimeStampReadable"
        ]
    # print(chargeable_subscriber_path)
    chargeable_subscriber_info = get_nested(event_data, *chargeable_subscriber_path, default={})
    
    # Apply TBCD decoding for IMSI and MSISDN
    raw_imsi = chargeable_subscriber_info.get('imsi')
    raw_msisdn = chargeable_subscriber_info.get('msisdn')
    
    # Decode IMSI
    flat_data['imsi'] = decode_imsi_tbcd(raw_imsi) if raw_imsi else None
    
    flat_data['msisdn'] = decode_msisdn_tbcd(raw_msisdn) if raw_msisdn else None
    
    equipment_id = get_nested(event_data, *equipment_path)
    if equipment_id:
        # Handle ImeiOrEsn CHOICE structure from TAP files
        if isinstance(equipment_id, dict):
            # TAP ImeiOrEsn is a CHOICE: {'imei': value} or {'esn': value}
            if 'imei' in equipment_id:
                raw_imei = equipment_id['imei']
                # IMEI is already TBCD decoded by decode_bcd_fields
                flat_data['imei'] = raw_imei
            elif 'esn' in equipment_id:
                # Handle ESN if present
                flat_data['imei'] = None
            else:
                flat_data['imei'] = None
        elif isinstance(equipment_id, list) and len(equipment_id) == 2 and equipment_id[0] == 'imei':
            # Handle legacy format: ['imei', value]
            raw_imei = equipment_id[1]
            # IMEI is already TBCD decoded by decode_bcd_fields
            flat_data['imei'] = raw_imei
        elif isinstance(equipment_id, str):
            # Handle case where IMEI might be directly a string
            # IMEI is already TBCD decoded by decode_bcd_fields
            flat_data['imei'] = equipment_id
        else:
            flat_data['imei'] = None
    else:
        flat_data['imei'] = None
    
    flat_data['localTimeStampReadable'] = get_nested(event_data, *start_time_path)

    # --- Flatten event-specific fields ---
    if event_type == 'gprsCall':
        basic_info = get_nested(event_data, "gprsBasicCallInformation", default={})
        service_used = get_nested(event_data, "gprsServiceUsed", default={})
        
        flat_data.update({
            'totalCallEventDuration': get_nested(basic_info, 'totalCallEventDuration'),
            'chargingId': get_nested(basic_info, 'chargingId'),
            'dataVolumeIncoming': get_nested(service_used, 'dataVolumeIncoming'),
            'dataVolumeOutgoing': get_nested(service_used, 'dataVolumeOutgoing')
        })
        flat_data['event_type'] = event_type
        charge_info_list = get_nested(service_used, "chargeInformationList", default=[])
        if charge_info_list:
            charge_info = charge_info_list[0]
            charge_detail = get_nested(charge_info, 'chargeDetailList', 0, default={})
            
            # Use a variable for charge to avoid multiple lookups
            charge_value = charge_detail.get('charge')
            flat_data['charge'] = charge_value
            flat_data['chargeableUnits'] = charge_detail.get('chargeableUnits')

            # Flatten tax info using the new required format (taxCode1, taxCode2, etc.)
            tax_list = get_nested(charge_info, 'taxInformation') or []
            for i, tax_item in enumerate(tax_list):
                # if i < 2:  # Only take first two tax codes as per expected format
                tax_code = str(tax_item.get('taxCode'))
                tax_value = tax_item.get('taxValue')
                
                header_tax = header_data.get('_tax_codes', {}).get(int(tax_code), {})
                tax_rate = header_tax.get('taxRate')
                flat_data['taxCode'+tax_code] = tax_code
                flat_data['taxrate'+tax_code] = tax_rate
                flat_data['taxValue'+tax_code] = tax_value
                
                # Create the new format: taxCode1, taxCode2 with their corresponding values
                # if tax_code is not None:
                #     display_name = get_tax_code_display_name(tax_code, tax_list)
                #     if display_name:
                #         flat_data[display_name] = str(tax_value) if tax_value is not None else None
                
                # Also keep taxableAmount if charge_value exists
                if charge_value is not None:
                    flat_data['taxableAmount'] = charge_value

    elif event_type == 'mobileOriginatedCall':
        basic_info = get_nested(event_data, "basicCallInformation", default={})
        location_info = get_nested(event_data, "locationInformation", "networkLocation", default={})
        
        flat_data.update({
            "calledNumber": decode_tbcd(get_nested(basic_info, "destination", "calledNumber")),
            "dialledDigits": get_nested(basic_info, "destination", "dialledDigits"),
            "sMSDestinationNumber": get_nested(basic_info, "destination", "sMSDestinationNumber"),
            "totalCallEventDuration": get_nested(basic_info, "totalCallEventDuration"),
            "recEntityCode": get_nested(location_info, "recEntityCode"),
            "locationArea": get_nested(location_info, "locationArea"),
            "cellId": get_nested(location_info, "cellId")
        })
        flat_data['event_type'] = event_type

        basicServiceUsedList = get_nested(event_data, "basicServiceUsedList", default={})
        for service in basicServiceUsedList:
            flat_data["teleServiceCode"] = get_nested(service, "basicService", "serviceCode","teleServiceCode")
            charge_info_list = get_nested(service, "chargeInformationList", default=[])
            if charge_info_list:
                charge_info = charge_info_list[0]
                charge_detail = get_nested(charge_info, 'chargeDetailList', 0, default={})
                
                # Use a variable for charge to avoid multiple lookups
                charge_value = charge_detail.get('charge')
                flat_data['charge'] = charge_value
                flat_data['chargeableUnits'] = charge_detail.get('chargeableUnits')

                # Flatten tax info using the new required format (taxCode1, taxCode2, etc.)
                tax_list = get_nested(charge_info, 'taxInformation') or []
                for i, tax_item in enumerate(tax_list):
                    # if i < 2:  # Only take first two tax codes as per expected format
                    tax_code = str(tax_item.get('taxCode'))
                    tax_value = tax_item.get('taxValue')
                    
                    header_tax = header_data.get('_tax_codes', {}).get(int(tax_code), {})
                    tax_rate = header_tax.get('taxRate')
                    flat_data['taxCode'+tax_code] = tax_code
                    flat_data['taxrate'+tax_code] = tax_rate
                    flat_data['taxValue'+tax_code] = tax_value
                    
                    # Create the new format: taxCode1, taxCode2 with their corresponding values
                    # if tax_code is not None:
                    #     display_name = get_tax_code_display_name(tax_code, tax_list)
                    #     if display_name:
                    #         flat_data[display_name] = str(tax_value) if tax_value is not None else None
                    
                    # Also keep taxableAmount if charge_value exists
                    if charge_value is not None:
                        flat_data['taxableAmount'] = charge_value
        # if service_info:
        #     flat_data["teleServiceCode"] = get_nested(service_info, "basicService", "serviceCode", 1)
        #     charge_info = get_nested(service_info, "chargeInformationList", 0, default={})
        #     flat_data["chargedItem"] = charge_info.get("chargedItem")
        #     charge_value = get_nested(charge_info, "chargeDetailList", 0, "charge")
        #     flat_data["charge"] = charge_value
            
        #     # Add callReference if available
        #     flat_data["callReference"] = get_nested(basic_info, "callReference")

        #     tax_list = get_nested(charge_info, 'taxInformation') or []
        #     for i, tax_item in enumerate(tax_list):
        #         if i < 2:  # Only take first two tax codes as per expected format
        #             tax_code = tax_item.get('taxCode')
        #             tax_value = tax_item.get('taxValue')
                    
        #             # Create the new format: taxCode1, taxCode2 with their corresponding values
        #             if tax_code is not None:
        #                 display_name = get_tax_code_display_name(tax_code, tax_list)
        #                 if display_name:
        #                     flat_data[display_name] = str(tax_value) if tax_value is not None else None
                    
        #             # Also keep taxableAmount if charge_value exists
        #             if charge_value is not None:
        #                 flat_data['taxableAmount'] = charge_value

    elif event_type == 'mobileTerminatedCall':
        basic_info = get_nested(event_data, "basicCallInformation", default={})
        location_info = get_nested(event_data, "locationInformation", "networkLocation", default={})

        flat_data.update({
            "callingNumber": decode_tbcd(get_nested(basic_info, "callOriginator", "callingNumber")),
            "totalCallEventDuration": get_nested(basic_info, "totalCallEventDuration"),
            "recEntityCode": get_nested(location_info, "recEntityCode"),
            "locationArea": get_nested(location_info, "locationArea"),
            "cellId": get_nested(location_info, "cellId"),
        })
        flat_data['event_type'] = event_type
        basicServiceUsedList = get_nested(event_data, "basicServiceUsedList", default={})
        for service in basicServiceUsedList:
            service_info = service
        
            # service_info = get_nested(event_data, "basicServiceUsedList", 0, default={})
            if service_info:
                flat_data["teleServiceCode"] = get_nested(service_info, "basicService", "serviceCode", "teleServiceCode")
                charge_info_list = get_nested(service_info, "chargeInformationList", default=[])
                for charge_info in charge_info_list:
                    
                    # charge_info = get_nested(service_info, "chargeInformationList", 0, default={})
                    flat_data["chargedItem"] = charge_info.get("chargedItem")
                    charge_value = get_nested(charge_info, "chargeDetailList", 0, "charge")
                    flat_data["charge"] = charge_value

                    tax_list = get_nested(charge_info, 'taxInformation') or []
                    for i, tax_item in enumerate(tax_list):
                        # if i < 2:  # Only take first two tax codes as per expected format
                        tax_code = str(tax_item.get('taxCode'))
                        tax_value = tax_item.get('taxValue')
                        
                        header_tax = header_data.get('_tax_codes', {}).get(int(tax_code), {})
                        tax_rate = header_tax.get('taxRate')
                    flat_data['taxCode'+tax_code] = tax_code
                    flat_data['taxrate'+tax_code] = tax_rate
                    flat_data['taxValue'+tax_code] = tax_value
                            
                        # # Create the new format: taxCode1, taxCode2 with their corresponding values
                        # if tax_code is not None:
                        #     display_name = get_tax_code_display_name(tax_code, tax_list)
                        #     if display_name:
                        #         flat_data[display_name] = str(tax_value) if tax_value is not None else None
                        
                        # Also keep taxableAmount if charge_value exists
                        if charge_value is not None:
                            flat_data['taxableAmount'] = charge_value

    # Remove keys with None values before returning, but preserve important fields like IMEI
    preserved_fields = ['imei', 'imsi', 'msisdn']  # Fields that should be preserved even if None
    result = {}
    for k, v in flat_data.items():
        if v is not None or k in preserved_fields:
            result[k] = v
    
    return result

def decode_tbcd(tbcd_data):
    """
    Decodes T-BCD (Telephony BCD) data by swapping nibbles.
    
    Steps:
    1. Split into digit pairs: 91 42 93 40 43 09 58
    2. Flip each pair: 91 → 19, 42 → 24, etc.
    3. Concatenate: 19243904349085
    4. Remove prefix (91) → Final: 243904349085
    
    Args:
        tbcd_data: The TBCD encoded data (bytes, str, or hex string)
    
    Returns:
        str: Decoded number string
    """
    if not isinstance(tbcd_data, (bytes, str)):
        return tbcd_data
    
    # Convert to bytes if it's a string
    if isinstance(tbcd_data, str):
        # If it's a hex string, convert to bytes
        if (len(tbcd_data) % 2 == 0 and 
            all(c in '0123456789abcdefABCDEF' for c in tbcd_data)):
            try:
                tbcd_data = bytes.fromhex(tbcd_data)
            except ValueError:
                tbcd_data = tbcd_data.encode('latin-1')
        else:
            tbcd_data = tbcd_data.encode('latin-1')
    
    digits = []
    
    for i, byte in enumerate(tbcd_data):
        lsb = byte & 0x0F  # Lower nibble
        msb = (byte >> 4) & 0x0F  # Upper nibble
        
        # Stop if filler nibble (0xF) is encountered
        if lsb == 0x0F:
            break
        digits.append(str(lsb))
        
        if msb == 0x0F:
            break
        digits.append(str(msb))
    
    result = ''.join(digits)
    
    # Remove country code prefix if present (common prefixes: 91, 1, 44, etc.)
    if result.startswith('91') and len(result) > 10:  # India
        result = result[2:]
    elif result.startswith('1') and len(result) > 10:  # US/Canada
        result = result[1:]
    elif result.startswith('44') and len(result) > 10:  # UK
        result = result[2:]
    
    return result
def decode_tap_file(encoded_data):
    try:
        decoded_data = tap_compiler.decode("DataInterChange", encoded_data)
        wrapped_data = {
            "tapRecord": [decoded_data[0], decoded_data[1]]
        }
        return wrapped_data
    except Exception as e:
        print(f"Error decoding TAP file: {e}")
        return {"error": str(e)}


def decode_imsi_tbcd(imsi_data):
    """
    Decodes IMSI from TBCD format.
    
    Args:
        imsi_data: The TBCD encoded IMSI
    
    Returns:
        str: Decoded IMSI without dashes
    """
    if not imsi_data:
        return imsi_data
    
    decoded = decode_tbcd(imsi_data)
    
    # IMSI format: MCC (3) + MNC (2-3) + MSIN (up to 10)
    # Return clean numeric string without dashes
    if len(decoded) >= 5:
        mcc = decoded[:3]
        mnc = decoded[3:5] if len(decoded) >= 5 else decoded[3:]
        msin = decoded[5:] if len(decoded) > 5 else ""
        result = f"{mcc}{mnc}{msin}"
        return result
    
    return decoded


def decode_msisdn_tbcd(msisdn_data):
    """
    Decodes MSISDN from TBCD format.
    
    Args:
        msisdn_data: The TBCD encoded MSISDN
    
    Returns:
        str: Decoded MSISDN
    """
    if not msisdn_data:
        return msisdn_data
    
    decoded = decode_tbcd(msisdn_data)
    
    return decoded


def decode_imei_tbcd(imei_data):
    """
    Decodes IMEI from TBCD format.
    
    Args:
        imei_data: The TBCD encoded IMEI
    
    Returns:
        str: Decoded IMEI (exactly 15 digits)
    """
    if not imei_data:
        return imei_data
    
    decoded = decode_tbcd(imei_data)
    
    # IMEI should be exactly 15 digits
    if len(decoded) == 16:
        # Remove the leading zero if it's 16 digits
        if decoded.startswith('0'):
            decoded = decoded[1:]
        else:
            # If it doesn't start with 0, truncate the last digit
            decoded = decoded[:15]
    elif len(decoded) != 15:
        pass  # IMEI length is not 15, but we'll return it as is
    
    return decoded


def decode_bcd_fields(data):
    """
    Recursively finds and decodes various BCD/TBCD fields into 
    human-readable strings using TAP-specific functions.
    """
    if isinstance(data, dict):
        for key, value in list(data.items()):
            # T-BCD encoded fields (swapped nibbles)
            if key in ['imsi', 'msisdn', 'callingNumber', 'calledNumber', 'destinationNumber', 'thirdPartyNumber']:
                if isinstance(value, str): 
                    data[key] = decode_tbcd(value)

            # Equipment Identifier (IMEI is TBCD)
            elif key == 'equipmentIdentifier':
                if isinstance(value, dict):
                    # TAP ImeiOrEsn CHOICE structure: 
                    # {'imei': value} or {'esn': value}
                    if 'imei' in value and isinstance(value['imei'], str):
                        value['imei'] = decode_imei_tbcd(value['imei'])
                elif isinstance(value, list) and len(value) == 2:
                    # Legacy format: ['imei', value]
                    id_type, id_val = value
                    if id_type == 'imei' and isinstance(id_val, str):
                        data[key] = ['imei', decode_imei_tbcd(id_val)]
                else:
                    print(f"  Unknown equipment identifier format")

            # Recurse into nested structures
            else:
                decode_bcd_fields(value)

    elif isinstance(data, list):
        for item in data:
            decode_bcd_fields(item)
            
    return data

def decode_timestamp_fields(data):
    """
    Recursively finds and converts 'localTimeStamp' fields into a 
    more readable ISO format.
    """
    if isinstance(data, dict):
        for key, value in list(data.items()):
            if key == 'localTimeStamp' and isinstance(value, str) and len(value) == 14:
                try:
                    # Add a new, human-readable field
                    dt_obj = datetime.strptime(value, "%Y%m%d%H%M%S")
                    data[f"{key}Readable"] = dt_obj.isoformat()
                except ValueError:
                    # If parsing fails, leave it as is
                    pass
            else:
                # Recurse into nested dictionaries and lists
                decode_timestamp_fields(value)
    elif isinstance(data, list):
        for item in data:
            decode_timestamp_fields(item)
    return data 

def map_coded_values(data):
    """
    Recursively finds and maps known integer codes to human-readable
    string values, adding new 'Decoded' fields.
    """
    if isinstance(data, dict):
        for key, value in list(data.items()):
            # --- Mappings ---
            if key == 'callTypeLevel1' and value in MAPPINGS['callTypeLevel1']:
                data[f"{key}Decoded"] = MAPPINGS['callTypeLevel1'][value]
            
            elif key == 'recEntityType' and value in MAPPINGS['recEntityType']:
                 data[f"{key}Decoded"] = MAPPINGS['recEntityType'][value]

            # Recurse into nested structures
            else:
                map_coded_values(value)

    elif isinstance(data, list):
        for item in data:
            map_coded_values(item)
            
    return data

def encode_binary_fields(data):
    """
    Recursively finds binary-like string fields and adds a hex-encoded
    version for readability.
    """
    if isinstance(data, dict):
        for key, value in list(data.items()):
            if key == 'callReference' and isinstance(value, str):
                try:
                    # Add a new, hex-encoded field for readability
                    data[f"{key}Hex"] = value.encode('latin-1').hex()
                except Exception:
                    pass # Ignore if encoding fails
            else:
                encode_binary_fields(value)
    
    elif isinstance(data, list):
        for item in data:
            encode_binary_fields(item)

    return data

def get_header_data(transfer_batch):
    """
    Extracts and flattens required header fields from the transferBatch.
    """
    header = {}
    if not isinstance(transfer_batch, dict):
        return header

    # Extract sender, recipient, and fileSequenceNumber
    bci = transfer_batch.get('batchControlInfo', {})
    if bci:
        header.update({
            "sender": bci.get("sender"),
            "recipient": bci.get("recipient")
        })

    # Add fixed tax rates for roaming header as per configuration requirements (right after sender/recipient)
    # header["taxrate1"] = 1000000
    # header["taxrate2"] = 1600000

    # Continue with fileSequenceNumber and other header fields
    if bci:
        header["fileSequenceNumber"] = bci.get("fileSequenceNumber")

    # Extract and flatten taxation and currency info from accountingInfo
    ai = transfer_batch.get('accountingInfo', {})
    if ai:
        # Remove header tax code collection since we only want event-level tax codes
        
        # Extract currency conversion info (only the first one)
        currency_info_list = ai.get("currencyConversionInfo", [])
        if currency_info_list:
            header["numberOfDecimalPlaces"] = currency_info_list[0].get("numberOfDecimalPlaces")
            header["exchangeRate"] = currency_info_list[0].get("exchangeRate")
            
        taxation = ai.get("taxation", {})
        tax_info_dict = {}
        if taxation:
            for tax_info in taxation:
                tax_code = tax_info.get("taxCode")
                if tax_code:
                    tax_info_dict[tax_code] = tax_info
        
        header["_tax_codes"] = tax_info_dict
        header["tapDecimalPlaces"] = ai.get("tapDecimalPlaces")

    return header

def normalize_tap_file(raw_data,filename, operator):
    """
    Normalizes a raw TAP JSON file and enriches records with header data.
    """
    original_filename = filename
    base_name = os.path.splitext(original_filename)[0]
    # raw_data = dict(raw_data)  # Ensure it's a dict

    try:
        # Navigate to the transferBatch and callEventDetails
        transfer_batch = raw_data['tapRecord'][1]
        call_events = transfer_batch.get('callEventDetails', [])
        
        # Extract header data once
        header_data = get_header_data(transfer_batch)

    except (KeyError, IndexError, TypeError) as e:
        print(f"Error navigating the raw JSON structure: {e}")
        traceback.print_exc()
        return

    if not call_events:
        # print("No 'callEventDetails' found to process.")
        return

    # print(f"Normalizing {len(call_events)} call events...")
    normalized_records = []
    # print(type(call_events))

    for event in call_events:
        # print(event_type, event_data)
        for event_type, event_data in event.items():
            # event_type, event_data = event
            
            if not event_data:
                continue

            # --- Normalization and Decoding Pipeline ---
            event_data = decode_bcd_fields(event_data)
            event_data = decode_timestamp_fields(event_data)
            event_data = map_coded_values(event_data)
            event_data = encode_binary_fields(event_data)
            
            # --- Merging Header and Event Data ---
            # Create a new dictionary to control the order and content
            normalized_record = {}
            
            # 1. Add common metadata first
            normalized_record['eventType'] = event_type
            
            # 2. Add enriched header data (excluding internal tax lists)
            # Remove tax codes and rates since we only want event-level tax codes
            header_copy = header_data.copy()
            
            # 3. Remove the header tax code processing since we only want event-level tax codes

            # 4. Add the flattened event data
            # Flatten event data
            normalized_record.update(flatten_event_data(event_type, event_data, header_copy))
            header_copy.pop("_tax_codes", [])
            header_copy.pop("_tax_rates", [])
            normalized_record.update(header_copy)
            
            # 5. Add remaining metadata
            normalized_record['filename'] = original_filename
            normalized_record['parsed_time'] = datetime.now(timezone.utc).isoformat()
            normalized_record['operator'] = operator
            normalized_records.append(normalized_record)
            # Get or create the output file for this event type

    return normalized_records