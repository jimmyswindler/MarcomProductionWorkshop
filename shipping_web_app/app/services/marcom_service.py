
import requests
import xml.etree.ElementTree as ET
from shared_lib.config import get_env_var

import urllib3

# Suppress InsecureRequestWarning
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def send_packing_slip(line_item_id, tracking_number):
    """
    Sends a SOAP request to Marcom Central to close the order line item.
    Returns a dict with: success (bool), status (str), message (str), packing_slip_id (str or None)
    """
    
    # Load Config
    url = get_env_var("MARCOM_API_URL", "https://services.printable.com/trans/1.0/PackingSlip.asmx")
    token = get_env_var("MARCOM_PARTNER_TOKEN")
    action = get_env_var("MARCOM_SOAP_ACTION", "http://www.printable.com/WebService/PackingSlip/CreatePackingSlipByLineItem")
    carrier = "UPS" # Hardcoded as per legacy app

    if not token:
        return {
            "success": False,
            "status": "CONFIG_ERROR",
            "message": "Missing MARCOM_PARTNER_TOKEN",
            "packing_slip_id": None,
            "code": None
        }

    # Construct XML
    soap_xml = f"""
    <soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:pac="http://www.printable.com/WebService/PackingSlip">
        <soapenv:Header/>
        <soapenv:Body>
            <pac:CreatePackingSlipByLineItem>
                <pac:pRequest>
                    <PartnerCredentials>
                        <Token>{token}</Token>
                    </PartnerCredentials>
                    <PackingSlipNode>
                        <CarrierName>{carrier}</CarrierName>
                        <TrackingNumber>{tracking_number}</TrackingNumber>
                    </PackingSlipNode>
                    <LineItems>
                        <LineItem>
                            <ID type="Printable">{line_item_id}</ID>
                        </LineItem>
                    </LineItems>
                </pac:pRequest>
            </pac:CreatePackingSlipByLineItem>
        </soapenv:Body>
    </soapenv:Envelope>
    """

    # --- LOGGING SETUP ---
    import datetime
    import os
    
    log_dir = "LOGS/soap_requests"
    os.makedirs(log_dir, exist_ok=True)
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    log_base = f"{log_dir}/{timestamp}_{line_item_id}"
    
    # Write Request
    with open(f"{log_base}_req.xml", "w") as f:
        f.write(soap_xml)
    
    headers = {
        'Content-Type': 'text/xml; charset=utf-8',
        'SOAPAction': action
    }

    try:
        # Note: Legacy app used verify=False. We should try to use True but fallback or allow config if needed.
        # For now, we will trust the system's CA store.
        response = requests.post(url, data=soap_xml.encode('utf-8'), headers=headers, timeout=30, verify=False)
        
        # Write Response
        with open(f"{log_base}_resp.xml", "w") as f:
             f.write(f"Status: {response.status_code}\n")
             f.write(response.text)
        
        if response.status_code != 200:
             return {
                "success": False,
                "status": "HTTP_ERROR",
                "message": f"HTTP {response.status_code}: {response.text[:200]}",
                "packing_slip_id": None,
                "code": str(response.status_code)
            }
            
        return parse_soap_response(response.text)

    except Exception as e:
        return {
            "success": False,
            "status": "NETWORK_ERROR",
            "message": str(e),
            "packing_slip_id": None,
            "code": "NET_ERR"
        }

def parse_soap_response(xml_text):
    try:
        # Remove namespaces for easier parsing
        # (Naive regex removal or just ignoring namespaces in find)
        # Python's ElementTree with namespaces is verbose. 
        # Let's iterate and ignore namespaces by using local-name logic if needed, 
        # but pure strip is often easiest for simple SOAP.
        
        # However, let's keep it robust using ET.
        root = ET.fromstring(xml_text)
        
        # Helper to find elements ignoring namespace
        def find_any(node, tag_name):
            # Breadth-first or depth-first search for a tag with specific local name
            for elem in node.iter():
                if elem.tag.endswith(f"}}{tag_name}") or elem.tag == tag_name:
                    return elem
            return None
            
        def find_all_any(node, tag_name):
            matches = []
            for elem in node.iter():
                if elem.tag.endswith(f"}}{tag_name}") or elem.tag == tag_name:
                    matches.append(elem)
            return matches

        # 1. Check Top-Level Status in Result
        # Struct: Body -> CreatePackingSlipByLineItemResponse -> CreatePackingSlipByLineItemResult -> Status
        result_node = find_any(root, "CreatePackingSlipByLineItemResult")
        
        if result_node is not None:
            # Check Result Status
            status_node = find_any(result_node, "Status")
            if status_node is not None:
                main_status = status_node.get("Status")
                main_code = status_node.get("Code")
                main_message = status_node.get("Message")
                
                # Codes: 1=Success, 3=Complete, 5=Done (Check details)
                if main_code in ["1", "3", "5"]:
                    # Success at top level, now look for line item details
                    pass
                else:
                    # Top level failure
                    return {
                        "success": False,
                        "status": f"API_ERROR_{main_code}",
                        "message": f"{main_status}: {main_message}",
                        "packing_slip_id": None,
                        "code": main_code
                    }
            
            # 2. Look for Line Item Responses
            line_items = find_all_any(result_node, "LineItem")
            
            for li in line_items:
                # We assume 1 line item per request based on usage
                li_status_node = find_any(li, "Status")
                li_status = li_status_node.get("Status") if li_status_node is not None else "Unknown"
                li_code = li_status_node.get("Code") if li_status_node is not None else None
                li_message = li_status_node.get("Message") if li_status_node is not None else ""
                
                # Check for Action/ReferenceID (Packing Slip ID)
                action_node = find_any(li, "Action")
                if action_node is not None:
                    ref_id = action_node.get("ReferenceId") or action_node.get("ReferenceID")
                    
                    if ref_id:
                        # If we have an ID, we consider it a success or recovered state
                        return {
                            "success": True,
                            "status": "SUCCESS" if li_status in ["ProcessComplete", "ProcessSuccess"] else li_status,
                            "message": li_message,
                            "packing_slip_id": ref_id,
                            "code": li_code
                        }
                
                # If no Action/ID found, check if it was a failure
                if li_status not in ["ProcessComplete", "ProcessSuccess", "ProcessDone"]:
                     return {
                        "success": False,
                        "status": "API_FAILURE",
                        "message": f"{li_status}: {li_message}",
                        "packing_slip_id": None,
                        "code": li_code
                    }

            # If we got here, maybe top level was success but no line items found?
            return {
                "success": False,
                "status": "NO_DATA",
                "message": "Top level success but no line item info found",
                "packing_slip_id": None,
                "code": None
            }

        # Fallback to legacy "Action" search if Result node missing
        action_node = find_any(root, "Action")
        if action_node is not None:
            # Legacy/Fallback logic
            status = action_node.get("Status")
            code = action_node.get("Code")
            message = action_node.get("Message")
            ref_id = action_node.get("ReferenceId")
            
            if status == "ProcessComplete":
                return {
                    "success": True,
                    "status": "SUCCESS",
                    "message": message,
                    "packing_slip_id": ref_id,
                    "code": code
                }
            else:
                return {
                    "success": False,
                    "status": "API_FAILURE",
                    "message": f"{status}: {message}",
                    "packing_slip_id": None,
                    "code": code
                }
        
        return {
            "success": False,
            "status": "PARSE_ERROR",
            "message": "Could not find Result or Action node in response",
            "packing_slip_id": None,
            "code": None
        }

    except Exception as e:
        return {
            "success": False,
            "status": "PARSE_ERROR",
            "message": f"XML Parse Error: {str(e)}",
            "packing_slip_id": None,
            "code": None
        }
