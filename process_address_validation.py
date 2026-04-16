#!/usr/bin/env python3
import sys
import os
import time
import logging
from datetime import datetime
import json
from concurrent.futures import ThreadPoolExecutor

# Setup import path to include project root
project_root = os.path.dirname(os.path.abspath(__file__))
sys.path.append(project_root)

from shared_lib.database import get_db_connection, get_real_dict_cursor
from shared_lib.ups_api import UPSAddressValidator
from shared_lib.config import get_env_var

# Setup Logging
log_dir = os.path.join(project_root, 'LOGS')
os.makedirs(log_dir, exist_ok=True)
logging.basicConfig(
    filename=os.path.join(log_dir, f'validator_{datetime.now().strftime("%Y%m%d")}.log'),
    level=logging.INFO,
    format='%(asctime)s - validator - %(levelname)s - %(message)s'
)
console = logging.StreamHandler()
console.setLevel(logging.INFO)
logging.getLogger('').addHandler(console)

def _get_db_string(val):
    if val is None:
        return ""
    return str(val).strip()

def process_order(order, ups_validator):
    """
    Process a single order validation.
    Returns (order_number, success, exception_details)
    """
    conn = get_db_connection()
    if not conn:
        return order['order_number'], False, "DB connection failed"
    cur = get_real_dict_cursor(conn)
    
    try:
        order_number = order['order_number']
        orig_company = _get_db_string(order.get('ship_to_company'))
        orig_attention = _get_db_string(order.get('ship_to_name'))
        orig_addr1 = _get_db_string(order.get('address1'))
        orig_addr2 = _get_db_string(order.get('address2'))
        orig_addr3 = _get_db_string(order.get('address3'))
        orig_addr4 = _get_db_string(order.get('address4'))
        orig_city = _get_db_string(order.get('city'))
        orig_state = _get_db_string(order.get('state'))
        orig_zip = _get_db_string(order.get('zip'))
        orig_country = _get_db_string(order.get('country')) or 'US'
        
        # Step A: WorldShip Constraint Validation
        violations = []
        if len(orig_company) > 35: violations.append("CompanyOrName: 35 maximum character count exceeded")
        if len(orig_attention) > 35: violations.append("Attention: 35 maximum character count exceeded")
        
        if not orig_addr1: violations.append("Address1: required field missing")
        elif len(orig_addr1) > 35: violations.append("Address1: 35 maximum character count exceeded")
        
        if len(orig_addr2) > 35: violations.append("Address2: 35 maximum character count exceeded")
        if len(orig_addr3) > 35: violations.append("Address3: 35 maximum character count exceeded")
        if orig_addr4: violations.append("Address4: data exists but WorldShip only supports 3 lines. Please condense into Address 1-3.")
        
        if orig_country == 'US' and not orig_city: violations.append("City: required field missing for US")
        if len(orig_city) > 30: violations.append("City: 30 maximum character count exceeded")
        
        VALID_US_STATES = {'AK', 'AL', 'AR', 'AZ', 'CA', 'CO', 'CT', 'DC', 'DE', 'FL', 'GA', 'HI', 'IA', 'ID', 'IL', 'IN', 'KS', 'KY', 'LA', 'MA', 'MD', 'ME', 'MI', 'MN', 'MO', 'MS', 'MT', 'NC', 'ND', 'NE', 'NH', 'NJ', 'NM', 'NV', 'NY', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC', 'SD', 'TN', 'TX', 'UT', 'VA', 'VT', 'WA', 'WI', 'WV', 'WY', 'AA', 'AE', 'AP'}
        US_TERRITORIES = {'PR', 'GU', 'VI', 'AS', 'MP'}
        
        is_pr_zip = orig_zip.startswith(('006', '007', '009'))
        if is_pr_zip:
            if orig_country != 'PR':
                violations.append("Country: Puerto Rico ZIP codes require Country to be 'PR'.")
            if orig_state.upper() != 'PR':
                violations.append("State: Puerto Rico ZIP codes require State to be 'PR'.")
        else:
            if orig_country == 'US':
                if not orig_state:
                    violations.append("State: required field missing for US")
                else:
                    state_upper = orig_state.upper()
                    if state_upper in US_TERRITORIES:
                        violations.append(f"State: US Territories (e.g. {state_upper}) must be input as the 'Country' code, not 'State'.")
                    elif state_upper not in VALID_US_STATES:
                        violations.append(f"State: '{orig_state}' is not a valid US state abbreviation.")
                    
        if orig_state and len(orig_state) > 5: violations.append("State: 5 maximum character count exceeded")
        if len(orig_country) > 50: violations.append("Country: 50 maximum character count exceeded")
        
        if violations:
            violation_details = {"status": "EXCEPTION", "violations": violations, "message": "WorldShip Schema Constraints Failed"}
            update_sql = """
                UPDATE orders SET
                    address_validation_status = 'EXCEPTION',
                    address_validation_details = %s
                WHERE order_number = %s
            """
            cur.execute(update_sql, (json.dumps(violation_details), order_number))
            conn.commit()
            logging.info(f"Order {order_number} flagged as EXCEPTION due to WorldShip constraints.")
            return order_number, False, "WorldShip Constraints Failed"
            
        # Step B: UPS API
        lines = [orig_addr1, orig_addr2, orig_addr3]
        city = orig_city
        state = orig_state
        country = orig_country
        
        ups_res = ups_validator.validate_address(lines, city, state, orig_zip, country)
        ups_status = ups_res.get('status', 'ERROR')
        
        # Step C: UPS Valid
        if ups_status == 'VALID':
            d = ups_res.get('data')
            if d:
                zip_val = f"{d.get('zip')}-{d.get('zip_extension')}" if d.get('zip_extension') else d.get('zip')
                update_sql = """
                    UPDATE orders SET
                        address1 = %s, address2 = %s, address3 = %s,
                        city = %s, state = %s, zip = %s,
                        address_validated = TRUE,
                        address_validation_status = 'VALID',
                        address_validation_details = %s
                    WHERE order_number = %s
                """
                cur.execute(update_sql, (
                    d.get('address1'), d.get('address2'), d.get('address3'),
                    d.get('city'), d.get('state'), zip_val,
                    json.dumps(ups_res), order_number
                ))
                conn.commit()
                logging.info(f"Order {order_number} validated successfully by UPS.")
                return order_number, True, None
                
        # Step D: New Address Book Fallback (Exact Match)
        # We need to extract the first 5 digits of the zip
        orig_zip_5 = orig_zip[:5] if len(orig_zip) >= 5 else orig_zip
        
        exact_book_sql = """
            SELECT * FROM address_book 
            WHERE upper(address1) = upper(%s) 
            AND substring(zip FOR 5) = substring(%s FOR 5)
        """
        cur.execute(exact_book_sql, (orig_addr1, orig_zip_5))
        book_match = cur.fetchone()
        
        if book_match:
            msg = {'msg': f"Auto-corrected using Exact Address Book Match (Store {book_match['store_number']})"}
            update_sql = """
                UPDATE orders SET
                    address1 = %s, address2 = %s, address3 = %s,
                    city = %s, state = %s, zip = %s, country = 'US',
                    address_validated = TRUE,
                    address_validation_status = 'ADDRESS_BOOK_VERIFIED',
                    address_validation_details = %s,
                    store_number = %s
                WHERE order_number = %s
            """
            cur.execute(update_sql, (
                book_match.get('address1'), book_match.get('address2'), book_match.get('address3'),
                book_match.get('city'), book_match.get('state'), book_match.get('zip'),
                json.dumps(msg), book_match['store_number'], order_number
            ))
            conn.commit()
            logging.info(f"Order {order_number} resolved via Exact Address Book Match (Store {book_match['store_number']}).")
            return order_number, True, None

        # Step E: Fails all automated checks -> EXCEPTION
        final_status = ups_status if ups_status not in ('ERROR', 'VALID') else 'EXCEPTION'
        update_sql = """
            UPDATE orders SET
                address_validation_status = %s,
                address_validation_details = %s
            WHERE order_number = %s
        """
        cur.execute(update_sql, (final_status, json.dumps(ups_res), order_number))
        conn.commit()
        logging.info(f"Order {order_number} flagged as {final_status}.")
        return order_number, False, final_status
        
    except Exception as e:
        conn.rollback()
        logging.error(f"Error processing order {order.get('order_number')}: {e}")
        return order.get('order_number'), False, str(e)
    finally:
        cur.close()
        conn.close()

def main():
    conn = get_db_connection()
    if not conn:
        logging.error("Failed to connect to Database. Exiting validator.")
        return
        
    ups_client_id = get_env_var("UPS_CLIENT_ID")
    ups_client_secret = get_env_var("UPS_CLIENT_SECRET")
    
    if not ups_client_id or not ups_client_secret:
        logging.error("UPS Credentials missing. Cannot run validation queue.")
        conn.close()
        return
        
    ups_validator = UPSAddressValidator(ups_client_id, ups_client_secret)
    cur = get_real_dict_cursor(conn)
    
    try:
        # Fetch PENDING orders
        cur.execute("SELECT * FROM orders WHERE address_validation_status = 'PENDING' ORDER BY order_date ASC")
        pending_orders = cur.fetchall()
        
        if not pending_orders:
            # logging.debug("No PENDING validation tasks found.")
            return

        logging.info(f"Found {len(pending_orders)} pending orders. Starting validation...")
        
        success_count = 0
        exception_count = 0
        
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(process_order, order, ups_validator) for order in pending_orders]
            for future in futures:
                try:
                    order_num, success, _ = future.result()
                    if success:
                        success_count += 1
                    else:
                        exception_count += 1
                except Exception as e:
                    logging.error(f"Future error: {e}")
                    exception_count += 1
                    
        logging.info(f"Validation Batch Complete: {success_count} auto-resolved, {exception_count} moved to exceptions.")

    except Exception as e:
        logging.error(f"Error checking pending queue: {e}")
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    main()
