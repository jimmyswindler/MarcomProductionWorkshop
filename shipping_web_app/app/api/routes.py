
from flask import Blueprint, jsonify, request
from ..services import order_service, shipment_service

api_bp = Blueprint('api', __name__)

@api_bp.route('/order/<string:lookup_id>', methods=['GET'])
def get_job(lookup_id):
    data, error = order_service.get_job_details(lookup_id)
    if error:
        status = 404 if "not found" in error.lower() else 500
        return jsonify({"error": error}), status
    return jsonify(data)

@api_bp.route('/cartons', methods=['GET'])
def get_cartons():
    mapping, error = shipment_service.get_shipping_cartons()
    if error: return jsonify({"error": error}), 500
    return jsonify(mapping)

@api_bp.route('/order/search', methods=['GET'])
def search_orders():
    query = request.args.get('q', '')
    if not query: return jsonify([])
    
    results, error = order_service.search_orders(query)
    if error: return jsonify({"error": error}), 500
    
    return jsonify(results)

@api_bp.route('/shipment/process', methods=['POST'])
def process_shipment():
    data = request.json
    orders = data.get('orders', [])
    scanned = data.get('scanned_barcodes', []) or data.get('scanned_boxes', [])
    pkgs = data.get('package_list', [])
    
    result, status = shipment_service.process_shipment_logic(orders, scanned, pkgs)
    return jsonify(result), status


@api_bp.route('/activity_feed', methods=['GET'])
def get_feed():
    try:
        from ..services import feedback_loop
        
        # 1. Process output files (ALWAYS run this to catch live files too)
        feedback_loop.process_ups_output_files()

    except Exception as e:
        print(f"Feed Processing Error: {e}")

    data, error = shipment_service.get_recent_shipments()
    if error: return jsonify({"error": error}), 500
    return jsonify(data)



@api_bp.route('/order/compare', methods=['POST'])
def compare_order():
    data = request.json
    current_addr = data.get('current_address')
    new_id = data.get('new_order_id')
    
    result, error = order_service.compare_addresses(current_addr, new_id)
    if error: return jsonify({"error": error}), 500
    return jsonify(result)


@api_bp.route('/status', methods=['GET'])
def get_system_status():
    import os
    import requests
    from shared_lib.database import get_db_connection
    from shared_lib.config import get_env_var

    status = {
        "db": False,
        "marcom": False,
        "ups_folder": False,
        "ups_auto_import": False
    }

    # 1. DB Check
    try:
        conn = get_db_connection()
        if conn:
            cur = conn.cursor()
            cur.execute("SELECT 1")
            cur.fetchone()
            conn.close()
            status["db"] = True
    except:
        pass

    # 2. Marcom API Check
    try:
        url = get_env_var("MARCOM_API_URL", "https://services.printable.com/trans/1.0/PackingSlip.asmx")
        # Just check connectivity with a fast timeout
        # Using verify=False to match legacy settings, though risky in prod
        requests.get(url, timeout=5, verify=False)
        # 405 or 200 or 500 means server is reachable. ConnectionError means not reachable.
        status["marcom"] = True
    except:
        pass

    # 3. UPS Worldship Folder and Lock File
    # /Volumes/XML Auto Import/WSXMLAIFOLDERLOCK.dat
    folder_path = "/Volumes/XML Auto Import"
    if os.path.exists(folder_path) and os.path.isdir(folder_path):
        status["ups_folder"] = True
        
    lock_file = os.path.join(folder_path, "WSXMLAIFOLDERLOCK.dat")
    if os.path.exists(lock_file):
        status["ups_auto_import"] = True
    
    return jsonify(status)

