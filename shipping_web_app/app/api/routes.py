
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
    # Trigger Simulation & Feedback Loop "Just In Time" for the demo
    # In production, this would be a background job
    try:
        from ..services import simulation_service, feedback_loop
        simulation_service.simulate_ups_worldship_processing()
        feedback_loop.process_ups_output_files()
        
        simulation_service.simulate_marcom_response()
        feedback_loop.process_marcom_responses()
    except Exception as e:
        print(f"Simulation Trigger Error: {e}")

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
