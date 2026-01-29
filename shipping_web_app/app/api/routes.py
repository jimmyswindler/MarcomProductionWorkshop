
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

@api_bp.route('/shipment/process', methods=['POST'])
def process_shipment():
    data = request.json
    orders = data.get('orders', [])
    scanned = data.get('scanned_boxes', [])
    pkgs = data.get('package_list', [])
    
    result, status = shipment_service.process_shipment_logic(orders, scanned, pkgs)
    return jsonify(result), status
