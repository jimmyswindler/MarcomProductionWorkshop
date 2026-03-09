from flask import Blueprint

file_errors_bp = Blueprint('file_errors', __name__)

# Future Expansion
# This Blueprint will handle:
# - File Error Issues queue (e.g., PDF stream errors, image errors).
# - Marking associated Jobs as `FAILED_PREFLIGHT`.

@file_errors_bp.route('/file-errors')
def file_errors():
    return "File Errors View - Coming Soon"
