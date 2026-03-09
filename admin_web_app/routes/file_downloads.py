from flask import Blueprint

file_downloads_bp = Blueprint('file_downloads', __name__)

# Future Expansion
# This Blueprint will handle:
# - File Download Issues queue.
# - Marking associated Jobs as `FAILED_PREFLIGHT` (retaining DB record but disqualifying from production).

@file_downloads_bp.route('/file-downloads')
def file_downloads():
    return "File Downloads View - Coming Soon"
