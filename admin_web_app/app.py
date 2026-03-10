from flask import Flask
import os
from dotenv import load_dotenv

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
load_dotenv(os.path.join(project_root, '.env'))

app = Flask(__name__)
app.secret_key = 'admin_secret_key_change_in_prod'

from routes.dashboard import dashboard_bp
from routes.api import api_bp
from routes.address_book import address_book_bp
from routes.overdue import overdue_bp
from routes.address_issues import address_issues_bp
from routes.file_downloads import file_downloads_bp
from routes.file_errors import file_errors_bp
from routes.production import production_bp
from routes.cartons import cartons_bp

app.register_blueprint(dashboard_bp)
app.register_blueprint(api_bp)
app.register_blueprint(address_book_bp)
app.register_blueprint(overdue_bp)
app.register_blueprint(address_issues_bp)
app.register_blueprint(file_downloads_bp)
app.register_blueprint(file_errors_bp)
app.register_blueprint(production_bp)
app.register_blueprint(cartons_bp)

if __name__ == '__main__':
    app.run(host='0.0.0.0', debug=False, port=5002)
