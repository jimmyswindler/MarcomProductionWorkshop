
from flask import Flask, render_template
from flask_cors import CORS

def create_app():
    app = Flask(__name__, template_folder='templates', static_folder='static')
    CORS(app)
    
    from .api.routes import api_bp
    app.register_blueprint(api_bp, url_prefix='/api')
    
    @app.route('/')
    def index():
        from shared_lib.database import get_db_connection
        conn = get_db_connection()
        cartons = []
        if conn:
            cur = conn.cursor()
            cur.execute("SELECT code, name FROM shipping_cartons ORDER BY code;")
            for row in cur.fetchall():
                cartons.append({'code': row[0], 'name': row[1]})
            cur.close()
            conn.close()
        return render_template('shipping_station.html', cartons=cartons)
    
    return app
