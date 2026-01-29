
import sys
import os

# Add root to path to allow shared_lib import
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from shipping_web_app.app import create_app

app = create_app()

if __name__ == '__main__':
    app.run(port=5001, debug=True)
