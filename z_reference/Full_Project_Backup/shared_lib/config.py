import os
import yaml
from dotenv import load_dotenv

# Load env immediately upon import? Or explicit init?
# Better to have explicit init or load once.
load_dotenv(os.path.join(os.path.dirname(os.path.dirname(__file__)), '.env'))

def get_env_var(key, default=None):
    return os.getenv(key, default)

def load_yaml_config(config_path=None):
    if config_path is None:
        # Default to ../config/config.yaml
        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        config_path = os.path.join(base_dir, 'config', 'config.yaml')
    
    if not os.path.exists(config_path):
        return {}
    
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)
