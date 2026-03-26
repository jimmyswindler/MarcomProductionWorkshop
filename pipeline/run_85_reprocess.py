import os
import sys
import json
import yaml

# Set up paths
script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(script_dir)
if project_root not in sys.path:
    sys.path.insert(0, project_root)

config_path = os.path.join(project_root, 'config', 'config.yaml')
target_script = os.path.join(project_root, 'pipeline', '85_ImposeSingleJobs_24up.py')

# Load the config to json
with open(config_path, 'r') as f:
    config = yaml.safe_load(f)

config_json = json.dumps(config)

input_excel = '/Volumes/AraxiVolume_Jobs/Jobs/DigimasterProductionNew/MARCOM orders/MarcomOrderDate_2026-03-24/_DataFiles_LogFiles/MarcomOrderDate_2026-03-24 copy.xlsx'
one_up_folder = '/Volumes/AraxiVolume_Jobs/Jobs/DigimasterProductionNew/MARCOM orders/MarcomOrderDate_2026-03-24/WorkUp/OneUpFiles'
output_dir = '/Users/jimmyswindler/Desktop/MarcomProductionWorkshop/Output_Test_Folder'

os.makedirs(output_dir, exist_ok=True)

# Important to pass absolute path, stringified arguments explicitly
# Import the main function from the script to easily call it
import importlib.util
spec = importlib.util.spec_from_file_location("impose_script", target_script)
impose_module = importlib.util.module_from_spec(spec)
sys.modules["impose_script"] = impose_module
spec.loader.exec_module(impose_module)

print("Starting custom reprocessing via 85_ImposeSingleJobs_24up.py ...")
try:
    impose_module.main(input_excel, one_up_folder, output_dir, config_json)
    print("Reprocessing completed successfully.")
except Exception as e:
    print(f"Error occurred during reprocessing: {e}")
