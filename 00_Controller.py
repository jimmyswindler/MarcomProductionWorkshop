# 00_Controller.py (MODIFIED)
import os
import sys
import subprocess
import glob
import shutil
import datetime
import yaml
import logging
import traceback
import json

# --- Configuration Loading (Unchanged) ---
def load_config(config_path="config.yaml"):
    # ... (no changes needed) ...
    if not os.path.exists(config_path):
        print(f"FATAL ERROR: Configuration file not found at '{config_path}'")
        sys.exit(1)
    try:
        with open(config_path, 'r') as f: config = yaml.safe_load(f)
        print("✓ Central configuration loaded successfully.")
        return config
    except yaml.YAMLError as e: print(f"FATAL ERROR: Could not parse YAML file: {e}"); sys.exit(1)
    except Exception as e: print(f"FATAL ERROR: An unexpected error occurred while loading the config: {e}"); sys.exit(1)

# --- Logging Setup (Unchanged) ---
def setup_controller_logging(log_dir):
    # ... (no changes needed) ...
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, f"controller_log_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.txt")
    logger = logging.getLogger()
    for handler in logger.handlers[:]:
        handler.close()
        logger.removeHandler(handler)
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - CONTROLLER - %(message)s',
                        datefmt='%Y-%m-%d %H:%M%S', handlers=[logging.FileHandler(log_file), logging.StreamHandler(sys.stdout)], force=True)
    logging.info(f"Controller logging initialized. Log file: {log_file}")

# --- Execution Engine (Unchanged) ---
def run_script(script_path, args=None):
    """
    MODIFIED: This function now streams stdout in real-time.
    It uses subprocess.Popen instead of subprocess.run to read
    the output line by line as it is generated.
    """
    if not os.path.exists(script_path): 
        raise FileNotFoundError(f"Script not found: {script_path}")
    
    command = [sys.executable, script_path]
    if args: 
        command.extend(args)
        
    script_name = os.path.basename(script_path)
    logging.info(f"--- RUNNING SCRIPT: {script_name} ---"); 
    logging.info(f"Command: {' '.join(command)}")

    try:
        process = subprocess.Popen(command, 
                                   stdout=subprocess.PIPE, 
                                   stderr=subprocess.STDOUT, 
                                   text=True, 
                                   encoding='utf-8', 
                                   bufsize=1) 

        logging.info(f"--- Real-time Output from {script_name} ---")
        
        for line in process.stdout:
            logging.info(line.strip())
        
        process.wait() 
        logging.info(f"--- End of Output from {script_name} ---")

        if process.returncode != 0:
            logging.error(f"[!] FATAL ERROR in {script_name} [!]"); 
            logging.error(f"Return Code: {process.returncode}")
            raise Exception(f"Script {script_name} failed with exit code {process.returncode}.")
        
        logging.info(f"--- SUCCESS: {script_name} completed. ---")
        return True
        
    except FileNotFoundError: 
        logging.error(f"Could not find Python executable or script: {script_path}"); 
        raise
    except Exception as e: 
        logging.error(f"An unexpected error occurred while trying to run {script_name}: {e}"); 
        logging.error(traceback.format_exc()); 
        raise

# --- Main Workflow (MODIFIED) ---
def main_workflow():
    """Orchestrates the entire multi-stage workflow."""

    config = load_config()
    paths = config.get('paths', {}); script_paths = paths.get('scripts', {})
    
    stage1_paths = paths.get('stage1_collect', {})
    
    dynamic_build_root = paths.get('dynamic_build_root')
    if not dynamic_build_root:
        print("FATAL ERROR: 'dynamic_build_root' path missing in config.yaml."); sys.exit(1)

    if not all([script_paths, stage1_paths]):
        print("FATAL ERROR: Required path sections missing in config.yaml."); sys.exit(1)

    # --- MODIFIED: Added check for new 'sort' script ---
    if 'sort' not in script_paths or 'bundle' not in script_paths:
        print("FATAL ERROR: 'paths.scripts.sort' or 'paths.scripts.bundle' missing in config.yaml."); sys.exit(1)

    # --- Ensure Stage 1 directories exist ---
    s1_staging_dir = stage1_paths.get('staging_dir') 
    if not s1_staging_dir:
        print("FATAL ERROR: 'stage1_collect.staging_dir' missing in config.yaml."); sys.exit(1)
    dirs_to_create = { stage1_paths.get('input_dir'), s1_staging_dir, }
    for d in dirs_to_create:
        if d:
             try: os.makedirs(d, exist_ok=True); print(f"Ensured static directory exists: {d}") 
             except Exception as e: print(f"Could not create static directory {d}: {e}"); sys.exit(1)
        else: print("A Stage 1 directory path is missing in config.yaml.")

    consolidated_report_path = None; bundled_report_path = None; fragmentation_map = {}
    original_source_files_staging_map = {} 

    try:
        # --- Stage 1: Data Collection (Unchanged) ---
        print("Starting Stage 1: Data Collection") 
        s1_input_dir = stage1_paths.get('input_dir')
        
        source_base_names = config.get('stage1_source_files', {})
        if len(source_base_names) != 3: raise ValueError("Config must define 'stage1_source_files' with 3 keys.")
        file_paths_map = {}
        for key, base_name in source_base_names.items():
            found = glob.glob(os.path.join(s1_input_dir, f"{base_name}*.xlsx"))
            if not found: raise FileNotFoundError(f"Stage 1 input file starting with '{base_name}' not found in {s1_input_dir}")
            if len(found) > 1: raise FileExistsError(f"Multiple files starting with '{base_name}' found in {s1_input_dir}")
            file_paths_map[key] = found[0]; print(f"Found source file for '{key}': {os.path.basename(found[0])}")
            original_source_files_staging_map[os.path.basename(found[0])] = os.path.join(s1_staging_dir, os.path.basename(found[0]))

        print(f"Running Stage 1. All outputs will go to: {s1_staging_dir}")
        s1_args = [s1_staging_dir, json.dumps(file_paths_map)]
        run_script(script_paths['collect'], s1_args) 
        
        consolidated_reports = glob.glob(os.path.join(s1_staging_dir, 'MarcomOrderDate*.xlsx'))
        if not consolidated_reports: raise FileNotFoundError(f"No consolidated report (MarcomOrderDate*.xlsx) found in Stage 1 output: {s1_staging_dir}")
        consolidated_reports.sort(key=os.path.getmtime, reverse=True)
        consolidated_report_path = consolidated_reports[0]; print(f"Found consolidated report in staging: {os.path.basename(consolidated_report_path)}")

        # --- DYNAMIC PATH GENERATION (MODIFIED) ---
        print("--- Creating dynamic job folders ---")
        dynamic_base_name = os.path.splitext(os.path.basename(consolidated_report_path))[0]
        dynamic_job_folder = os.path.join(dynamic_build_root, dynamic_base_name)
        print(f"Dynamic job root set to: {dynamic_job_folder}")

        # --- NEW: Load dynamic job structure from config ---
        job_structure = config.get('paths', {}).get('dynamic_job_structure', {})
        if not job_structure:
            print("FATAL ERROR: Config missing 'paths.dynamic_job_structure'. Cannot proceed.")
            sys.exit(1)
        print("✓ Loaded dynamic folder structure from config.")

        # Build paths using config values (with fallbacks just in case)
        job_tickets_dir = os.path.join(dynamic_job_folder, job_structure.get('job_tickets', '_JobTickets_DEFAULT'))
        data_files_logs_dir = os.path.join(dynamic_job_folder, job_structure.get('data_logs', '_DataFiles_LogFiles_DEFAULT'))
        workup_dir = os.path.join(dynamic_job_folder, job_structure.get('workup', 'WorkUp_DEFAULT'))
        oneup_files_dir = os.path.join(workup_dir, job_structure.get('one_up_files', 'OneUpFiles_DEFAULT'))
        originals_dir = os.path.join(workup_dir, job_structure.get('originals', 'Originals_DEFAULT'))
        production_imposed_dir = os.path.join(dynamic_job_folder, job_structure.get('production_imposed', 'ProductionImposed_DEFAULT'))
        production_imposed_subfolders = job_structure.get('imposed_subfolders', [])
        # --- END NEW BLOCK ---

        controller_log_dir = data_files_logs_dir # Controller log goes into the (now dynamic) data/logs dir
        
        # --- Setup Logging (Unchanged) ---
        setup_controller_logging(controller_log_dir)
        logging.info(f"Dynamic job root set to: {dynamic_job_folder}")
        logging.info(f"Logging re-initialized in dynamic job folder: {controller_log_dir}")

        # --- Create dynamic directories (MODIFIED) ---
        # Paths are now based on the variables built from the config
        new_dirs_to_create = {
            job_tickets_dir, data_files_logs_dir, production_imposed_dir,
            workup_dir, oneup_files_dir, originals_dir,
        }
        for subfolder in production_imposed_subfolders:
            new_dirs_to_create.add(os.path.join(production_imposed_dir, subfolder))
        
        for d in new_dirs_to_create:
            if d:
                 try: os.makedirs(d, exist_ok=True); logging.info(f"Ensured dynamic directory exists: {d}")
                 except Exception as e: logging.error(f"Could not create dynamic directory {d}: {e}"); sys.exit(1)
        # --- END MODIFIED BLOCK ---
        
        # --- Move consolidated report (Unchanged) ---
        logging.info(f"Moving consolidated report from '{s1_staging_dir}' to '{data_files_logs_dir}'")
        try:
            final_consolidated_path = os.path.join(data_files_logs_dir, os.path.basename(consolidated_report_path))
            if os.path.exists(consolidated_report_path):
                shutil.move(consolidated_report_path, final_consolidated_path)
                consolidated_report_path = final_consolidated_path
                logging.info(f"Moved consolidated report to: {final_consolidated_path}")
            else:
                logging.error(f"FATAL: Consolidated report not found in staging: {consolidated_report_path}")
                raise FileNotFoundError(f"Consolidated report not found in staging: {consolidated_report_path}")
        except Exception as move_err:
            logging.error(f"FATAL: Could not move consolidated report to {data_files_logs_dir}: {move_err}")
            raise 

        # --- RENAME BLOCK (Unchanged) ---
        try:
            base, ext = os.path.splitext(consolidated_report_path)
            new_report_path = f"{base}_UNSORTED{ext}"
            shutil.move(consolidated_report_path, new_report_path)
            logging.info(f"Renamed original report to: {os.path.basename(new_report_path)}")
            consolidated_report_path_unsorted = new_report_path # IMPORTANT: This is the _UNSORTED file
        except Exception as rename_err:
            logging.error(f"Could not rename consolidated report: {rename_err}. Archiving may fail.")


        # --- MODIFIED: Stage 2 is now split ---
        
        # --- Stage 2a: Data Sorting (Unchanged) ---
        logging.info("Starting Stage 2a: Data Sorting")
        config_file_path = "config.yaml"
        # Args: input_excel_path, output_dir, config_path
        s2a_args = [ consolidated_report_path_unsorted, data_files_logs_dir, config_file_path ]
        run_script(script_paths['sort'], s2a_args) # Calls 20a_DataSorter.py

        # --- Handoff 2a -> 2b: Find the new _CATEGORIZED file (Unchanged) ---
        logging.info("Searching for categorized checkpoint file...")
        categorized_reports = glob.glob(os.path.join(data_files_logs_dir, '*_CATEGORIZED.xlsx'))
        if not categorized_reports: 
            raise FileNotFoundError(f"No categorized report (*_CATEGORIZED.xlsx) found in Stage 2a output: {data_files_logs_dir}")
        categorized_reports.sort(key=os.path.getmtime, reverse=True)
        categorized_report_path = categorized_reports[0]
        logging.info(f"Found categorized file: {os.path.basename(categorized_report_path)}")

        # --- Stage 2b: Data Bundling (Unchanged) ---
        logging.info("Starting Stage 2b: Data Bundling")
        # Args: input_excel_path, output_dir, config_path
        s2b_args = [ categorized_report_path, data_files_logs_dir, config_file_path ]
        run_script(script_paths['bundle'], s2b_args) # Calls 20b_DataBundler.py
        
        # --- End of MODIFIED block ---


        # --- NEW: Move original source files (Unchanged) ---
        logging.info(f"Moving {len(original_source_files_staging_map)} original source files from '{s1_staging_dir}' to '{data_files_logs_dir}'")
        for filename, staging_path in original_source_files_staging_map.items():
            final_path = os.path.join(data_files_logs_dir, filename) 
            if os.path.exists(staging_path):
                try:
                    shutil.move(staging_path, final_path)
                    logging.info(f"Moved to data/logs folder: {filename}")
                except Exception as move_err:
                    logging.warning(f"Could not move source file {filename} from staging: {move_err}")
            else:
                logging.warning(f"Original source file {filename} not found in staging dir '{s1_staging_dir}' for moving.")
        
        # --- Handoff 2b -> 2c (was 2a -> 2b): Find FINAL Excel + Load Frag Map JSON (Unchanged) ---
        logging.info("Searching for FINAL bundled report...")
        bundled_reports = glob.glob(os.path.join(data_files_logs_dir, 'MarcomOrderDate*.xlsx'))
        # --- MODIFIED: Exclude both _UNSORTED and _CATEGORIZED ---
        bundled_reports = [
            f for f in bundled_reports 
            if "_UNSORTED" not in f and "_CATEGORIZED" not in f
        ]
        if not bundled_reports: raise FileNotFoundError(f"No FINAL bundled report (.xlsx) found in Stage 2b output: {data_files_logs_dir}")
        
        bundled_reports.sort(key=os.path.getmtime, reverse=True)
        bundled_report_path = bundled_reports[0]; logging.info(f"Found FINAL bundled report: {os.path.basename(bundled_report_path)}")

        frag_map_json_path = bundled_report_path.replace('.xlsx', '_fragmap.json')
        if os.path.exists(frag_map_json_path):
             try:
                 with open(frag_map_json_path, 'r') as f_frag: fragmentation_map = json.load(f_frag)
                 logging.info(f"Loaded fragmentation map from: {os.path.basename(frag_map_json_path)}")
             except Exception as json_err:
                 logging.warning(f"Could not load fragmentation map JSON, proceeding with empty map: {json_err}")
                 fragmentation_map = {}
        else:
             logging.warning(f"Fragmentation map JSON not found: {frag_map_json_path}. Proceeding with empty map.")
             fragmentation_map = {}

        # --- Stage 2c: PDF Runlist Generation (was 2b) (Unchanged) ---
        logging.info("Starting Stage 2c: PDF Runlist Generation")
        s2c_args = [bundled_report_path, dynamic_job_folder, json.dumps(config), json.dumps(fragmentation_map)]
        run_script(script_paths['pdfgen'], s2c_args)

        # --- Stage 3a: Generate Job Collateral (Unchanged) ---
        # Note: This script now receives the full paths built from the config
        logging.info("Starting Stage 3a: Generate Job Collateral")
        s3a_config_subset = {'WATERMARK_PATH': paths.get('watermark_path')}
        s3a_args = [
            bundled_report_path, 
            oneup_files_dir,       # <-- Pass config-driven path
            job_tickets_dir,       # <-- Pass config-driven path
            json.dumps(s3a_config_subset) 
        ]
        run_script(script_paths['collateral'], s3a_args)
        
        # --- Stage 3b: Prepare Press Files (Unchanged) ---
        # Note: This script now receives the full paths built from the config
        logging.info("Starting Stage 3b: Prepare Press Files")
        s3_files_dir = oneup_files_dir    # <-- Use config-driven path
        s3_originals_dir = originals_dir # <-- Use config-driven path
        
        s3b_config_subset = {}
        s3b_config_subset['shipping_box_rules'] = config.get('shipping_box_rules', {})
        icon_file_paths = {}
        if s3b_config_subset['shipping_box_rules']:
            icon_keys_needed = set()
            for material, quantities in s3b_config_subset['shipping_box_rules'].items():
                for qty, rules in quantities.items():
                    if 'icon_file' in rules:
                        icon_keys_needed.add(rules['icon_file'])
            for key in icon_keys_needed:
                path_key = key.replace('.pdf', '_path') 
                if path_key in paths:
                    icon_file_paths[key] = paths[path_key]
                else:
                    logging.warning(f"Config Warning: Icon file '{key}' is defined in 'shipping_box_rules' but its path '{path_key}' is not in 'paths'.")
        s3b_config_subset['icon_file_paths'] = icon_file_paths
        s3b_config_subset['COLOR_PALETTE_PATH'] = paths.get('color_palette_path')
        s3b_config_subset['HALF_BOX_ICON_PATH'] = paths.get('half_box_icon_path')
        s3b_config_subset['FULL_BOX_ICON_PATH'] = paths.get('full_box_icon_path')

        s3b_args = [bundled_report_path, s3_files_dir, s3_originals_dir, json.dumps(s3b_config_subset)]
        run_script(script_paths['pressprep'], s3b_args)

        # --- Handoff 3 -> 4: Find Gang Run Folders (Unchanged) ---
        # Note: This now searches inside the config-driven path
        logging.info("Searching for Gang Run folders for Imposition")
        gang_run_folders = [] 
        if os.path.exists(s3_files_dir):
            gang_run_folders = [
                os.path.join(s3_files_dir, f)
                for f in os.listdir(s3_files_dir)
                if '-GR-' in f and os.path.isdir(os.path.join(s3_files_dir, f))
            ]
            logging.info(f"Found {len(gang_run_folders)} Gang Run folders to impose.")
        else:
            logging.warning(f"OneUpFiles directory not found: {s3_files_dir}. Cannot search for Gang Run folders.")


        # --- Stage 4: Imposition (Loop) (Unchanged) ---
        # Note: This now saves to the config-driven path
        if not gang_run_folders:
            logging.info("No Gang Run folders found. Skipping Imposition stage.")
        else:
            logging.info("Starting Stage 4: Imposition")
            s4_config_subset = {
                 'imposition_profile': paths.get('imposition_profile_path'),
                 'marks_template': paths.get('marks_template_path')
            }
            # Find the "Gang" subfolder (as defined in config) inside the imposed dir
            s4_output_dir = os.path.join(production_imposed_dir, "Gang") # Assumes "Gang" is still the name
            
            for batch_folder in gang_run_folders:
                logging.info(f"Imposing batch: {os.path.basename(batch_folder)}")
                s4_args = [batch_folder, s4_output_dir, json.dumps(s4_config_subset)]
                run_script(script_paths['impose'], s4_args)

        # --- Stage 5: Send Email Notification (Unchanged) ---
        # Note: This now uses the config-driven paths to find attachments
        logging.info("Starting Stage 5: Email Notification")
        try:
            email_script_path = script_paths.get('email')
            if not email_script_path:
                logging.warning("Email script path ('paths.scripts.email') not defined in config. Skipping notification.")
            else:
                # Need to find the *original* base name without _UNSORTED
                original_dynamic_base_name = os.path.splitext(os.path.basename(bundled_report_path))[0]
                
                pdf_runlist_path = os.path.join(dynamic_job_folder, f"{original_dynamic_base_name}_RunLists.pdf")
                config_file_path = "config.yaml" 

                s5_args = [
                    original_dynamic_base_name, # Arg 1: dynamic_folder_name
                    bundled_report_path,        # Arg 2: bundled_excel_path
                    pdf_runlist_path,           # Arg 3: runlist_pdf_path
                    oneup_files_dir,            # Arg 4: oneup_files_dir (config-driven)
                    job_tickets_dir,            # Arg 5: job_tickets_dir (config-driven)
                    config_file_path            # Arg 6: config_path
                ]
                
                logging.info(f"Executing email script: {email_script_path}")
                run_script(email_script_path, s5_args)
                logging.info("✓ Email notification script executed.")

        except Exception as email_err:
            logging.warning(f"Email notification FAILED: {email_err}")
            logging.warning(traceback.format_exc())
        
        # --- Workflow Complete (Unchanged) ---
        logging.info("\n\n--- [ WORKFLOW COMPLETE ] ---")
        logging.info("All files are in their final locations.")


    except (FileNotFoundError, FileExistsError, ValueError) as config_err:
         print(f"\n\Video_Title: YouTube Video Title\nVideo_URL: https://www.youtube.com/watch?v=VIDEO_ID\nVideo_Summary: Brief summary of the YouTube video content.\n\n--- [ WORKFLOW FAILED - CONFIGURATION/FILE ERROR ] ---")
         print(f"ERROR: {config_err}"); print(traceback.format_exc())
         sys.exit(1) 
    except Exception as e:
        logging.critical(f"\n\n--- [ WORKFLOW FAILED - UNEXPECTED ERROR ] ---")
        logging.critical(f"ERROR: {e}"); logging.critical(traceback.format_exc())
        sys.exit(1) 

if __name__ == "__main__":
    main_workflow()
    print("\nController script finished.")
    if sys.platform == "win32":
        input("Press Enter to exit...")