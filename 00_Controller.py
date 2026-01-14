# 00_Controller.py
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
import re
import utils_ui  # <--- New UI Utility

# --- Helper: Strip ANSI Codes ---
def strip_ansi(text):
    """Removes ANSI escape sequences from text."""
    ansi_escape = re.compile(r'\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])')
    return ansi_escape.sub('', text)

# --- Configuration Loading ---
def load_config(config_path="config.yaml"):
    utils_ui.print_info("Loading configuration...")
    if not os.path.exists(config_path):
        utils_ui.print_error(f"FATAL ERROR: Configuration file not found at '{config_path}'")
        sys.exit(1)
    try:
        with open(config_path, 'r') as f: config = yaml.safe_load(f)
        utils_ui.print_success("Central configuration loaded successfully.")
        return config
    except yaml.YAMLError as e: 
        utils_ui.print_error(f"FATAL ERROR: Could not parse YAML file: {e}")
        sys.exit(1)
    except Exception as e: 
        utils_ui.print_error(f"FATAL ERROR: An unexpected error occurred while loading the config: {e}")
        sys.exit(1)

# --- Logging Setup ---
def setup_controller_logging(log_dir):
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, f"controller_log_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.txt")
    
    # Use the unified logging setup
    utils_ui.setup_logging(log_file)
    logging.info(f"Controller logging initialized. Log file: {log_file}")

# --- Execution Engine ---
def run_script(script_path, args=None):
    """
    Executes a script and streams stdout in real-time.
    """
    if not os.path.exists(script_path): 
        raise FileNotFoundError(f"Script not found: {script_path}")
    
    command = [sys.executable, script_path]
    if args: 
        command.extend(args)
        
    script_name = os.path.basename(script_path)
    
    # We use logging.info for the file log, but rely on the child script's own output
    # for the console display to avoid double-printing if possible.
    # However, since we are capturing stdout pipe, we MUST print it here to show it.
    
    logging.info(f"--- RUNNING SCRIPT: {script_name} ---")
    logging.info(f"Command: {' '.join(command)}")

    try:
        # We pass env to force unbuffered output if possible, or force color
        env = os.environ.copy()
        env["PYTHONUNBUFFERED"] = "1"
        env["FORCE_COLOR"] = "1" # Hint for rich to be colorful even if piped

        process = subprocess.Popen(command, 
                                   stdout=subprocess.PIPE, 
                                   stderr=subprocess.STDOUT, 
                                   text=True, 
                                   encoding='utf-8', 
                                   bufsize=1,
                                   env=env) 

        logging.info(f"--- Real-time Output from {script_name} ---")
        
        # Read line by line
        for line in process.stdout:
            line = line.rstrip()
            # Print to console directly (utils_ui handles rich printing if enabled)
            # We don't use print_info here to avoid adding "ℹ" to every line of child output
            if utils_ui.console:
                from rich.text import Text
                utils_ui.console.print(Text.from_ansi(line))
            else:
                print(line)
            
            # Also log to file - BUT STRIP ANSI CODES FIRST
            logging.info(strip_ansi(line))
        
        process.wait() 
        logging.info(f"--- End of Output from {script_name} ---")

        if process.returncode != 0:
            utils_ui.print_error(f"FATAL ERROR in {script_name}")
            logging.error(f"Return Code: {process.returncode}")
            raise Exception(f"Script {script_name} failed with exit code {process.returncode}.")
        
        utils_ui.print_success(f"{script_name} completed.")
        return True
        
    except FileNotFoundError: 
        utils_ui.print_error(f"Could not find Python executable or script: {script_path}")
        raise
    except Exception as e: 
        utils_ui.print_error(f"An unexpected error occurred while trying to run {script_name}: {e}")
        logging.error(traceback.format_exc())
        raise

# --- Main Workflow ---
def main_workflow():
    """Orchestrates the entire multi-stage workflow."""
    
    # Initial logging before file setup (goes to stdout only)
    utils_ui.setup_logging(None)
    utils_ui.print_banner("Marcom Production Suite", "Automated Workflow Controller")

    config = load_config()
    paths = config.get('paths', {}); script_paths = paths.get('scripts', {})
    
    stage1_paths = paths.get('stage1_collect', {})
    
    dynamic_build_root = paths.get('dynamic_build_root')
    if not dynamic_build_root:
        utils_ui.print_error("'dynamic_build_root' path missing in config.yaml.")
        sys.exit(1)

    if not all([script_paths, stage1_paths]):
        utils_ui.print_error("Required path sections missing in config.yaml.")
        sys.exit(1)

    # --- Check for sort script ---
    if 'sort' not in script_paths or 'bundle' not in script_paths:
        utils_ui.print_error("'paths.scripts.sort' or 'paths.scripts.bundle' missing in config.yaml.")
        sys.exit(1)

    # --- Ensure Stage 1 directories exist ---
    s1_staging_dir = stage1_paths.get('staging_dir') 
    if not s1_staging_dir:
        utils_ui.print_error("'stage1_collect.staging_dir' missing in config.yaml.")
        sys.exit(1)
        
    dirs_to_create = { stage1_paths.get('input_dir'), s1_staging_dir, }
    for d in dirs_to_create:
        if d:
             try: 
                 os.makedirs(d, exist_ok=True)
                 # utils_ui.print_info(f"Ensured static directory exists: {d}") 
             except Exception as e: 
                 utils_ui.print_error(f"Could not create static directory {d}: {e}")
                 sys.exit(1)
        else: 
            utils_ui.print_warning("A Stage 1 directory path is missing in config.yaml.")

    consolidated_report_path = None; bundled_report_path = None; fragmentation_map = {}
    original_source_files_staging_map = {} 

    try:
        # --- Stage 1: Data Collection ---
        utils_ui.print_section("Stage 1: Data Collection")
        s1_input_dir = stage1_paths.get('input_dir')
        
        source_base_names = config.get('stage1_source_files', {})
        # if len(source_base_names) != 3: raise ValueError("Config must define 'stage1_source_files' with 3 keys.")
        
        file_paths_map = {}
        file_paths_map = {}
        for key, base_name in source_base_names.items():
            # Determine extension based on key or config (simple heuristic: if key has 'xml', use .xml)
            ext = ".xml" if "xml" in key.lower() else ".xlsx"
            
            pattern = os.path.join(s1_input_dir, f"{base_name}*{ext}")
            found = glob.glob(pattern)
            
            if not found: 
                raise FileNotFoundError(f"Stage 1 input file(s) starting with '{base_name}' ({ext}) not found in {s1_input_dir}")
            
            # Sort files to ensure chronological processing (relying on timestamp in filename)
            found.sort()
            
            # Store as LIST of paths
            file_paths_map[key] = found
            
            utils_ui.print_info(f"Found source '{key}': {len(found)} file(s)")
            for f in found:
                utils_ui.print_info(f"  - {os.path.basename(f)}")
                original_source_files_staging_map[os.path.basename(f)] = os.path.join(s1_staging_dir, os.path.basename(f))

        remapping_map = config.get('product_id_remapping', {})
        s1_args = [s1_staging_dir, json.dumps(file_paths_map), json.dumps(remapping_map)]
        run_script(script_paths['collect'], s1_args) 
        
        consolidated_reports = glob.glob(os.path.join(s1_staging_dir, 'MarcomOrderDate*.xlsx'))
        if not consolidated_reports: raise FileNotFoundError(f"No consolidated report (MarcomOrderDate*.xlsx) found in Stage 1 output: {s1_staging_dir}")
        consolidated_reports.sort(key=os.path.getmtime, reverse=True)
        consolidated_report_path = consolidated_reports[0]
        utils_ui.print_success(f"Found consolidated report: {os.path.basename(consolidated_report_path)}")

        # --- Stage 1.5: Database Ingest ---
        if 'ingest' in script_paths:
            utils_ui.print_section("Stage 1.5: Database Ingest")
            s15_args = [s1_staging_dir]
            run_script(script_paths['ingest'], s15_args)

        # --- Dynamic Path Generation ---
        utils_ui.print_info("Setting up dynamic job folders...")
        dynamic_base_name = os.path.splitext(os.path.basename(consolidated_report_path))[0]
        dynamic_job_folder = os.path.join(dynamic_build_root, dynamic_base_name)
        # utils_ui.print_info(f"Dynamic job root: {dynamic_job_folder}")

        # --- Load dynamic job structure from config ---
        job_structure = config.get('paths', {}).get('dynamic_job_structure', {})
        if not job_structure:
            utils_ui.print_error("Config missing 'paths.dynamic_job_structure'. Cannot proceed.")
            sys.exit(1)

        # Build paths using config values
        job_tickets_dir = os.path.join(dynamic_job_folder, job_structure.get('job_tickets', '_JobTickets_DEFAULT'))
        data_files_logs_dir = os.path.join(dynamic_job_folder, job_structure.get('data_logs', '_DataFiles_LogFiles_DEFAULT'))
        workup_dir = os.path.join(dynamic_job_folder, job_structure.get('workup', 'WorkUp_DEFAULT'))
        oneup_files_dir = os.path.join(workup_dir, job_structure.get('one_up_files', 'OneUpFiles_DEFAULT'))
        originals_dir = os.path.join(workup_dir, job_structure.get('originals', 'Originals_DEFAULT'))
        production_imposed_dir = os.path.join(dynamic_job_folder, job_structure.get('production_imposed', 'ProductionImposed_DEFAULT'))
        production_imposed_subfolders = job_structure.get('imposed_subfolders', [])

        controller_log_dir = data_files_logs_dir 
        
        # --- Setup Logging (To File Now) ---
        setup_controller_logging(controller_log_dir)
        logging.info(f"Dynamic job root set to: {dynamic_job_folder}")

        # --- Create dynamic directories ---
        new_dirs_to_create = {
            job_tickets_dir, data_files_logs_dir, production_imposed_dir,
            workup_dir, oneup_files_dir, originals_dir,
        }
        for subfolder in production_imposed_subfolders:
            new_dirs_to_create.add(os.path.join(production_imposed_dir, subfolder))
        
        for d in new_dirs_to_create:
            if d:
                 try: os.makedirs(d, exist_ok=True)
                 except Exception as e: 
                     utils_ui.print_error(f"Could not create dynamic directory {d}: {e}")
                     sys.exit(1)

        
        # --- Move consolidated report ---
        try:
            final_consolidated_path = os.path.join(data_files_logs_dir, os.path.basename(consolidated_report_path))
            if os.path.exists(consolidated_report_path):
                shutil.move(consolidated_report_path, final_consolidated_path)
                consolidated_report_path = final_consolidated_path
                utils_ui.print_info(f"Moved report to Data/Logs: {os.path.basename(final_consolidated_path)}")
            else:
                raise FileNotFoundError(f"Consolidated report not found in staging: {consolidated_report_path}")
        except Exception as move_err:
            utils_ui.print_error(f"FATAL: Could not move consolidated report: {move_err}")
            raise 

        # --- Rename Block ---
        try:
            base, ext = os.path.splitext(consolidated_report_path)
            new_report_path = f"{base}_UNSORTED{ext}"
            shutil.move(consolidated_report_path, new_report_path)
            # utils_ui.print_info(f"Renamed original report to: {os.path.basename(new_report_path)}")
            consolidated_report_path_unsorted = new_report_path 
        except Exception as rename_err:
            logging.error(f"Could not rename consolidated report: {rename_err}")
            utils_ui.print_warning("Could not rename report (non-fatal)")


        # --- Stage 2: Data Sorting and Bundling ---
        
        # --- Stage 2a: Data Sorting ---
        utils_ui.print_section("Stage 2a: Data Sorting")
        config_file_path = "config.yaml"
        s2a_args = [ consolidated_report_path_unsorted, data_files_logs_dir, config_file_path ]
        run_script(script_paths['sort'], s2a_args) # Calls 20a_DataSorter.py

        # --- Handoff 2a -> 2b ---
        categorized_reports = glob.glob(os.path.join(data_files_logs_dir, '*_CATEGORIZED.xlsx'))
        if not categorized_reports: 
            raise FileNotFoundError(f"No categorized report found in: {data_files_logs_dir}")
        categorized_reports.sort(key=os.path.getmtime, reverse=True)
        categorized_report_path = categorized_reports[0]
        # utils_ui.print_success(f"Categorized file: {os.path.basename(categorized_report_path)}")

        # --- Stage 2b: Data Bundling ---
        utils_ui.print_section("Stage 2b: Data Bundling")
        s2b_args = [ categorized_report_path, data_files_logs_dir, config_file_path ]
        run_script(script_paths['bundle'], s2b_args) # Calls 20b_DataBundler.py
        
        # --- Move original source files ---
        utils_ui.print_info("Archiving source files...")
# No changes needed here, logic is compatible.
        
        # --- Handoff 2b -> 2c ---
        bundled_reports = glob.glob(os.path.join(data_files_logs_dir, 'MarcomOrderDate*.xlsx'))
        bundled_reports = [f for f in bundled_reports if "_UNSORTED" not in f and "_CATEGORIZED" not in f]
        if not bundled_reports: raise FileNotFoundError(f"No FINAL bundled report found in: {data_files_logs_dir}")
        
        bundled_reports.sort(key=os.path.getmtime, reverse=True)
        bundled_report_path = bundled_reports[0]
        utils_ui.print_success(f"FINAL Bundled Report: {os.path.basename(bundled_report_path)}")

        frag_map_json_path = bundled_report_path.replace('.xlsx', '_fragmap.json')
        if os.path.exists(frag_map_json_path):
             try:
                 with open(frag_map_json_path, 'r') as f_frag: fragmentation_map = json.load(f_frag)
             except Exception:
                 fragmentation_map = {}
        else:
             fragmentation_map = {}

        # --- Stage 2c: PDF Runlist Generation ---
        utils_ui.print_section("Stage 2c: PDF Runlist Generation")
        s2c_args = [bundled_report_path, dynamic_job_folder, json.dumps(config), json.dumps(fragmentation_map)]
        run_script(script_paths['pdfgen'], s2c_args)

        # --- Stage 3a.1: Acquire Job Assets ---
        utils_ui.print_section("Stage 3a.1: Acquire Job Assets")
        s3a1_args = [bundled_report_path, oneup_files_dir]
        run_script(script_paths['acquire_assets'], s3a1_args)

        # --- Stage 3a.2: Generate Job Tickets ---
        utils_ui.print_section("Stage 3a.2: Generate Job Tickets")
        s3a_config_subset = {
            'WATERMARK_PATH': paths.get('watermark_path'),
            'CALIBRI_LIGHT_PATH': paths.get('calibri_light_font_path'),
            'CALIBRI_BOLD_PATH': paths.get('calibri_bold_font_path')
        }
        
        s3a2_args = [
            bundled_report_path, 
            oneup_files_dir,
            job_tickets_dir,
            json.dumps(s3a_config_subset) 
        ]
        run_script(script_paths['generate_tickets'], s3a2_args)
        
        # --- Stage 3b: Prepare Press Files ---
        utils_ui.print_section("Stage 3b: Prepare Press Files")
        s3_files_dir = oneup_files_dir
        s3_originals_dir = originals_dir
        
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
        s3b_config_subset['icon_file_paths'] = icon_file_paths
        s3b_config_subset['COLOR_PALETTE_PATH'] = paths.get('color_palette_path')
        s3b_config_subset['HALF_BOX_ICON_PATH'] = paths.get('half_box_icon_path')
        s3b_config_subset['FULL_BOX_ICON_PATH'] = paths.get('full_box_icon_path')

        s3b_args = [bundled_report_path, s3_files_dir, s3_originals_dir, json.dumps(s3b_config_subset)]
        run_script(script_paths['pressprep'], s3b_args)

        # --- Handoff 3 -> 4: Find Gang Run Folders ---
        gang_run_folders = [] 
        if os.path.exists(s3_files_dir):
            gang_run_folders = [
                os.path.join(s3_files_dir, f)
                for f in os.listdir(s3_files_dir)
                if '-GR-' in f and os.path.isdir(os.path.join(s3_files_dir, f))
            ]
        
        # --- Stage 4: Imposition (Loop) ---
        if not gang_run_folders:
            utils_ui.print_info("No Gang Run folders found. Skipping Imposition.")
        else:
            utils_ui.print_section("Stage 4: Imposition")
            s4_config_subset = {
                 'imposition_profile': paths.get('imposition_profile_path'),
                 'marks_template': paths.get('marks_template_path')
            }
            s4_output_dir = os.path.join(production_imposed_dir, "Gang") 
            
            for batch_folder in gang_run_folders:
                utils_ui.print_info(f"Imposing batch: {os.path.basename(batch_folder)}")
                s4_args = [batch_folder, s4_output_dir, json.dumps(s4_config_subset)]
                run_script(script_paths['impose'], s4_args)

        # --- Stage 5: Send Email Notification ---
        utils_ui.print_section("Stage 5: Email Notification")
        try:
            email_script_path = script_paths.get('email')
            if not email_script_path:
                utils_ui.print_warning("Email script path not defined. Skipping.")
            else:
                original_dynamic_base_name = os.path.splitext(os.path.basename(bundled_report_path))[0]
                pdf_runlist_path = os.path.join(dynamic_job_folder, f"{original_dynamic_base_name}_RunLists.pdf")
                config_file_path = "config.yaml" 
                s5_args = [
                    original_dynamic_base_name, bundled_report_path, pdf_runlist_path,
                    oneup_files_dir, job_tickets_dir, config_file_path
                ]
                run_script(email_script_path, s5_args)

        except Exception as email_err:
            utils_ui.print_error(f"Email notification FAILED: {email_err}")
            logging.error(traceback.format_exc())
        
        # --- Workflow Complete ---
        utils_ui.print_banner("Workflow Complete", f"All files in: {dynamic_job_folder}")
        logging.info("--- [ WORKFLOW COMPLETE ] ---")

    except (FileNotFoundError, FileExistsError, ValueError) as config_err:
         utils_ui.print_banner("Workflow Failed", "Configuration or File Error")
         utils_ui.print_error(str(config_err))
         logging.critical(traceback.format_exc())
         sys.exit(1) 
    except Exception as e:
        utils_ui.print_banner("Workflow Failed", "Unexpected Error")
        utils_ui.print_error(str(e))
        logging.critical(traceback.format_exc())
        sys.exit(1) 

if __name__ == "__main__":
    try:
        main_workflow()
    except KeyboardInterrupt:
        utils_ui.print_warning("Workflow interrupted by user.")
    
    if sys.platform == "win32":
        input("Press Enter to exit...")
