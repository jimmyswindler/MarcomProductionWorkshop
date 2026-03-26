#!/usr/bin/env python3
import time
import os
import sys
import subprocess
import logging
from datetime import datetime, timedelta
import argparse

# Setup import path to include project root
project_root = os.path.dirname(os.path.abspath(__file__))

# Setup Logging
log_dir = os.path.join(project_root, 'LOGS')
os.makedirs(log_dir, exist_ok=True)
logging.basicConfig(
    filename=os.path.join(log_dir, f'daemon_{datetime.now().strftime("%Y%m%d")}.log'),
    level=logging.INFO,
    format='%(asctime)s - daemon - %(levelname)s - %(message)s'
)
console = logging.StreamHandler()
console.setLevel(logging.INFO)
logging.getLogger('').addHandler(console)

def run_ingest(input_dir):
    script_path = os.path.join(project_root, 'ingest_daily.py')
    cmd = [sys.executable, script_path]
    if input_dir:
        cmd.extend(["--input-dir", input_dir])
    
    try:
        logging.info("Triggering ingest_daily.py...")
        # Stream output directly to console instead of capturing
        result = subprocess.run(cmd, stdout=sys.stdout, stderr=sys.stderr)
        if result.returncode != 0:
            logging.error(f"Ingest script failed with code {result.returncode}")
        else:
            logging.info("Ingest script completed successfully.")
            
            # Trigger Background Validation Queue
            validator_path = os.path.join(project_root, 'process_address_validation.py')
            if os.path.exists(validator_path):
                logging.info("Triggering process_address_validation.py...")
                v_cmd = [sys.executable, validator_path]
                v_result = subprocess.run(v_cmd, stdout=sys.stdout, stderr=sys.stderr)
                if v_result.returncode != 0:
                    logging.error(f"Validator script failed with code {v_result.returncode}")
                else:
                    logging.info("Validator script completed successfully.")
                    
    except Exception as e:
        logging.error(f"Failed to execute scripts: {e}")

def has_xml_files(input_dir):
    try:
        files = os.listdir(input_dir)
        return any(f.lower().endswith('.xml') for f in files)
    except FileNotFoundError:
        logging.error(f"Input directory not found: {input_dir}")
        return False
    except Exception as e:
        logging.error(f"Error checking directory {input_dir}: {e}")
        return False

def main():
    parser = argparse.ArgumentParser(description="Continuous Polling Daemon for Daily Ingest")
    parser.add_argument("--input-dir", required=True, help="Path to input directory to monitor")
    parser.add_argument("--interval", type=int, default=300, help="Polling interval in seconds (default: 300)")
    
    args = parser.parse_args()
    
    logging.info(f"Starting Ingest Daemon.")
    logging.info(f"Monitoring Directory: {args.input_dir}")
    logging.info(f"Polling Interval: Hourly at the 30-minute mark")
    
    while True:
        try:
            if has_xml_files(args.input_dir):
                logging.info("XML files detected. Starting ingestion cycle.")
                run_ingest(args.input_dir)
            else:
                # Use debug for the frequent ping so it doesn't flood the logs
                logging.debug("No XML files found. Waiting...")
                
        except KeyboardInterrupt:
            logging.info("Daemon stopped by user.")
            break
        except Exception as e:
            logging.error(f"Unexpected error in daemon loop: {e}")
        now = datetime.now()
        if now.minute < 30:
            target = now.replace(minute=30, second=0, microsecond=0)
        else:
            target = (now + timedelta(hours=1)).replace(minute=30, second=0, microsecond=0)
            
        sleep_seconds = (target - now).total_seconds()
        logging.info(f"Next ingestion check scheduled for {target.strftime('%H:%M:%S')} (in {sleep_seconds:.0f} seconds).")
        time.sleep(sleep_seconds)

if __name__ == "__main__":
    main()
