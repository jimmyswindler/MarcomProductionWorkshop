# 80_email.py
import os
import sys
import argparse
import yaml
import glob
import traceback
import pandas as pd
import smtplib
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

def load_config(config_path):
    """Loads the central YAML configuration file."""
    try:
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)
    except FileNotFoundError:
        print(f"FATAL: Config file not found at {config_path}")
        sys.exit(1)
    except yaml.YAMLError as e:
        print(f"FATAL: Error parsing YAML file: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"FATAL: Unexpected error loading config: {e}")
        sys.exit(1)

def attach_files(msg, file_paths):
    """Attaches a list of files to the email message object."""
    print(f"\nAttaching {len(file_paths)} file(s)...")
    for file_path in file_paths:
        if not file_path or not os.path.exists(file_path):
            print(f"  - WARNING: File not found, skipping attachment: {file_path}")
            continue
        try:
            with open(file_path, 'rb') as f:
                part = MIMEApplication(
                    f.read(),
                    Name=os.path.basename(file_path)
                )
            part['Content-Disposition'] = f'attachment; filename="{os.path.basename(file_path)}"'
            msg.attach(part)
            print(f"  ✓ Attached: {os.path.basename(file_path)}")
        except Exception as e:
            print(f"  - ERROR: Could not attach file {file_path}: {e}")
    return msg

def main():
    parser = argparse.ArgumentParser(description="80 - Send Email Notification.")
    parser.add_argument("dynamic_folder_name", help="Base name of the job, used as email subject.")
    parser.add_argument("bundled_excel_path", help="Path to the bundled Excel file.")
    parser.add_argument("runlist_pdf_path", help="Path to the PDF Run List file.")
    parser.add_argument("oneup_files_dir", help="Path to the '.../WorkUp/OneUpFiles' directory.")
    parser.add_argument("job_tickets_dir", help="Path to the '.../_JobTickets' directory.")
    parser.add_argument("config_path", help="Path to the central config.yaml file.")
    args = parser.parse_args()

    print("--- Starting 80: Email Notification ---")

    # --- 1. Load Config and Credentials ---
    config = load_config(args.config_path)
    email_settings = config.get('email_settings', {})
    
    smtp_server = email_settings.get('smtp_server')
    smtp_port = email_settings.get('smtp_port', 587)
    sender_email = email_settings.get('sender_email')
    recipient_list = email_settings.get('recipient_list')
    
    # Load credentials directly from config file
    smtp_user = email_settings.get('smtp_user')
    smtp_pass = email_settings.get('smtp_pass')

    if not smtp_user or not smtp_pass:
        print("FATAL: 'smtp_user' or 'smtp_pass' missing from email_settings in config.yaml.")
        sys.exit(1)

    if not all([smtp_server, sender_email, recipient_list]):
        print("FATAL: 'email_settings' (server, sender, recipients) missing in config.yaml.")
        sys.exit(1)
    
    # --- 2. Initialize Email with Default Content ---
    subject = args.dynamic_folder_name
    recipients = [r.strip() for r in recipient_list.split(',')]
    
    msg = MIMEMultipart()
    msg['From'] = sender_email
    msg['To'] = ", ".join(recipients)

    # Default body copy
    body_lines = [
        "Attachments are for reference only. No outside services are required for these orders."
    ]
    standard_attachments = [args.bundled_excel_path, args.runlist_pdf_path]
    conditional_attachments = []

    # --- 3. Conditional Logic for 'Outsource' ---
    try:
        print(f"\nChecking for 'Outsource' sheet in: {args.bundled_excel_path}")
        xls = pd.ExcelFile(args.bundled_excel_path)
        
        if 'Outsource' in xls.sheet_names:
            print("  - 'Outsource' sheet FOUND.")
            
            # --- a. Modify Subject ---
            subject += " OUTSIDE SERVICES REQUIRED"
            
            # --- b. Read sheet to get job numbers and find files ---
            df_outsource = pd.read_excel(xls, sheet_name='Outsource')
            
            if 'job_ticket_number' not in df_outsource.columns:
                print("  - WARNING: 'job_ticket_number' column not found in Outsource sheet.")
                body_lines = [
                    "ATTENTION: 'Outsource' sheet was found, but 'job_ticket_number' column is missing.",
                    "Unable to list jobs or attach files.",
                    "\nOther attachments are for reference only."
                ]
            else:
                outsource_oneup_dir = os.path.join(args.oneup_files_dir, 'Outsource')
                outsource_ticket_dir = os.path.join(args.job_tickets_dir, 'Outsource')
                unique_base_jobs = set()

                for job_ticket in df_outsource['job_ticket_number'].dropna().astype(str):
                    # Find One-Up PDF (e.g., "12345-01.pdf")
                    one_up_path = os.path.join(outsource_oneup_dir, f"{job_ticket}.pdf")
                    if os.path.exists(one_up_path):
                        conditional_attachments.append(one_up_path)
                    else:
                        print(f"  - Note: One-up file not found, will not attach: {one_up_path}")
                    
                    # Find Ticket PDF (e.g., "12345_TICKETwPROOFS.pdf")
                    # Parse base number (e.g., "12345-01" -> "12345")
                    base_job_num = job_ticket.rsplit('-', 1)[0]
                    unique_base_jobs.add(base_job_num)
                    ticket_path = os.path.join(outsource_ticket_dir, f"{base_job_num}_TICKETwPROOFS.pdf")
                    
                    if os.path.exists(ticket_path):
                        conditional_attachments.append(ticket_path)
                    else:
                        # Don't log missing ticket file every time, it would be redundant
                        pass 

                # De-duplicate the list of files
                conditional_attachments = sorted(list(set(conditional_attachments)))

                # --- c. Modify Body ---
                outsource_job_numbers = sorted(list(unique_base_jobs))
                body_lines = [
                    "The following Job Numbers require outside services:",
                    f"({', '.join(outsource_job_numbers)})",
                    "\nOther attachments are for reference only."
                ]
        else:
            print("  - 'Outsource' sheet not found. Sending standard email.")
            # Subject and body remain the default values

    except Exception as e:
        print(f"  - ERROR: Could not process Excel file for 'Outsource' check: {e}")
        body_lines.append("\nWARNING: An error occurred while checking for Outsource files.")
        traceback.print_exc()

    # --- 4. Assemble and Send Email ---
    
    # Apply the (potentially modified) subject
    msg['Subject'] = subject 
    
    final_body = "\n".join(body_lines)
    msg.attach(MIMEText(final_body, 'plain'))
    
    all_attachments = standard_attachments + conditional_attachments
    msg = attach_files(msg, all_attachments)

    try:
        print(f"\nConnecting to SMTP server: {smtp_server}:{smtp_port}")
        server = smtplib.SMTP(smtp_server, smtp_port)
        server.starttls()
        if smtp_user and smtp_pass:
            print("  - Logging in with credentials...")
            server.login(smtp_user, smtp_pass)
        print("  - Sending email...")
        server.sendmail(sender_email, recipients, msg.as_string())
        print("✓ Email sent successfully.")
    except Exception as e:
        print(f"FATAL: Failed to send email: {e}")
        traceback.print_exc()
        sys.exit(1) # Exit with error
    finally:
        if 'server' in locals() and server:
            server.quit()

    print("--- Finished 80 ---")

if __name__ == "__main__":
    main()