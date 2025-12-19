# 80_email.py
import os
import sys
import argparse
import yaml
import traceback
import pandas as pd
import smtplib
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import utils_ui

def load_config(config_path):
    try:
        with open(config_path, 'r') as f: return yaml.safe_load(f)
    except Exception as e:
        utils_ui.print_error(f"Config Load Error: {e}"); sys.exit(1)

def attach_files(msg, file_paths):
    utils_ui.print_info(f"Attaching {len(file_paths)} files...")
    for file_path in file_paths:
        if not file_path or not os.path.exists(file_path):
            utils_ui.print_warning(f"File not found, skipping: {file_path}"); continue
        try:
            with open(file_path, 'rb') as f:
                part = MIMEApplication(f.read(), Name=os.path.basename(file_path))
            part['Content-Disposition'] = f'attachment; filename="{os.path.basename(file_path)}"'
            msg.attach(part)
            # utils_ui.print_success(f"Attached: {os.path.basename(file_path)}")
        except Exception as e:
            utils_ui.print_error(f"Attachment Error {file_path}: {e}")
    return msg

def main():
    parser = argparse.ArgumentParser(description="80 - Send Email Notification.")
    parser.add_argument("dynamic_folder_name")
    parser.add_argument("bundled_excel_path")
    parser.add_argument("runlist_pdf_path")
    parser.add_argument("oneup_files_dir")
    parser.add_argument("job_tickets_dir")
    parser.add_argument("config_path")
    args = parser.parse_args()

    utils_ui.setup_logging(None)
    utils_ui.print_banner("80 - Email Notification")

    config = load_config(args.config_path)
    email_settings = config.get('email_settings', {})
    smtp_server = email_settings.get('smtp_server')
    smtp_port = email_settings.get('smtp_port', 587)
    sender_email = email_settings.get('sender_email')
    recipient_list = email_settings.get('recipient_list')
    smtp_user = email_settings.get('smtp_user')
    smtp_pass = email_settings.get('smtp_pass')

    if not all([smtp_user, smtp_pass, smtp_server, sender_email, recipient_list]):
        utils_ui.print_error("Missing email settings in config."); sys.exit(1)
    
    subject = args.dynamic_folder_name
    recipients = [r.strip() for r in recipient_list.split(',')]
    msg = MIMEMultipart(); msg['From'] = sender_email; msg['To'] = ", ".join(recipients)

    body_lines = ["Attachments are for reference only. No outside services are required for these orders."]
    standard_attachments = [args.bundled_excel_path, args.runlist_pdf_path]
    conditional_attachments = []

    try:
        # utils_ui.print_info(f"Checking for 'Outsource' in: {os.path.basename(args.bundled_excel_path)}")
        xls = pd.ExcelFile(args.bundled_excel_path)
        if 'Outsource' in xls.sheet_names:
            utils_ui.print_warning("'Outsource' sheet FOUND.")
            subject += " OUTSIDE SERVICES REQUIRED"
            df_outsource = pd.read_excel(xls, sheet_name='Outsource')
            if 'job_ticket_number' in df_outsource.columns:
                outsource_oneup_dir = os.path.join(args.oneup_files_dir, 'Outsource')
                outsource_ticket_dir = os.path.join(args.job_tickets_dir, 'Outsource')
                unique_base_jobs = set()
                for job_ticket in df_outsource['job_ticket_number'].dropna().astype(str):
                    one_up_path = os.path.join(outsource_oneup_dir, f"{job_ticket}.pdf")
                    if os.path.exists(one_up_path): conditional_attachments.append(one_up_path)
                    
                    base_job_num = job_ticket.rsplit('-', 1)[0]
                    unique_base_jobs.add(base_job_num)
                    ticket_path = os.path.join(outsource_ticket_dir, f"{base_job_num}_TICKETwPROOFS.pdf")
                    if os.path.exists(ticket_path): conditional_attachments.append(ticket_path)

                conditional_attachments = sorted(list(set(conditional_attachments)))
                outsource_job_numbers = sorted(list(unique_base_jobs))
                body_lines = ["The following Job Numbers require outside services:", f"({', '.join(outsource_job_numbers)})", "\nOther attachments are for reference only."]
            else:
                body_lines = ["ATTENTION: 'Outsource' sheet found but missing job numbers."]
        else:
            utils_ui.print_info("'Outsource' sheet not found.")

    except Exception as e:
        utils_ui.print_error(f"Excel Check Error: {e}"); body_lines.append("\nWARNING: Error checking Outsource files.")

    msg['Subject'] = subject 
    msg.attach(MIMEText("\n".join(body_lines), 'plain'))
    msg = attach_files(msg, standard_attachments + conditional_attachments)

    try:
        utils_ui.print_info(f"Connecting to {smtp_server}:{smtp_port}...")
        server = smtplib.SMTP(smtp_server, smtp_port)
        server.starttls()
        server.login(smtp_user, smtp_pass)
        server.sendmail(sender_email, recipients, msg.as_string())
        utils_ui.print_success("Email sent successfully.")
        server.quit()
    except Exception as e:
        utils_ui.print_error(f"Send Failed: {e}"); sys.exit(1)

if __name__ == "__main__":
    main()