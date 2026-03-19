# 85_ImposeSingleJobs_24up.py
import os
import io
import json
import math
import traceback
import sys
import argparse

# --- Path Injection for shared_lib ---
script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(script_dir)
if project_root not in sys.path:
    sys.path.insert(0, project_root)

import pandas as pd
import psycopg2
import psycopg2.extras

import utils_ui

try:
    from pypdf import PdfReader, PdfWriter, PageObject, Transformation
    from pypdf.generic import RectangleObject
    from reportlab.pdfgen import canvas
    from reportlab.lib.units import inch
    from reportlab.pdfbase import pdfmetrics
    from reportlab.graphics.barcode import code128
except ImportError:
    utils_ui.print_error("Required libraries not found: pypdf, reportlab, psycopg2, pandas")
    sys.exit(1)

# ==============================================================================
# DATABASE CONNECTION
# ==============================================================================
def get_db_connection():
    try:
        from shared_lib.database import get_db_connection as get_shared_conn
        return get_shared_conn()
    except Exception as e:
        utils_ui.print_error(f"Could not connect using shared_lib: {e}")
        return None

def fetch_item_boxes_for_job(conn, job_ticket_number):
    """Fetches up to 8 barcodes for the given job."""
    if not conn: return []
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        # Join jobs -> items -> item_boxes
        cur.execute("""
            SELECT b.barcode_value, b.box_sequence 
            FROM jobs j
            JOIN items i ON j.id = i.job_id
            JOIN item_boxes b ON i.order_item_id = b.order_item_id
            WHERE i.job_ticket_display_id = %s OR j.job_ticket_number = %s
            ORDER BY i.order_item_id, b.box_sequence
            LIMIT 8;
        """, (job_ticket_number, job_ticket_number))
        rows = cur.fetchall()
        cur.close()
        return [row['barcode_value'] for row in rows]
    except Exception as e:
        utils_ui.print_error(f"DB Fetch Error for job {job_ticket_number}: {e}")
        return []

# ==============================================================================
# STAGE 2: PAGE COLLECTION & STANDARDIZATION
# ==============================================================================
def standardize_pages(file_path, profile):
    """
    Standardizes a PDF for the 24up Gang Run: Head-to-Head Duplex formatting.
    Processes the raw file pages dynamically into uniform card sizes.
    """
    try:
        reader = PdfReader(file_path)
        if len(reader.pages) == 0: return None
        
        c_w, c_h = profile['card_width_pts'], profile['card_height_pts'] 
        # c_w = 2.25", c_h = 3.75"  (Landscape rotated to Portrait)
        
        all_pages = []
        for i in range(len(reader.pages)):
            orig_page = reader.pages[i]
            # Create standard blank portrait page for our imposition
            pos_canvas = PageObject.create_blank_page(width=c_w, height=c_h)
            
            itb = orig_page.trimbox
            if not itb or (itb.width == orig_page.mediabox.width and itb.height == orig_page.mediabox.height):
                bx, by = profile['bleed_left'], profile['bleed_top']
                itb = RectangleObject((orig_page.mediabox.left + bx, orig_page.mediabox.bottom + by, orig_page.mediabox.right - bx, orig_page.mediabox.top - by))
            
            cx = float((itb.left + itb.right) / 2)
            cy = float((itb.bottom + itb.top) / 2)
            
            w = float(itb.width)
            h = float(itb.height)
            
            transform = Transformation().translate(-cx, -cy)
            
            if w > h:  # Original is landscape (e.g. 3.75x2.25) -> Rotate to portrait
                if i % 2 != 0: # Back page head-to-head rotation
                    transform = transform.rotate(90)
                else:          # Front page rotation
                    transform = transform.rotate(90)
            else: # Already portrait
                if i % 2 != 0: # Back page head-to-head rotation
                    transform = transform.rotate(0)
                else:
                    transform = transform.rotate(0)
            
            transform = transform.translate(c_w / 2, c_h / 2)
            
            pos_canvas.merge_transformed_page(orig_page, transform)
            all_pages.append(pos_canvas)
        
        # Ensure we always have an even number of pages for duplex
        if len(all_pages) % 2 != 0:
            all_pages.append(PageObject.create_blank_page(width=c_w, height=c_h))
            
        utils_ui.print_info(f"  Standardized {len(all_pages)} pages.")
        return all_pages
    except Exception as e:
        utils_ui.print_warning(f"Error standardizing {os.path.basename(file_path)}: {e}")
        return None

# ==============================================================================
# STAGE 3: CORE IMPOSITION ENGINE
# ==============================================================================
def impose_content(standardized_pages, profile, qty_ordered):
    total_pages = len(standardized_pages)
    cards_per_sheet = profile['columns'] * profile['rows']
    
    # Notice total_pages includes fronts and backs. 
    # Fronts go on odd sheets, Backs go on even sheets.
    fronts = [standardized_pages[i] for i in range(0, total_pages, 2)]
    backs  = [standardized_pages[i] for i in range(1, total_pages, 2)]
    
    unique_cards = len(fronts)
    if unique_cards == 0:
        return PdfWriter()
        
    # Calculate how many copies of each card we need
    # (e.g. 500 qty / 1 unique card = 500 copies per card)
    # (e.g. 2500 qty / 2500 unique cards = 1 copy per card)
    copies_per_card = math.ceil(qty_ordered / unique_cards)
    total_cards_to_place = unique_cards * copies_per_card
    
    # How many front sheets?
    num_front_sheets = math.ceil(total_cards_to_place / cards_per_sheet)
    num_sheets = num_front_sheets * 2
    
    writer = PdfWriter()
    
    trim_w = profile['card_width_pts'] - (2 * profile['bleed_left'])
    trim_h = profile['card_height_pts'] - (2 * profile['bleed_top'])
    
    for sheet_idx in range(num_sheets):
        press_sheet = PageObject.create_blank_page(width=profile['paper_width'], height=profile['paper_height'])
        is_back = (sheet_idx % 2) != 0
        front_sheet_idx = sheet_idx // 2
        
        src_pile = backs if is_back else fronts
        
        for row in range(profile['rows']):
            for col in range(profile['columns']):
                slot = (row * profile['columns']) + col
                global_card_index = (front_sheet_idx * cards_per_sheet) + slot
                
                # Stop placing if we fulfilled the quantity exactly
                if global_card_index >= total_cards_to_place: continue
                
                # Map the global_card_index back to the source PDF's page pool
                pdf_page_index = global_card_index // copies_per_card
                if pdf_page_index >= len(src_pile): continue
                
                card = src_pile[pdf_page_index]
                curr_col = (profile['columns'] - 1) - col if is_back else col
                
                # We start layout from top or bottom? The 25up started from bottom.
                # Let's place explicitly.
                # Left Margin is actual bleed edge
                x = profile['start_x'] + (curr_col * (profile['card_width_pts'] + profile['h_gutter']))
                # Y is inverted (from bottom up)
                y = profile['start_y'] + (row * (profile['card_height_pts'] + profile['v_gutter']))
                
                press_sheet.merge_transformed_page(card, Transformation().translate(tx=x, ty=y))
                
        writer.add_page(press_sheet)

    return writer

# ==============================================================================
# STAGE 4: FINISHING
# ==============================================================================
def create_overlays(profile, imposed_filename, sheet_num, total_sheets, row_data, barcodes):
    packet = io.BytesIO()
    c = canvas.Canvas(packet, pagesize=(profile['paper_width'], profile['paper_height']))
    
    # --- Top Slug ---
    text = f"{imposed_filename}  *  Sheet {sheet_num} of {total_sheets}"
    c.saveState()
    c.setFont("Helvetica-Bold", 30) # Hardcoded for reliability
    tw = pdfmetrics.stringWidth(text, "Helvetica-Bold", 14)
    cx = (profile['paper_width'] / 2) - (tw / 2)
    # Place text 0.5 inches from top
    c.drawString(cx, profile['paper_height'] - 0.5 * inch, text)
    c.restoreState()
    
    # --- Bottom Mini-Slugs ---
    c.saveState()
    zone_width = 2.25 * 72 # 2.25 inches
    y_baseline = 0.5 * 72   # Half inch from bottom 
    
    # MECHANISM EXPLANATION:
    # -------------------------------------------------------------
    # The `create_overlays` function loops through the `barcodes` array.
    # Each barcode gets a predefined 2.375-inch "zone" horizontally along the bottom of the 19" sheet.
    # If the `barcodes` array is empty (often happening for multi-line items because the query failed to find boxes),
    # the loop never executes, which means the text (Store, Order, Job) also skips printing.
    # By ensuring at least one dummy iteration when barcodes are missing, the text will always print.
    # -------------------------------------------------------------
    
    store = str(row_data.get('cost_center', '')).split('-')[0].strip()
    order = str(row_data.get('order_number', ''))
    job   = str(row_data.get('job_ticket_number', ''))

    base_x = 0.5 * 72 # Mini slugs span the entire width of the sheet from 0 to 19

    # Force at least one iteration if no barcodes exist to guarantee subtext prints
    iterable_barcodes = barcodes if barcodes else [""]

    for i, barcode_val in enumerate(iterable_barcodes):
        x_center = base_x + (i * zone_width) + (0.5 * zone_width) # Center of the 2.25" column
        
        # 1. Barcode (Skip if dummy barcode_val)
        bh = 0.25 * 72
        if barcode_val:
            try:
                bc = code128.Code128(str(barcode_val), barHeight=bh, barWidth=1.0)
                bc_x = x_center - (bc.width / 2) # Use computed exact width
                bc_y = y_baseline + 8
                bc.drawOn(c, bc_x, bc_y)
            except Exception as e:
                 bc_y = y_baseline + 8
                 pass
        else:
            bc_y = y_baseline + 8
        
        c.setFont("Helvetica", 6)
        
        # 2. Text under barcode
        if barcode_val:
            txt_w = pdfmetrics.stringWidth(str(barcode_val), "Helvetica", 6)
            c.drawString(x_center - (txt_w / 2), bc_y - 6, str(barcode_val))
        
        # 3. Store, Order, Job Text
        subtext = f"ST: {store} | ORD: {order} | JOB: {job}"
        st_w = pdfmetrics.stringWidth(subtext, "Helvetica", 6)
        # Avoid overlapping barcode text
        c.drawString(x_center - (st_w / 2), bc_y - 14, subtext)
        
    c.restoreState()
    c.save()
    packet.seek(0)
    return PdfReader(packet).pages[0]

# ==============================================================================
# MAIN
# ==============================================================================
def main(input_excel_path, one_up_files_folder, output_dir, central_config_json):
    utils_ui.setup_logging(None)
    utils_ui.print_banner("85 - Single Job Imposition 24up")
    
    try: 
        config = json.loads(central_config_json)
        tmpl_path = config.get('paths', {}).get('marks_template_24up_path')
    except Exception as e: 
        utils_ui.print_error(f"Config Error: {e}"); return
        
    os.makedirs(output_dir, exist_ok=True)
    
    # Connect to database for item box codes
    conn = get_db_connection()
    if not conn: utils_ui.print_warning("No Database Connection. Barcodes will not be generated.")

    profile = {
        'paper_width': 19 * 72,
        'paper_height': 13 * 72,
        'columns': 8,
        'rows': 3,
        'v_gutter': 0.0 * 72,
        'h_gutter': 0.0 * 72,
        'bleed_left': 0.125 * 72,
        'bleed_top': 0.125 * 72,
        'card_width_pts': 2.25 * 72,
        'card_height_pts': 3.75 * 72, # (3.5" + 0.25" bleed overall)
        'start_x': (0.625 - 0.125) * 72, # Margin minus left bleed 
        'start_y': (1.0 - 0.125) * 72    # Bottom margin minus bottom bleed
    }

    xls = pd.ExcelFile(input_excel_path)
    # Process only non-GR sheets
    target_categories = [cat.strip() for cat in ['16ptBusinessCard', '12ptBounceBack']]
    
    for sheet_name in xls.sheet_names:
        if sheet_name in target_categories:
            df = pd.read_excel(xls, sheet_name=sheet_name)
            if df.empty: continue
            
            utils_ui.print_section(f"Processing Sheet: {sheet_name}")
            
            # The category folder name matches sheet name, e.g., '16ptBusinessCard'
            cat_input_folder = os.path.join(one_up_files_folder, sheet_name)
            
            with utils_ui.create_progress() as progress:
                task = progress.add_task(f"Imposing {sheet_name}", total=len(df))
                
                for _, row in df.iterrows():
                    job_ticket = row.get("job_ticket_number")
                    if pd.isna(job_ticket): continue
                    
                    qty_ordered = int(pd.to_numeric(row.get("quantity_ordered"), errors='coerce') or 1)
                    file_path = os.path.join(cat_input_folder, f"{job_ticket}.pdf")
                    
                    if not os.path.exists(file_path):
                        utils_ui.print_warning(f"File not found: {file_path}")
                        progress.update(task, advance=1)
                        continue
                        
                    std_pages = standardize_pages(file_path, profile)
                    if not std_pages: 
                        progress.update(task, advance=1)
                        continue
                        
                    imp_writer = impose_content(std_pages, profile, qty_ordered)
                    
                    # Fetch barcodes
                    barcodes = fetch_item_boxes_for_job(conn, job_ticket) if conn else []
                    
                    # Apply finishing
                    final_writer = PdfWriter()
                    total_sheets = len(imp_writer.pages) // 2 # Front and back makes a sheet
                    
                    # Ensure marks template is applied correctly
                    try:
                        tmpl_reader = PdfReader(tmpl_path)
                        tmpl_front = tmpl_reader.pages[0]
                        tmpl_back = tmpl_reader.pages[1] if len(tmpl_reader.pages) > 1 else tmpl_front
                    except Exception as e:
                        utils_ui.print_warning(f"Missing Marks Template at {tmpl_path}. {e}")
                        tmpl_front = tmpl_back = None
                    
                    for i in range(len(imp_writer.pages)):
                        sheet = imp_writer.pages[i]
                        is_back = (i % 2) != 0
                        sheet_num = (i // 2) + 1
                        
                        # Apply marks
                        if tmpl_front: 
                            sheet.merge_page(tmpl_back if is_back else tmpl_front)
                            
                        # Apply overlays (only on the very first front sheet)
                        if i == 0:
                            imposed_filename = f"{job_ticket}_24up.pdf"
                            overlay = create_overlays(profile, imposed_filename, sheet_num, total_sheets, row, barcodes)
                            sheet.merge_page(overlay)
                            
                        final_writer.add_page(sheet)
                        
                    # Save output
                    cat_out_dir = os.path.join(output_dir, sheet_name)
                    os.makedirs(cat_out_dir, exist_ok=True)
                    out_path = os.path.join(cat_out_dir, f"{job_ticket}_24up.pdf")
                    try:
                        with open(out_path, "wb") as f: final_writer.write(f)
                    except Exception as e:
                        utils_ui.print_error(f"Failed to write {out_path}: {e}")
                        
                    progress.update(task, advance=1)
                    
    if conn: conn.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("input_excel_path")
    parser.add_argument("one_up_folder")
    parser.add_argument("output_dir")
    parser.add_argument("central_config_json")
    args = parser.parse_args()
    main(args.input_excel_path, args.one_up_folder, args.output_dir, args.central_config_json)
