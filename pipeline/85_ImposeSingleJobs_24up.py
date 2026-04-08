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
    """Fetches all barcodes for the given job to construct full header cards."""
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
            ORDER BY i.order_item_id, b.box_sequence;
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
                    transform = transform.rotate(-90)
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
# HEADER CARD GENERATION
# ==============================================================================
def generate_header_card(barcode_val, row_data, profile, qty_ordered, std_pages, target_icon_path=None):
    packet = io.BytesIO()
    card_w = profile['card_width_pts']
    card_h = profile['card_height_pts']
    c = canvas.Canvas(packet, pagesize=(card_w, card_h))
    
    # We want text visually centered
    trim_width = 2 * 72
    safe_margin = 6
    trim_x = (card_w - trim_width) / 2
    trim_y = (card_h - (3.5 * 72)) / 2 
    
    current_y = card_h - trim_y - safe_margin - 9 # Baseline 246 pt (cap height hits top safe edge at 255)
    
    fn_text = str(row_data.get("job_ticket_number", ""))
    c.setFont("Helvetica-Bold", 12)
    c.drawString((card_w - pdfmetrics.stringWidth(fn_text, "Helvetica-Bold", 12))/2, current_y, fn_text)
    current_y -= 12
    
    qty_text = f"Total Qty: {qty_ordered}"
    c.setFont("Helvetica", 7)
    c.drawString((card_w - pdfmetrics.stringWidth(qty_text, "Helvetica", 7))/2, current_y, qty_text)
    current_y -= 22
    
    store = str(row_data.get("cost_center", "")).split('-')[0].strip()
    st_text = f"Store: {store}"
    c.setFont("Helvetica-Bold", 12)
    c.drawString((card_w - pdfmetrics.stringWidth(st_text, "Helvetica-Bold", 12))/2, current_y, st_text)
    current_y -= 12
    
    order = str(row_data.get("order_number", ""))
    ord_text = f"Order: {order}"
    c.setFont("Helvetica", 7)
    c.drawString((card_w - pdfmetrics.stringWidth(ord_text, "Helvetica", 7))/2, current_y, ord_text)
    current_y -= 28
    
    if barcode_val:
        try:
            target_w = 1.5 * 72
            temp_bc = code128.Code128(str(barcode_val), barWidth=1.0, quiet=False)
            actual_bar_width = target_w / temp_bc.width if temp_bc.width > 0 else 1.0
            bc = code128.Code128(str(barcode_val), barHeight=18, barWidth=actual_bar_width, quiet=False)
            
            c.saveState()
            bc_x = (card_w - target_w) / 2
            bc.drawOn(c, bc_x, current_y)
            c.restoreState()
            
            current_y -= 8
            c.setFont("Helvetica", 6)
            c.drawString((card_w - pdfmetrics.stringWidth(str(barcode_val), "Helvetica", 6))/2, current_y, str(barcode_val))
        except Exception:
            pass

    scale_factor = 63.0 / card_w
    preview_w = card_w * scale_factor
    preview_h = card_h * scale_factor
    preview_gap = 6.0
    left_tx = trim_x + safe_margin
    right_tx = left_tx + preview_w + preview_gap
    bottom_ty = trim_y + safe_margin
    
    c.setLineWidth(0.5)
    c.setStrokeColorRGB(0,0,0)
    c.rect(left_tx, bottom_ty, preview_w, preview_h)
    c.rect(right_tx, bottom_ty, preview_w, preview_h)
            
    c.save()
    packet.seek(0)
    page = PdfReader(packet).pages[0]
    
    if target_icon_path:
        if os.path.exists(target_icon_path):
            try:
                icon_reader = PdfReader(target_icon_path)
                if len(icon_reader.pages) > 0:
                    icon_page = icon_reader.pages[0]
                    icon_w = float(icon_page.mediabox.width)
                    icon_h = float(icon_page.mediabox.height)
                    icon_target_h = 18.0
                    
                    if icon_w > 0 and icon_h > 0:
                        icon_scale = icon_target_h / icon_h
                        icon_target_w = icon_w * icon_scale
                        icon_tx = (card_w - icon_target_w) / 2
                        icon_ty = 133.0 # Centered perfectly between barcode and previews
                        
                        utils_ui.print_info(f"    - Embedding Icon: {os.path.basename(target_icon_path)} | Scale: {icon_scale:.4f} | Size: {icon_target_w:.1f}x{icon_target_h:.1f}")
                        
                        page.merge_transformed_page(icon_page, Transformation().scale(sx=icon_scale, sy=icon_scale).translate(tx=icon_tx, ty=icon_ty))
            except Exception as e:
                utils_ui.print_warning(f"Failed to place icon {target_icon_path}: {e}")
        else:
            utils_ui.print_warning(f"Icon path not found on disk: {target_icon_path}")
            
    if std_pages and len(std_pages) > 0:
        front_art = std_pages[0]
        back_art = std_pages[1] if len(std_pages) > 1 else front_art
        page.merge_transformed_page(front_art, Transformation().scale(sx=scale_factor, sy=scale_factor).translate(tx=left_tx, ty=bottom_ty))
        page.merge_transformed_page(back_art, Transformation().scale(sx=scale_factor, sy=scale_factor).translate(tx=right_tx, ty=bottom_ty))
        
    return page

# ==============================================================================
# STAGE 3: CORE IMPOSITION ENGINE
# ==============================================================================
def impose_content(standardized_pages, profile, qty_ordered, row_data, barcodes, target_icon_path=None):
    total_pages = len(standardized_pages)
    cards_per_sheet = profile['columns'] * profile['rows']
    
    fronts = [standardized_pages[i] for i in range(0, total_pages, 2)]
    backs  = [standardized_pages[i] for i in range(1, total_pages, 2)]
    
    unique_cards = len(fronts)
    if unique_cards == 0:
        return PdfWriter()
        
    copies_per_card = math.ceil(qty_ordered / unique_cards)
    total_production_cards = unique_cards * copies_per_card
    
    # 1. GENERATE HEADER CARDS AND BLANKS
    header_fronts = []
    header_backs = []
    blank_front = PageObject.create_blank_page(width=profile['card_width_pts'], height=profile['card_height_pts'])
    blank_back = PageObject.create_blank_page(width=profile['card_width_pts'], height=profile['card_height_pts'])
    
    if not barcodes:
        barcodes = [None]

    for bc in barcodes:
        hf = generate_header_card(bc, row_data, profile, qty_ordered, standardized_pages, target_icon_path)
        header_fronts.append(hf)
        header_backs.append(blank_back)
        
    num_headers = len(header_fronts)
    
    # Calculate sheets needed for all items combined
    total_slots_needed = num_headers + total_production_cards
    num_front_sheets = math.ceil(total_slots_needed / cards_per_sheet)
    num_sheets = num_front_sheets * 2
    
    pad_blanks_needed = (num_front_sheets * cards_per_sheet) - total_slots_needed
    
    # 2. BUILD THE FLAT SEQUENCES
    job_fronts = []
    job_backs = []
    for i in range(unique_cards):
        for _ in range(copies_per_card):
            job_fronts.append(fronts[i])
            job_backs.append(backs[i])
            
    # Calculate row-specific blanks based on total pad_blanks_needed
    b_mid = min(pad_blanks_needed, 8)
    b_btm = max(0, pad_blanks_needed - 8)
    
    # Calculate how many jobs fill out the rest of the 3 rows on sheet 1
    j1 = 8 - num_headers
    j2 = 8 - b_mid
    j3 = 8 - b_btm
    
    front_sequence = []
    back_sequence = []
    
    jobs_used = 0
    
    # --- FIRST SHEET (24 Slots) ---
    # ROW 1 (Top)
    front_sequence.extend(header_fronts)
    back_sequence.extend(header_backs)
    front_sequence.extend(job_fronts[jobs_used : jobs_used + j1])
    back_sequence.extend(job_backs[jobs_used : jobs_used + j1])
    jobs_used += j1
    
    # ROW 2 (Mid)
    front_sequence.extend([blank_front] * b_mid)
    back_sequence.extend([blank_back] * b_mid)
    front_sequence.extend(job_fronts[jobs_used : jobs_used + j2])
    back_sequence.extend(job_backs[jobs_used : jobs_used + j2])
    jobs_used += j2
    
    # ROW 3 (Btm)
    front_sequence.extend([blank_front] * b_btm)
    back_sequence.extend([blank_back] * b_btm)
    front_sequence.extend(job_fronts[jobs_used : jobs_used + j3])
    back_sequence.extend(job_backs[jobs_used : jobs_used + j3])
    jobs_used += j3
    
    # --- REMAINING SHEETS ---
    front_sequence.extend(job_fronts[jobs_used:])
    back_sequence.extend(job_backs[jobs_used:])
            
    writer = PdfWriter()
    
    for sheet_idx in range(num_front_sheets):
        press_sheet_f = PageObject.create_blank_page(width=profile['paper_width'], height=profile['paper_height'])
        press_sheet_b = PageObject.create_blank_page(width=profile['paper_width'], height=profile['paper_height'])
        
        # New sequence map: left-to-right, top-to-bottom
        for slot in range(cards_per_sheet):
            row_logical = slot // profile['columns']
            row = (profile['rows'] - 1) - row_logical
            col = slot % profile['columns']
            
            global_idx = (sheet_idx * cards_per_sheet) + slot
            
            c_front = front_sequence[global_idx]
            c_back = back_sequence[global_idx]
            
            x_f = profile['start_x'] + (col * (profile['card_width_pts'] + profile['h_gutter']))
            y = profile['start_y'] + (row * (profile['card_height_pts'] + profile['v_gutter']))
            
            curr_col_b = (profile['columns'] - 1) - col
            x_b = profile['start_x'] + (curr_col_b * (profile['card_width_pts'] + profile['h_gutter']))
            
            press_sheet_f.merge_transformed_page(c_front, Transformation().translate(tx=x_f, ty=y))
            press_sheet_b.merge_transformed_page(c_back, Transformation().translate(tx=x_b, ty=y))
                
        writer.add_page(press_sheet_f)
        writer.add_page(press_sheet_b)

    return writer

# ==============================================================================
# STAGE 4: FINISHING
# ==============================================================================
def create_overlays(profile, imposed_filename, sheet_num, total_sheets, row_data, barcodes):
    packet = io.BytesIO()
    c = canvas.Canvas(packet, pagesize=(profile['paper_width'], profile['paper_height']))
    
    qty_ordered = int(pd.to_numeric(row_data.get("quantity_ordered"), errors='coerce') or 1)
    store = str(row_data.get('cost_center', '')).split('-')[0].strip()
    
    parts = [
        (imposed_filename, "Helvetica-Bold"),
        ("  •  ", "Helvetica-Bold"),
        ("Quantity: ", "Helvetica"),
        (str(qty_ordered), "Helvetica-Bold"),
        ("  •  ", "Helvetica-Bold"),
        ("Store: ", "Helvetica"),
        (store, "Helvetica-Bold"),
        ("  •  ", "Helvetica-Bold"),
        ("Sheet ", "Helvetica"),
        (str(sheet_num), "Helvetica-Bold"),
        (" of ", "Helvetica"),
        (str(total_sheets), "Helvetica-Bold")
    ]
    
    total_w = sum(pdfmetrics.stringWidth(txt, font, 30) for txt, font in parts)
    
    c.saveState()
    start_x = (profile['paper_width'] - total_w) / 2
    y = profile['paper_height'] - 0.5 * inch
    
    tx = c.beginText(start_x, y)
    for txt, font in parts:
        tx.setFont(font, 30)
        tx.textOut(txt)
    c.drawText(tx)
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
        paths = config.get('paths', {})
        tmpl_path = paths.get('marks_template_24up_path')
        shipping_box_rules = config.get('shipping_box_rules', {})
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
    
    # --- Progress Update ---
    total_items = 0
    batches_info = {}
    for sheet_name in xls.sheet_names:
        if sheet_name in target_categories:
            df_count = pd.read_excel(xls, sheet_name=sheet_name)
            cat_len = len(df_count)
            if cat_len > 0:
                cat = "12ptBB" if "12pt" in sheet_name else "16ptBC"
                for _, row in df_count.iterrows():
                    job_tk = str(row.get("job_ticket_number", ""))
                    if not job_tk or job_tk == "nan": continue
                    batches_info[job_tk] = {"category": cat, "pct": 0, "total": 1}
                    total_items += 1
            
    import utils_progress
    run_name = os.environ.get('PIPELINE_RUN_NAME', 'MOCK_RUN')
    observer = utils_progress.get_observer(run_name)
    observer.start_stage('stage_6_imposition_single_status', total=total_items, details={"batches": batches_info})
    
    for sheet_name in xls.sheet_names:
        if sheet_name in target_categories:
            df = pd.read_excel(xls, sheet_name=sheet_name)
            if df.empty: continue
            
            utils_ui.print_section(f"Processing Sheet: {sheet_name}")
            
            # The category folder name matches sheet name, e.g., '16ptBusinessCard'
            cat_input_folder = os.path.join(one_up_files_folder, sheet_name)
            
            category = None
            if "12ptBounceBack" in sheet_name or "12ptBB" in sheet_name: category = "12ptBounceBack"
            elif "16ptBusinessCard" in sheet_name or "16ptBC" in sheet_name: category = "16ptBusinessCard"
            
            with utils_ui.create_progress() as progress:
                total_cat = len(df)
                task = progress.add_task(f"Imposing {sheet_name}", total=total_cat)
                
                for row_idx, (_, row) in enumerate(df.iterrows()):
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
                        
                    # Extract barcodes directly from the row payload (box_A, box_B, etc.)
                    barcodes = []
                    for col in row.keys():
                        if str(col).startswith('box_') and pd.notna(row[col]) and str(row[col]).strip() not in ['nan', '']:
                            barcodes.append(str(row[col]).strip())
                    
                    if not barcodes:
                        barcodes = [None] # Guarantee at least one header
                    
                    target_icon_path = None
                    if category and str(qty_ordered) in shipping_box_rules.get(category, {}):
                        rule = shipping_box_rules[category][str(qty_ordered)]
                        icon_filename = rule.get('icon_file')
                        if icon_filename:
                            path_key = icon_filename.replace('.pdf', '_path')
                            target_icon_path = paths.get(path_key)
                            utils_ui.print_info(f"  > Matched Icon Rule: Qty {qty_ordered} -> {target_icon_path}")
                    else:
                        utils_ui.print_warning(f"  > No Box Icon Rule for: Cat='{category}', Qty='{qty_ordered}'")
                            
                    imp_writer = impose_content(std_pages, profile, qty_ordered, row, barcodes, target_icon_path)
                    
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
                    
                    # --- Progress Update ---
                    import utils_progress
                    run_name = os.environ.get('PIPELINE_RUN_NAME', 'MOCK_RUN')
                    obs = utils_progress.get_observer(run_name)
                    obs.check_cancellation()
                    obs.update_stage('stage_6_imposition_single_status', increment=1)
                    obs.update_batch_progress('stage_6_imposition_single_status', str(job_ticket), pct=100)
                    
    observer.reload_stage_from_db('stage_6_imposition_single_status')
    observer.finish_stage('stage_6_imposition_single_status')
    if conn: conn.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("input_excel_path")
    parser.add_argument("one_up_folder")
    parser.add_argument("output_dir")
    parser.add_argument("central_config_json")
    args = parser.parse_args()
    main(args.input_excel_path, args.one_up_folder, args.output_dir, args.central_config_json)
