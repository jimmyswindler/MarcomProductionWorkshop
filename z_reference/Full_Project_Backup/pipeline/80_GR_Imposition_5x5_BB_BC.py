# 70_GR_Imposition_5x5_BB_BC.py
import os
import io
import json
import math
import traceback
import sys
from datetime import datetime
import argparse

import utils_ui # <--- New UI Utility

try:
    from pypdf import PdfReader, PdfWriter, PageObject, Transformation
    from pypdf.generic import RectangleObject
    from reportlab.pdfgen import canvas
    from reportlab.lib.units import inch
    from reportlab.pdfbase import pdfmetrics
    from reportlab.pdfbase.ttfonts import TTFont
except ImportError:
    utils_ui.print_error("Required libraries not found: pypdf, reportlab")
    sys.exit(1)

# ==============================================================================
# STAGE 1: CONFIGURATION & PLANNING
# ==============================================================================
def load_and_plan(central_config):
    profile_path = central_config['imposition_profile']
    utils_ui.print_section(f"Stage 1: Profile Planning")
    
    try:
        with open(profile_path, 'r') as f: config_data = json.load(f)
        grid = config_data['xtools'][0]['packedSteps'][0]
        hf = config_data['xtools'][0]['packedSteps'][1]

        profile = {
            'paper_width': float(grid['paperWidth']) * 72,
            'paper_height': float(grid['paperHeight']) * 72,
            'columns': int(grid['columns']),
            'rows': int(grid['rows']),
            'v_gutter': float(grid['verticalGutterWidth']) * 72,
            'h_gutter': float(grid['horizontalGutterWidth']) * 72,
            'center': grid.get('center', False),
            'bleed_left': float(grid.get('fixedBleedLeft', 0.0)) * 72,
            'bleed_top': float(grid.get('fixedBleedTop', 0.0)) * 72,
            'header_footer': {
                'text': hf['text'],
                'font': hf['fontName'],
                'size': float(hf['fontSizeInPoints']),
                'margin': float(hf['margins']) * 72
            },
            'card_width_pts': 2.25 * 72,
            'card_height_pts': 3.75 * 72
        }
        return profile
    except Exception as e:
        utils_ui.print_error(f"Stage 1 Failed: Could not load profile '{profile_path}': {e}")
        return None

# ==============================================================================
# STAGE 2: PAGE COLLECTION & STANDARDIZATION
# ==============================================================================
def standardize_pages(file_paths, profile):
    utils_ui.print_section(f"Stage 2: Standardizing {len(file_paths)} Files")
    all_pages = []
    
    with utils_ui.create_progress() as progress:
        task = progress.add_task("Standardizing...", total=len(file_paths))
        
        for file_path in file_paths:
            try:
                reader = PdfReader(file_path)
                for page in reader.pages:
                    c_w, c_h = profile['card_width_pts'], profile['card_height_pts']
                    pos_canvas = PageObject.create_blank_page(width=c_w, height=c_h)
                    itb = page.trimbox
                    if not itb or (itb.width == page.mediabox.width and itb.height == page.mediabox.height):
                        bx, by = profile['bleed_left'], profile['bleed_top']
                        itb = RectangleObject((page.mediabox.left + bx, page.mediabox.bottom + by, page.mediabox.right - bx, page.mediabox.top - by))

                    dx = (c_w / 2) - ((itb.left + itb.right) / 2)
                    dy = (c_h / 2) - ((itb.bottom + itb.top) / 2)
                    pos_canvas.merge_transformed_page(page, Transformation().translate(dx, dy))
                    all_pages.append(pos_canvas)
            except Exception as e:
                utils_ui.print_warning(f"Error reading {os.path.basename(file_path)}: {e}")
            progress.update(task, advance=1)
            
    utils_ui.print_info(f"Standardized {len(all_pages)} total pages.")
    return all_pages

# ==============================================================================
# STAGE 3: CORE IMPOSITION ENGINE
# ==============================================================================
def impose_content(standardized_pages, profile):
    total_pages = len(standardized_pages)
    cards_per_sheet = profile['columns'] * profile['rows']
    num_sheets = math.ceil(total_pages / cards_per_sheet)
    
    utils_ui.print_section(f"Stage 3: Imposing onto {num_sheets} Sheets")
    writer = PdfWriter()
    
    trim_w = profile['card_width_pts'] - (2 * profile['bleed_left'])
    trim_h = profile['card_height_pts'] - (2 * profile['bleed_top'])
    block_w = (profile['columns'] * trim_w) + ((profile['columns'] - 1) * profile['h_gutter'])
    block_h = (profile['rows'] * trim_h) + ((profile['rows'] - 1) * profile['v_gutter'])
    mx = (profile['paper_width'] - block_w) / 2 if profile['center'] else profile['bleed_left']
    my = (profile['paper_height'] - block_h) / 2 if profile['center'] else profile['bleed_top']

    with utils_ui.create_progress() as progress:
        task = progress.add_task("Imposing Sheets...", total=num_sheets)
        
        for sheet_idx in range(num_sheets):
            press_sheet = PageObject.create_blank_page(width=profile['paper_width'], height=profile['paper_height'])
            is_back = (sheet_idx % 2) != 0

            for row in range(profile['rows']):
                for col in range(profile['columns']):
                    slot = (row * profile['columns']) + col
                    p_idx = (slot * num_sheets) + sheet_idx
                    if p_idx >= total_pages: continue

                    card = standardized_pages[p_idx]
                    curr_col = (profile['columns'] - 1) - col if is_back else col
                    x = mx - profile['bleed_left'] + (curr_col * (trim_w + profile['h_gutter']))
                    y = my - profile['bleed_top'] + (row * (trim_h + profile['v_gutter']))
                    press_sheet.merge_transformed_page(card, Transformation().translate(tx=x, ty=y))
            
            writer.add_page(press_sheet)
            progress.update(task, advance=1)

    return writer

# ==============================================================================
# STAGE 4: FINISHING
# ==============================================================================
def create_slug_line_overlay(profile, batch_name, sheet_num, total_sheets):
    packet = io.BytesIO()
    c = canvas.Canvas(packet, pagesize=(profile['paper_width'], profile['paper_height']))
    hf = profile['header_footer']
    c.saveState()
    text = hf['text'].replace('[file-name]', batch_name).replace('[page-number]', str(sheet_num)).replace('[page-count]', str(total_sheets))
    c.setFont(hf['font'], hf['size'])
    tw = pdfmetrics.stringWidth(text, hf['font'], hf['size'])
    cy = (profile['paper_height'] / 2) + (tw / 2)
    c.translate(0.25 * inch, cy)
    c.rotate(270)
    c.drawString(0, 0, text)
    c.restoreState()
    c.save()
    packet.seek(0)
    return PdfReader(packet).pages[0]

def apply_finishing(imposed_writer, profile, batch_name, central_config):
    utils_ui.print_section("Stage 4: Finishing Marks")
    tmpl_path = central_config['marks_template']
    
    try:
        tmpl_reader = PdfReader(tmpl_path); tmpl_page = tmpl_reader.pages[0]
    except Exception as e:
        utils_ui.print_error(f"Stage 4 Failed: Could not open template '{tmpl_path}': {e}")
        return None

    final_writer = PdfWriter()
    total = len(imposed_writer.pages)
    
    with utils_ui.create_progress() as progress:
        task = progress.add_task("Applying Marks...", total=total)
        for i, sheet in enumerate(imposed_writer.pages):
            sheet.merge_page(tmpl_page)
            sheet.merge_page(create_slug_line_overlay(profile, batch_name, i+1, total))
            final_writer.add_page(sheet)
            progress.update(task, advance=1)
            
    return final_writer

# ==============================================================================
# MAIN
# ==============================================================================
def main(batch_folder, output_dir, central_config_json):
    utils_ui.setup_logging(None)
    utils_ui.print_banner("70 - Imposition Engine")
    
    try: central_config = json.loads(central_config_json)
    except Exception as e: utils_ui.print_error(f"Config Error: {e}"); return
    
    os.makedirs(output_dir, exist_ok=True)
    batch_name = os.path.basename(batch_folder)
    utils_ui.print_info(f"Processing Batch: {batch_name}")

    if not os.path.isdir(batch_folder): utils_ui.print_error(f"Batch folder not found: {batch_folder}"); return

    profile = load_and_plan(central_config)
    if not profile: return

    file_paths = [os.path.join(batch_folder, f) for f in sorted(os.listdir(batch_folder)) if f.lower().endswith(".pdf")]
    if not file_paths: utils_ui.print_warning("No PDF files found."); return
        
    std_pages = standardize_pages(file_paths, profile)
    if not std_pages: utils_ui.print_error("Standardization failed."); return
        
    imp_writer = impose_content(std_pages, profile)
    final_writer = apply_finishing(imp_writer, profile, batch_name, central_config)
    if not final_writer: return

    out_path = os.path.join(output_dir, f"{batch_name}.pdf")
    try:
        with open(out_path, "wb") as f: final_writer.write(f)
        utils_ui.print_success(f"Saved: {out_path} ({len(final_writer.pages)} sheets)")
    except Exception as e:
        utils_ui.print_error(f"Save Failed: {e}"); traceback.print_exc()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("batch_folder_to_impose")
    parser.add_argument("output_dir")
    parser.add_argument("central_config_json")
    args = parser.parse_args()
    main(args.batch_folder_to_impose, args.output_dir, args.central_config_json)