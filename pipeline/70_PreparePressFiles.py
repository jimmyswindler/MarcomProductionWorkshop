# 60_PreparePressFiles.py
import os
import shutil
import re
import pandas as pd
import traceback
import math
import sys
import argparse
import json
from io import BytesIO

import utils_ui

try:
    import fitz
    from pypdf import PdfReader, PdfWriter, PageObject, Transformation
    from pypdf.generic import RectangleObject
    from reportlab.graphics.barcode import code128
    from reportlab.pdfgen import canvas as rl_canvas
except ImportError:
    utils_ui.print_error("Required library not found: pymupdf, pypdf, reportlab")
    sys.exit(1)

GANG_RUN_TRIGGER = "-GR-"

# Configuration for Header Pages
HEADER_FONT_SIZE = 18
HEADER_TOP_MARGIN = 72
HEADER_PAGE_WIDTH = 2.25 * 72
HEADER_PAGE_HEIGHT = 3.75 * 72
HEADER_TRIM_WIDTH = 2 * 72
HEADER_TRIM_HEIGHT = 3.5 * 72

# Fixed Layout Constants
FN_FONT_SIZE, QTY_FONT_SIZE, STORE_FONT_SIZE = 12, 7, 12
ORDER_FONT_SIZE, BARCODE_TEXT_SIZE = 7, 6
BARCODE_HEIGHT, ICON_HEIGHT = 18, 18
BLOCK_SPACING, LINE_SPACING = 6, 2

def _create_barcode_pdf_in_memory(data_string, width, height):
    buffer = BytesIO(); c = rl_canvas.Canvas(buffer, pagesize=(width, height))
    barcode = code128.Code128(data_string, barHeight=height, barWidth=1.4) 
    barcode.drawOn(c, (width - barcode.width) / 2, 0)
    c.save(); buffer.seek(0)
    return buffer

def create_header_page(pdf_path, order_number=None, segment=None, total_segments=None, total_quantity=None, background_color=None, store_number=None, icon_path=None, icon_cards=0, box_value=None):
    header_doc, src_doc = None, None
    try:
        header_doc = fitz.open(); header_page = header_doc.new_page(width=HEADER_PAGE_WIDTH, height=HEADER_PAGE_HEIGHT)
        if background_color and len(background_color) == 4: header_page.draw_rect(header_page.rect, color=background_color, fill=background_color)
        
        trim_x = (HEADER_PAGE_WIDTH - HEADER_TRIM_WIDTH) / 2; trim_y = (HEADER_PAGE_HEIGHT - HEADER_TRIM_HEIGHT) / 2
        offset = 0.125 * 72; p1_rect, p2_rect = None, None

        try:
# ... (PDF preview logic mostly same)
            src_doc = fitz.open(pdf_path)
            if src_doc.page_count > 0:
                page1 = src_doc[0]; is_landscape = page1.rect.width > page1.rect.height
                if is_landscape:
                    avail_w = HEADER_TRIM_WIDTH - (2 * offset)
                    scale = avail_w / page1.rect.width if page1.rect.width > 0 else 0
                    if src_doc.page_count > 1: scale = min(scale, avail_w / src_doc[1].rect.width if src_doc[1].rect.width > 0 else 0)
                    p1_w, p1_h = page1.rect.width * scale, page1.rect.height * scale
                    p1_x = trim_x + (HEADER_TRIM_WIDTH - p1_w) / 2
                    bot_y = HEADER_PAGE_HEIGHT - trim_y - offset + 36
                    if src_doc.page_count > 1:
                        p2_w, p2_h = src_doc[1].rect.width * scale, src_doc[1].rect.height * scale
                        bot_y = bot_y - p2_h - offset
                        p2_rect = fitz.Rect(trim_x + (HEADER_TRIM_WIDTH - p2_w)/2, bot_y, trim_x + (HEADER_TRIM_WIDTH - p2_w)/2 + p2_w, bot_y + p2_h)
                        bot_y -= offset
                    p1_rect = fitz.Rect(p1_x, bot_y - p1_h, p1_x + p1_w, bot_y)
                else:
                    scale = 0.62; p1_w, p1_h = page1.rect.width * scale, page1.rect.height * scale
                    p1_rect = fitz.Rect(trim_x + offset, HEADER_PAGE_HEIGHT - trim_y - offset + 40 - p1_h, trim_x + offset + p1_w, HEADER_PAGE_HEIGHT - trim_y - offset + 40)
                    if src_doc.page_count > 1:
                        p2_w, p2_h = src_doc[1].rect.width * scale, src_doc[1].rect.height * scale
                        p2_rect = fitz.Rect(HEADER_PAGE_WIDTH - trim_x - offset - p2_w, trim_y + 1.5*72 + 36, HEADER_PAGE_WIDTH - trim_x - offset, trim_y + 1.5*72 + 36 + p2_h)
        except Exception: pass

        font_reg, font_bold = "helvetica", "helvetica-bold"; current_y = trim_y + offset
        
        # Block A: Filename / Qty
        fn_text = os.path.splitext(os.path.basename(pdf_path))[0]
        header_page.insert_text(fitz.Point((HEADER_PAGE_WIDTH - fitz.get_text_length(fn_text, fontname=font_bold, fontsize=FN_FONT_SIZE))/2, current_y + FN_FONT_SIZE), fn_text, fontname=font_bold, fontsize=FN_FONT_SIZE)
        current_y += FN_FONT_SIZE + LINE_SPACING
        qty_text = f"Total Qty: {total_quantity}" if total_quantity is not None else "Total Qty: N/A"
        header_page.insert_text(fitz.Point((HEADER_PAGE_WIDTH - fitz.get_text_length(qty_text, fontname=font_reg, fontsize=QTY_FONT_SIZE))/2, current_y + QTY_FONT_SIZE), qty_text, fontname=font_reg, fontsize=QTY_FONT_SIZE)
        current_y += QTY_FONT_SIZE + BLOCK_SPACING

        # Block B: Store/Order
        if store_number:
            st_text = f"Store: {store_number}"
            header_page.insert_text(fitz.Point((HEADER_PAGE_WIDTH - fitz.get_text_length(st_text, fontname=font_bold, fontsize=STORE_FONT_SIZE))/2, current_y + STORE_FONT_SIZE), st_text, fontname=font_bold, fontsize=STORE_FONT_SIZE)
        current_y += STORE_FONT_SIZE + LINE_SPACING
        if order_number:
            ord_text = f"Order: {order_number}"
            header_page.insert_text(fitz.Point((HEADER_PAGE_WIDTH - fitz.get_text_length(ord_text, fontname=font_reg, fontsize=ORDER_FONT_SIZE))/2, current_y + ORDER_FONT_SIZE), ord_text, fontname=font_reg, fontsize=ORDER_FONT_SIZE)
        current_y += ORDER_FONT_SIZE + BLOCK_SPACING

        # Block C: Barcode
        will_draw_box = False
        if total_segments:
            # Default "standard" spacing is every 2 segments (500 sheets)
            will_draw_box = ((segment % 2 == 0) or (segment == total_segments))

            # Special spacing for 12pt 2500 (10 segments)
            if total_quantity == 2500 and total_segments == 10:
                will_draw_box = (segment % 3 == 0) or (segment == total_segments)

            # Ensure we only draw if we actually have an icon to show (avoid ghost boxes)
            if not (icon_path and os.path.exists(icon_path)):
                will_draw_box = False 

        if will_draw_box and box_value:
            bc_w = 1.75 * 72; bc_x = (HEADER_PAGE_WIDTH - bc_w) / 2
            wb_w, wb_h = 136, 20
            header_page.draw_rect(fitz.Rect((HEADER_PAGE_WIDTH-wb_w)/2, current_y - (wb_h-BARCODE_HEIGHT)/2, (HEADER_PAGE_WIDTH-wb_w)/2+wb_w, current_y - (wb_h-BARCODE_HEIGHT)/2+wb_h), color=(1,1,1), fill=(1,1,1))
            with fitz.open("pdf", _create_barcode_pdf_in_memory(box_value, bc_w, BARCODE_HEIGHT)) as bd: header_page.show_pdf_page(fitz.Rect(bc_x, current_y, bc_x + bc_w, current_y + BARCODE_HEIGHT), bd, 0)
            current_y += BARCODE_HEIGHT + 2
            header_page.insert_text(fitz.Point((HEADER_PAGE_WIDTH - fitz.get_text_length(box_value, fontname='helvetica', fontsize=BARCODE_TEXT_SIZE))/2, current_y + BARCODE_TEXT_SIZE), box_value, fontname='helvetica', fontsize=BARCODE_TEXT_SIZE)
            current_y += BARCODE_TEXT_SIZE + BLOCK_SPACING
        else: current_y += BARCODE_HEIGHT + 2 + BARCODE_TEXT_SIZE + BLOCK_SPACING

        # Block D: Icon
        if will_draw_box:
            def place_icon(doc, x, y):
                 if not doc or doc.page_count == 0: return 0
                 p = doc[0]; ar = p.rect.width / p.rect.height if p.rect.height > 0 else 1
                 tw = ICON_HEIGHT * ar; header_page.show_pdf_page(fitz.Rect(x, y, x + tw, y + ICON_HEIGHT), doc, 0); return tw
            
            try:
                # Use passed icon_path. 
                # Note: We now ignore icon_cards qty for placement, assuming the PDF itself 
                # represents the correct visual (e.g. 2 boxes for 1000 qty).
                if icon_path and os.path.exists(icon_path):
                     with fitz.open(icon_path) as d:
                         w = place_icon(d, 0, -999) # calc width
                         place_icon(d, (HEADER_PAGE_WIDTH - w)/2, current_y)
            except Exception: pass
        elif total_segments and total_segments > 1:
            stk_text = f"Stack {segment} of {total_segments}"
            header_page.insert_text(fitz.Point((HEADER_PAGE_WIDTH - fitz.get_text_length(stk_text, fontname=font_reg, fontsize=10))/2, current_y + 10), stk_text, fontname=font_reg, fontsize=10)

        # Previews
        if src_doc and src_doc.page_count > 0:
            if p1_rect: header_page.insert_image(p1_rect, stream=src_doc[0].get_pixmap(dpi=144).tobytes("png")); header_page.draw_rect(p1_rect, color=(0,0,0), width=0.5)
            if p2_rect and src_doc.page_count > 1: header_page.insert_image(p2_rect, stream=src_doc[1].get_pixmap(dpi=144).tobytes("png")); header_page.draw_rect(p2_rect, color=(0,0,0), width=0.5)

        packet = BytesIO(); header_doc.save(packet, garbage=4, deflate=True); packet.seek(0)
        final = PdfReader(packet).pages[0]
        final.trimbox = RectangleObject([trim_x, trim_y, HEADER_PAGE_WIDTH - trim_x, HEADER_PAGE_HEIGHT - trim_y])
        return final
    finally:
        if header_doc: header_doc.close()
        if src_doc: src_doc.close()


def add_segmented_headers_to_pdf(orientation_path, target_path, order=None, qty=None, bg=None, store=None, icon_path=None, icon_cards=0, box_vals={}):
    try:
        reader = PdfReader(target_path); total = len(reader.pages)
        if total == 0: return False
        
        sorted_vals = [box_vals.get(k) for k in sorted(box_vals.keys())]
        num_segments = (total + 499) // 500; writer = PdfWriter()
        
        xm = (HEADER_PAGE_WIDTH - HEADER_TRIM_WIDTH)/2; ym = (HEADER_PAGE_HEIGHT - HEADER_TRIM_HEIGHT)/2
        blank = PageObject.create_blank_page(width=HEADER_PAGE_WIDTH, height=HEADER_PAGE_HEIGHT)
        blank.trimbox = RectangleObject([xm, ym, HEADER_PAGE_WIDTH - xm, HEADER_PAGE_HEIGHT - ym])

        bc_idx = 0
        for i in range(num_segments):
            seg_num = i + 1
            will_draw = False
            if total > 0:
                 # Logic must match create_header_page
                 will_draw = ((seg_num % 2 == 0) or (seg_num == num_segments))
                 if qty == 2500 and num_segments == 10:
                     will_draw = (seg_num % 3 == 0) or (seg_num == num_segments)
                 
                 # New check: only draw if icon path exists
                 if not (icon_path and os.path.exists(icon_path)): will_draw = False

            val = None
            if will_draw and bc_idx < len(sorted_vals):
                raw = sorted_vals[bc_idx]
                if raw and str(raw).lower() != 'nan': val = raw
                bc_idx += 1
            
            writer.add_page(create_header_page(orientation_path, order, seg_num, num_segments, qty, bg, store, icon_path, icon_cards, val))
            writer.add_page(blank)
            for p in reader.pages[i*500:(i+1)*500]: writer.add_page(p)

        with open(target_path, "wb") as f: writer.write(f)
        return True
    except Exception as e: utils_ui.print_error(f"Header Add Failed: {e}"); return False

def sanitize_filename(filename): return re.sub(r'[\\:*?"<>|]', '', str(filename).replace('/', '-')).strip()
def natural_keys(text): return [int(c) if c.isdigit() else c for c in re.split(r'(\d+)', str(text))]

def standardize_pdf_for_gang_run(pdf_path):
    try:
        reader_check = PdfReader(pdf_path)
        if not reader_check.pages or reader_check.pages[0].mediabox.width <= reader_check.pages[0].mediabox.height:
            return False

        writer = PdfWriter()
        reader = PdfReader(pdf_path)
        for i, original_page in enumerate(reader.pages):
            page_number = i + 1
            width = float(original_page.mediabox.width)
            height = float(original_page.mediabox.height)
            new_page = PageObject.create_blank_page(width=height, height=width)
            
            if page_number % 2 != 0:
                transform = Transformation().rotate(-90).translate(tx=0, ty=width)
                recalculate_box = lambda box: RectangleObject((box.lower_left[1], width - box.upper_right[0], box.upper_right[1], width - box.lower_left[0]))
            else:
                transform = Transformation().rotate(90).translate(tx=height, ty=0)
                recalculate_box = lambda box: RectangleObject((height - box.upper_right[1], box.lower_left[0], height - box.lower_left[1], box.upper_right[0]))
            
            boxes_to_transform = {
                "mediabox": original_page.mediabox, 
                "cropbox": getattr(original_page, "cropbox", original_page.mediabox),
                "bleedbox": getattr(original_page, "bleedbox", original_page.mediabox), 
                "trimbox": getattr(original_page, "trimbox", original_page.mediabox),
                "artbox": getattr(original_page, "artbox", original_page.mediabox)
            }
            
            for box_name, box_obj in boxes_to_transform.items():
                if box_obj: # Ensure not None
                     setattr(new_page, box_name, recalculate_box(box_obj))
            
            new_page.merge_transformed_page(original_page, transform)
            writer.add_page(new_page)
            
        with open(pdf_path, "wb") as f: writer.write(f)
        return True
    except Exception as e: utils_ui.print_error(f"Standardization Failed: {e}"); return False

def process_dataframe(df, files_path, originals_path, sheet_name, palette_path=None, config_icons={}, shipping_rules={}):
    if GANG_RUN_TRIGGER not in sheet_name.upper(): utils_ui.print_info(f"Skipping Standard Sheet: {sheet_name}"); return
    
    utils_ui.print_section(f"Processing Gang Run: {sheet_name}")
    color_map = {}
    if palette_path and os.path.exists(palette_path):
        try:
            pdf = pd.read_csv(palette_path, encoding='utf-8-sig')
            pal = [tuple(r) for r in pdf[['C', 'M', 'Y', 'K']].to_numpy()] if {'C','M','Y','K'}.issubset(pdf.columns) else []
            if pal:
                data = [{'idx': i, 'qty': int(pd.to_numeric(r.get("quantity_ordered"), errors='coerce') or 0), 't': str(r.get("job_ticket_number",""))} for i, r in df.iterrows()]
                data.sort(key=lambda x: (-x['qty'], natural_keys(x['t'])))
                color_map = {d['idx']: pal[i] for i, d in enumerate(data) if i < len(pal)}
        except Exception as e: utils_ui.print_warning(f"Palette Error: {e}")

    # Determine Product Category for rules lookup from sheet_name
    # Sheet names match keys like "12ptBounceBack" or have prefix
    # Logic in 20_DataSorter might produce names like '12ptBounceBack_CATEGORIZED' or '12ptBB-GR-144'
    # The key in shipping_box_rules is either exact or has wildcards? 
    # Current config has "12ptBounceBack" and "12ptBB-GR-*"
    category = None
    if "12ptBounceBack" in sheet_name or "12ptBB" in sheet_name: category = "12ptBounceBack"
    elif "16ptBusinessCard" in sheet_name or "16ptBC" in sheet_name: category = "16ptBusinessCard"

    rows_data = list(df.iterrows())
    utils_ui.print_info(f"Preparing {len(rows_data)} files...")
    with utils_ui.create_progress() as progress:
        task = progress.add_task("Preparing Files...", total=len(rows_data))
        for idx, row in rows_data:
            try:
                base = sanitize_filename(str(row.get("job_ticket_number")))
                prod_path = os.path.join(files_path, f"{base}.pdf")
                if not os.path.exists(prod_path): progress.update(task, advance=1); continue

                box_vals = {f'box_{chr(65+i)}': str(row.get(f'box_{chr(65+i)}')).strip() for i in range(8) if pd.notna(row.get(f'box_{chr(65+i)}'))}
                os.makedirs(originals_path, exist_ok=True)
                arch_path = os.path.join(originals_path, f"{base}.pdf")
                shutil.copy2(prod_path, arch_path)
                
                qty = int(pd.to_numeric(row.get("quantity_ordered"), errors='coerce') or 1)

                reader = PdfReader(arch_path)
                if len(reader.pages) in [1, 2]:
                    writer = PdfWriter()
                    pages = [reader.pages[0]] if len(reader.pages)==1 else [reader.pages[0], reader.pages[1]]
                    if len(pages)==1: pages.append(PageObject.create_blank_page(width=pages[0].mediabox.width, height=pages[0].mediabox.height))
                    for _ in range(qty): 
                        for p in pages: writer.add_page(p)
                    with open(prod_path, "wb") as f: writer.write(f)
                
                standardize_pdf_for_gang_run(prod_path)
                
                # Resolve Icon Logic per Row
                target_icon_path = None; icon_cards = 0
                if category and str(qty) in shipping_rules.get(category, {}):
                    rule = shipping_rules[category][str(qty)]
                    icon_cards = rule.get('icon_cards', 0)
                    icon_filename = rule.get('icon_file') # e.g. "icon_A.pdf"
                    if icon_filename:
                         # config_icons keys are "icon_A.pdf" -> "/path/to/icon_A.pdf"
                         target_icon_path = config_icons.get(icon_filename)

                store = str(row.get("cost_center", "")).split('-')[0].strip()
                add_segmented_headers_to_pdf(arch_path, prod_path, str(row.get("order_number", "")), qty, color_map.get(idx), store, target_icon_path, icon_cards, box_vals)
            except Exception as e: utils_ui.print_error(f"Row {idx} Failed: {e}")
            progress.update(task, advance=1)

def main(input_excel_path, files_base_folder, originals_base_folder, central_config_json):
    utils_ui.setup_logging(None)
    utils_ui.print_banner("60 - Prepare Press Files")
    try:
        config = json.loads(central_config_json)
        
        # New: Parse full icon paths and rules
        icon_file_paths = config.get('icon_file_paths', {})
        shipping_box_rules = config.get('shipping_box_rules', {})
        
        xls = pd.ExcelFile(input_excel_path)
        for sheet_name in xls.sheet_names:
            if GANG_RUN_TRIGGER in sheet_name.upper():
                df = pd.read_excel(xls, sheet_name=sheet_name, dtype={f'box_{chr(65+i)}': str for i in range(8)})
                process_dataframe(df, os.path.join(files_base_folder, sanitize_filename(sheet_name)), os.path.join(originals_base_folder, sanitize_filename(sheet_name)), sheet_name, config.get('COLOR_PALETTE_PATH'), icon_file_paths, shipping_box_rules)
    except Exception as e: utils_ui.print_error(f"Fatal Error: {e}"); sys.exit(1)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("input_excel_path")
    parser.add_argument("files_base_folder")
    parser.add_argument("originals_folder")
    parser.add_argument("central_config_json")
    args = parser.parse_args()
    main(args.input_excel_path, args.files_base_folder, args.originals_folder, args.central_config_json)