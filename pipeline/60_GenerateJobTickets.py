import os
import time
import shutil
import re
import pandas as pd
import traceback
import math
import sys
import argparse
import json 
from itertools import groupby
from datetime import datetime, timedelta
from io import BytesIO

import utils_ui

# PDF Libraries
try:
    import fitz  # PyMuPDF
    from pypdf import PdfReader, PdfWriter, PageObject, Transformation
    from pypdf.generic import DictionaryObject, NameObject
    from reportlab.lib.pagesizes import letter, landscape
    from reportlab.lib.units import inch
    from reportlab.lib.utils import ImageReader
    from PIL import Image
    from reportlab.graphics.barcode import code128
    from reportlab.pdfgen import canvas as rl_canvas
except ImportError:
    utils_ui.print_error("Required PDF libraries not found. Please install: pymupdf, pypdf, reportlab, pillow")
    sys.exit(1)

GANG_RUN_TRIGGER = "-GR-"

def sanitize_filename(filename):
    filename = str(filename).replace('/', '-')
    return re.sub(r'[\\:*?"<>|]', '', filename).strip()

def clean_text(value):
    if pd.isna(value): return ""
    text = str(value).replace('_x000D_', ' ')
    text = re.sub(r'<style.*?>.*?</style>', '', text, flags=re.IGNORECASE | re.DOTALL)
    replacements = {'’': "'", '‘': "'", '”': '"', '“': '"', '—': '--', '–': '-', '…': '...', '™': '(TM)', '®': '(R)', '©': '(C)'}
    for unicode_char, ascii_char in replacements.items(): text = text.replace(unicode_char, ascii_char)
    text = re.sub(r'[\x00-\x1F\x7F-\x9F]', '', text)
    return re.sub(r'\s+', ' ', text).strip()

def create_proof_in_memory(input_path, filename_text, order_number, sku="", qty=""):
    doc = None
    proof_doc = None
    try:
        if not os.path.exists(input_path): return None
        doc = fitz.open(input_path)
        page_count = doc.page_count
        if page_count == 0: return None
        proof_doc = fitz.open()
        PAGE_W, PAGE_H = fitz.paper_size("letter-l")
        proof_page = proof_doc.new_page(width=PAGE_W, height=PAGE_H)
        font_reg, font_bold = "helvetica", "helvetica-bold"
        SIDE_MARGIN, TOP_BOTTOM_MARGIN = 0.5 * 72, 1.0 * 72
        header_filename = os.path.basename(filename_text)
        qty_text, order_text, sku_text = f"Qty: {qty}", f"Order: {order_number}", f"SKU: {sku}"
        y1 = 30
        proof_page.insert_text(fitz.Point(SIDE_MARGIN, y1), header_filename, fontname=font_bold, fontsize=14)
        qty_len = fitz.get_text_length(qty_text, fontname=font_bold, fontsize=14)
        proof_page.insert_text(fitz.Point((PAGE_W - qty_len) / 2, y1), qty_text, fontname=font_bold, fontsize=14)
        order_len = fitz.get_text_length(order_text, fontname=font_bold, fontsize=14)
        proof_page.insert_text(fitz.Point(PAGE_W - SIDE_MARGIN - order_len, y1), order_text, fontname=font_bold, fontsize=14)
        y2 = 57
        sku_len = fitz.get_text_length(sku_text, fontname=font_reg, fontsize=12)
        proof_page.insert_text(fitz.Point((PAGE_W - sku_len) / 2, y2), sku_text, fontname=font_reg, fontsize=12)
        avail_rect = fitz.Rect(SIDE_MARGIN, TOP_BOTTOM_MARGIN, PAGE_W - SIDE_MARGIN, PAGE_H - TOP_BOTTOM_MARGIN)
        def format_inches(val): return f"{val:.3f}".rstrip('0').rstrip('.')
        scale = 1.0
        if page_count > 25:
            GAP = 36
            max_w_per_page = (avail_rect.width - GAP) / 2
            page1, page2 = doc[0], doc[1]
            scale1 = min(max_w_per_page / page1.rect.width, avail_rect.height / page1.rect.height) if page1.rect.width > 0 else 0
            scale2 = min(max_w_per_page / page2.rect.width, avail_rect.height / page2.rect.height) if page2.rect.width > 0 else 0
            final_scale = min(scale1, scale2, 1.0)
            p1_w, p1_h = page1.rect.width*final_scale, page1.rect.height*final_scale
            p2_w, p2_h = page2.rect.width*final_scale, page2.rect.height*final_scale
            total_content_w = p1_w + GAP + p2_w
            start_x = avail_rect.x0 + (avail_rect.width - total_content_w) / 2
            p1_y0 = avail_rect.y0 + (avail_rect.height - p1_h)/2
            p1_rect = fitz.Rect(start_x, p1_y0, start_x + p1_w, p1_y0 + p1_h)
            p2_x0 = p1_rect.x1 + GAP
            p2_y0 = avail_rect.y0 + (avail_rect.height - p2_h)/2
            p2_rect = fitz.Rect(p2_x0, p2_y0, p2_x0 + p2_w, p2_y0 + p2_h)
            proof_page.show_pdf_page(p1_rect, doc, 0); proof_page.draw_rect(p1_rect, color=(0,0,0), width=0.5)
            proof_page.show_pdf_page(p2_rect, doc, 1); proof_page.draw_rect(p2_rect, color=(0,0,0), width=0.5)
        elif 1 < page_count <= 25:
            GAP = 10
            cols = min(5, int(math.ceil(math.sqrt(page_count))))
            rows = int(math.ceil(page_count / cols))
            cell_w = (avail_rect.width - (cols - 1) * GAP) / cols
            cell_h = (avail_rect.height - (rows - 1) * GAP) / rows
            art_page = doc[0]
            scale = min(cell_w / art_page.rect.width, cell_h / art_page.rect.height, 1.0) if art_page.rect.width > 0 and art_page.rect.height > 0 else 0
            thumb_w, thumb_h = art_page.rect.width * scale, art_page.rect.height * scale
            for i in range(page_count):
                row, col = i // cols, i % cols
                cell_x0 = avail_rect.x0 + col * (cell_w + GAP)
                cell_y0 = avail_rect.y0 + row * (cell_h + GAP)
                thumb_x0 = cell_x0 + (cell_w - thumb_w) / 2
                thumb_y0 = cell_y0 + (cell_h - thumb_h) / 2
                target_rect = fitz.Rect(thumb_x0, thumb_y0, thumb_x0 + thumb_w, thumb_y0 + thumb_h)
                proof_page.show_pdf_page(target_rect, doc, i)
                proof_page.draw_rect(target_rect, color=(0,0,0), width=0.5)
        else:
            art_page = doc[0]
            scale = min(avail_rect.width / art_page.rect.width, avail_rect.height / art_page.rect.height, 1.0) if art_page.rect.width > 0 and art_page.rect.height > 0 else 0
            target_w, target_h = art_page.rect.width * scale, art_page.rect.height * scale
            x0 = avail_rect.x0 + (avail_rect.width - target_w) / 2
            y0 = avail_rect.y0 + (avail_rect.height - target_h) / 2
            target_rect = fitz.Rect(x0, y0, x0 + target_w, y0 + target_h)
            proof_page.show_pdf_page(target_rect, doc, 0)
            proof_page.draw_rect(target_rect, color=(0,0,0), width=0.5)
        art_page = doc[0]
        final_scale = scale
        if page_count > 25:
            page_text = f"Displaying Pages 1-2 of {page_count}"
        elif 1 < page_count <= 25:
            page_text = f"Displaying Pages 1-{page_count} of {page_count}"
        else:
            page_text = "Displaying Page 1 of 1"
        media_w, media_h = format_inches(art_page.rect.width/72), format_inches(art_page.rect.height/72)
        trim_box = art_page.trimbox or art_page.rect
        trim_w, trim_h = format_inches(trim_box.width/72), format_inches(trim_box.height/72)
        line1 = f'Media Size: {media_w}" × {media_h}"  Trim Size: {trim_w}" × {trim_h}"  Proof Scale: {final_scale * 100:.1f}%'
        line2 = page_text
        footer_y1, footer_y2 = PAGE_H - 40, PAGE_H - 22
        line1_len = fitz.get_text_length(line1, fontname=font_reg, fontsize=10)
        proof_page.insert_text(fitz.Point((PAGE_W - line1_len)/2, footer_y1), line1, fontname=font_reg, fontsize=10)
        line2_len = fitz.get_text_length(line2, fontname=font_reg, fontsize=10)
        proof_page.insert_text(fitz.Point((PAGE_W - line2_len)/2, footer_y2), line2, fontname=font_reg, fontsize=10)
        return proof_doc
    except Exception as e:
        utils_ui.print_error(f"Proof creation failed for {input_path}: {e}")
        return None
    finally:
        if doc and not doc.is_closed: doc.close()

def extract_numerics(value): return re.sub(r'[^0-9]', '', str(value)) if pd.notna(value) else ""

def extract_cost_center_number(cost_center):
    if pd.isna(cost_center): return "0000"
    match = re.match(r'^(\d{1,4})', str(cost_center))
    return match.group(1).zfill(4)[:4] if match else "0000"

def adjust_for_weekend(date):
    if date.weekday() >= 5: return date + timedelta(days=(7 - date.weekday()))
    return date

def format_zip_code(zip_code):
    if pd.isna(zip_code): return ""
    val = str(zip_code).strip()
    if val.endswith('.0'): val = val[:-2]
    parts = val.split('-')
    if len(parts[0]) > 0 and len(parts[0]) < 5: parts[0] = parts[0].zfill(5)
    return '-'.join(parts)

def _create_barcode_pdf_in_memory(data_string, width, height):
    buffer = BytesIO()
    c = rl_canvas.Canvas(buffer, pagesize=(width, height))
    barcode = code128.Code128(data_string, barHeight=height, barWidth=1.4) 
    barcode_actual_width = barcode.width 
    x_centered = (width - barcode_actual_width) / 2
    barcode.drawOn(c, x_centered, 0)
    c.save(); buffer.seek(0)
    return buffer

def generate_ticket_pymupdf(ticket_rows, base_job_number, gang_run_name=None, total_counts_map=None, sheet_name=None, watermark_path=None):
    main_row = ticket_rows[0]
    doc = fitz.open()
    PAGE_W, PAGE_H = fitz.paper_size("letter-l")
    LEFT_INDENT, RIGHT_INDENT = 0.25*72, 0.25*72
    FIELD_VALUE_X = LEFT_INDENT + 2.0*72
    MAX_LINE_WIDTH = PAGE_W - RIGHT_INDENT - FIELD_VALUE_X
    title_style, sec_style = ("helvetica-bold", 14), ("helvetica-bold", 14)
    fname_style, fval_style, line_style = ("helvetica-bold", 11), ("helvetica", 11), ("helvetica", 11)
    sheet_name_style = ("helvetica", 10)
    
    ticket_number = str(base_job_number)
    due_date = ""
    try:
        date_str = main_row.get("ship_date", "")
        if pd.notna(date_str) and str(date_str).strip():
            due_date = pd.to_datetime(date_str).strftime('%m/%d/%Y')
    except Exception: pass
        
    order_number_raw = clean_text(main_row.get("order_number", ""))
    order_number = extract_numerics(order_number_raw)
    cost_center = extract_cost_center_number(main_row.get("cost_center", ""))

    def draw_header(page, is_first_page=True):
        y_top_line = 0.5 * 72
        page.insert_text(fitz.Point(LEFT_INDENT, y_top_line), f"JOB NUMBER: {ticket_number}", fontname=title_style[0], fontsize=title_style[1])
        if order_number_raw:
            order_text = f"ORDER: {order_number_raw}"
            order_text_len = fitz.get_text_length(order_text, fontname=title_style[0], fontsize=title_style[1])
            page.insert_text(fitz.Point((PAGE_W - order_text_len) / 2, y_top_line), order_text, fontname=title_style[0], fontsize=title_style[1])

        ship_text = f"SHIP DATE: {due_date}" if due_date else "SHIP DATE: TBD"
        ship_text_len = fitz.get_text_length(ship_text, fontname=title_style[0], fontsize=title_style[1])
        page.insert_text(fitz.Point(PAGE_W - RIGHT_INDENT - ship_text_len, y_top_line), ship_text, fontname=title_style[0], fontsize=title_style[1])
        
        if sheet_name:
            page.insert_text(fitz.Point(LEFT_INDENT, 0.75*72), str(sheet_name), fontname=sheet_name_style[0], fontsize=sheet_name_style[1])
        
        if is_first_page:
            center_x = PAGE_W - RIGHT_INDENT - (ship_text_len / 2)
            y = y_top_line + 20 
            barcode_w, barcode_h = 2.0*72, 0.375*72
            
            if order_number: 
                try:
                    barcode_x0 = center_x - (barcode_w / 2)
                    rect = fitz.Rect(barcode_x0, y, barcode_x0 + barcode_w, y + barcode_h)
                    with fitz.open("pdf", _create_barcode_pdf_in_memory(order_number, barcode_w, barcode_h)) as barcode_doc: page.show_pdf_page(rect, barcode_doc, 0)
                    text_y = rect.y1 + 4
                    text = f"Order Number: {order_number_raw}"; text_len = fitz.get_text_length(text, fontname='helvetica', fontsize=11)
                    page.insert_text(fitz.Point(center_x - (text_len / 2), text_y + 10), text, fontname='helvetica', fontsize=11)
                    y = text_y + 12 + 24
                except Exception: pass
            if cost_center:
                try:
                    barcode_x0 = center_x - (barcode_w / 2)
                    rect = fitz.Rect(barcode_x0, y, barcode_x0 + barcode_w, y + barcode_h)
                    with fitz.open("pdf", _create_barcode_pdf_in_memory(cost_center, barcode_w, barcode_h)) as barcode_doc: page.show_pdf_page(rect, barcode_doc, 0)
                    text_y = rect.y1 + 4
                    text = f"Store Number: {cost_center}"; text_len = fitz.get_text_length(text, fontname='helvetica', fontsize=11)
                    page.insert_text(fitz.Point(center_x - (text_len / 2), text_y + 10), text, fontname='helvetica', fontsize=11)
                    y = text_y + 12 + 10
                except Exception: pass
            
            if watermark_path and os.path.exists(watermark_path):
                try:
                    watermark_w, watermark_h = 1.0*72, 1.0*72
                    watermark_x0 = center_x - (watermark_w / 2)
                    rect = fitz.Rect(watermark_x0, y, watermark_x0 + watermark_w, y + watermark_h)
                    page.insert_image(rect, filename=watermark_path)
                except Exception: pass

    def draw_right_aligned(page, text, y, font, size):
        FIELD_NAME_RIGHT_EDGE = LEFT_INDENT + 1.875*72
        text_len = fitz.get_text_length(text, fontname=font, fontsize=size)
        page.insert_text(fitz.Point(FIELD_NAME_RIGHT_EDGE - text_len, y), text, fontname=font, fontsize=size)

    page = doc.new_page(width=PAGE_W, height=PAGE_H)
    draw_header(page, is_first_page=True)
    y = 1.25*72
    def new_page_check(y_pos, min_y_from_bottom=1.0*72):
        nonlocal page
        if y_pos > PAGE_H - min_y_from_bottom:
            page = doc.new_page(width=PAGE_W, height=PAGE_H)
            draw_header(page, is_first_page=False)
            return 1.5*72
        return y_pos

    page.insert_text(fitz.Point(LEFT_INDENT, y), "PRODUCT INFORMATION", fontname=sec_style[0], fontsize=sec_style[1]); y += 0.25*72
    for field, display_name in [("product_id", "Product ID"), ("product_name", "Product Name")]:
        y = new_page_check(y)
        draw_right_aligned(page, f"{display_name}:", y, fname_style[0], fname_style[1])
        page.insert_text(fitz.Point(FIELD_VALUE_X, y), clean_text(main_row.get(field, "")), fontname=fval_style[0], fontsize=fval_style[1]); y += 0.25*72
    y += 0.25*72; y = new_page_check(y)
    
    page.insert_text(fitz.Point(LEFT_INDENT, y), "SHIPPING DETAILS", fontname=sec_style[0], fontsize=sec_style[1]); y += 0.25*72
    for field, display_name in [("cost_center", "Cost Center")]:
        y = new_page_check(y)
        draw_right_aligned(page, f"{display_name}:", y, fname_style[0], fname_style[1])
        page.insert_text(fitz.Point(FIELD_VALUE_X, y), clean_text(main_row.get(field, "")), fontname=fval_style[0], fontsize=fval_style[1]); y += 0.25*72
        
    ship_company = clean_text(main_row.get("ship_to_company", "")); ship_attn = clean_text(main_row.get("ship_attn", "")); ship_addr4 = clean_text(main_row.get("address4", ""))
    addr1 = clean_text(main_row.get("address1", "")); addr2 = clean_text(main_row.get("address2", "")); addr3 = clean_text(main_row.get("address3", ""))
    city = clean_text(main_row.get("city", "")); state = clean_text(main_row.get("state", "")); zip_code = format_zip_code(main_row.get("zip", ""))
    if ship_company:
        y = new_page_check(y); draw_right_aligned(page, "Ship Company:", y, fname_style[0], fname_style[1]); page.insert_text(fitz.Point(FIELD_VALUE_X, y), ship_company, fontname=fval_style[0], fontsize=fval_style[1]); y += 0.25*72
    combined_attention_line = ' '.join(filter(None, [ship_attn, ship_addr4]))
    if combined_attention_line:
        y = new_page_check(y); draw_right_aligned(page, "Attention:", y, fname_style[0], fname_style[1]); page.insert_text(fitz.Point(FIELD_VALUE_X, y), combined_attention_line, fontname=fval_style[0], fontsize=fval_style[1]); y += 0.25*72
    address_lines = [line for line in [addr1, addr2, addr3] if line]; last_line = ' '.join(filter(None, [city, state, zip_code]))
    if last_line: address_lines.append(last_line)
    if address_lines:
        y = new_page_check(y); draw_right_aligned(page, "Ship Address:", y, fname_style[0], fname_style[1]); addr_y = y
        for i, line in enumerate(address_lines):
            page.insert_text(fitz.Point(FIELD_VALUE_X, addr_y), line, fontname=fval_style[0], fontsize=fval_style[1])
            if i < len(address_lines) - 1: addr_y += 0.18 * 72
        y = addr_y + 0.05 * 72
    y += 0.25*72; y = new_page_check(y)
    
    try: total_items = int(main_row.get("job_total_line_items"))
    except (ValueError, TypeError, AttributeError): total_items = total_counts_map.get(str(ticket_number), len(ticket_rows))
    
    indices = []
    for r in ticket_rows:
        try: indices.append(int(r.get('line_item_suffix', '0')))
        except (ValueError, TypeError): indices.append(0)
    
    header_text = f"LINE ITEMS ({total_items} Total)" if len(ticket_rows) == total_items else f"LINE ITEMS {min(indices)}-{max(indices)} ({total_items} Total)"
    page.insert_text(fitz.Point(LEFT_INDENT, y), header_text, fontname=sec_style[0], fontsize=sec_style[1]); y += 0.25*72
    page.draw_line(fitz.Point(LEFT_INDENT, y), fitz.Point(PAGE_W - RIGHT_INDENT, y)); y += 0.1875*72

    line_item_height = 0.2 * 72 
    x_item_col = LEFT_INDENT
    x_qty_col = LEFT_INDENT + 1.0 * 72
    x_label_right_edge = x_qty_col + 1.9 * 72
    x_value_col = x_label_right_edge + 10 
    barcode_w, barcode_h, barcode_gap = 1.75 * 72, 0.25 * 72, 10
    max_value_width = (PAGE_W - RIGHT_INDENT - barcode_w - barcode_gap) - x_value_col

    for row in ticket_rows:
        y = new_page_check(y, min_y_from_bottom=1.5*72)
        current_y = y; line_y = current_y

        item_part = f"Item {row.get('line_item_suffix', '??')}:"
        qty_part = f"Qty: {str(row.get('quantity_ordered', ''))}"
        sku_label, sku_value = "SKU:", str(row.get("sku", ""))
        sku_desc_label, sku_desc_value = "SKU Desc:", clean_text(row.get("sku_description", ""))
        order_item_id = str(row.get("order_item_id", "")).strip()

        page.insert_text(fitz.Point(x_item_col, current_y), item_part, fontname=line_style[0], fontsize=line_style[1])
        page.insert_text(fitz.Point(x_qty_col, current_y), qty_part, fontname=fname_style[0], fontsize=fname_style[1])

        barcode_bottom_y = current_y 
        if order_item_id:
            try:
                barcode_x0 = PAGE_W - RIGHT_INDENT - barcode_w
                y_offset = -2
                rect = fitz.Rect(barcode_x0, current_y + y_offset, barcode_x0 + barcode_w, current_y + barcode_h + y_offset)
                with fitz.open("pdf", _create_barcode_pdf_in_memory(order_item_id, barcode_w, barcode_h)) as barcode_doc:
                    page.show_pdf_page(rect, barcode_doc, 0)
                text_y = rect.y1 + 2
                text_len = fitz.get_text_length(order_item_id, fontname='helvetica', fontsize=9)
                text_x = barcode_x0 + (barcode_w - text_len) / 2
                page.insert_text(fitz.Point(text_x, text_y + 8), order_item_id, fontname='helvetica', fontsize=9)
                barcode_bottom_y = text_y + 10 
            except Exception: pass

        if sku_desc_value:
            label_w = fitz.get_text_length(sku_desc_label, fontname=fname_style[0], fontsize=fname_style[1])
            page.insert_text(fitz.Point(x_label_right_edge - label_w, current_y), sku_desc_label, fontname=fname_style[0], fontsize=fname_style[1])
            words, current_line = sku_desc_value.split(), []; line_y = current_y
            for word in words:
                if fitz.get_text_length(' '.join(current_line + [word]), fontname=fval_style[0], fontsize=fval_style[1]) > max_value_width:
                    if current_line: line_y = new_page_check(line_y); page.insert_text(fitz.Point(x_value_col, line_y), ' '.join(current_line), fontname=fval_style[0], fontsize=fval_style[1]); line_y += line_item_height
                    current_line = [word]
                else: current_line.append(word)
            if current_line: line_y = new_page_check(line_y); page.insert_text(fitz.Point(x_value_col, line_y), ' '.join(current_line), fontname=fval_style[0], fontsize=fval_style[1])
            current_y = line_y + line_item_height 

        if sku_value:
            label_w = fitz.get_text_length(sku_label, fontname=fname_style[0], fontsize=fname_style[1])
            page.insert_text(fitz.Point(x_label_right_edge - label_w, current_y), sku_label, fontname=fname_style[0], fontsize=fname_style[1])
            words, current_line = sku_value.split(), []; line_y = current_y 
            for word in words:
                if fitz.get_text_length(' '.join(current_line + [word]), fontname=fval_style[0], fontsize=fval_style[1]) > max_value_width:
                    if current_line: line_y = new_page_check(line_y); page.insert_text(fitz.Point(x_value_col, line_y), ' '.join(current_line), fontname=fval_style[0], fontsize=fval_style[1]); line_y += line_item_height
                    current_line = [word]
                else: current_line.append(word)
            if current_line: line_y = new_page_check(line_y); page.insert_text(fitz.Point(x_value_col, line_y), ' '.join(current_line), fontname=fval_style[0], fontsize=fval_style[1])
            current_y = line_y + line_item_height
        
        final_y = max(line_y, barcode_bottom_y)
        y = final_y + 0.125*72 
        page.draw_line(fitz.Point(LEFT_INDENT, y), fitz.Point(PAGE_W - RIGHT_INDENT, y)); y += 0.1875*72
    
    y += 0.25*72; y = new_page_check(y)
    page.insert_text(fitz.Point(LEFT_INDENT, y), "PRODUCTION INSTRUCTIONS", fontname=sec_style[0], fontsize=sec_style[1]); y += 0.3*72
    instruction_fields = {"general_description": "General Desc.", "paper_description": "Paper Desc.", "press_instructions": "Press Inst.", "bindery_instructions": "Bindery Inst.", "job_ticket_shipping_instructions": "Shipping Inst."}
    for field, display_name in instruction_fields.items():
        value = clean_text(main_row.get(field, ""))
        if not value: continue
        y = new_page_check(y)
        draw_right_aligned(page, f"{display_name}:", y, fname_style[0], fname_style[1])
        words, current_line, line_y = value.split(), [], y
        for word in words:
            if fitz.get_text_length(' '.join(current_line + [word]), fontname=fval_style[0], fontsize=fval_style[1]) > MAX_LINE_WIDTH:
                if current_line: line_y = new_page_check(line_y); page.insert_text(fitz.Point(FIELD_VALUE_X, line_y), ' '.join(current_line), fontname=fval_style[0], fontsize=fval_style[1]); line_y += 0.2 * 72
                current_line = [word]
            else: current_line.append(word)
        if current_line: line_y = new_page_check(line_y); page.insert_text(fitz.Point(FIELD_VALUE_X, line_y), ' '.join(current_line), fontname=fval_style[0], fontsize=fval_style[1])
        y = line_y + (0.2 * 72) + 0.05 * 72
    return doc

def process_dataframe(df, files_path, tickets_path, sheet_name, watermark_path=None):
    is_gang_run = GANG_RUN_TRIGGER in sheet_name.upper()
    job_type = "GANG RUN" if is_gang_run else "STANDARD"
    utils_ui.print_section(f"Sheet: {sheet_name} ({job_type})")

    ticket_col_name = "job_ticket_number"
    def parse_job_number(job_str):
        job_str = str(job_str).strip()
        if '-' in job_str:
            parts = job_str.rsplit('-', 1); base, suffix = parts[0], parts[1]
            if not suffix.isdigit(): return job_str, '1'
            return base, suffix
        else: return job_str, '1'

    parsed_data = df[ticket_col_name].apply(parse_job_number)
    df[['base_job_number', 'line_item_suffix']] = pd.DataFrame(parsed_data.tolist(), index=parsed_data.index, columns=['base_job_number', 'line_item_suffix'])
    
    global_counts = df.groupby('base_job_number').size().to_dict()
    total_counts_map = {str(k): v for k, v in global_counts.items()}
    
    rows_with_index = list(df.reset_index().to_dict('records'))
    rows_with_index.sort(key=lambda r: str(r.get("job_ticket_number", "")))
    
    grouped_jobs = []
    for base_job_num, group in groupby(rows_with_index, key=lambda r: r.get('base_job_number', "")):
        grouped_jobs.append((base_job_num, list(group)))

    if grouped_jobs:
        utils_ui.print_info(f"Generating tickets for {len(grouped_jobs)} jobs...")
        with utils_ui.create_progress() as progress:
            ticket_task = progress.add_task("Generating Tickets...", total=len(grouped_jobs))
            
            for base_job_num, ticket_rows in grouped_jobs:
                if "blank" in str(base_job_num).lower(): 
                    progress.update(ticket_task, advance=1); continue
                
                final_doc = None
                try:
                    base_name = sanitize_filename(str(base_job_num))
                    combined_path = os.path.join(tickets_path, f"{base_name}_TICKETwPROOFS.pdf")
                    final_doc = generate_ticket_pymupdf(ticket_rows, base_job_num, gang_run_name=None, total_counts_map=total_counts_map, sheet_name=sheet_name, watermark_path=watermark_path)

                    for row in ticket_rows:
                        # Logic to find the downloaded file
                        file_base = sanitize_filename(str(row.get("job_ticket_number")))
                        production_artwork_path = os.path.join(files_path, f"{file_base}.pdf")
                        
                        if os.path.exists(production_artwork_path):
                            proof_source_path = production_artwork_path
                            sku_val, qty_val = clean_text(row.get('sku')), clean_text(row.get('quantity_ordered'))
                            proof_doc = create_proof_in_memory(proof_source_path, production_artwork_path, str(row.get("order_number")), sku=sku_val, qty=qty_val)
                            if proof_doc: final_doc.insert_pdf(proof_doc); proof_doc.close()
                
                    if final_doc and final_doc.page_count > 0:
                        pdf_buffer = BytesIO(); final_doc.save(pdf_buffer, garbage=4, deflate=True); pdf_buffer.seek(0)
                        reader = PdfReader(pdf_buffer); writer = PdfWriter(); writer.append_pages_from_reader(reader)
                        writer.root_object[NameObject("/ViewerPreferences")] = DictionaryObject({NameObject("/Duplex"): NameObject("/Simplex"), NameObject("/Staple"): NameObject("/TopRight")})
                        with open(combined_path, "wb") as f_out: writer.write(f_out)
                except Exception as e: utils_ui.print_error(f"Error processing ticket {base_job_num}: {e}")
                finally:
                    if final_doc and not final_doc.is_closed: final_doc.close()
                
                progress.update(ticket_task, advance=1)

def main(input_excel_path, files_base_folder, tickets_base_folder, central_config_json):
    utils_ui.setup_logging(None)
    utils_ui.print_banner("40b - Generate Job Tickets")
    start_time = time.time()

    try: central_config = json.loads(central_config_json)
    except Exception: utils_ui.print_error("Invalid Config JSON"); sys.exit(1)

    watermark_path = central_config.get('WATERMARK_PATH')
    if watermark_path: utils_ui.print_info(f"Watermark: {os.path.basename(watermark_path)}")

    try:
        os.makedirs(tickets_base_folder, exist_ok=True)

        xls = pd.ExcelFile(input_excel_path)
        for sheet_name in xls.sheet_names:
            df = pd.read_excel(xls, sheet_name=sheet_name)
            if 'order_item_id' in df.columns:
                df['order_item_id'] = df['order_item_id'].astype(str).str.strip().apply(lambda x: x[:-2] if x.endswith('.0') else x)
            
            if df.empty: continue

            sanitized_sheet_name = sanitize_filename(sheet_name)
            sheet_files_path = os.path.join(files_base_folder, sanitized_sheet_name)
            sheet_tickets_path = os.path.join(tickets_base_folder, sanitized_sheet_name)
            os.makedirs(sheet_tickets_path, exist_ok=True)

            process_dataframe(df, sheet_files_path, sheet_tickets_path, sheet_name, watermark_path=watermark_path)

    except Exception as e:
        utils_ui.print_error(f"Processing Failed: {e}"); traceback.print_exc(); sys.exit(1)
    
    utils_ui.print_success(f"Ticket Generation Complete: {time.time() - start_time:.2f}s")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="40b - Generate Job Tickets")
    parser.add_argument("input_excel_path", help="Input Excel")
    parser.add_argument("files_base_folder", help="Files source folder (Assets)")
    parser.add_argument("tickets_base_folder", help="Tickets Output Base")
    parser.add_argument("central_config_json", help="Config JSON")
    args = parser.parse_args()

    main(args.input_excel_path, args.files_base_folder, args.tickets_base_folder, args.central_config_json)
