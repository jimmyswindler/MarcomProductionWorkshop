# 60_PreparePressFiles.py
import os
import time
import shutil
import re
import pandas as pd
import traceback
import math
from datetime import datetime, timedelta
from io import BytesIO
import sys
from itertools import groupby
import argparse
import json

# PDF Libraries
import fitz  # PyMuPDF
from pypdf import PdfReader, PdfWriter, PageObject, Transformation
from pypdf.generic import DictionaryObject, NameObject, RectangleObject
from reportlab.lib.pagesizes import letter, landscape
from reportlab.lib.units import inch
from reportlab.lib.utils import ImageReader
from PIL import Image

# --- Define the string that identifies a gang run sheet ---
GANG_RUN_TRIGGER = "-GR-"

# Configuration for Header Pages
HEADER_FONT_SIZE = 18
HEADER_TOP_MARGIN = 72
HEADER_PAGE_WIDTH = 2.25 * 72
HEADER_PAGE_HEIGHT = 3.75 * 72
HEADER_TRIM_WIDTH = 2 * 72
HEADER_TRIM_HEIGHT = 3.5 * 72

def create_header_page(pdf_path, order_number=None, segment=None, total_segments=None, total_quantity=None, background_color=None, store_number=None, half_box_icon_path=None, full_box_icon_path=None):
    """
    Creates a new PDF page in memory to serve as a header. Includes filename,
    order number, and a conditional icon or text line based on new packing rules.
    The bottom of the card contains RASTERIZED previews to prevent PDF self-referencing errors.
    Optionally applies a background color for gang run identification using a CMYK tuple.
    """
    header_doc = None
    src_doc = None
    try:
        header_doc = fitz.open()
        header_page = header_doc.new_page(width=HEADER_PAGE_WIDTH, height=HEADER_PAGE_HEIGHT)
        
        if background_color and len(background_color) == 4:
            header_page.draw_rect(header_page.rect, color=background_color, fill=background_color)
            
        p1_rect, p2_rect = None, None
        is_landscape_original = False
        preview_top_y = HEADER_PAGE_HEIGHT

        trim_x_margin = (HEADER_PAGE_WIDTH - HEADER_TRIM_WIDTH) / 2
        trim_y_margin = (HEADER_PAGE_HEIGHT - HEADER_TRIM_HEIGHT) / 2
        trim_left_edge, trim_right_edge = trim_x_margin, HEADER_PAGE_WIDTH - trim_x_margin
        trim_top_edge, trim_bottom_edge = trim_y_margin, HEADER_PAGE_HEIGHT - trim_y_margin
        offset = 0.0625 * 72

        try:
            src_doc = fitz.open(pdf_path)
            if src_doc.page_count > 0:
                page1 = src_doc[0]
                is_landscape_original = page1.rect.width > page1.rect.height

                if is_landscape_original:
                    available_width = HEADER_TRIM_WIDTH - (2 * offset)
                    scale1 = available_width / page1.rect.width if page1.rect.width > 0 else 0
                    scale = scale1
                    if src_doc.page_count > 1:
                        page2 = src_doc[1]
                        scale2 = available_width / page2.rect.width if page2.rect.width > 0 else 0
                        scale = min(scale1, scale2)

                    p1_w, p1_h = page1.rect.width * scale, page1.rect.height * scale
                    p1_x0 = trim_left_edge + (HEADER_TRIM_WIDTH - p1_w) / 2
                    
                    current_y_bottom = trim_bottom_edge - offset + 27 
                    if src_doc.page_count > 1:
                        page2 = src_doc[1]
                        p2_w, p2_h = page2.rect.width * scale, page2.rect.height * scale
                        p2_x0 = trim_left_edge + (HEADER_TRIM_WIDTH - p2_w) / 2
                        p2_y1 = current_y_bottom
                        p2_y0 = p2_y1 - p2_h
                        p2_rect = fitz.Rect(p2_x0, p2_y0, p2_x0 + p2_w, p2_y1)
                        current_y_bottom = p2_y0 - offset

                    p1_y1 = current_y_bottom
                    p1_y0 = p1_y1 - p1_h
                    p1_rect = fitz.Rect(p1_x0, p1_y0, p1_x0 + p1_w, p1_y1)
                    preview_top_y = p1_rect.y0
                else:
                    scale = 0.62
                    p1_w, p1_h = page1.rect.width * scale, page1.rect.height * scale
                    p1_y1 = trim_bottom_edge - offset + 27; p1_y0 = p1_y1 - p1_h
                    p1_x0 = trim_left_edge + offset
                    p1_rect = fitz.Rect(p1_x0, p1_y0, p1_x0 + p1_w, p1_y1)
                    
                    if src_doc.page_count > 1:
                        page2 = src_doc[1]; p2_w, p2_h = page2.rect.width * scale, page2.rect.height * scale
                        p2_x1 = trim_right_edge - offset; p2_x0 = p2_x1 - p2_w
                        p2_y0 = trim_top_edge + 1.5 * 72 + 27
                        p2_rect = fitz.Rect(p2_x0, p2_y0, p2_x0 + p2_w, p2_y0 + p2_h)
                    preview_top_y = min(p1_rect.y0, p2_rect.y0) if p2_rect else p1_rect.y0
        except Exception as e:
            print(f"  - Could not open/process preview doc: {e}")

        font_reg, font_bold = "helvetica", "helvetica-bold"
        
        lines_to_draw = [{'text': os.path.splitext(os.path.basename(pdf_path))[0], 'base_size': 14, 'font': font_bold}]
        if total_quantity is not None: lines_to_draw.append({'text': f"Total Qty: {total_quantity}", 'base_size': 10, 'font': font_reg})
        if store_number: lines_to_draw.append({'text': f"Store: {store_number}", 'base_size': 14, 'font': font_bold})
        if order_number: lines_to_draw.append({'text': f"Order: {order_number}", 'base_size': 10, 'font': font_reg})
        
        safe_text_width = HEADER_TRIM_WIDTH - (2 * offset)
        text_block_top_y = trim_top_edge + offset
        available_space = preview_top_y - text_block_top_y - 5
        special_line_height = 27 if total_segments else 0

        unscaled_line_spacing = 4
        available_for_text = available_space - special_line_height - (unscaled_line_spacing if total_segments else 0)
        
        horizontal_scale = 1.0
        for line in lines_to_draw:
            line_len = fitz.get_text_length(line['text'], fontname=line['font'], fontsize=line['base_size'])
            if line_len > safe_text_width: horizontal_scale = min(horizontal_scale, safe_text_width / line_len)

        total_unscaled_text_height = sum(line['base_size'] for line in lines_to_draw) + (len(lines_to_draw) - 1) * unscaled_line_spacing
        
        vertical_scale = 1.0
        if available_for_text > 0 and total_unscaled_text_height > 0:
            if total_unscaled_text_height > available_for_text:
                vertical_scale = available_for_text / total_unscaled_text_height
        elif available_for_text <= 0:
            vertical_scale = 0.1
        
        final_scale = min(vertical_scale, horizontal_scale)
        line_spacing = unscaled_line_spacing * final_scale

        total_scaled_text_height = sum(line['base_size'] * final_scale for line in lines_to_draw)
        total_spacing = (len(lines_to_draw) - 1) * line_spacing + (line_spacing if total_segments else 0)
        total_block_height = total_scaled_text_height + total_spacing + special_line_height
        
        current_y = text_block_top_y + (available_space - total_block_height) / 2

        for line in lines_to_draw:
            font_size = line['base_size'] * final_scale
            baseline_y = current_y + font_size
            text_len = fitz.get_text_length(line['text'], fontname=line['font'], fontsize=font_size)
            x = (HEADER_PAGE_WIDTH - text_len) / 2
            header_page.insert_text(fitz.Point(x, baseline_y), line['text'], fontname=line['font'], fontsize=font_size)
            current_y = baseline_y + line_spacing

        stacks_per_box = 2
        is_completion_stack = (segment % stacks_per_box == 0) or (segment == total_segments)

        if total_segments and is_completion_stack:
            half_box_doc, full_box_doc = None, None
            try:
                def place_icon(page, doc, x_pos, y_pos):
                    if not doc or doc.page_count == 0: return 0
                    icon_page = doc[0]
                    aspect_ratio = icon_page.rect.width / icon_page.rect.height if icon_page.rect.height > 0 else 1
                    target_h = 27
                    target_w = target_h * aspect_ratio
                    target_rect = fitz.Rect(x_pos, y_pos, x_pos + target_w, y_pos + target_h)
                    page.show_pdf_page(target_rect, doc, 0)
                    return target_w

                if total_quantity == 250:
                    if half_box_icon_path and os.path.exists(half_box_icon_path):
                        half_box_doc = fitz.open(half_box_icon_path)
                        icon_w = place_icon(header_page, half_box_doc, -999, current_y) 
                        start_x = (HEADER_PAGE_WIDTH - icon_w) / 2
                        place_icon(header_page, half_box_doc, start_x, current_y)
                    else: print(f" - WARNING: Icon PDF not found at {half_box_icon_path}")
                elif total_quantity == 500:
                    if full_box_icon_path and os.path.exists(full_box_icon_path):
                        full_box_doc = fitz.open(full_box_icon_path)
                        icon_w = place_icon(header_page, full_box_doc, -999, current_y)
                        start_x = (HEADER_PAGE_WIDTH - icon_w) / 2
                        place_icon(header_page, full_box_doc, start_x, current_y)
                    else: print(f" - WARNING: Icon PDF not found at {full_box_icon_path}")
                elif total_quantity == 1000:
                    if full_box_icon_path and os.path.exists(full_box_icon_path):
                        full_box_doc = fitz.open(full_box_icon_path)
                        box_gap = 4
                        icon_w = place_icon(header_page, full_box_doc, -999, -999)
                        total_icon_width = (2 * icon_w) + box_gap
                        start_x = (HEADER_PAGE_WIDTH - total_icon_width) / 2
                        place_icon(header_page, full_box_doc, start_x, current_y)
                        place_icon(header_page, full_box_doc, start_x + icon_w + box_gap, current_y)
                    else: print(f" - WARNING: Icon PDF not found at {full_box_icon_path}")
            finally:
                if half_box_doc and not half_box_doc.is_closed: half_box_doc.close()
                if full_box_doc and not full_box_doc.is_closed: full_box_doc.close()
        elif total_segments and total_segments > 1:
            stack_text = f"Stack {segment} of {total_segments}"
            font_size = 10 * final_scale
            text_len = fitz.get_text_length(stack_text, fontname=font_reg, fontsize=font_size)
            x = (HEADER_PAGE_WIDTH - text_len) / 2
            baseline_y = current_y + (27 - font_size) / 2 + font_size
            header_page.insert_text(fitz.Point(x, baseline_y), stack_text, fontname=font_reg, fontsize=font_size)

        if src_doc and src_doc.page_count > 0:
            def render_preview(page_index, rect):
                pix = src_doc[page_index].get_pixmap(dpi=144)
                header_page.insert_image(rect, stream=pix.tobytes("png"))
                header_page.draw_rect(rect, color=(0,0,0), width=0.5)
            if p1_rect: render_preview(0, p1_rect)
            if p2_rect and src_doc.page_count > 1: render_preview(1, p2_rect)

        packet = BytesIO()
        header_doc.save(packet, garbage=4, deflate=True)
        packet.seek(0)
        final_header_page = PdfReader(packet).pages[0]
        trimbox_coords = [trim_x_margin, trim_y_margin, HEADER_PAGE_WIDTH - trim_x_margin, HEADER_PAGE_HEIGHT - trim_y_margin]
        final_header_page.trimbox = RectangleObject(trimbox_coords)
        return final_header_page
    finally:
        if header_doc and not header_doc.is_closed: header_doc.close()
        if src_doc and not src_doc.is_closed: src_doc.close()

def add_segmented_headers_to_pdf(orientation_check_path, target_pdf_path, order_number=None, total_quantity=None, background_color=None, store_number=None, half_box_icon_path=None, full_box_icon_path=None):
    try:
        reader = PdfReader(target_pdf_path)
        total_pages = len(reader.pages)
        if total_pages == 0: return False

        num_segments = (total_pages + 499) // 500
        print(f"  - Adding headers to {os.path.basename(target_pdf_path)} ({total_pages} pages -> {num_segments} segment(s))")
        writer = PdfWriter()
        
        x_margin = (HEADER_PAGE_WIDTH - HEADER_TRIM_WIDTH) / 2
        y_margin = (HEADER_PAGE_HEIGHT - HEADER_TRIM_HEIGHT) / 2
        centered_trimbox = RectangleObject([x_margin, y_margin, HEADER_PAGE_WIDTH - x_margin, HEADER_PAGE_HEIGHT - y_margin])
        blank_header_page = PageObject.create_blank_page(width=HEADER_PAGE_WIDTH, height=HEADER_PAGE_HEIGHT)
        blank_header_page.trimbox = centered_trimbox

        for i in range(num_segments):
            text_header = create_header_page(
                orientation_check_path, 
                order_number, 
                i + 1, 
                num_segments, 
                total_quantity, 
                background_color,
                store_number=store_number,
                half_box_icon_path=half_box_icon_path,
                full_box_icon_path=full_box_icon_path
            )
            writer.add_page(text_header)
            writer.add_page(blank_header_page)
            start_index, end_index = i * 500, (i + 1) * 500
            for page in reader.pages[start_index:end_index]: writer.add_page(page)

        with open(target_pdf_path, "wb") as out_file: writer.write(out_file)
        return True
    except Exception as e:
        print(f"  - FAILED to add segmented headers to {os.path.basename(target_pdf_path)}: {e}")
        traceback.print_exc()
        return False

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

def natural_keys(text):
    """
    Helper function for natural sorting.
    Splits text into a list of integers and strings.
    e.g., 'CLB3548-01' -> ['CLB', 3548, '-', 1]
    """
    return [int(c) if c.isdigit() else c for c in re.split(r'(\d+)', str(text))]

def standardize_pdf_for_gang_run(pdf_path):
    try:
        reader_check = PdfReader(pdf_path)
        if not reader_check.pages or reader_check.pages[0].mediabox.width <= reader_check.pages[0].mediabox.height:
            return False
        print(f"  - Landscape file detected: {os.path.basename(pdf_path)}. Applying odd/even rotation...")
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
                "mediabox": original_page.mediabox, "cropbox": getattr(original_page, "cropbox", original_page.mediabox),
                "bleedbox": getattr(original_page, "bleedbox", original_page.mediabox), "trimbox": getattr(original_page, "trimbox", original_page.mediabox),
                "artbox": getattr(original_page, "artbox", original_page.mediabox)
            }
            for box_name, box_obj in boxes_to_transform.items():
                setattr(new_page, box_name, recalculate_box(box_obj))
            new_page.merge_transformed_page(original_page, transform)
            writer.add_page(new_page)
        with open(pdf_path, "wb") as out_file:
            writer.write(out_file)
        print(f"  - Standardization complete for: {os.path.basename(pdf_path)}")
        return True
    except Exception as e:
        print(f"  - Standardization failed for {os.path.basename(pdf_path)}: {e}")
        traceback.print_exc()
        return False

def process_dataframe(df, files_path, originals_path, sheet_name, color_palette_path=None, icon_paths=None):
    """
    Reads the dataframe and prepares the color map for gang runs based on:
    1. Quantity (Highest Priority) -> Descending
    2. Job Ticket Number (Secondary Priority) -> Natural Sort Ascending
    
    Only the top N items (where N is palette size) get a color. 
    Others default to No Color (White).
    """
    is_gang_run = GANG_RUN_TRIGGER in sheet_name.upper()
    gang_run_name = sanitize_filename(sheet_name) if is_gang_run else None

    if is_gang_run:
        print(f"\nProcessing sheet '{sheet_name}' as a GANG RUN (Press Prep).")
    else:
        print(f"\nProcessing sheet '{sheet_name}' as STANDARD (Press Prep). No modifications needed.")
        if not is_gang_run:
            return

    start_time = time.time()
    
    color_map = None
    if is_gang_run:
        loaded_palette = []
        try:
            if color_palette_path and os.path.exists(color_palette_path):
                palette_df = pd.read_csv(color_palette_path, encoding='utf-8-sig')
                required_cols = ['C', 'M', 'Y', 'K']
                if all(col in palette_df.columns for col in required_cols):
                    loaded_palette = [tuple(row) for row in palette_df[required_cols].to_numpy()]
                    print(f"  - Successfully loaded {len(loaded_palette)} colors from palette file.")
                else:
                    print(f"  - WARNING: Palette file is missing required CMYK columns.")
            else:
                print(f"  - WARNING: Color palette file not found at {color_palette_path}.")
        except Exception as e:
            print(f"  - WARNING: Could not load or parse color palette. Error: {e}")

        if loaded_palette:
            # Prepare data for sorting without modifying original DF index
            sort_data = []
            for idx, row in df.iterrows():
                # Extract Quantity
                try:
                    qty_val = pd.to_numeric(row.get("quantity_ordered"), errors='coerce')
                    qty = int(qty_val) if pd.notna(qty_val) else 0
                except:
                    qty = 0
                
                # Extract Job Ticket
                ticket = str(row.get("job_ticket_number", ""))
                
                sort_data.append({
                    'original_index': idx,
                    'qty': qty,
                    'ticket': ticket
                })

            # Define sorting key:
            # 1. Quantity DESCENDING (use negative for reverse sort)
            # 2. Job Ticket ASCENDING (using natural keys)
            def priority_sort_key(item):
                return (-item['qty'], natural_keys(item['ticket']))

            # Sort the list
            sort_data.sort(key=priority_sort_key)
            print("  - Sorted jobs by Priority (Qty Desc -> Job# Asc).")

            # Assign colors strictly to the top items
            color_map = {}
            palette_len = len(loaded_palette)
            
            for rank, item in enumerate(sort_data):
                # If we have colors left in the palette, assign one
                if rank < palette_len:
                    color_map[item['original_index']] = loaded_palette[rank]
                else:
                    # Run out of colors - implicitly handled as None (White) in process_rows/create_header
                    pass
            
            print(f"  - Assigned colors to top {min(len(sort_data), palette_len)} jobs. Remaining {max(0, len(sort_data) - palette_len)} jobs will be White.")

    # Process all rows. This script modifies files individually.
    process_rows(
        rows=df,
        files_path=files_path,
        originals_path=originals_path,
        is_gang_run=is_gang_run,
        sheet_start_time=start_time,
        color_map=color_map,
        icon_paths=icon_paths
    )
    
    elapsed = time.time() - start_time
    print(f"\nPress prep complete for sheet '{sheet_name}' - Total time: {int(elapsed//3600):02d}:{int((elapsed%3600)//60):02d}:{int(elapsed%60):02d}")

def process_rows(rows, files_path, originals_path, is_gang_run=False, sheet_start_time=0, color_map=None, icon_paths=None):
    """
    Iterates over rows and performs IN-PLACE press preparation on the files found in 'files_path'.
    """
    
    for idx, row in rows.iterrows():
        try:
            file_base = sanitize_filename(str(row.get("job_ticket_number")))
            production_artwork_path = os.path.join(files_path, f"{file_base}.pdf")

            if not os.path.exists(production_artwork_path):
                print(f"  - WARNING: Artwork file not found, skipping press prep for {file_base}.pdf")
                continue

            file_was_modified = False
            
            if is_gang_run:
                os.makedirs(originals_path, exist_ok=True)
                archived_original_path = os.path.join(originals_path, os.path.basename(production_artwork_path))
                shutil.copy2(production_artwork_path, archived_original_path)
                
                try:
                    reader = PdfReader(archived_original_path)
                    if len(reader.pages) in [1, 2]:
                        quantity = int(pd.to_numeric(row.get("quantity_ordered"), errors='coerce'))
                        if quantity > 1:
                            writer = PdfWriter()
                            if len(reader.pages) == 1:
                                art_page = reader.pages[0]
                                blank_page = PageObject.create_blank_page(width=art_page.mediabox.width, height=art_page.mediabox.height)
                                for box in ["mediabox", "cropbox", "bleedbox", "trimbox", "artbox"]:
                                    if hasattr(art_page, box): setattr(blank_page, box, getattr(art_page, box))
                                for _ in range(quantity): writer.add_page(art_page); writer.add_page(blank_page)
                            else:
                                for _ in range(quantity): writer.add_page(reader.pages[0]); writer.add_page(reader.pages[1])
                            with open(production_artwork_path, "wb") as f_out: writer.write(f_out)
                except Exception as e:
                    print(f"  - FAILED to duplicate file {os.path.basename(production_artwork_path)}: {e}"); traceback.print_exc()
                
                if standardize_pdf_for_gang_run(production_artwork_path): file_was_modified = True
                
                line_item_quantity = int(pd.to_numeric(row.get('quantity_ordered'), errors='coerce') or 0)
                
                # Lookup color in map. If not found (low priority), returns None (White).
                file_color = color_map.get(idx) if color_map else None
                
                store_number = ""
                cost_center_val = row.get("cost_center")
                if pd.notna(cost_center_val):
                    store_number = str(cost_center_val).split('-')[0].strip()

                if add_segmented_headers_to_pdf(
                    orientation_check_path=archived_original_path,
                    target_pdf_path=production_artwork_path,
                    order_number=str(row.get("order_number", "")),
                    total_quantity=line_item_quantity,
                    background_color=file_color,
                    store_number=store_number,
                    half_box_icon_path=icon_paths.get('HALF_BOX_ICON_PATH'),
                    full_box_icon_path=icon_paths.get('FULL_BOX_ICON_PATH')
                ): file_was_modified = True

            elapsed = time.time() - sheet_start_time
            sys.stdout.write(f"Pre-pressing file {file_base} - elapsed: {int(elapsed//3600):02d}:{int((elapsed%3600)//60):02d}:{int(elapsed%60):02d}\n"); sys.stdout.flush()
        
        except Exception as e:
            print(f"\nError processing press prep for row {idx}: {e}"); traceback.print_exc()

def main(input_excel_path, files_base_folder, originals_base_folder, central_config_json):
    """
    Main processing function for 60.
    Accepts central_config as JSON string.
    """
    start_time = time.time()
    filename = os.path.basename(input_excel_path)
    print(f"Processing File (Press Prep): {filename}")

    # --- Load config from JSON string ---
    try:
        central_config = json.loads(central_config_json)
        print("✓ Successfully loaded configuration from controller.")
    except json.JSONDecodeError as e:
        print(f"FATAL ERROR: Could not parse central_config JSON: {e}")
        sys.exit(1)

    # Get config paths
    color_palette_path = central_config.get('COLOR_PALETTE_PATH')
    icon_paths = {
        'HALF_BOX_ICON_PATH': central_config.get('HALF_BOX_ICON_PATH'),
        'FULL_BOX_ICON_PATH': central_config.get('FULL_BOX_ICON_PATH')
    }
    
    if color_palette_path:
        print(f"Using Color Palette: {color_palette_path}")
    else:
        print("No Color Palette path provided in config.")
        
    if icon_paths['HALF_BOX_ICON_PATH']:
        print(f"Using Half Box Icon: {icon_paths['HALF_BOX_ICON_PATH']}")
    if icon_paths['FULL_BOX_ICON_PATH']:
        print(f"Using Full Box Icon: {icon_paths['FULL_BOX_ICON_PATH']}")

    try:
        os.makedirs(originals_base_folder, exist_ok=True)

        xls = pd.ExcelFile(input_excel_path)
        for sheet_name in xls.sheet_names:
            print(f"\nProcessing sheet: {sheet_name}")
            
            if GANG_RUN_TRIGGER not in sheet_name.upper():
                print(f"   - Skipping non-gang run sheet '{sheet_name}'.")
                continue

            df = pd.read_excel(xls, sheet_name=sheet_name)
            print(f"    - Discovered {len(df)} rows.")
            
            sanitized_sheet_name = sanitize_filename(sheet_name)
            sheet_files_path = os.path.join(files_base_folder, sanitized_sheet_name)
            sheet_originals_path = os.path.join(originals_base_folder, sanitized_sheet_name)
            
            if not os.path.isdir(sheet_files_path):
                 print(f"   - WARNING: Input files path not found, skipping sheet: {sheet_files_path}")
                 continue

            process_dataframe(
                df, 
                sheet_files_path, 
                sheet_originals_path, 
                sheet_name,
                color_palette_path=color_palette_path,
                icon_paths=icon_paths
            )
            
    except Exception as e:
        print(f"Error processing spreadsheet {filename}: {e}"); traceback.print_exc()
        sys.exit(1)
    elapsed = time.time() - start_time
    print(f"\nTotal press prep time for file: {int(elapsed//3600):02d}:{int((elapsed%3600)//60):02d}:{int(elapsed%60):02d}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="60 - Prepare Press Files (Archive, Duplicate, Rotate, Add Headers).")
    parser.add_argument("input_excel_path", help="Path to the input Excel file (used to find files to modify).")
    parser.add_argument("files_base_folder", help="Path to the base FILES folder (e.g., .../output/FILES) where artwork from 50 was saved.")
    parser.add_argument("originals_folder", help="Path to the base ORIGINALS folder (e.g., .../output/ORIGINALS) where originals will be archived.")
    parser.add_argument("central_config_json", help="Central configuration dictionary (subset with COLOR/ICON paths) passed as a JSON string.")

    args = parser.parse_args()

    print("--- Starting 60: Prepare Press Files ---")
    main(args.input_excel_path, args.files_base_folder, args.originals_folder, args.central_config_json)
    print("--- Finished 60 ---")