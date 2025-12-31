# 30_PdfRunlistGenerator.py
import pandas as pd
import os
import yaml
import sys
import traceback
import time
import json
import argparse
import utils_ui  # <--- New UI Utility

# --- PDF Generation Libraries ---
try:
    from reportlab.pdfgen import canvas
    from reportlab.lib import colors
    from reportlab.lib.pagesizes import ELEVENSEVENTEEN
    from reportlab.lib.units import inch
    from reportlab.lib.utils import simpleSplit
    from reportlab.pdfbase import pdfmetrics
    from reportlab.pdfbase.ttfonts import TTFont
except ImportError:
    utils_ui.print_error("Required library not found. Please install 'reportlab': pip install reportlab")
    sys.exit(1)

# =========================================================
# GLOBAL FONT CONSTANTS
# =========================================================
CUSTOM_FONT_REGULAR = "Calibri-Light"
CUSTOM_FONT_BOLD = "Calibri-Bold"

def register_custom_fonts(config):
    global CUSTOM_FONT_REGULAR, CUSTOM_FONT_BOLD
    paths = config.get('paths', {})
    light_path = paths.get('calibri_light_font_path')
    bold_path = paths.get('calibri_bold_font_path')

    if not light_path or not bold_path:
        utils_ui.print_warning("Font paths missing in config.yaml. Using Helvetica.")
        CUSTOM_FONT_REGULAR = "Helvetica"
        CUSTOM_FONT_BOLD = "Helvetica-Bold"
        return False
        
    try:
        if not os.path.exists(light_path) or not os.path.exists(bold_path):
            raise FileNotFoundError(f"Calibri TTF files not found at: {light_path} or {bold_path}")

        pdfmetrics.registerFont(TTFont(CUSTOM_FONT_REGULAR, light_path))
        pdfmetrics.registerFont(TTFont(CUSTOM_FONT_BOLD, bold_path))
        # utils_ui.print_info(f"Fonts registered: {CUSTOM_FONT_REGULAR}, {CUSTOM_FONT_BOLD}")
        return True

    except Exception as e:
        utils_ui.print_warning(f"Font registration failed: {e}. Using Helvetica.")
        CUSTOM_FONT_REGULAR = "Helvetica"
        CUSTOM_FONT_BOLD = "Helvetica-Bold"
        return False

def load_run_history(history_path="run_history.yaml"):
    if not os.path.exists(history_path):
        utils_ui.print_info("Creating default run history file.")
        default_history = {'monthly_pace_job_number': 100000, 'last_used_gang_run_suffix': 0}
        with open(history_path, 'w') as f: yaml.dump(default_history, f)
        return default_history
    try:
        with open(history_path, 'r') as f: return yaml.safe_load(f)
    except Exception as e:
        utils_ui.print_error(f"Could not parse run history: {e}"); sys.exit(1)

# =========================================================
# PDF GENERATION
# =========================================================
def generate_pdf_run_list(excel_path, pdf_path, config, history, fragmentation_map=None):
    utils_ui.print_section("Generating PDF Run List")
    
    if fragmentation_map is None:
        fragmentation_map = {'store_report_map': {}, 'unclaimed_report_map': {}}
    store_report_map = fragmentation_map.get('store_report_map', {})
    unclaimed_report_map = fragmentation_map.get('unclaimed_report_map', {})
    
    c = canvas.Canvas(pdf_path, pagesize=ELEVENSEVENTEEN); width, height = ELEVENSEVENTEEN
    margin = 0.375 * inch; frame_padding = 0.05 * inch; printable_width = width - 2 * margin
    store_padding = 5
    col_names = config.get('column_names', {})
    col_order_num = col_names.get('order_number')
    col_base_job = 'Base Job Ticket Number'
    col_cost_center = col_names.get('cost_center')
    col_prod_id = col_names.get('product_id')

    default_widths = {
        'Job\nNumber': 1.*inch, 'Order\nNumber': 1.125*inch, 'Store\nNumber': 0.75*inch,
        'Product\nID': 0.75*inch, 'Qty': 0.625*inch, 'SKU': 2.625*inch,
        'Product\nDescription': 2.625*inch
    }
    
    sku_col = 'SKU'; desc_col = 'Product\nDescription'; sku_source = col_names.get('sku'); desc_source = col_names.get('product_description')
    fixed_cols_width = default_widths['Job\nNumber'] + default_widths['Order\nNumber'] + default_widths['Store\nNumber'] + default_widths['Product\nID'] + default_widths['Qty']
    available_table_width = (printable_width - (2 * frame_padding) - (2 * store_padding)) - fixed_cols_width
    dynamic_cols_default_width = default_widths[sku_col] + default_widths[desc_col]

    if dynamic_cols_default_width > available_table_width:
        # utils_ui.print_info("Adjusting PDF SKU/Desc widths.")
        excess = dynamic_cols_default_width - available_table_width; min_width = 1.5 * inch
        current_sku_w = default_widths[sku_col]; current_desc_w = default_widths[desc_col]
        total_adj_width = current_sku_w + current_desc_w
        if total_adj_width > 0:
            sku_prop = current_sku_w / total_adj_width; desc_prop = current_desc_w / total_adj_width
            default_widths[sku_col] = max(min_width, current_sku_w - excess * sku_prop)
            default_widths[desc_col] = max(min_width, current_desc_w - excess * desc_prop)

    PDF_COLS = {
        'Job\nNumber': {'source': col_names.get('job_ticket_number'), 'width': default_widths['Job\nNumber'], 'align': 'left'},
        'Qty': {'source': col_names.get('quantity_ordered'), 'width': default_widths['Qty'], 'align': 'center'},
        'Order\nNumber': {'source': col_order_num, 'width': default_widths['Order\nNumber'], 'align': 'center'},
        'Store\nNumber': {'source': col_cost_center, 'width': default_widths['Store\nNumber'], 'align': 'center'},
        'Product\nID': {'source': col_prod_id, 'width': default_widths['Product\nID'], 'align': 'center'},
        'SKU': {'source': sku_source, 'width': default_widths[sku_col], 'align': 'left'},
        'Product\nDescription': {'source': desc_source, 'width': default_widths[desc_col], 'align': 'left'}
    }
    if not all(v['source'] for v in PDF_COLS.values()): utils_ui.print_warning("PDF column sources missing.")

    def _build_fragmentation_lines(entities_to_report, store_report_map, unclaimed_report_map, sheet_name):
        messages_to_build = set()
        def get_store_num(store_id_str): return str(store_id_str).split('-', 1)[0].strip()
        def format_dests(dests_list, current_sheet): return ", ".join(sorted([str(d) for d in dests_list if d != current_sheet]))
            
        for (store_id, order_id, job_id) in entities_to_report:
            # 1. Check Store Report
            map_store = store_report_map.get(store_id)
            if map_store and map_store.get('is_fragmented'):
                store_num_display = get_store_num(store_id); context_parts = []
                map_order = map_store.get('fragmented_orders', {}).get(order_id)
                if map_order and map_order.get('is_fragmented'):
                    context_parts.append(f"Order {order_id}")
                    map_job = map_order.get('fragmented_jobs', {}).get(job_id)
                    if map_job and map_job.get('is_fragmentED'): context_parts.append(f"JOB {job_id}")
                dests_display = format_dests(map_store.get('destinations', []), sheet_name)
                if dests_display:
                    msg = f"Store {store_num_display} ({', '.join(context_parts)}) has content in: {dests_display}" if context_parts else f"Store {store_num_display} has content in: {dests_display}"
                    messages_to_build.add(msg)
            
            # 2. Check Unclaimed Orders (Parent store not reported/fragmented)
            unclaimed_orders = unclaimed_report_map.get('orders', {})
            if order_id in unclaimed_orders:
                u_order = unclaimed_orders[order_id]
                store_num_display = get_store_num(store_id)
                
                # Check Order itself
                if u_order.get('is_fragmented'):
                    dests_display = format_dests(u_order.get('destinations', []), sheet_name)
                    if dests_display:
                        messages_to_build.add(f"Store {store_num_display} (Order {order_id}) has content in: {dests_display}")
                
                # Check Jobs within Unclaimed Order
                u_jobs_in_order = u_order.get('fragmented_jobs', {})
                if job_id in u_jobs_in_order:
                     u_job = u_jobs_in_order[job_id]
                     if u_job.get('is_fragmented'):
                         dests_display = format_dests(u_job.get('destinations', []), sheet_name)
                         if dests_display:
                             messages_to_build.add(f"Store {store_num_display} (Order {order_id}, Job {job_id}) has content in: {dests_display}")

            # 3. Check Unclaimed Jobs (Directly)
            unclaimed_jobs = unclaimed_report_map.get('jobs', {})
            if job_id in unclaimed_jobs:
                u_job = unclaimed_jobs[job_id]
                if u_job.get('is_fragmented'):
                    dests_display = format_dests(u_job.get('destinations', []), sheet_name)
                    if dests_display:
                         store_num_display = get_store_num(store_id)
                         messages_to_build.add(f"Store {store_num_display} (Job {job_id}) has content in: {dests_display}")

        return sorted(list(messages_to_build))

    def draw_message_block(c, lines_to_draw, current_y, font_size, line_height, start_x, drawable_width, page_bottom_y, new_page_callback):
        if not lines_to_draw: return current_y, False
        did_page_break = False; current_y -= 5
        for line in lines_to_draw:
            if current_y - line_height < page_bottom_y: current_y, did_page_break = new_page_callback()
            c.setFont(CUSTOM_FONT_BOLD, font_size); c.setFillColor(colors.black)
            wrapped_lines = simpleSplit(line, CUSTOM_FONT_BOLD, font_size, drawable_width)
            for w_line in wrapped_lines:
                if current_y - line_height < page_bottom_y: 
                    current_y, did_page_break = new_page_callback(); c.setFont(CUSTOM_FONT_BOLD, font_size); c.setFillColor(colors.black)
                c.drawString(start_x, current_y - (font_size * 0.9), w_line); current_y -= line_height
        return current_y, did_page_break

    try:
        if not os.path.exists(excel_path): utils_ui.print_error(f"Excel file not found: {excel_path}"); return False
        xls = pd.ExcelFile(excel_path)
        
        for sheet_name in xls.sheet_names:
            try: df_sheet = xls.parse(sheet_name)
            except Exception as e: utils_ui.print_warning(f"Cannot parse sheet '{sheet_name}'. {e}"); continue
            if df_sheet.empty: continue
            
            # --- Get Dates ---
            order_date_col = col_names.get('order_date'); ship_date_col = col_names.get('ship_date'); earliest_order_date, earliest_ship_date = pd.NaT, pd.NaT
            if order_date_col and order_date_col in df_sheet.columns: df_sheet[order_date_col] = pd.to_datetime(df_sheet[order_date_col], errors='coerce'); earliest_order_date = df_sheet[order_date_col].min()
            if ship_date_col and ship_date_col in df_sheet.columns: df_sheet[ship_date_col] = pd.to_datetime(df_sheet[ship_date_col], errors='coerce'); earliest_ship_date = df_sheet[ship_date_col].min()
            order_date_str = earliest_order_date.strftime('%a %m/%d/%Y') if pd.notna(earliest_order_date) else "N/A"
            ship_date_str = earliest_ship_date.strftime('%a %m/%d/%Y') if pd.notna(earliest_ship_date) else "N/A"

            header_height, footer_height, row_height, table_header_height = 0.875*inch, 0.5*inch, 0.22*inch, (0.5*inch) + 7
            header_gap = 0.22 * inch; store_gap = 0.22 * inch; order_gap = 0.22 * inch; job_gap = 0.22 * inch
            order_box_line_width = 2.0; text_cell_padding = 5; page_bottom_margin_y = margin + footer_height
            frag_msg_font_size = 14; frag_msg_line_height = frag_msg_font_size * 1.3
            frag_msg_start_x = margin + frame_padding + 5; frag_msg_drawable_width = printable_width - (2 * frame_padding) - 10
            page_bottom_content_area_y = page_bottom_margin_y + (0.5 * inch)
            
            if not all(col in df_sheet.columns for col in [col_cost_center, col_order_num, col_base_job]): 
                utils_ui.print_error("Missing required columns for PDF generation."); continue
            
            try:
                df_sheet[col_cost_center] = df_sheet[col_cost_center].astype(str).fillna('N/A')
                df_sheet[col_order_num] = df_sheet[col_order_num].astype(str).fillna('N/A')
                df_sheet[col_base_job] = df_sheet[col_base_job].astype(str).fillna('N/A')
                # Sort by Job Ticket Number only
                sort_col = col_names.get('job_ticket_number')
                
                if sort_col and sort_col in df_sheet.columns:
                    df_sheet.sort_values(by=[sort_col], inplace=True)
                else:
                    df_sheet.sort_values(by=[col_base_job, col_order_num], inplace=True)
                df_sheet.reset_index(drop=True, inplace=True)
            except Exception as e: utils_ui.print_error(f"Sort failed for sheet '{sheet_name}'. {e}"); continue
                
            current_row_index = 0; page_num = 0; is_continuing_store_box = False
            current_store, current_order, current_job = None, None, None; entities_in_current_store_box = set(); store_start_y = None

            def draw_new_page_headers(page_num):
                label_font, label_size = CUSTOM_FONT_BOLD, 14
                value_font, value_size = CUSTOM_FONT_BOLD, 22
                y_label = height - margin - 0.25 * inch; y_value = y_label - 26
                c.setFont(value_font, value_size); max_sheet_name_width = (width / 3) - 10; sheet_name_display = sheet_name
                if c.stringWidth(sheet_name_display, value_font, value_size) > max_sheet_name_width and len(sheet_name_display) > 5: sheet_name_display = sheet_name_display[:-4] + "..."
                sheet_name_width = c.stringWidth(sheet_name_display, value_font, value_size); left_x_start = margin + 5; c.drawString(left_x_start, y_value, sheet_name_display)
                c.setFont(label_font, label_size); job_num_center_x = left_x_start + (sheet_name_width / 2); c.drawCentredString(job_num_center_x, y_label, str(history.get('monthly_pace_job_number', 'N/A')))
                c.setFont(value_font, value_size); right_x_end = width - margin - 5; c.drawRightString(right_x_end, y_value, ship_date_str)
                c.setFont(label_font, label_size); c.drawCentredString(right_x_end - (c.stringWidth(ship_date_str, value_font, value_size) / 2), y_label, "Ship Date:")
                order_date_center_x = (left_x_start + sheet_name_width + right_x_end - c.stringWidth(ship_date_str, value_font, value_size)) / 2
                c.drawCentredString(order_date_center_x, y_label, "Order Date:"); c.setFont(value_font, value_size); c.drawCentredString(order_date_center_x, y_value, order_date_str)
                c.setLineWidth(0.5); c.rect(margin, margin, printable_width, height - (2*margin))
                
                y_pos = height - margin - header_height; x_pos = margin + frame_padding + store_padding
                c.setFont(CUSTOM_FONT_BOLD, 11); header_line_height = 11 * 1.3; header_v_center = y_pos - (table_header_height / 2)
                c.setLineWidth(order_box_line_width); c.rect(margin + frame_padding, y_pos - table_header_height, printable_width - (2 * frame_padding), table_header_height)
                c.setLineWidth(0.5)
                c.rect(margin + frame_padding + store_padding, y_pos - table_header_height + store_padding, printable_width - (2 * frame_padding) - (2 * store_padding), table_header_height - (2 * store_padding))
                
                page_qty_total_x, page_qty_total_y = None, None
                for pdf_col_name, col_props in PDF_COLS.items():
                    col_width = col_props.get('width', 1*inch); align = col_props.get('align', 'left')
                    if x_pos > margin + frame_padding + store_padding: c.line(x_pos, y_pos - store_padding, x_pos, y_pos - table_header_height + store_padding)
                    header_lines = pdf_col_name.split('\n'); h_center = x_pos + (col_width / 2); h_left = x_pos + text_cell_padding
                    text_v_offset = 11 / 2.5
                    if pdf_col_name == 'Qty':
                        c.drawCentredString(h_center, header_v_center + (header_line_height / 2) - text_v_offset, "Qty"); page_qty_total_x = h_center; page_qty_total_y = header_v_center - (header_line_height / 2) - text_v_offset
                    elif len(header_lines) == 2:
                        y1 = header_v_center + (header_line_height / 2) - text_v_offset; y2 = header_v_center - (header_line_height / 2) - text_v_offset
                        if align == 'center': c.drawCentredString(h_center, y1, header_lines[0]); c.drawCentredString(h_center, y2, header_lines[1])
                        else: c.drawString(h_left, y1, header_lines[0]); c.drawString(h_left, y2, header_lines[1])
                    else:
                        y1 = header_v_center - text_v_offset; c.drawCentredString(h_center, y1, header_lines[0]) if align == 'center' else c.drawString(h_left, y1, header_lines[0])
                    x_pos += col_width
                y_pos -= table_header_height
                return y_pos, page_qty_total_x, page_qty_total_y

            def trigger_page_break_for_messages():
                nonlocal y_pos, page_qty_total_x, page_qty_total_y, page_total_qty, page_num
                if page_qty_total_x is not None: c.setFont(CUSTOM_FONT_BOLD, 11); c.drawCentredString(page_qty_total_x, page_qty_total_y, str(int(page_total_qty)))
                c.showPage(); page_num += 1
                y_pos, page_qty_total_x, page_qty_total_y = draw_new_page_headers(page_num); page_total_qty = 0
                return y_pos, True

            while current_row_index < len(df_sheet):
                page_num += 1; page_total_qty = 0
                y_pos, page_qty_total_x, page_qty_total_y = draw_new_page_headers(page_num)
                y_pos -= header_gap
                row_line_start_x = margin + frame_padding + store_padding; row_line_end_x = width - margin - frame_padding - store_padding
                store_start_y_on_page = y_pos
                if is_continuing_store_box: store_start_y = y_pos; y_pos -= store_padding; is_continuing_store_box = False
                
                font_size_row = 10; page_has_ended = False

                for index in range(current_row_index, len(df_sheet)):
                    row = df_sheet.iloc[index]
                    row_store = row[col_cost_center]; row_order = row[col_order_num]; row_job = row[col_base_job]
                    
                    if current_store is None: current_store, current_order, current_job = row_store, row_order, row_job; store_start_y = y_pos; y_pos -= store_padding
                        
                    if row_store != current_store:
                        c.setLineWidth(order_box_line_width); y_pos -= store_padding
                        c.rect(margin + frame_padding, y_pos, printable_width - (2 * frame_padding), store_start_y - y_pos, stroke=1, fill=0)
                        lines_to_draw = _build_fragmentation_lines(entities_in_current_store_box, store_report_map, unclaimed_report_map, sheet_name)
                        y_pos, _ = draw_message_block(c, lines_to_draw, y_pos, frag_msg_font_size, frag_msg_line_height, frag_msg_start_x, frag_msg_drawable_width, page_bottom_margin_y, trigger_page_break_for_messages)
                        if y_pos - store_gap < page_bottom_content_area_y: current_row_index = index; page_has_ended = True; is_continuing_store_box = False; break
                        c.setLineWidth(0.5); c.line(row_line_start_x, y_pos, row_line_end_x, y_pos); y_pos -= store_gap
                        current_store, current_order, current_job = row_store, row_order, row_job; store_start_y = y_pos; y_pos -= store_padding; entities_in_current_store_box.clear()

                    elif row_order != current_order:
                        if y_pos - order_gap < page_bottom_content_area_y: current_row_index = index; page_has_ended = True; is_continuing_store_box = True; break
                        c.setLineWidth(0.5); c.line(row_line_start_x, y_pos, row_line_end_x, y_pos); y_pos -= order_gap; current_order, current_job = row_order, row_job
                    
                    elif row_job != current_job:
                        if y_pos - job_gap < page_bottom_content_area_y: current_row_index = index; page_has_ended = True; is_continuing_store_box = True; break
                        c.setLineWidth(0.5); c.line(row_line_start_x, y_pos, row_line_end_x, y_pos); y_pos -= job_gap; current_job = row_job

                    if y_pos - row_height < page_bottom_content_area_y: current_row_index = index; page_has_ended = True; is_continuing_store_box = True; break
                    
                    x_pos = margin + frame_padding + store_padding; c.line(row_line_start_x, y_pos, row_line_end_x, y_pos)
                    row_text_y = y_pos - (row_height / 2) - (font_size_row / 2.5); c.setLineWidth(0.5)
                    try:
                        entities_in_current_store_box.add((str(row.get(col_cost_center, "N/A")), str(row.get(col_order_num, "N/A")), str(row.get(col_base_job, "N/A"))))
                    except: pass

                    for pdf_col_name, col_props in PDF_COLS.items():
                        if x_pos > margin + frame_padding: c.line(x_pos, y_pos, x_pos, y_pos - row_height)
                        raw_text = str(row.get(col_props.get('source'), "")).replace('nan', '').strip()
                        if pdf_col_name == 'Store\nNumber': raw_text = raw_text.split('-', 1)[0].strip()
                        if pdf_col_name == 'Qty' and pd.to_numeric(raw_text, errors='coerce'): page_total_qty += pd.to_numeric(raw_text, errors='coerce')

                        col_width = col_props.get('width'); allowed_width = col_width - (2 * text_cell_padding)
                        font_to_use = CUSTOM_FONT_BOLD if pdf_col_name == 'Store\nNumber' else CUSTOM_FONT_REGULAR
                        try: raw_text = simpleSplit(raw_text, font_to_use, font_size_row, allowed_width)[0] if simpleSplit(raw_text, font_to_use, font_size_row, allowed_width) else ""
                        except: raw_text = raw_text[:int(allowed_width/6)]
                        
                        cell_font_size = 12 if pdf_col_name == 'Store\nNumber' else font_size_row
                        c.setFont(font_to_use, cell_font_size)
                        c.drawCentredString(x_pos + (col_width / 2), row_text_y, raw_text) if col_props.get('align') == 'center' else c.drawString(x_pos + text_cell_padding, row_text_y, raw_text)
                        x_pos += col_width
                    
                    c.line(row_line_end_x, y_pos, row_line_end_x, y_pos - row_height)
                    c.line(row_line_start_x, y_pos - row_height, row_line_end_x, y_pos - row_height)
                    c.setFont(CUSTOM_FONT_REGULAR, font_size_row); y_pos -= row_height
                    
                c.setLineWidth(order_box_line_width); y_pos -= store_padding
                c.rect(margin + frame_padding, y_pos, printable_width - (2 * frame_padding), store_start_y - y_pos, stroke=1, fill=0)
                
                if not page_has_ended:
                    current_row_index = len(df_sheet)
                    lines_to_draw = _build_fragmentation_lines(entities_in_current_store_box, store_report_map, unclaimed_report_map, sheet_name)
                    y_pos, _ = draw_message_block(c, lines_to_draw, y_pos, frag_msg_font_size, frag_msg_line_height, frag_msg_start_x, frag_msg_drawable_width, page_bottom_margin_y, trigger_page_break_for_messages)

                if page_qty_total_x is not None: c.setFont(CUSTOM_FONT_BOLD, 11); c.drawCentredString(page_qty_total_x, page_qty_total_y, str(int(page_total_qty)))
                c.showPage()
        
        if c.getPageNumber() == 0: utils_ui.print_warning("No PDF pages generated."); return False
        c.save()
        utils_ui.print_success(f"PDF Saved: {os.path.basename(pdf_path)}")
        return True
    except Exception as e:
        utils_ui.print_error(f"PDF Gen Error: {e}"); traceback.print_exc(); return False

def main(bundled_excel_path, output_dir, central_config_json, fragmentation_map_json):
    utils_ui.setup_logging(None)
    utils_ui.print_banner("30 - PDF Runlist Generator")

    try: central_config = json.loads(central_config_json)
    except Exception as e: utils_ui.print_error(f"Config JSON Error: {e}"); sys.exit(1)

    try: fragmentation_map = json.loads(fragmentation_map_json)
    except Exception: fragmentation_map = {'store_report_map': {}, 'unclaimed_report_map': {}}

    register_custom_fonts(central_config)
    start_time = time.time()
    
    base_name = os.path.splitext(os.path.basename(bundled_excel_path))[0]
    pdf_output_path = os.path.join(output_dir, f"{base_name}_RunLists.pdf")

    try:
        pdf_settings = central_config.get('pdf_settings', {})
        if pdf_settings.get('generate_pdf_run_lists', False):
             history_path = central_config.get('paths', {}).get('run_history_path', 'run_history.yaml')
             history = load_run_history(history_path)
             if not generate_pdf_run_list(bundled_excel_path, pdf_output_path, central_config, history, fragmentation_map):
                 raise Exception("PDF generation failed.")
        else:
            utils_ui.print_warning("PDF generation disabled in config.")

    except Exception as e:
        utils_ui.print_error(f"Critical Error: {e}")
        traceback.print_exc()
        sys.exit(1)
    finally:
        utils_ui.print_success(f"Processing Time: {time.time() - start_time:.2f}s")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="30 - Generate PDF Run Lists")
    parser.add_argument("bundled_excel_path", help="Path to bundled Excel.")
    parser.add_argument("output_dir", help="Output directory.")
    parser.add_argument("central_config_json", help="Config JSON.")
    parser.add_argument("fragmentation_map_json", help="Frag Map JSON.")
    args = parser.parse_args()
    main(args.bundled_excel_path, args.output_dir, args.central_config_json, args.fragmentation_map_json)
