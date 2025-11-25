import os
import io
import json
import math
import traceback
import sys
from datetime import datetime
import argparse  # <-- IMPORT ADDED

# pypdf is used for the core imposition engine due to its robust and memory-efficient object handling.
from pypdf import PdfReader, PdfWriter, PageObject, Transformation
from pypdf.generic import RectangleObject

# reportlab is used ONLY to dynamically generate the slug line text overlay.
from reportlab.pdfgen import canvas
from reportlab.lib.units import inch  # Import inch for easy measurements
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont

# ==============================================================================
# SCRIPT CONFIGURATION HAS BEEN REMOVED
# ==============================================================================

def debug_print(message):
    """Prints a message with a timestamp for logging."""
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {message}")

# ==============================================================================
# STAGE 1: CONFIGURATION & PLANNING
# ==============================================================================
def load_and_plan(central_config):
    """
    Loads the JSON profile (path provided by central_config) and
    calculates initial layout parameters.
    This is the single source of truth for the entire imposition job.
    """
    profile_path = central_config['imposition_profile']
    debug_print(f"Stage 1: Loading and planning with profile: {profile_path}")
    
    try:
        with open(profile_path, 'r') as f:
            config_data = json.load(f)

        grid_settings = config_data['xtools'][0]['packedSteps'][0]
        header_footer_settings = config_data['xtools'][0]['packedSteps'][1]

        profile = {
            'paper_width': float(grid_settings['paperWidth']) * 72,
            'paper_height': float(grid_settings['paperHeight']) * 72,
            'columns': int(grid_settings['columns']),
            'rows': int(grid_settings['rows']),
            'v_gutter': float(grid_settings['verticalGutterWidth']) * 72,
            'h_gutter': float(grid_settings['horizontalGutterWidth']) * 72,
            'center': grid_settings.get('center', False),
            'bleed_left': float(grid_settings.get('fixedBleedLeft', 0.0)) * 72,
            'bleed_top': float(grid_settings.get('fixedBleedTop', 0.0)) * 72,
            'header_footer': {
                'text': header_footer_settings['text'],
                'font': header_footer_settings['fontName'],
                'size': float(header_footer_settings['fontSizeInPoints']),
                'margin': float(header_footer_settings['margins']) * 72
            }
        }
        
        # Define the standard canvas size for individual cards
        profile['card_width_pts'] = 2.25 * 72
        profile['card_height_pts'] = 3.75 * 72
        
        return profile
    except Exception as e:
        debug_print(f"  [!] FATAL ERROR in Stage 1: Could not load or parse profile '{profile_path}': {e}")
        return None

# ==============================================================================
# STAGE 2: PAGE COLLECTION & STANDARDIZATION
# ==============================================================================
def standardize_pages(file_paths, profile):
    """
    Reads all pages from all files and standardizes them onto a common canvas.
    Returns a single list of memory-efficient PageObject items.
    """
    debug_print(f"Stage 2: Standardizing pages from {len(file_paths)} file(s)...")
    all_pages = []
    for file_path in file_paths:
        try:
            filename = os.path.basename(file_path)
            reader = PdfReader(file_path)
            debug_print(f"  - Reading {filename} ({len(reader.pages)} pages)")
            for page in reader.pages:
                # Create the standard-sized canvas for the card to be placed on.
                position_canvas = PageObject.create_blank_page(width=profile['card_width_pts'], height=profile['card_height_pts'])

                # Use the "Hierarchy of Trust" to find the reliable content area (TrimBox) of the source page.
                input_trim_box = page.trimbox
                if not input_trim_box or (input_trim_box.width == page.mediabox.width and input_trim_box.height == page.mediabox.height):
                    bleed_x, bleed_y = profile['bleed_left'], profile['bleed_top']
                    input_trim_box = RectangleObject((
                        page.mediabox.left + bleed_x, page.mediabox.bottom + bleed_y,
                        page.mediabox.right - bleed_x, page.mediabox.top - bleed_y,
                    ))

                # Calculate the translation needed to align the center of the source's content with the center of our canvas.
                canvas_trim_center_x = profile['card_width_pts'] / 2
                canvas_trim_center_y = profile['card_height_pts'] / 2
                input_trim_center_x = (input_trim_box.left + input_trim_box.right) / 2
                input_trim_center_y = (input_trim_box.bottom + input_trim_box.top) / 2
                dx = canvas_trim_center_x - input_trim_center_x
                dy = canvas_trim_center_y - input_trim_center_y

                # Merge the source page onto the canvas with the calculated transformation.
                position_canvas.merge_transformed_page(page, Transformation().translate(dx, dy))
                all_pages.append(position_canvas)
        except Exception as e:
            debug_print(f"  [!] WARNING: Error reading or standardizing {os.path.basename(file_path)}: {e}")
    
    debug_print(f"  -> Stage 2 Complete: {len(all_pages)} total pages standardized.")
    return all_pages

# ==============================================================================
# STAGE 3: CORE IMPOSITION ENGINE
# ==============================================================================
def impose_content(standardized_pages, profile):
    """
    Takes a list of standardized pages and places them onto press sheets
    using the "cut-and-stack" algorithm. This stage does not add any marks.
    """
    total_pages = len(standardized_pages)
    cards_per_sheet = profile['columns'] * profile['rows']
    num_sheets = math.ceil(total_pages / cards_per_sheet)
    
    debug_print(f"Stage 3: Imposing {total_pages} pages onto {num_sheets} press sheets...")

    writer = PdfWriter()
    
    trim_width = profile['card_width_pts'] - (2 * profile['bleed_left'])
    trim_height = profile['card_height_pts'] - (2 * profile['bleed_top'])
    
    total_block_width = (profile['columns'] * trim_width) + ((profile['columns'] - 1) * profile['h_gutter'])
    total_block_height = (profile['rows'] * trim_height) + ((profile['rows'] - 1) * profile['v_gutter'])
    
    margin_x = (profile['paper_width'] - total_block_width) / 2 if profile['center'] else profile['bleed_left']
    margin_y = (profile['paper_height'] - total_block_height) / 2 if profile['center'] else profile['bleed_top']

    for sheet_index in range(num_sheets):
        press_sheet = PageObject.create_blank_page(width=profile['paper_width'], height=profile['paper_height'])
        is_back_sheet = (sheet_index % 2) != 0

        for row in range(profile['rows']):
            for col in range(profile['columns']):
                # The core "cut-and-stack" formula.
                slot_index = (row * profile['columns']) + col
                page_index = (slot_index * num_sheets) + sheet_index
                
                if page_index >= total_pages: continue

                card_page = standardized_pages[page_index]
                
                # For work-and-turn, back sheets have their horizontal placement mirrored.
                current_col = (profile['columns'] - 1) - col if is_back_sheet else col

                x_pos = margin_x - profile['bleed_left'] + (current_col * (trim_width + profile['h_gutter']))
                y_pos = margin_y - profile['bleed_top'] + (row * (trim_height + profile['v_gutter']))
                
                press_sheet.merge_transformed_page(card_page, Transformation().translate(tx=x_pos, ty=y_pos))
        
        writer.add_page(press_sheet)

    debug_print("  -> Stage 3 Complete: Content imposed.")
    return writer

# ==============================================================================
# STAGE 4: FINISHING
# ==============================================================================
def create_slug_line_overlay(profile, batch_name, sheet_num, total_sheets):
    """
    Uses reportlab to create an in-memory PDF containing only the dynamic
    slug line text, rotated and positioned as needed.
    """
    packet = io.BytesIO()
    c = canvas.Canvas(packet, pagesize=(profile['paper_width'], profile['paper_height']))
    
    hf_conf = profile['header_footer']
    
    font_name_from_json = hf_conf['font']
    font_size_from_json = hf_conf['size']
    
    c.saveState()
    
    text = hf_conf['text'].replace('[file-name]', batch_name).replace('[page-number]', str(sheet_num)).replace('[page-count]', str(total_sheets))
    c.setFont(font_name_from_json, font_size_from_json)

    # --- NEW: Logic to calculate the vertical center ---
    # 1. Calculate the rendered width of the text string in points.
    text_width = pdfmetrics.stringWidth(text, font_name_from_json, font_size_from_json)
    
    # 2. Calculate the starting Y-coordinate for perfect centering.
    #    (Page Center) + (Half of the text's length)
    center_y_pos = (profile['paper_height'] / 2) + (text_width / 2)
    # --- END NEW ---

    # Updated translate function now uses our calculated center position.
    # The X position (0.25 * inch) remains unchanged as requested.
    c.translate(0.25 * inch, center_y_pos)
    c.rotate(270)
    c.drawString(0, 0, text)
    
    c.restoreState()

    c.save()
    packet.seek(0)
    
    return PdfReader(packet).pages[0]

def apply_finishing(imposed_writer, profile, batch_name, central_config):
    """
    Applies finishing marks by merging a static template PDF (path from
    central_config) and a dynamic slug line overlay onto the imposed sheets.
    """
    debug_print("Stage 4: Applying finishing marks...")
    
    template_path = central_config['marks_template']
    
    try:
        template_reader = PdfReader(template_path)
        template_page = template_reader.pages[0]
    except Exception as e:
        debug_print(f"  [!] FATAL ERROR in Stage 4: Could not open marks template '{template_path}': {e}")
        return None

    final_writer = PdfWriter()
    total_sheets = len(imposed_writer.pages)
    
    for i, sheet in enumerate(imposed_writer.pages):
        sheet_num = i + 1
        
        # 1. Merge the static marks from the template file.
        sheet.merge_page(template_page)
        
        # 2. Create and merge the dynamic slug line text overlay.
        slug_overlay = create_slug_line_overlay(profile, batch_name, sheet_num, total_sheets)
        sheet.merge_page(slug_overlay)
        
        final_writer.add_page(sheet)
        
    debug_print("  -> Stage 4 Complete: Finishing applied.")
    return final_writer

# ==============================================================================
# MAIN ORCHESTRATOR
# ==============================================================================
def main(batch_folder_to_impose, output_dir, central_config_json): # <-- MODIFIED
    """
    Runs the full imposition process for a single batch folder.

    Args:
        batch_folder_to_impose (str): The full path to the batch folder (e.g., '.../input/Batch_01').
        output_dir (str): The full path to the root output folder (e.g., '.../output_final').
        central_config_json (str): A JSON string containing config paths. <-- MODIFIED
    """
    
    # --- MODIFIED: Load config from JSON string ---
    try:
        central_config = json.loads(central_config_json)
        debug_print("✓ Successfully loaded configuration from controller.")
    except json.JSONDecodeError as e:
        debug_print(f"[!] FATAL ERROR: Could not parse central_config JSON: {e}")
        return # Exit gracefully
    # ---
    
    # Ensure the root output directory exists
    os.makedirs(output_dir, exist_ok=True)
    
    # Get the batch name from the folder path
    batch_name = os.path.basename(batch_folder_to_impose)
    debug_print(f"\n----- Processing Batch: {batch_name} -----")

    if not os.path.isdir(batch_folder_to_impose):
        debug_print(f"  [!] ERROR: Batch folder not found: {batch_folder_to_impose}. Skipping.")
        return

    # Stage 1 (Passes the loaded dictionary)
    profile = load_and_plan(central_config)
    if not profile:
        debug_print("  [!] ERROR: Failed to load profile. Skipping batch.")
        return

    # Stage 2
    file_paths = [os.path.join(batch_folder_to_impose, f) for f in sorted(os.listdir(batch_folder_to_impose)) if f.lower().endswith(".pdf")]
    if not file_paths:
        debug_print("  [!] WARNING: No PDF files found in this batch. Skipping.")
        return
        
    standardized_pages = standardize_pages(file_paths, profile)
    if not standardized_pages:
        debug_print("  [!] ERROR: No pages could be standardized. Skipping batch.")
        return
        
    # Stage 3
    imposed_writer = impose_content(standardized_pages, profile)

    # Stage 4 (Passes the loaded dictionary)
    final_writer = apply_finishing(imposed_writer, profile, batch_name, central_config)
    if not final_writer:
        debug_print("  [!] ERROR: Finishing failed. Skipping batch.")
        return

    # Final Step: Save the output file
    output_path = os.path.join(output_dir, f"{batch_name}.pdf")
    try:
        with open(output_path, "wb") as out_file:
            final_writer.write(out_file)
        debug_print(f"SUCCESS! Batch '{batch_name}' saved to: {output_path}")
        debug_print(f"  Total press sheets created: {len(final_writer.pages)}")
    except Exception as e:
        debug_print(f"  [!] FATAL ERROR: Could not write output PDF: {e}")
        traceback.print_exc()

# ==============================================================================
# SCRIPT ENTRY POINT
# ==============================================================================
if __name__ == "__main__":
    # --- THIS BLOCK IS REPLACED TO BE CALLED BY THE CONTROLLER ---
    
    parser = argparse.ArgumentParser(description="70 - Impose PDF files for a single batch.")
    
    # Arg 1: The batch folder to impose
    parser.add_argument("batch_folder_to_impose", help="Full path to the batch folder containing PDFs.")
    
    # Arg 2: The root output directory
    parser.add_argument("output_dir", help="Full path to the root output folder for imposed PDFs.")
    
    # Arg 3: The JSON config string
    parser.add_argument("central_config_json", help="Central configuration dictionary (subset) passed as a JSON string.")
    
    args = parser.parse_args()

    print("--- Starting 70: Imposition Assembly ---")
    try:
        # Call main with the parsed arguments
        main(
            args.batch_folder_to_impose,
            args.output_dir,
            args.central_config_json
        )
    except Exception as e:
        debug_print(f"CRITICAL UNHANDLED ERROR in 70_Imposition: {e}")
        traceback.print_exc()
        sys.exit(1) # Exit with an error code for the controller
        
    print("--- Finished 70 ---")