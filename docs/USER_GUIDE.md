# Marcom Production Workshop: User Guide

This guide explains how to operate the Marcom Production pipeline and Shipping Web Application.

## 1. Running the Production Pipeline

The pipeline processes incoming XML orders, generates job tickets, creates PDF run lists, and prepares files for valid imposition.

### Prerequisites

Before running the pipeline, ensure your input files are in the correct location:

* **Input Directory**: `data/_REPORT_INPUT/`
* **Required Files**: You need the set of XML/XLSX files (Orders, JobTickets_Display, JobTickets_Order) exported from Marcom Central.

### How to Run

1. Navigate to the `MarcomProductionWorkshop` folder on the Desktop.
2. Double-click **`run_pipeline.command`**.
3. A terminal window will open and display progress.
4. When providing the prompt, press any key to close the window once finished.

### Where are the Files?

The pipeline creates a new **Job Folder** in the root directory (e.g., `MarcomOrderDate_2026-01-19_...`).

Inside this folder, you will find:

* **Run Lists**: A PDF summary of all items (`*_RunLists.pdf`).
* **Job Tickets**: Individual PDF tickets in the `_JobTickets` folder.
* **Imposed Files**: Ready-to-print PDFs in `ProductionImposed/Gang`.
* **One-Up Files**: The original PDF assets in `WorkUp/OneUpFiles`.

---

## 2. Running the Shipping Web App

The Shipping App runs locally to help scan barcodes and generate packing slips.

### How to Run

1. Navigate to the `MarcomProductionWorkshop` folder.
2. Double-click **`run_webapp.command`**.
3. A terminal window will open showing that the server is running. **Do not close this window.**
4. Open your web browser (Chrome/Safari) and go to:
    **[http://localhost:5000](http://localhost:5000)**

### How to Use

* Use the web interface to scan job tickets and Generate Packing Slips.
* **Output**: Generated XML files for WorldShip and Marcom verification are saved in `shipping_web_app/xml_output/`.

---

## Troubleshooting

* **Logs**: Check the `LOGS/` folder for detailed error reports if the pipeline fails.
* **Configuration**: Settings can be found in `config/config.yaml`.
* **Passwords**: Database and Email passwords are managed in the `.env` file (hidden file in the root folder).
