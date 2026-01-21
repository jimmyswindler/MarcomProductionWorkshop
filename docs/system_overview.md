# Marcom Production Suite: System Overview

## 1. Scope & Purpose (The "Stranger Explanation")

**What does this system do?**

The Marcom Production Suite is an **automated factory control system** for a high-volume digital print production environment.

Imagine thousands of individual print orders coming in from the web—business cards, brochures, flyers—each with different quantities, shipping addresses, and design files.

This system acts as the "brain" of the production floor. It:

1. **Ingests** the raw order data.
2. **Organizes** it into efficient batches (bundles) to maximize press usage.
3. **Generates** all the necessary instructions (Job Tickets) and tracking lists (Runlists) for human operators.
4. **Prepares** the actual digital files (Imposition) for the printing presses.

It turns a chaotic stream of digital orders into structured, physical manufacturing steps, ensuring that the right product gets printed, packed, and shipped to the right person.

Crucially, it also includes a **Shipping Station App**: a touch-screen interface for the warehouse team. It verifies that every single box is scanned before it leaves the building and automatically tells UPS's WorldShip software what to print on the shipping label.

---

## 2. Key Features & Functions

* **Automated Data Ingestion**: Seamlessly merges "Orders" (who bought what) with "Job Tickets" (how to print it), checking for errors or missing data automatically.
* **Smart Bundling Algorithm**: This is the core intelligence. It groups thousands of orders into "Bundles" (e.g., 6,250 items) optimized for specific press runs. It uses advanced logic (like "Giant Slayer" and "Subset Sum") to fit orders together like Tetris blocks, minimizing waste and using "Filler/Blanks" only when necessary.
* **Gang Run Imposition**: Automatically places multiple different designs onto a single large sheet of paper (e.g., a 5x5 layout) to print efficiently.
* **Dynamic Asset Retrieval**: automatically hunts down the correct PDF artwork for every order.
* **Paperwork Generation**: Creates ready-to-print PDFs for "Runlists" (operator checklists) and "Job Tickets" (individual box labels/instructions).
* **Fragmentation Handling**: Tracks when a large order is split across multiple bundles and generates specific alerts so operators know to combine them later.
* **Shipping Validation App**: A web-based scanning station that prevents "partial ships" (missing boxes) and calculates shipping weights based on complex business logic before generating UPS labels.

---

## 3. Workflow Outline

The system operates in a sequential "Stage" model, controlled by `00_Controller.py`.

### **Stage 1: Data Collection**

* **Script**: `10_DataCollection.py`
* **Action**: Reads raw XML files (`Orders_*.xml`, `JobTickets_*.xml`) from the input folder.
* **Logic**: Joins order details with manufacturing instructions. Checks for duplicates and "orphaned" tickets.
* **Output**: A consolidated Master Excel Report.

### **Stage 2a: Sorting**

* **Script**: `20_DataSorter.py`
* **Action**: Categorizes raw data into production streams (e.g., "16pt Business Cards", "12pt Bounce Backs", "Large Format").
* **Output**: A `_CATEGORIZED.xlsx` file.

### **Stage 2b: Automatic Bundling**

* **Script**: `30_DataBundler.py`
* **Action**: The "Brain". Groups categorized items into bundles of ~6,250.
* **Logic**:
  * **Phase 1 (Giant Slayer)**: Breaks down massive orders into manageable chunks.
  * **Phase 2 (Combiner)**: Combines smaller stores to fill bundles perfectly.
  * **Phase 3 (Top-Up)**: Fills remaining gaps with small orders or blanks.
* **Output**: A final Bundled Excel Report and a `fragmap.json` (tracking split orders).

### **Stage 2c: Paperwork**

* **Script**: `40_PdfRunlistGenerator.py`
* **Action**: Reads the bundled data and draws a visual PDF "Runlist" for operators to trace work on the floor.

### **Stage 3: Asset & Ticket Prep**

* **Scripts**: `50_AcquireJobAssets.py` & `60_GenerateJobTickets.py`
* **Action**: Copies artwork PDFs to a working directory and generates a printable Job Ticket for every order item.

### **Stage 3b: Press Prep**

* **Script**: `70_PreparePressFiles.py`
* **Action**: Organizes files into folders for the press, handling special logic for "Header Cards" (box labels with icons).

### **Stage 4: Imposition**

* **Script**: `80_GR_Imposition_5x5_BB_BC.py`
* **Action**: Takes the prepared files and uses a "Gang Run" profile to layout 25 images per sheet for high-speed printing.

### **Stage 5: Notification**

* **Script**: `90_email.py`
* **Action**: Emails the production team with a summary and links to the final output.

---

## 4. Shipping App (The Warehouse Frontend)

While the main Python suite runs the "Factory Logic" (Ingest -> Print), the **ShippingApp** manages the "Exit Logic" (Pack -> Ship).

* **Technology**: Python Flask (Backend) + HTML/JS (Frontend) + PostgreSQL (Database).
* **Location**: `/ShippingApp/`
* **Key Controls**: `app.py` (API), `shipping_station.html` (UI).

### **Workflow:**

1. **Scan**: Operator scans a Job Ticket barcode at the touch screen.
2. **Verify**: The app pulls expected box data from the database. It displays a checklist of every box needed for that order.
3. **Validate**: As the operator scans boxes, they turn green. The app **blocks** the shipment if any boxes are missing (preventing "Partial Ships").
4. **Pack**: Operator scans a carton barcode (e.g., `#105`, `#115`). The app calculates total weight based on product density rules + carton weight.
5. **Output**: It generates an XML file in `xml_output/` that is instantly picked up by UPS WorldShip to print the final 4x6" shipping label.

### **Why it matters:**

It acts as the final "Quality Control" gate. Even if the printing was perfect, missing one box in a 50-box order ruins the customer experience. This app makes that impossible.
