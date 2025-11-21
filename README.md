A. Create the three Marcom Order Reports Navigate to: https://admin.marcomcentral.app.pti.com/Reports/Edit 
(1) Create and Save these report templates, use the following name convention (case, spacing):  Job Ticket Summary Total Order Summary Order And Ship Detail  
(2) Modify the columns For each saved template, check or activate all available columns. For each saved template, select Sort By Job Ticket Number.
(3) Customize the Date Range and Export to Excel for each saved template.

B. Place the three Input Files
Navigate to: /Users/jimmyswindler/Documents/- Controller X.X/_REPORT_INPUT folder.
Place the three Excel files in this folder. The files must start with the following names:
Job_Ticket_Summary...xlsx
Total_Order_Summary...xlsx
Order_And_Ship_Detail...xlsx

 C. Run the Script
Navigate to: /Users/jimmyswindler/Documents/- Controller X.X folder.
Click and drag 00_Controller.py script onto the 00 Python Launcher shortcut.
A terminal or console window will open. You will see the script's progress as it runs through each stage (Data Collection, Bundling, PDF Generation, etc.).
Wait for the script to finish, 10-15 minutes. A successful run will end with a [WORKFLOW COMPLETE] message. If you see a [WORKFLOW FAILED] message, the process did not complete. Report the error.

D. Find the Output Files
All final files are saved to a new folder named after the order date range (MarcomOrderDate YYYY-MM-DD).
This main job folder is located here:
/Volumes/AraxiVolume_Jobs/Jobs/DigimasterProductionNew/MARCOM orders/
Inside your new job folder (.../*MARCOM orders/MarcomOrderDate YYYY-MM-DD/), you will find the production files, RunList and Job Tickets:  

E. Print the RunList
Inside this folder find MarcomOrderDate YYYY-MM-DD_RunLists.pdf Print the RunList on 11x17 paper to the Ricoh 7502 printer in Mike’s office. Fold these sheets in half.

F. Print the Job Tickets
Navigate to: .../MARCOM orders/MarcomOrderDate YYYY-MM-DD/_JobTickets/ Print all of the Tickets in this folder on 11x8.5 paper to the Ricoh 8210S printer in the digital press area. Gather and collate the tickets into their respective RunLists.

G. Inspect the production files
Navigate to .../MARCOM orders/MarcomOrderDate YYYY-MM-DD/ProductionImposed/Gang and inspect the gang run files for proper page count, proper coversheets, slug line, etc.

H. Deliver the paper Job Tickets
Deliver GangRun tickets to the Digital Press production table. Deliver LargeFormat tickets to the LargeFormat production table. Deliver Outsource and Exception tickets to the Marcom CSR. Deliver PrintOnDemand, 16ptBusinessCard, and 12ptBounceBack tickets to the Digital PrePress production table.
