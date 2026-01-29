import os
import glob

XML_OUTPUT_FOLDER = os.path.join('shipping_web_app', 'xml_output')
print(f"Checking: {os.path.abspath(XML_OUTPUT_FOLDER)}")

if os.path.exists(XML_OUTPUT_FOLDER):
    print("Folder exists.")
    files = glob.glob(os.path.join(XML_OUTPUT_FOLDER, "*.xml"))
    print(f"XML Files found: {len(files)}")
    for f in files[:3]:
        print(f" - {f}")
else:
    print("Folder NOT found.")
