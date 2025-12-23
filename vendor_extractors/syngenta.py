# import os
# import re
# import fitz  # PyMuPDF library for working with PDF files
# import time
# import requests
# from typing import List, Dict, Tuple
# from db_logger import log_processing_event

# # --- Azure Configuration ---
# # Load credentials from environment variables to secure API access
# AZURE_ENDPOINT = os.getenv("AZURE_ENDPOINT")
# AZURE_KEY = os.getenv("AZURE_KEY")

# def extract_text_with_azure_ocr(pdf_bytes: bytes) -> List[str]:
#     """
#     Performs OCR on in-memory PDF bytes using Azure Form Recognizer.
#     Used when standard text extraction fails (e.g., scanned documents).
#     """
#     # Safety check: Ensure we have credentials before attempting API call
#     if not AZURE_ENDPOINT or not AZURE_KEY:
#         print("Warning: Azure credentials not set. Skipping OCR.")
#         return []

#     # Headers required for Azure Form Recognizer API authentication and content definition
#     headers = {
#         "Ocp-Apim-Subscription-Key": AZURE_KEY,
#         "Content-Type": "application/pdf"
#     }
    
#     # Construct URL for the prebuilt layout model (good for tables and structure)
#     url = f"{AZURE_ENDPOINT}formrecognizer/documentModels/prebuilt-layout:analyze?api-version=2023-07-31"
    
#     try:
#         # Step 1: Submit the document for analysis (POST request)
#         response = requests.post(url, headers=headers, data=pdf_bytes)
        
#         # 202 'Accepted' is the only valid success status for this async operation
#         if response.status_code != 202:
#             print(f"Azure OCR request failed: {response.status_code} - {response.text}")
#             return []

#         # Extract the URL to poll for results from the response headers
#         result_url = response.headers["Operation-Location"]

#         # Step 2: Poll for results (Azure processing takes time)
#         for _ in range(30): # Try for 30 seconds max
#             time.sleep(1.0) # Wait 1 second between checks
            
#             # Check status (GET request)
#             result = requests.get(result_url, headers={"Ocp-Apim-Subscription-Key": AZURE_KEY}).json()
#             status = result.get("status")
            
#             # Step 3: Parse results if processing is complete
#             if status == "succeeded":
#                 lines = []
#                 # Navigate JSON structure: analyzeResult -> pages -> lines -> content
#                 for page in result.get("analyzeResult", {}).get("pages", []):
#                     for line in page.get("lines", []):
#                         content = line.get("content", "").strip()
#                         if content:
#                             lines.append(content)
#                 return lines
#             elif status == "failed":
#                 print("Azure OCR analysis failed.")
#                 return []
                
#         print("Azure OCR timed out.")
#         return []
#     except Exception as e:
#         print(f"Error during Azure OCR: {e}")
#         return []

# def parse_analysis_text(text: str, filename: str) -> Dict[str, Dict]:
#     """
#     Parses text specifically for Syngenta 'Report of Analysis' data.
#     Extracts Lot Number, Purity, Inert matter, and Germination data.
#     """
#     data = {}
#     lot_no = None

#     print(f"--- Parsing Report Text ({filename}) ---")

#     # --- Extract Lot Number ---
#     # Strategy: Prioritize the specific Syngenta pattern (3 letters + 6 digits + optional letter)
#     # This avoids capturing "designated" or "Variety" text.
    
#     # regex: word boundary, 3 letters, 6 digits, optional 1 letter, word boundary
#     syngenta_lot_pattern = re.compile(r"\b([A-Z]{3}\d{6}[A-Z]?)\b")
    
#     # 1. Look for pattern near "Lot Number" label first (Contextual search)
#     m_label = re.search(r"Lot\s*Number\s*[:\.]?[\s\"\'\n,]*([A-Z0-9\s]+)", text, re.IGNORECASE)
#     if m_label:
#         # Check if the text captured by label contains the valid pattern
#         label_text = m_label.group(1)
#         m_precise = syngenta_lot_pattern.search(label_text)
#         if m_precise:
#             lot_no = m_precise.group(1)
#             print(f"   > Found Lot No (Label Match): {lot_no}")

#     # 2. Fallback: Scan entire text for the pattern if not found yet (Global search)
#     if not lot_no:
#         # Find all candidates
#         candidates = syngenta_lot_pattern.findall(text)
#         # Filter out common false positives if any (though this pattern is quite specific)
#         if candidates:
#             lot_no = candidates[0] # Take the first one found
#             print(f"   > Found Lot No (Pattern Scan): {lot_no}")

#     # If no lot number matches the Syngenta format, abort parsing this file
#     if not lot_no:
#         print("   > No valid Lot No found in report text.")
#         return {}

#     # --- Extract Purity & Inert ---
#     # Strategy: Find "1000." (grams analyzed) then grab numbers with DECIMALS
#     # "1000." is a consistent anchor in these specific lab reports
#     start_match = re.search(r"1000\.", text)
#     if start_match:
#         # Grab a chunk of text immediately following the anchor
#         sub_text = text[start_match.end():start_match.end()+400]
        
#         # Regex: Matches numbers that explicitly have a decimal point/comma and decimals
#         # e.g. "99.99", "0.00", "100.0"
#         # It will skip plain integers like "7" or "400" which might be unrelated data
#         decimal_pattern = re.compile(r"(\d+[\.,]\d+)")
        
#         # Find all matches
#         raw_numbers = decimal_pattern.findall(sub_text)
        
#         # Convert found strings to floats, handling European style commas if present
#         valid_floats = []
#         for raw in raw_numbers:
#             try:
#                 val = float(raw.replace(",", "."))
#                 valid_floats.append(val)
#             except: pass
            
#         # Expected sequence after "1000." => Pure | Other | Inert | Weed | Germ
#         # If Germ is "100.0", it matches. If "98.00", it matches.
        
#         # We need at least 5 decimal numbers to map to the expected columns
#         if len(valid_floats) >= 5:
#             try:
#                 raw_purity = valid_floats[0] # Index 0: Pure
#                 raw_inert = valid_floats[2]  # Index 2: Inert
#                 raw_germ = valid_floats[4]   # Index 4: Germ

#                 # Business Rules for Purity
#                 # If report says 100%, we adjust to 99.99% so Inert is never 0 (database requirement usually)
#                 if raw_purity == 100.0:
#                     data["Purity"] = 99.99
#                     data["Inert"] = 0.01
#                 else:
#                     data["Purity"] = raw_purity
#                     data["Inert"] = raw_inert

#                 # Business Rules for Germination
#                 # If report says 100% germ, cap it at 99% (industry standard conservatism)
#                 if raw_germ == 100.0:
#                     data["CertificateGerm"] = 99
#                 else:
#                     data["CertificateGerm"] = int(raw_germ)
                    
#                 print(f"   > Extracted: Pure={data.get('Purity')}, Inert={data.get('Inert')}, Germ={data.get('CertificateGerm')}")
#             except Exception as e:
#                 print(f"   > Error logic assigning values: {e}")
#         else:
#             print(f"   > Not enough decimal values found after 1000.: {valid_floats}")
#     else:
#         print("   > Anchor '1000.' not found in text.")

#     # --- Extract Certificate Germ Date ---
#     # Look for the date under "Germination Information" block explicitly
#     germ_info_match = re.search(r"Germination Information\s*[\r\n\s]+.*?Date Tested:\s*[\r\n\s]+.*?(\d{2}/\d{2}/\d{4})", text, re.DOTALL | re.IGNORECASE)
#     if germ_info_match:
#         data["CertificateGermDate"] = germ_info_match.group(1)
#     else:
#         # Fallback: Find all dates in the document
#         all_dates = re.findall(r"\d{2}/\d{2}/\d{4}", text)
#         if len(all_dates) >= 3:
#              # Heuristic: The 3rd date is often the test date in this specific layout
#              data["CertificateGermDate"] = all_dates[2]
#         elif all_dates:
#              # If fewer dates, the last one is usually the most recent (test date)
#              data["CertificateGermDate"] = all_dates[-1]

#     # Only return map if data was found
#     if data:
#         return {lot_no: data}
#     return {}

# def process_item_block(block_lines: List[str], global_po: str, invoice_no: str, is_kamterter: bool) -> Dict:
#     """
#     Process a list of strings representing a single item block from Invoice.
#     Converts raw text lines into structured dictionary data.
#     """
#     # Initialize default structure
#     item_data = {
#         "VendorInvoiceNo": invoice_no,
#         "PurchaseOrder": None,
#         "VendorItemNumber": block_lines[0], # Assumption: First line of block is always Item Number
#         "VendorItemDescription": "",
#         "VendorTreatment": "",
#         "VendorLotNo": None,
#         "VendorBatchNo": None,
#         "OriginCountry": None,
#         "SeedCount": None,
#         "SeedSize": None,
#         "QuantityLine": 0.0,
#         "Unit Price": 0.0,
#         "TotalPrice": 0.0,
#         "PackageDescription": None,
#         "TotalQuantity": 0.0,
#         "USD_Actual_Cost_$": 0.0,
#         "CurrentGerm": None,      # Explicitly None (filled later by linking analysis)
#         "CurrentGermDate": None   # Explicitly None
#     }

#     # 1. Identify Anchor Points (Find specific line indices for key fields)
#     idx_material = -1
#     idx_ea = -1
#     idx_trt_code = -1
#     idx_lot = -1
    
#     for i, line in enumerate(block_lines):
#         # Material: Expecting exactly 8 digits, usually on line 1
#         if i == 1 and re.match(r"^\d{8}$", line):
#             idx_material = i
        
#         # Financials: "EA" is the unit of measure anchor for pricing
#         if line.strip() == "EA":
#             idx_ea = i
            
#         # TRT CODE marker
#         if "TRT CODE" in line:
#             idx_trt_code = i
            
#         # Lot: Syngenta lots in this block often start with '/' in the layout
#         if line.startswith("/"):
#             idx_lot = i
#             item_data["VendorLotNo"] = line.replace("/", "").strip()

#     # 2. Extract Financials (Relative to "EA" position)
#     if idx_ea > 0:
#         try:
#             # Line before "EA" is Quantity
#             qty_line = block_lines[idx_ea - 1].replace(",", "")
#             item_data["QuantityLine"] = float(qty_line)
#         except: pass
        
#         if idx_ea + 1 < len(block_lines):
#             try:
#                 # Line after "EA" is Unit Price
#                 price_line = block_lines[idx_ea + 1].replace(",", "")
#                 item_data["Unit Price"] = float(price_line)
#             except: pass
            
#         if idx_ea + 2 < len(block_lines):
#             try:
#                 # 2 lines after "EA" is Total Price
#                 total_line = block_lines[idx_ea + 2].replace(",", "")
#                 item_data["TotalPrice"] = float(total_line)
#             except: pass

#     # 3. Extract Batch (Located exactly one line before the Lot Number)
#     if idx_lot > 0:
#         # Check line before Lot for 8 digits pattern
#         prev_line = block_lines[idx_lot - 1].strip()
#         if re.match(r"^\d{8}$", prev_line):
#             item_data["VendorBatchNo"] = prev_line

#     # 4. Extract Description & Treatment
#     # Logic: Text located between Material Number and the next anchor (Treatment, Lot, or Price)
#     start_idx = (idx_material if idx_material != -1 else 0) + 1
#     # Find the earliest occurring anchor after the start index to define the end of description
#     candidates = [x for x in [idx_trt_code, idx_lot - 1 if idx_lot > 0 else -1, idx_ea - 1 if idx_ea > 0 else -1] if x > start_idx]
#     end_idx = min(candidates) if candidates else len(block_lines)
    
#     raw_text_lines = block_lines[start_idx:end_idx]
#     full_text = " ".join(raw_text_lines)
    
#     # Split Desc/Treatment based on "KS" (Kernel Size/Count marker)
#     if "KS" in full_text:
#         parts = full_text.split("KS", 1)
#         item_data["VendorItemDescription"] = parts[0].strip() + " KS"
#         # Treatment usually follows KS
#         raw_treatment = parts[1]
#         raw_treatment = raw_treatment.replace("Bag", "").replace("Foil", "").strip()
#         item_data["VendorTreatment"] = raw_treatment
#     else:
#         item_data["VendorItemDescription"] = full_text

#     # 5. Extract Metadata (Regex scan across all lines in the block)
#     for line in block_lines:
#         # Check for PO override within the line item
#         if "PO#" in line or "PO :" in line:
#             m = re.search(r"(\d{5})", line)
#             if m: item_data["PurchaseOrder"] = f"PO-{m.group(1)}"
#         # Extract Seeds per Pound
#         if "Seeds/LB:" in line:
#             m = re.search(r"Seeds/LB:\s*([\d,]+)", line)
#             if m: item_data["SeedCount"] = int(m.group(1).replace(",", ""))
#         # Extract Seed Size Code
#         if "Size:" in line:
#             m = re.search(r"Size:\s*([A-Z]{2})", line)
#             if m: item_data["SeedSize"] = m.group(1)
#         # Extract Origin
#         if "Origin:" in line:
#             m = re.search(r"Origin:\s*([A-Z]{2})", line)
#             if m: item_data["OriginCountry"] = m.group(1)
    
#     # Check if Item Level Search (Step 5) found anything. 
#     # If NOT (it is still None), then fallback to the Global PO passed into the function.
#     if item_data["PurchaseOrder"] is None:
#         item_data["PurchaseOrder"] = global_po

#     # 6. Calculations and Packaging
#     pkg_size_ks = 0
#     # Extract numeric KS value from description (e.g., "100 KS")
#     m_ks = re.search(r"(\d+)\s*KS", item_data["VendorItemDescription"])
#     if m_ks:
#         pkg_size_ks = int(m_ks.group(1))
    
#     # Define Package Description based on provider or size
#     if is_kamterter:
#         item_data["PackageDescription"] = "SUBCON BULK-MS"
#     elif pkg_size_ks > 0:
#         item_data["PackageDescription"] = f"{pkg_size_ks * 1000:,} SEEDS"

#     # Calculate Total Seeds (Quantity Line * Package Size)
#     if pkg_size_ks > 0:
#         item_data["TotalQuantity"] = item_data["QuantityLine"] * pkg_size_ks
#     else:
#         item_data["TotalQuantity"] = item_data["QuantityLine"]

#     # Calculate Per-Seed Cost (Total Price / Total Seeds)
#     if item_data["TotalQuantity"] > 0:
#         item_data["USD_Actual_Cost_$"] = round(item_data["TotalPrice"] / item_data["TotalQuantity"], 4)

#     # Fallback Seed Size extraction from Description if not found in metadata
#     if not item_data["SeedSize"]:
#         d = item_data["VendorItemDescription"].upper()
#         if "LR" in d: item_data["SeedSize"] = "LR"
#         elif "MF" in d: item_data["SeedSize"] = "MF"
#         elif "MR" in d: item_data["SeedSize"] = "MR"
#         elif "LF" in d: item_data["SeedSize"] = "LF"

#     return item_data

# def parse_invoice_text(text: str, filename: str, global_po: str, invoice_no: str) -> List[Dict]:
#     """
#     Splits invoice text into item blocks and processes them individually.
#     """
#     # Clean noise text
#     text = re.sub(r"The following table:", "", text)
#     lines = [l.strip() for l in text.splitlines() if l.strip()]
    
#     # Check if this is a Kamterter invoice (affects packaging logic)
#     is_kamterter = "KAMTERTER" in text.upper()
    
#     # Identification: Find start indices of items
#     # Pattern: 5 digit line (Item No) followed immediately by 8 digit line (Material No)
#     item_indices = []
#     for i, line in enumerate(lines):
#         if re.match(r"^\d{5}$", line):
#             if i + 1 < len(lines) and re.match(r"^\d{8}$", lines[i+1]):
#                 item_indices.append(i)
    
#     # Slicing: Cut the list of lines into blocks based on indices
#     items = []
#     for k, start_idx in enumerate(item_indices):
#         # End index is the start of the next item, or end of file
#         end_idx = item_indices[k+1] if k + 1 < len(item_indices) else len(lines)
#         block_lines = lines[start_idx:end_idx]
        
#         # Process the specific block
#         item = process_item_block(block_lines, global_po, invoice_no, is_kamterter)
#         if item:
#             items.append(item)
            
#     return items

# def extract_syngenta_data_from_bytes(pdf_files: List[Tuple[str, bytes]], pkg_desc_list: list) -> Dict[str, List[Dict]]:
#     """
#     Main Orchestrator Function.
#     Iterates through PDF bytes, decides extraction method (OCR vs Text), 
#     parses data based on doc type, and links Invoices to Analysis reports.
#     """
#     analysis_map = {}      # Stores extracted Analysis data keyed by Lot Number
#     temp_invoice_items = {} # Stores Invoice items keyed by filename
    
#     print("\n=== START SYNGENTA EXTRACTION (Azure OCR Enabled) ===")

#     for filename, pdf_bytes in pdf_files:
#         try:
#             # 1. Try simple Text Extraction first (Fast/Cheap)
#             doc = fitz.open(stream=pdf_bytes, filetype="pdf")
#             page_count = doc.page_count
#             full_text = ""
#             for page in doc:
#                 full_text += page.get_text() + "\n"
#             doc.close()

#             extraction_method = "PyMuPDF"
#             stripped_text = full_text.strip()
            
#             # Heuristic: Scan/Image PDF detection logic
#             # If text length is tiny OR it contains key phrase "Report of Analysis" but we can't find critical data "PURITY ANALYSIS", use OCR
#             needs_ocr = False
#             if len(stripped_text) < 100:
#                 needs_ocr = True
#             elif "REPORT OF ANALYSIS" in full_text.upper():
#                 # Critical check: If the visual text is there but not extractable, the anchor "PURITY ANALYSIS" will be missing
#                 if "PURITY ANALYSIS" not in full_text.upper():
#                     needs_ocr = True
            
#             # Perform OCR if heuristics indicate it's a scanned image
#             if needs_ocr:
#                 print(f"   > Detected scanned/image PDF for {filename}. Attempting Azure OCR...")
#                 ocr_lines = extract_text_with_azure_ocr(pdf_bytes)
#                 if ocr_lines:
#                     full_text = "\n".join(ocr_lines)
#                     extraction_method = "Azure OCR"
#                 else:
#                     print("   > Azure OCR returned no text. Using original text.")

#             # --- Differentiate Document Type & Parse ---
            
#             # Type A: Analysis Report
#             if "REPORT OF ANALYSIS" in full_text.upper() or "PURITY ANALYSIS" in full_text.upper():
#                 result = parse_analysis_text(full_text, filename)
#                 if result: analysis_map.update(result)
            
#             # Type B: Invoice
#             elif ("INVOICE" in full_text.upper() and "SYNGENTA" in full_text.upper()) or ("ITEM" in full_text.upper() and "MATERIAL" in full_text.upper()):
#                 print(f"Processing Invoice: {filename}")
                
#                 # Extract Top-level Invoice Number
#                 invoice_no = None
#                 if m_inv := re.search(r"Invoice:\s*(\d{6,})", full_text):
#                     invoice_no = m_inv.group(1)

#                 # Extract PO Number
#                 po_number = None
#                 if m_po := re.search(r"PO:\s*(.*?)(?:\n|$)", full_text):
#                     raw = m_po.group(1).strip()
#                     # Filter out internal "DLL" codes, look for 5-digit PO
#                     if "DLL" not in raw:
#                         digits = re.search(r"(\d{5})", raw)
#                         if digits: po_number = f"PO-{digits.group(1)}"

#                 # Parse individual line items
#                 items = parse_invoice_text(full_text, filename, po_number, invoice_no)
#                 if items:
#                     temp_invoice_items[filename] = items
#                     # Log successful processing to DB
#                     log_processing_event("Syngenta", filename, {"method": extraction_method, "page_count": page_count}, po_number)

#         except Exception as e:
#             print(f"Error processing {filename}: {e}")

#     print(f"--- Analysis Map Keys: {list(analysis_map.keys())} ---")

#     # Link Analysis Data to Invoice Items
#     # This matches the extracted Lot Number from the Invoice to the key in the Analysis Map
#     grouped_results = {}
#     for filename, items in temp_invoice_items.items():
#         final_items = []
#         for item in items:
#             lot = item.get("VendorLotNo")
            
#             print(f"Linking Invoice Lot: '{lot}' ...")
#             if lot and lot in analysis_map:
#                 print(f"   >>> MATCH FOUND for {lot}")
#                 # Enrich Invoice item with Biological data from Analysis Map
#                 ana = analysis_map[lot]
#                 item["CurrentGerm"] = ana.get("CertificateGerm")
#                 item["CurrentGermDate"] = ana.get("CertificateGermDate")
#                 item["Purity"] = ana.get("Purity")
#                 item["Inert"] = ana.get("Inert")
#                 item["GrowerGerm"] = ana.get("CertificateGerm")
#                 item["GrowerGermDate"] = ana.get("CertificateGermDate")
#             else:
#                 print(f"   >>> NO MATCH in Analysis Data for {lot}")
                
#             final_items.append(item)
#         grouped_results[filename] = final_items
    
#     print("=== END EXTRACTION ===\n")
#     return grouped_results

import os
import re
import fitz  # PyMuPDF
import time
import requests
from typing import List, Dict, Tuple, Set
from db_logger import log_processing_event

# --- Azure Configuration ---
AZURE_ENDPOINT = os.getenv("AZURE_ENDPOINT")
AZURE_KEY = os.getenv("AZURE_KEY")

def extract_text_with_azure_ocr(pdf_bytes: bytes) -> List[str]:
    """
    Performs OCR on in-memory PDF bytes using Azure Form Recognizer.
    """
    if not AZURE_ENDPOINT or not AZURE_KEY:
        print("Warning: Azure credentials not set. Skipping OCR.")
        return []

    headers = {
        "Ocp-Apim-Subscription-Key": AZURE_KEY,
        "Content-Type": "application/pdf"
    }
    
    url = f"{AZURE_ENDPOINT}formrecognizer/documentModels/prebuilt-layout:analyze?api-version=2023-07-31"
    
    try:
        response = requests.post(url, headers=headers, data=pdf_bytes)
        if response.status_code != 202:
            print(f"Azure OCR request failed: {response.status_code} - {response.text}")
            return []

        result_url = response.headers["Operation-Location"]

        for _ in range(30):
            time.sleep(1.0)
            result = requests.get(result_url, headers={"Ocp-Apim-Subscription-Key": AZURE_KEY}).json()
            status = result.get("status")
            
            if status == "succeeded":
                lines = []
                for page in result.get("analyzeResult", {}).get("pages", []):
                    for line in page.get("lines", []):
                        content = line.get("content", "").strip()
                        if content:
                            lines.append(content)
                return lines
            elif status == "failed":
                print("Azure OCR analysis failed.")
                return []
                
        print("Azure OCR timed out.")
        return []
    except Exception as e:
        print(f"Error during Azure OCR: {e}")
        return []

def parse_analysis_text(text: str, filename: str, known_lots: Set[str]) -> Dict[str, Dict]:
    """
    Parses text specifically for Syngenta 'Report of Analysis' data.
    Prioritizes looking for lots known from the invoices.
    """
    data = {}
    lot_no = None

    print(f"--- Parsing Report Text ({filename}) ---")

    # --- Extract Lot Number ---
    
    # 1. Strategy: Search for Known Lots from Invoices (Highest Priority)
    for known_lot in sorted(known_lots, key=len, reverse=True):
        if known_lot in text:
            lot_no = known_lot
            print(f"   > Found Lot No (Linked from Invoice): {lot_no}")
            break

    # 2. Strategy: Look for Lot after "/" 
    if not lot_no:
        slash_matches = re.findall(r"/\s*([A-Z0-9]{3,})", text)
        for candidate in slash_matches:
            if re.search(r"[A-Z]", candidate) or len(candidate) > 5:
                lot_no = candidate
                print(f"   > Found Lot No (Slash Pattern): {lot_no}")
                break

    # 3. Strategy: Fallback Regex
    if not lot_no:
        syngenta_lot_pattern = re.compile(r"\b([A-Z]{3}\d{6}[A-Z]?)\b")
        m_label = re.search(r"Lot\s*Number\s*[:\.]?[\s\"\'\n,]*([A-Z0-9\s]+)", text, re.IGNORECASE)
        
        if m_label:
            label_text = m_label.group(1)
            m_precise = syngenta_lot_pattern.search(label_text)
            if m_precise:
                lot_no = m_precise.group(1)
        
        if not lot_no:
            candidates = syngenta_lot_pattern.findall(text)
            if candidates:
                lot_no = candidates[0]
                print(f"   > Found Lot No (Regex Fallback): {lot_no}")

    if not lot_no:
        print("   > No valid Lot No found in report text.")
        return {}

    # --- Extract Purity & Inert ---
    # Strategy: Find "Analyzed:" label and consume the value following it.
    #start_match = re.search(r"Analyzed\s*[:\.]?\s*[\d\.,]*", text, re.IGNORECASE)
    start_match = re.search(r"Analyzed:", text)
    
    if start_match:
        sub_text = text[start_match.end():start_match.end()+400]
        
        # UPDATED REGEX: Matches decimal OR integer numbers (e.g. "99.99", "99", "0.00")
        decimal_pattern = re.compile(r"(\d+(?:[\.,]\d+)?)")
        raw_numbers = decimal_pattern.findall(sub_text)
        
        valid_floats = []
        for raw in raw_numbers:
            try:
                val = float(raw.replace(",", "."))
                valid_floats.append(val)
            except: pass
            
        # Expected sequence: [Purity, Other, Inert, Weed, Germ]
        if len(valid_floats) >= 5:
            try:
                raw_purity = valid_floats[0] # Index 0: Pure
                raw_inert = valid_floats[2]  # Index 2: Inert
                raw_germ = valid_floats[4]   # Index 4: Germ

                # Sanity Check: Germination cannot be > 100 or < 70.
                if raw_germ > 100 or raw_germ < 70:
                    print(f"   > raw_germ {raw_germ} > 100 or < 70. Likely misalignment. Skipping biological data.")
                else:
                    if raw_purity == 100.0:
                        data["Purity"] = 99.99
                        data["Inert"] = 0.01
                    else:
                        data["Purity"] = raw_purity
                        data["Inert"] = raw_inert

                    if raw_germ == 100.0:
                        data["CertificateGerm"] = 99
                    else:
                        data["CertificateGerm"] = int(raw_germ)
                    
                    print(f"   > Extracted: Pure={data.get('Purity')}, Inert={data.get('Inert')}, Germ={data.get('CertificateGerm')}")
            except Exception as e:
                print(f"   > Error logic assigning values: {e}")
        else:
            print(f"   > Not enough values found after anchor: {valid_floats}")
    else:
        print("   > Anchor 'Analyzed:' not found in text.")

    # --- Extract Certificate Germ Date ---
    germ_info_match = re.search(r"Germination Information\s*[\r\n\s]+.*?Date Tested:\s*[\r\n\s]+.*?(\d{2}/\d{2}/\d{4})", text, re.DOTALL | re.IGNORECASE)
    if germ_info_match:
        data["CertificateGermDate"] = germ_info_match.group(1)
    else:
        all_dates = re.findall(r"\d{2}/\d{2}/\d{4}", text)
        if len(all_dates) >= 3:
             data["CertificateGermDate"] = all_dates[2]
        elif all_dates:
             data["CertificateGermDate"] = all_dates[-1]

    if data:
        return {lot_no: data}
    return {}

# def parse_analysis_text(text: str, filename: str, known_lots: Set[str]) -> Dict[str, Dict]:
#     """
#     Parses text specifically for Syngenta 'Report of Analysis' data.
#     Prioritizes looking for lots known from the invoices.
#     """
#     data = {}
#     lot_no = None

#     print(f"--- Parsing Report Text ({filename}) ---")
#     print("Extracted text from certificate:")
#     print(text)

#     # --- Extract Lot Number ---
    
#     # 1. Strategy: Search for Known Lots from Invoices (Highest Priority)
#     # Sort by length descending to match most specific lot first if overlaps exist
#     for known_lot in sorted(known_lots, key=len, reverse=True):
#         if known_lot in text:
#             lot_no = known_lot
#             print(f"   > Found Lot No (Linked from Invoice): {lot_no}")
#             break

#     # 2. Strategy: Look for Lot after "/" (User suggestion: "lot number after '/' does work in most cases")
#     # Matches patterns like "Batch: / LOT123" or "Ref / LOTABC"
#     if not lot_no:
#         # Regex looks for forward slash, optional spaces, then alphanumeric characters (at least 3)
#         # Avoids matching dates like /2023 by ensuring it's not just 4 digits or looks like a lot
#         slash_matches = re.findall(r"/\s*([A-Z0-9]{3,})", text)
#         for candidate in slash_matches:
#             # Simple heuristic: Syngenta lots usually contain at least one letter or are specific IDs
#             if re.search(r"[A-Z]", candidate) or len(candidate) > 5:
#                 lot_no = candidate
#                 print(f"   > Found Lot No (Slash Pattern): {lot_no}")
#                 break

#     # 3. Strategy: Fallback Regex (Original Logic - Lowest Priority)
#     if not lot_no:
#         syngenta_lot_pattern = re.compile(r"\b([A-Z]{3}\d{6}[A-Z]?)\b")
#         m_label = re.search(r"Lot\s*Number\s*[:\.]?[\s\"\'\n,]*([A-Z0-9\s]+)", text, re.IGNORECASE)
        
#         if m_label:
#             label_text = m_label.group(1)
#             m_precise = syngenta_lot_pattern.search(label_text)
#             if m_precise:
#                 lot_no = m_precise.group(1)
        
#         if not lot_no:
#             candidates = syngenta_lot_pattern.findall(text)
#             if candidates:
#                 lot_no = candidates[0]
#                 print(f"   > Found Lot No (Regex Fallback): {lot_no}")

#     if not lot_no:
#         print("   > No valid Lot No found in report text.")
#         return {}
    
#     # --- Extract Purity & Inert ---
#     # Strategy: Find "Grams Analyzed" label and consume the value following it.
#     # We use regex to match the label + optional punctuation + optional number.
#     # This ensures start_match.end() is AFTER the grams value (e.g. 1000.0),
#     # so the next number found is Purity.
#     start_match = re.search(r"Analyzed\s*[:\.]?\s*[\d\.,]*", text, re.IGNORECASE)
    
#     if start_match:
#         sub_text = text[start_match.end():start_match.end()+400]
        
#         # Matches decimal numbers e.g. "99.99", "0.00", "100.0"
#         decimal_pattern = re.compile(r"(\d+[\.,]\d+)")
#         raw_numbers = decimal_pattern.findall(sub_text)

#     # # --- Extract Purity & Inert ---
#     # # Strategy: Find "1000." (grams analyzed) then grab numbers with DECIMALS
#     # start_match = re.search(r"1000\.", text)
#     # if start_match:
#     #     sub_text = text[start_match.end():start_match.end()+400]
#     #     decimal_pattern = re.compile(r"(\d+[\.,]\d+)")
#     #     raw_numbers = decimal_pattern.findall(sub_text)
        
#         valid_floats = []
#         for raw in raw_numbers:
#             try:
#                 val = float(raw.replace(",", "."))
#                 valid_floats.append(val)
#             except: pass
            
#         if len(valid_floats) >= 5:
#             try:
#                 raw_purity = valid_floats[0] # Index 0: Pure
#                 raw_inert = valid_floats[2]  # Index 2: Inert
#                 raw_germ = valid_floats[4]   # Index 4: Germ

#                 if raw_purity == 100.0:
#                     data["Purity"] = 99.99
#                     data["Inert"] = 0.01
#                 else:
#                     data["Purity"] = raw_purity
#                     data["Inert"] = raw_inert

#                 if raw_germ == 100.0:
#                     data["CertificateGerm"] = 99
#                 else:
#                     data["CertificateGerm"] = int(raw_germ)
                    
#                 print(f"   > Extracted: Pure={data.get('Purity')}, Inert={data.get('Inert')}, Germ={data.get('CertificateGerm')}")
#             except Exception as e:
#                 print(f"   > Error logic assigning values: {e}")
#         else:
#             print(f"   > Not enough decimal values found after 1000.: {valid_floats}")
#     else:
#         print("   > Anchor 'Analyzed:' not found in text.")

#     # --- Extract Certificate Germ Date ---
#     germ_info_match = re.search(r"Germination Information\s*[\r\n\s]+.*?Date Tested:\s*[\r\n\s]+.*?(\d{2}/\d{2}/\d{4})", text, re.DOTALL | re.IGNORECASE)
#     if germ_info_match:
#         data["CertificateGermDate"] = germ_info_match.group(1)
#     else:
#         all_dates = re.findall(r"\d{2}/\d{2}/\d{4}", text)
#         if len(all_dates) >= 3:
#              data["CertificateGermDate"] = all_dates[2]
#         elif all_dates:
#              data["CertificateGermDate"] = all_dates[-1]

#     if data:
#         return {lot_no: data}
#     return {}

def process_item_block(block_lines: List[str], global_po: str, invoice_no: str, is_kamterter: bool) -> Dict:
    """
    Process a list of strings representing a single item block from Invoice.
    """
    item_data = {
        "VendorInvoiceNo": invoice_no,
        "PurchaseOrder": global_po,
        "VendorItemNumber": block_lines[0], 
        "VendorItemDescription": "",
        "VendorTreatment": "",
        "VendorLotNo": None,
        "VendorBatchNo": None,
        "OriginCountry": None,
        "SeedCount": None,
        "SeedSize": None,
        "QuantityLine": 0.0,
        "Unit Price": 0.0,
        "TotalPrice": 0.0,
        "PackageDescription": None,
        "TotalQuantity": 0.0,
        "USD_Actual_Cost_$": 0.0,
        "CurrentGerm": None,
        "CurrentGermDate": None
    }

    idx_material = -1
    idx_ea = -1
    idx_trt_code = -1
    idx_lot = -1
    
    for i, line in enumerate(block_lines):
        if i == 1 and re.match(r"^\d{8}$", line):
            idx_material = i
        if line.strip() == "EA":
            idx_ea = i
        if "TRT CODE" in line:
            idx_trt_code = i
        if line.startswith("/"):
            idx_lot = i
            item_data["VendorLotNo"] = line.replace("/", "").strip()

    if idx_ea > 0:
        try:
            qty_line = block_lines[idx_ea - 1].replace(",", "")
            item_data["QuantityLine"] = float(qty_line)
        except: pass
        
        if idx_ea + 1 < len(block_lines):
            try:
                price_line = block_lines[idx_ea + 1].replace(",", "")
                item_data["Unit Price"] = float(price_line)
            except: pass
            
        if idx_ea + 2 < len(block_lines):
            try:
                total_line = block_lines[idx_ea + 2].replace(",", "")
                item_data["TotalPrice"] = float(total_line)
            except: pass

    if idx_lot > 0:
        prev_line = block_lines[idx_lot - 1].strip()
        if re.match(r"^\d{8}$", prev_line):
            item_data["VendorBatchNo"] = prev_line

    start_idx = (idx_material if idx_material != -1 else 0) + 1
    candidates = [x for x in [idx_trt_code, idx_lot - 1 if idx_lot > 0 else -1, idx_ea - 1 if idx_ea > 0 else -1] if x > start_idx]
    end_idx = min(candidates) if candidates else len(block_lines)
    
    raw_text_lines = block_lines[start_idx:end_idx]
    full_text = " ".join(raw_text_lines)
    
    if "KS" in full_text:
        parts = full_text.split("KS", 1)
        item_data["VendorItemDescription"] = parts[0].strip() + " KS"
        raw_treatment = parts[1]
        raw_treatment = raw_treatment.replace("Bag", "").replace("Foil", "").strip()
        item_data["VendorTreatment"] = raw_treatment
    else:
        item_data["VendorItemDescription"] = full_text

    for line in block_lines:
        if "PO#" in line or "PO :" in line:
            m = re.search(r"(\d{5})", line)
            if m: item_data["PurchaseOrder"] = f"PO-{m.group(1)}"
        if "Seeds/LB:" in line:
            m = re.search(r"Seeds/LB:\s*([\d,]+)", line)
            if m: item_data["SeedCount"] = int(m.group(1).replace(",", ""))
        if "Size:" in line:
            m = re.search(r"Size:\s*([A-Z]{2})", line)
            if m: item_data["SeedSize"] = m.group(1)
        if "Origin:" in line:
            m = re.search(r"Origin:\s*([A-Z]{2})", line)
            if m: item_data["OriginCountry"] = m.group(1)

    pkg_size_ks = 0
    m_ks = re.search(r"(\d+)\s*KS", item_data["VendorItemDescription"])
    if m_ks:
        pkg_size_ks = int(m_ks.group(1))
    
    if is_kamterter:
        item_data["PackageDescription"] = "SUBCON BULK-MS"
    elif pkg_size_ks > 0:
        item_data["PackageDescription"] = f"{pkg_size_ks * 1000:,} SEEDS"

    if pkg_size_ks > 0:
        item_data["TotalQuantity"] = item_data["QuantityLine"] * pkg_size_ks
    else:
        item_data["TotalQuantity"] = item_data["QuantityLine"]

    if item_data["TotalQuantity"] > 0:
        item_data["USD_Actual_Cost_$"] = round(item_data["TotalPrice"] / item_data["TotalQuantity"], 4)

    if not item_data["SeedSize"]:
        d = item_data["VendorItemDescription"].upper()
        if "LR" in d: item_data["SeedSize"] = "LR"
        elif "MF" in d: item_data["SeedSize"] = "MF"
        elif "MR" in d: item_data["SeedSize"] = "MR"
        elif "LF" in d: item_data["SeedSize"] = "LF"

    return item_data

def parse_invoice_text(text: str, filename: str, global_po: str, invoice_no: str) -> List[Dict]:
    """
    Splits invoice text into item blocks and processes them.
    """
    text = re.sub(r"The following table:", "", text)
    lines = [l.strip() for l in text.splitlines() if l.strip()]
    is_kamterter = "KAMTERTER" in text.upper()
    
    item_indices = []
    for i, line in enumerate(lines):
        if re.match(r"^\d{5}$", line):
            if i + 1 < len(lines) and re.match(r"^\d{8}$", lines[i+1]):
                item_indices.append(i)
    
    items = []
    for k, start_idx in enumerate(item_indices):
        end_idx = item_indices[k+1] if k + 1 < len(item_indices) else len(lines)
        block_lines = lines[start_idx:end_idx]
        item = process_item_block(block_lines, global_po, invoice_no, is_kamterter)
        if item:
            items.append(item)
            
    return items

def extract_syngenta_data_from_bytes(pdf_files: List[Tuple[str, bytes]], pkg_desc_list: list) -> Dict[str, List[Dict]]:
    analysis_map = {}
    temp_invoice_items = {}
    
    # Store tuples of (filename, text) for analysis reports to process in Pass 2
    analysis_files_queue = []
    
    print("\n=== START SYNGENTA EXTRACTION (Azure OCR Enabled) ===")

    # --- PASS 1: Extract Text & Process Invoices ---
    for filename, pdf_bytes in pdf_files:
        try:
            # 1. Try simple Text Extraction first
            doc = fitz.open(stream=pdf_bytes, filetype="pdf")
            page_count = doc.page_count
            full_text = ""
            for page in doc:
                full_text += page.get_text() + "\n"
            doc.close()

            extraction_method = "PyMuPDF"
            stripped_text = full_text.strip()
            
            needs_ocr = False
            if len(stripped_text) < 100:
                needs_ocr = True
            elif ("REPORT OF ANALYSIS" in full_text.upper() or "PURITY ANALYSIS" in full_text.upper()):
                if "1000." not in full_text:
                    needs_ocr = True
            
            if needs_ocr:
                print(f"   > Detected scanned/image PDF for {filename}. Attempting Azure OCR...")
                ocr_lines = extract_text_with_azure_ocr(pdf_bytes)
                if ocr_lines:
                    full_text = "\n".join(ocr_lines)
                    extraction_method = "Azure OCR"
                else:
                    print("   > Azure OCR returned no text. Using original text.")

            # --- Categorize Document ---
            is_analysis = "REPORT OF ANALYSIS" in full_text.upper() or "PURITY ANALYSIS" in full_text.upper()
            is_invoice = ("INVOICE" in full_text.upper() and "SYNGENTA" in full_text.upper()) or ("ITEM" in full_text.upper() and "MATERIAL" in full_text.upper())

            if is_invoice:
                print(f"Processing Invoice: {filename}")
                invoice_no = None
                if m_inv := re.search(r"Invoice:\s*(\d{6,})", full_text):
                    invoice_no = m_inv.group(1)

                po_number = None
                if m_po := re.search(r"PO:\s*(.*?)(?:\n|$)", full_text):
                    raw = m_po.group(1).strip()
                    if "DLL" not in raw:
                        digits = re.search(r"(\d{5})", raw)
                        if digits: po_number = f"PO-{digits.group(1)}"

                items = parse_invoice_text(full_text, filename, po_number, invoice_no)
                if items:
                    temp_invoice_items[filename] = items
                    log_processing_event("Syngenta", filename, {"method": extraction_method, "page_count": page_count}, po_number)
            
            elif is_analysis:
                # Queue for Pass 2 (we need invoice lots first)
                analysis_files_queue.append((filename, full_text))
            
        except Exception as e:
            print(f"Error processing {filename}: {e}")

    # --- PASS 2: Process Analysis Reports with Known Lots ---
    
    # Collect all known lots from extracted invoices
    all_invoice_lots = set()
    for items in temp_invoice_items.values():
        for item in items:
            if item.get("VendorLotNo"):
                all_invoice_lots.add(item.get("VendorLotNo"))
    
    print(f"--- Known Lots from Invoices: {all_invoice_lots} ---")

    for filename, text in analysis_files_queue:
        result = parse_analysis_text(text, filename, all_invoice_lots)
        if result:
            analysis_map.update(result)

    print(f"--- Analysis Map Keys: {list(analysis_map.keys())} ---")

    # --- Link Analysis Data ---
    grouped_results = {}
    for filename, items in temp_invoice_items.items():
        final_items = []
        for item in items:
            lot = item.get("VendorLotNo")
            
            print(f"Linking Invoice Lot: '{lot}' ...")
            if lot and lot in analysis_map:
                print(f"   >>> MATCH FOUND for {lot}")
                ana = analysis_map[lot]
                item["CurrentGerm"] = ana.get("CertificateGerm")
                item["CurrentGermDate"] = ana.get("CertificateGermDate")
                item["Purity"] = ana.get("Purity")
                item["Inert"] = ana.get("Inert")
                item["GrowerGerm"] = ana.get("CertificateGerm")
                item["GrowerGermDate"] = ana.get("CertificateGermDate")
            else:
                print(f"   >>> NO MATCH in Analysis Data for {lot}")
                
            final_items.append(item)
        grouped_results[filename] = final_items
    
    print("=== END EXTRACTION ===\n")
    return grouped_results