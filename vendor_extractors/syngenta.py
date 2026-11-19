# import os
# import re
# import fitz  # PyMuPDF
# import time
# import requests
# from typing import List, Dict, Tuple, Set
# from db_logger import log_processing_event

# # --- Azure Configuration ---
# AZURE_ENDPOINT = os.getenv("AZURE_ENDPOINT")
# AZURE_KEY = os.getenv("AZURE_KEY")

# def extract_text_with_azure_ocr(pdf_bytes: bytes) -> List[str]:
#     """
#     Performs OCR on in-memory PDF bytes using Azure Form Recognizer.
#     """
#     if not AZURE_ENDPOINT or not AZURE_KEY:
#         print("Warning: Azure credentials not set. Skipping OCR.")
#         return []

#     headers = {
#         "Ocp-Apim-Subscription-Key": AZURE_KEY,
#         "Content-Type": "application/pdf"
#     }
    
#     url = f"{AZURE_ENDPOINT}formrecognizer/documentModels/prebuilt-layout:analyze?api-version=2023-07-31"
    
#     try:
#         response = requests.post(url, headers=headers, data=pdf_bytes)
#         if response.status_code != 202:
#             print(f"Azure OCR request failed: {response.status_code} - {response.text}")
#             return []

#         result_url = response.headers["Operation-Location"]

#         for _ in range(30):
#             time.sleep(1.0)
#             result = requests.get(result_url, headers={"Ocp-Apim-Subscription-Key": AZURE_KEY}).json()
#             status = result.get("status")
            
#             if status == "succeeded":
#                 lines = []
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

# def parse_analysis_text(text: str, filename: str, known_lots: Set[str]) -> Dict[str, Dict]:
#     """
#     Parses text specifically for Syngenta 'Report of Analysis' data.
#     Prioritizes looking for lots known from the invoices.
#     """
#     data = {}
#     lot_no = None

#     print(f"--- Parsing Report Text ({filename}) ---")

#     # --- Extract Lot Number ---
    
#     # 1. Strategy: Search for Known Lots from Invoices (Highest Priority)
#     for known_lot in sorted(known_lots, key=len, reverse=True):
#         if known_lot in text:
#             lot_no = known_lot
#             print(f"   > Found Lot No (Linked from Invoice): {lot_no}")
#             break

#     # 2. Strategy: Look for Lot after "/" 
#     if not lot_no:
#         slash_matches = re.findall(r"/\s*([A-Z0-9]{3,})", text)
#         for candidate in slash_matches:
#             if re.search(r"[A-Z]", candidate) or len(candidate) > 5:
#                 lot_no = candidate
#                 print(f"   > Found Lot No (Slash Pattern): {lot_no}")
#                 break

#     # 3. Strategy: Fallback Regex
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
#     # Strategy: Find "Analyzed:" label and consume the value following it.
#     start_match = re.search(r"Analyzed:", text)
    
#     if start_match:
#         # Reduced window size to avoid picking up footer numbers
#         sub_text = text[start_match.end():start_match.end()+250]
        
#         # Regex: Added \b to avoid capturing digits in "KNO3" (e.g. 3)
#         decimal_pattern = re.compile(r"\b(\d+(?:[\.,]\d+)?)\b")
#         raw_numbers = decimal_pattern.findall(sub_text)
        
#         valid_floats = []
#         for raw in raw_numbers:
#             try:
#                 val = float(raw.replace(",", "."))
#                 valid_floats.append(val)
#             except: pass
            
#         # LOGIC UPDATE: Handle Grams Analyzed Value
#         # If first value is > 100 (e.g. 510.0), it's the weight. Remove it.
#         if valid_floats and valid_floats[0] > 100:
#             print(f"   > Skipping 'Grams Analyzed' value: {valid_floats[0]}")
#             valid_floats.pop(0)

#         # Expected sequence: [Purity, Other, Inert, Weed, Germ]
#         if len(valid_floats) >= 5:
#             try:
#                 raw_purity = valid_floats[0] # Index 0: Pure
#                 raw_inert = valid_floats[2]  # Index 2: Inert
#                 raw_germ = valid_floats[4]   # Index 4: Germ

#                 # Sanity Check: Germination cannot be > 100.
#                 if raw_germ > 100:
#                     print(f"   > raw_germ {raw_germ} > 100. Likely misalignment. Skipping biological data.")
#                 else:
#                     if raw_purity == 100.0:
#                         data["Purity"] = 99.99
#                         data["Inert"] = 0.01
#                     else:
#                         data["Purity"] = raw_purity
#                         data["Inert"] = raw_inert

#                     if raw_germ == 100.0:
#                         data["CertificateGerm"] = 99
#                     else:
#                         data["CertificateGerm"] = int(raw_germ)
                    
#                     print(f"   > Extracted: Pure={data.get('Purity')}, Inert={data.get('Inert')}, Germ={data.get('CertificateGerm')}")
#             except Exception as e:
#                 print(f"   > Error logic assigning values: {e}")
#         else:
#             print(f"   > Not enough values found after anchor: {valid_floats}")
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

# def process_item_block(block_lines: List[str], global_po: str, invoice_no: str, is_kamterter: bool) -> Dict:
#     """
#     Process a list of strings representing a single item block from Invoice.
#     Robust version: Joins text first to preserve description context, then truncates footer noise.
#     """
#     # 1. CLEANUP: Remove "HWT" noise from all lines immediately
#     block_lines = [re.sub(r"HWT", "", line).strip() for line in block_lines if line.strip()]

#     item_data = {
#         "VendorInvoiceNo": invoice_no,
#         "PurchaseOrder": global_po,
#         "VendorItemNumber": block_lines[0] if block_lines else "", 
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
#         "CurrentGerm": None,
#         "CurrentGermDate": None
#     }

#     if not block_lines:
#         return item_data

#     used_indices = set()
#     used_indices.add(0)

#     # --- 2. Identify Material Number ---
#     for i, line in enumerate(block_lines):
#         if i == 1 and re.match(r"^\d{8}$", line):
#             used_indices.add(i)
#             break

#     # --- 3. Extract Financials (Anchor: "EA") ---
#     idx_ea = -1
#     for i, line in enumerate(block_lines):
#         if line.strip() == "EA":
#             idx_ea = i
#             used_indices.add(i)
#             break
    
#     if idx_ea > 0:
#         if idx_ea - 1 >= 0:
#             try:
#                 qty_line = block_lines[idx_ea - 1].replace(",", "")
#                 if re.match(r"^\d+(\.\d+)?$", qty_line):
#                     item_data["QuantityLine"] = float(qty_line)
#                     used_indices.add(idx_ea - 1)
#             except: pass
#         if idx_ea + 1 < len(block_lines):
#             try:
#                 price_line = block_lines[idx_ea + 1].replace(",", "")
#                 if re.match(r"^\d+(\.\d+)?$", price_line):
#                     item_data["Unit Price"] = float(price_line)
#                     used_indices.add(idx_ea + 1)
#             except: pass
#         if idx_ea + 2 < len(block_lines):
#             try:
#                 total_line = block_lines[idx_ea + 2].replace(",", "").replace(" ", "")
#                 if re.match(r"^\d+(\.\d+)?$", total_line):
#                     item_data["TotalPrice"] = float(total_line)
#                     used_indices.add(idx_ea + 2)
#             except: pass

#     # --- 4. Extract Lot Number ---
#     for i, line in enumerate(block_lines):
#         if line.startswith("/"):
#             extracted_lot = line.replace("/", "").strip()
#             used_indices.add(i)
#             for offset in range(1, 4):
#                 if i + offset < len(block_lines):
#                     next_line = block_lines[i + offset].strip()
#                     if re.match(r"^\d{1,5}$", next_line):
#                         extracted_lot += next_line
#                         used_indices.add(i + offset)
#                         break 
#                     if re.search(r"[a-zA-Z]", next_line):
#                         continue
#                     if "." in next_line:
#                         break
#             item_data["VendorLotNo"] = extracted_lot
#             break 

#     # --- 5. Extract Batch Number ---
#     for i, line in enumerate(block_lines):
#         if i in used_indices: continue
#         m_batch = re.search(r"\b(\d{8})\b|\b(\d{6}\s\d{2})\b", line)
#         if m_batch:
#             clean_batch = m_batch.group(0).replace(" ", "")
#             if "." not in line and len(clean_batch) == 8:
#                 item_data["VendorBatchNo"] = clean_batch
#                 used_indices.add(i)
#                 break

#     # --- 6. Extract Metadata (Keywords) ---
#     for i, line in enumerate(block_lines):
#         if "PO#" in line or "PO :" in line:
#             m = re.search(r"(\d{5})", line)
#             if m: 
#                 item_data["PurchaseOrder"] = f"PO-{m.group(1)}"
#                 used_indices.add(i)
        
#         if "Seeds/LB:" in line:
#             m = re.search(r"Seeds/LB:\s*([\d,]+)", line)
#             if m: 
#                 item_data["SeedCount"] = int(m.group(1).replace(",", ""))
#                 used_indices.add(i)
        
#         # --- FIXED SEED SIZE LOGIC ---
#         # Checks for "Size" generally to handle "Size: 5.5" or "Size : 5.5"
#         if "Size" in line:
#             # Capture numeric ranges (e.g. 5.5-6.0H) OR 2+ letter codes (e.g. LR, MEDIUM)
#             # \s*[:\.]? handles "Size:" or "Size :" or "Size."
#             m = re.search(r"Size\s*[:\.]?\s*([\d\.\-\s]+[A-Z]?|[A-Z]{2,}\b)", line, re.IGNORECASE)
#             if m:
#                 item_data["SeedSize"] = m.group(1).strip()
#                 # Remove from line so it doesn't pollute Treatment
#                 block_lines[i] = block_lines[i].replace(m.group(0), " ").strip()
#                 # If line becomes empty/garbage, mark used
#                 if len(block_lines[i]) < 3: 
#                     used_indices.add(i)

#         if "Origin:" in line:
#             m = re.search(r"Origin:\s*([A-Z]{2})", line)
#             if m: 
#                 item_data["OriginCountry"] = m.group(1)
#                 used_indices.add(i)
        
#         if "TRT CODE" in line:
#             used_indices.add(i)
#         if "Germ" in line and "%" in line:
#             used_indices.add(i)
#         if "Date:" in line:
#             used_indices.add(i)

#     # --- 7. Extract Description & Treatment ---
#     # Gather ALL remaining lines first (Do not pre-filter treatment lines)
#     remaining_lines = []
#     for i, line in enumerate(block_lines):
#         if i not in used_indices:
#             remaining_lines.append(line)

#     full_text = " ".join(remaining_lines).strip()

#     # >>> NOISE REMOVAL STEP <<<
#     # Cut off text starting with footer phrases found in OCR overflow
#     noise_pattern = re.compile(
#         r"(Sub Total|TOTAL\b|Total due|Thank you|This invoice is|Page\s*:|syngenta\s*Invoice|Item\s*Material|\d+%?\s*DISCOUNT|DISCOUNT\s*APPLIED)", 
#         re.IGNORECASE
#     )
#     match = noise_pattern.search(full_text)
#     if match:
#         full_text = full_text[:match.start()].strip()
#     # >>> END NOISE REMOVAL <<<

#     # Split Desc/Treatment on "KS" if present
#     if "KS" in full_text:
#         parts = full_text.split("KS", 1)
#         item_data["VendorItemDescription"] = parts[0].strip() + " KS"
        
#         remainder = parts[1].strip()
        
#         # Check if packaging type follows KS
#         pkg_match = re.match(r"^(Pail|Foil|Bag|Carton|Box)\b", remainder, re.IGNORECASE)
#         if pkg_match:
#             pkg_type = pkg_match.group(0)
#             item_data["VendorItemDescription"] += " " + pkg_type
#             remainder = remainder[len(pkg_type):].strip()
            
#         item_data["VendorTreatment"] = remainder.replace("Pail", "").replace("Foil", "").replace("Bag", "").strip()
        
#     elif "Metal-" in full_text:
#         parts = full_text.split("Metal-", 1)
#         item_data["VendorItemDescription"] = parts[0].strip()
#         item_data["VendorTreatment"] = "Metal-" + parts[1].strip()
        
#     elif "FarMore" in full_text:
#         parts = full_text.split("FarMore", 1)
#         item_data["VendorItemDescription"] = parts[0].strip()
#         item_data["VendorTreatment"] = "FarMore" + parts[1].strip()
        
#     else:
#         item_data["VendorItemDescription"] = full_text

#     # --- 8. Final Cleanups & Calcs ---
#     if item_data["PurchaseOrder"] is None:
#         item_data["PurchaseOrder"] = global_po

#     pkg_size_ks = 0
#     m_ks = re.search(r"(\d+)\s*KS", item_data["VendorItemDescription"])
#     if m_ks:
#         pkg_size_ks = int(m_ks.group(1))
    
#     if is_kamterter:
#         item_data["PackageDescription"] = "SUBCON BULK-MS"
#     elif pkg_size_ks > 0:
#         item_data["PackageDescription"] = f"{pkg_size_ks * 1000:,} SEEDS"

#     if pkg_size_ks > 0:
#         item_data["TotalQuantity"] = item_data["QuantityLine"] * pkg_size_ks
#     else:
#         item_data["TotalQuantity"] = item_data["QuantityLine"]

#     if item_data["TotalQuantity"] > 0:
#         item_data["USD_Actual_Cost_$"] = round(item_data["TotalPrice"] / item_data["TotalQuantity"], 4)

#     if not item_data["SeedSize"]:
#         d = item_data["VendorItemDescription"].upper()
#         if "LR" in d: item_data["SeedSize"] = "LR"
#         elif "MF" in d: item_data["SeedSize"] = "MF"
#         elif "MR" in d: item_data["SeedSize"] = "MR"
#         elif "LF" in d: item_data["SeedSize"] = "LF"

#     return item_data

# def parse_invoice_text(text: str, filename: str, global_po: str, invoice_no: str) -> List[Dict]:
#     """
#     Splits invoice text into item blocks and processes them.
#     """
#     lines = [l.strip() for l in text.splitlines() if l.strip()]
#     is_kamterter = "KAMTERTER" in text.upper()
    
#     item_indices = []
#     for i, line in enumerate(lines):
#         if re.match(r"^\d{5}$", line):
#             if i + 1 < len(lines) and re.match(r"^\d{8}$", lines[i+1]):
#                 item_indices.append(i)
    
#     items = []
#     for k, start_idx in enumerate(item_indices):
#         end_idx = item_indices[k+1] if k + 1 < len(item_indices) else len(lines)
#         block_lines = lines[start_idx:end_idx]
#         item = process_item_block(block_lines, global_po, invoice_no, is_kamterter)
#         if item:
#             items.append(item)
            
#     return items

# def extract_syngenta_data_from_bytes(pdf_files: List[Tuple[str, bytes]], pkg_desc_list: list) -> Dict[str, List[Dict]]:
#     analysis_map = {}
#     temp_invoice_items = {}
    
#     # Store tuples of (unique_page_id, text) for analysis reports to process in Pass 2
#     analysis_files_queue = []
    
#     print("\n=== START SYNGENTA EXTRACTION (Page-by-Page Logic) ===")

#     # --- PASS 1: Extract Text, Classify Pages & Process Invoices ---
#     for filename, pdf_bytes in pdf_files:
#         try:
#             doc = fitz.open(stream=pdf_bytes, filetype="pdf")
#             final_page_count = doc.page_count
            
#             # Buffer to hold multi-page invoice text
#             current_invoice_text = ""
#             invoice_pages_found = False
            
#             print(f"Processing File: {filename} ({final_page_count} pages)")

#             for i, page in enumerate(doc):
#                 page_num = i + 1
#                 page_text = page.get_text()
                
#                 stripped_text = page_text.strip()
#                 needs_ocr = False
                
#                 if len(stripped_text) < 50:
#                     needs_ocr = True
#                 elif ("REPORT OF ANALYSIS" not in page_text.upper() 
#                     or "PURITY ANALYSIS" not in page_text.upper()):
                    
#                     if ("INVOICE" not in page_text.upper() 
#                     or "SYNGENTA" not in page_text.upper()):
#                         needs_ocr = True
#                     elif ("INVOICE" in page_text.upper()):
#                         print(f"   > Detected {filename} as a searchable Invoice.")
                
#                 elif ("REPORT OF ANALYSIS" in page_text.upper() 
#                     or "PURITY ANALYSIS" in page_text.upper()):
#                     print(f"   > Detected {filename} as a searchable Certificate.")
                
#                 if needs_ocr:
#                     print(f"   > Page {page_num} appears scanned. Attempting Azure OCR...")
#                     new_doc = fitz.open()
#                     new_doc.insert_pdf(doc, from_page=i, to_page=i)
#                     page_bytes = new_doc.tobytes()
#                     new_doc.close()
                    
#                     ocr_lines = extract_text_with_azure_ocr(page_bytes)
#                     if ocr_lines:
#                         page_text = "\n".join(ocr_lines)
#                         print(f"   > OCR Successful for Page {page_num}")
#                     else:
#                         print(f"   > OCR failed/empty for Page {page_num}")

#                 # --- CLASSIFY PAGE ---
#                 page_upper = page_text.upper()
                
#                 is_analysis = "REPORT OF ANALYSIS" in page_upper and "VIABILITY" in page_upper
                
#                 is_invoice = ("INVOICE" in page_upper and "SYNGENTA" in page_upper and "STOKES" in page_upper)
                            
#                 if is_analysis:
#                     print(f"   > [Page {page_num}] Identified as Analysis Report.")
#                     unique_id = f"{filename}_pg{page_num}"
#                     analysis_files_queue.append((unique_id, page_text))
                
#                 elif is_invoice:
#                     print(f"   > [Page {page_num}] Identified as Invoice.")
#                     current_invoice_text += page_text + "\n"
#                     invoice_pages_found = True
                
#                 else:
#                     print(f"   > [Page {page_num}] Irrelevant/Unknown content. Skipping.")

#             doc.close()

#             # --- PROCESS ACCUMULATED INVOICE TEXT ---
#             if invoice_pages_found and current_invoice_text.strip():
#                 print(f"   > Parsing accumulated invoice text for {filename}...")
                
#                 invoice_no = None
#                 if m_inv := re.search(r"Invoice:\s*(\d{6,})", current_invoice_text):
#                     invoice_no = m_inv.group(1)

#                 po_number = None
#                 if m_po := re.search(r"PO:\s*(.*?)(?:\n|$)", current_invoice_text):
#                     raw = m_po.group(1).strip()
#                     if "DLL" not in raw:
#                         digits = re.search(r"(\d{5})", raw)
#                         if digits: po_number = f"PO-{digits.group(1)}"

#                 items = parse_invoice_text(current_invoice_text, filename, po_number, invoice_no)
#                 if items:
#                     temp_invoice_items[filename] = items
#                     # USE FINAL_PAGE_COUNT to avoid 'document closed' error
#                     log_processing_event("Syngenta", filename, {"method": "Mixed/Page-Level", "page_count": final_page_count}, po_number)

#         except Exception as e:
#             print(f"Error processing file {filename}: {e}")

#     # --- PASS 2: Process Analysis Reports with Known Lots ---
#     all_invoice_lots = set()
#     for items in temp_invoice_items.values():
#         for item in items:
#             if item.get("VendorLotNo"):
#                 all_invoice_lots.add(item.get("VendorLotNo"))
    
#     print(f"--- Known Lots from Invoices: {all_invoice_lots} ---")

#     for unique_id, text in analysis_files_queue:
#         result = parse_analysis_text(text, unique_id, all_invoice_lots)
#         if result:
#             analysis_map.update(result)

#     print(f"--- Analysis Map Keys: {list(analysis_map.keys())} ---")

#     # --- Link Analysis Data ---
#     grouped_results = {}
#     for filename, items in temp_invoice_items.items():
#         final_items = []
#         for item in items:
#             lot = item.get("VendorLotNo")
            
#             print(f"Linking Invoice Lot: '{lot}' ...")
#             if lot and lot in analysis_map:
#                 print(f"   >>> MATCH FOUND for {lot}")
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

import logging
import os
import re
import fitz  # PyMuPDF
import time
import requests
from datetime import datetime
from typing import List, Dict, Tuple, Set
from db_logger import log_processing_event
from vendor_extractors.certificate_ocr import (
    CUSTOMS_RELEASE_STAMP,
    certificate_fields_missing,
    fill_missing_certificate_fields,
)

# --- Azure Configuration ---
AZURE_ENDPOINT = os.getenv("AZURE_ENDPOINT")
AZURE_KEY = os.getenv("AZURE_KEY")
_CERTIFICATE_FIELDS = ("CertificateGerm", "CertificateGermDate", "Purity", "Inert")
_CERTIFICATE_DATE = r"\b\d{1,2}/\d{1,2}/\d{4}\b"
_PACKING_LIST = re.compile(r"\bPACKING\s+LIST\b", re.I)
_INVOICE_NUMBER = re.compile(r"\bInvoice(?:\s+(?:No\.?|Number))?\s*[:#]\s*(\d{6,})\b", re.I)
_PAGE_NUMBER = re.compile(r"\bPage\s*:?\s*(\d+)\s+of\s+(\d+)\b", re.I)
_INVOICE_TABLE = re.compile(r"\bBatch\s*/\s*Lot\b|\bUnit\s+Price\s+Total\s+USD\b", re.I)
_DAMAGED_ITEM_NUMBER = re.compile(r"^(?=[\d/|?]*[/|?])[\d/|?]{4,7}(?=[ \t]+\d{8}\b)")
_AMOUNT = r"-?(?:\d{1,3}(?:,\d{3})+|\d+)(?:\.\d+)?"
_QUANTITY_EA = re.compile(rf"(?<!\S){_AMOUNT}\s+EA\b", re.I)
_INLINE_AMOUNTS = re.compile(
    rf"(?<!\S)({_AMOUNT})[ \t]+EA[ \t]+\$?[ \t]*({_AMOUNT})[ \t]+\$?[ \t]*({_AMOUNT})(?!\S)", re.I,
)
_SUBTOTAL = re.compile(r"^\s*Sub[ \t]*Total\s*:?\s*\$?\s*(-?(?:\d{1,3}(?:,\d{3})+|\d+)\.\d{2})(?![\d.,])", re.I | re.M)
logger = logging.getLogger(__name__)

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
    """
    data = {}
    lot_no = None

    print(f"--- Parsing Report Text ({filename}) ---")

    # --- Extract Lot Number ---
    labeled_lots = set(re.findall(
        r"\bLOT(?:\s+(?:NUMBER|NO\.?)\s*:?|\s*:)\s*([A-Z0-9-]*\d[A-Z0-9-]*)\b", text.upper(),
    ))
    if len(labeled_lots) > 1:
        return {}
    if labeled_lots:
        labeled_lot = labeled_lots.pop().upper()
        lot_no = next((lot for lot in known_lots if lot.upper() == labeled_lot), labeled_lot)
    else:
        matches = {lot for lot in known_lots if re.search(rf"(?<!\w){re.escape(lot)}(?!\w)", text, re.I)}
        if len(matches) > 1:
            return {}
        lot_no = next(iter(matches), None)

    if not lot_no:
        slash_matches = re.findall(r"/\s*([A-Z0-9]{3,})", text)
        for candidate in slash_matches:
            if re.search(r"[A-Z]", candidate) or len(candidate) > 5:
                lot_no = candidate
                print(f"   > Found Lot No (Slash Pattern): {lot_no}")
                break

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

    # The result columns are purity, other crop, inert, weed, and germination.
    if start_match := re.search(r"\bAnalyzed\s*:", text, re.I):
        header = text[max(0, start_match.start() - 160):start_match.end()]
        has_sample_weight = re.search(r"\bGrams\s+(?:Analyzed|Pure)\b", header, re.I)
        sub_text = text[start_match.end():start_match.end() + 250]
        sub_text = CUSTOMS_RELEASE_STAMP.split(sub_text, maxsplit=1)[0]
        sub_text = re.split(_CERTIFICATE_DATE, sub_text, maxsplit=1)[0]
        values = [
            float(token.replace(",", ".")) if token[0].isdigit() else None
            for token in re.findall(r"\b(?:\d+(?:[.,]\d+)?|N/?A|TR|X)\b|(?<!\S)-(?!\S)", sub_text, re.I)
        ]
        if values and (has_sample_weight or (values[0] is not None and values[0] > 100)):
            values = values[1:]
        for index, field in ((0, "Purity"), (2, "Inert"), (4, "CertificateGerm")):
            if index < len(values) and values[index] is not None and values[index] <= 100:
                data[field] = values[index]
        if "CertificateGerm" in data:
            germ = data["CertificateGerm"]
            data["CertificateGerm"] = 99 if germ == 100 else int(germ)

    tested_label = r"\bDate\s+Tested\b\s*:?\s*"
    germ_date = None
    date_table = None
    expected_columns = 2
    if germination_header := re.search(r"(\bPurity\s+Information\s+)?\bGermination\s+Information\b", text, re.I):
        date_table = re.split(
            r"\b(?:Purity\s+(?:Information|Analysis)|Viability\s+Analysis)\b",
            text[germination_header.end():], maxsplit=1, flags=re.I,
        )[0]
        expected_columns = 2 if germination_header[1] else 1
    elif shared_table := re.search(
        r"(\bTest\s+Number\b.*?\bTest\s+Number\b.*?)\bPurity\s+Analysis\s+Viability\s+Analysis\b",
        text, re.I | re.S,
    ):
        date_table = shared_table[1]

    if date_table is not None:
        date_table = CUSTOMS_RELEASE_STAMP.split(date_table, maxsplit=1)[0]
        columns = re.split(tested_label, date_table, flags=re.I)[1:]
        dates = re.findall(_CERTIFICATE_DATE, "\n".join(columns))
        # The shared table lists the purity column first and the germination column second.
        if len(columns) == len(dates) == expected_columns:
            germ_date = dates[-1]
    elif len(re.findall(tested_label, text, re.I)) == 1:
        if match := re.search(rf"{tested_label}({_CERTIFICATE_DATE})", text, re.I):
            germ_date = match[1]

    if germ_date:
        try:
            data["CertificateGermDate"] = datetime.strptime(germ_date, "%m/%d/%Y").strftime("%m/%d/%Y")
        except ValueError:
            pass
    return {lot_no: data}


def process_item_block(block_lines: List[str], global_po: str, invoice_no: str, is_kamterter: bool) -> Dict:
    """
    Process a list of strings representing a single item block from Invoice.
    """
    # 1. CLEANUP
    block_lines = [re.sub(r"HWT", "", line).strip() for line in block_lines if line.strip()]

    # Extract 4 or 5 digit Vendor Item Number (e.g. 1400 or 62800)
    raw_item_num = block_lines[0] if block_lines else ""
    m_item = re.match(r"^(\d{4,5})\b", raw_item_num)
    clean_item_num = m_item.group(1) if m_item else raw_item_num

    # Remove Item Number from text line so it doesn't appear in description
    if damaged_item := _DAMAGED_ITEM_NUMBER.match(raw_item_num):
        clean_item_num = ""
        block_lines[0] = raw_item_num[damaged_item.end():].strip()
    elif m_item and block_lines:
        block_lines[0] = block_lines[0].replace(m_item.group(1), "", 1).strip()

    item_data = {
        "VendorInvoiceNo": invoice_no,
        "PurchaseOrder": global_po,
        "VendorItemNumber": clean_item_num,
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

    if not block_lines:
        return item_data

    used_indices = set()
    if not block_lines[0]: 
        used_indices.add(0) # Mark used if empty

    # --- 2. Identify & Remove Material Number (8 digits) ---
    for i, line in enumerate(block_lines):
        if i > 2: break
        m_mat = re.search(r"\b(\d{8})\b", line)
        if m_mat:
            block_lines[i] = block_lines[i].replace(m_mat.group(1), " ").strip()
            if not block_lines[i]:
                used_indices.add(i)
            break 

    # --- 3. Extract Financials (Anchor: "EA") ---
    idx_ea = next((i for i, line in enumerate(block_lines) if line.upper() == "EA"), None)
    if idx_ea is not None:
        used_indices.add(idx_ea)
        for offset, field in ((-1, "QuantityLine"), (1, "Unit Price"), (2, "TotalPrice")):
            index = idx_ea + offset
            if 0 <= index < len(block_lines):
                value = block_lines[index].lstrip("$ ")
                if offset == 2:
                    value = value.replace(" ", "")
                if re.fullmatch(_AMOUNT, value):
                    item_data[field] = float(value.replace(",", ""))
                    used_indices.add(index)
        item_data["LineAmountsRead"] = all(idx_ea + offset in used_indices for offset in (-1, 1, 2))
            
    # --- 4. Extract Lot Number ---
    for i, line in enumerate(block_lines):
        if lot := re.search(r"(?<!\S)/\s*([A-Z0-9-]{3,})(?:\s*)$", line):
            # User Rule: Accept ONLY if fully capitalized (interpreted as no lowercase characters)
            # This allows "/TVW077040X1" but rejects "/Picar/Thia Red" (Rejected).
            
            if not line.startswith("/") and (len(lot[1]) < 6 or not any(c.isdigit() for c in lot[1])):
                continue

            extracted_lot = lot[1]
            block_lines[i] = line[:lot.start()].strip()
            if not block_lines[i]:
                used_indices.add(i)
            
            # Look ahead for split lot suffix
            for offset in range(1, 4):
                if i + offset < len(block_lines):
                    if i + offset in used_indices:
                        break
                    next_line = block_lines[i + offset].strip()
                    if re.match(r"^\d{1,5}$", next_line):
                        extracted_lot += next_line
                        used_indices.add(i + offset)
                        break 
                    if re.search(r"[a-zA-Z]", next_line):
                        continue
                    if "." in next_line:
                        break
            item_data["VendorLotNo"] = extracted_lot
            break

    # # --- 4. Extract Lot Number ---
    # for i, line in enumerate(block_lines):
    #     if line.startswith("/"):
    #         extracted_lot = line.replace("/", "").strip()
    #         used_indices.add(i)
    #         # Look ahead for split lot suffix
    #         for offset in range(1, 4):
    #             if i + offset < len(block_lines):
    #                 next_line = block_lines[i + offset].strip()
    #                 if re.match(r"^\d{1,5}$", next_line):
    #                     extracted_lot += next_line
    #                     used_indices.add(i + offset)
    #                     break 
    #                 if re.search(r"[a-zA-Z]", next_line):
    #                     continue
    #                 if "." in next_line:
    #                     break
    #         item_data["VendorLotNo"] = extracted_lot
    #         break 

    # --- 5. Extract Batch Number ---
    for i, line in enumerate(block_lines):
        if i in used_indices or (idx_ea is not None and i > idx_ea and not line.upper().startswith("BATCH:")):
            continue
        m_batch = re.search(r"\b(\d{8})\b|\b(\d{6}\s\d{2})\b", line)
        if m_batch:
            clean_batch = m_batch.group(0).replace(" ", "")
            if "." not in line and len(clean_batch) == 8:
                item_data["VendorBatchNo"] = clean_batch
                block_lines[i] = (line[:m_batch.start()] + line[m_batch.end():]).strip()
                if not block_lines[i]:
                    used_indices.add(i)
                break

    # --- 6. Extract Metadata ---
    invoice_germ = re.search(
        r"\bGerm\s*:\s*(\d{1,3})\s*%(?:\s*Date\s*:\s*([\d/]+))?",
        "\n".join(block_lines), re.I,
    )
    if invoice_germ and int(invoice_germ[1]) <= 100:
        item_data["CurrentGerm"] = int(invoice_germ[1])
        if date := invoice_germ[2]:
            for date_format, field, output in (
                ("%m/%d/%Y", "CurrentGermDate", "%m/%d/%Y"),
                ("%m/%d/%y", "CurrentGermDate", "%m/%d/%Y"),
                ("%m/%Y", "InvoiceGermMonth", "%m/%Y"),
                ("%m/%y", "InvoiceGermMonth", "%m/%Y"),
            ):
                try:
                    item_data[field] = datetime.strptime(date, date_format).strftime(output)
                    break
                except ValueError:
                    continue

    for i, line in enumerate(block_lines):
        if "PO#" in line or "PO :" in line:
            m = re.search(r"(\d{5})", line)
            if m: 
                item_data["PurchaseOrder"] = f"PO-{m.group(1)}"
                used_indices.add(i)
        
        if "Seeds/LB:" in line:
            m = re.search(r"Seeds/LB:\s*([\d,]+)", line)
            if m: 
                item_data["SeedCount"] = int(m.group(1).replace(",", ""))
                used_indices.add(i)
        
        if "Size" in line:
            m = re.search(r"Size\s*[:\.]?\s*([\d\.\-\s]+[A-Z]*|[A-Z]{2,}\b)", line, re.IGNORECASE)
            if m:
                item_data["SeedSize"] = m.group(1).strip()
                block_lines[i] = block_lines[i].replace(m.group(0), " ").strip()
                if len(block_lines[i]) < 3: 
                    used_indices.add(i)

        if "Origin:" in line:
            m = re.search(r"Origin:\s*([A-Z]{2})", line)
            if m: 
                item_data["OriginCountry"] = m.group(1)
                used_indices.add(i)
        
        if "TRT CODE" in line:
            if code := re.search(r"\bTRT CODE\s*:\s*([A-Z0-9-]+)", line):
                item_data["VendorTreatmentCode"] = code[1]
            used_indices.add(i)
        if "Germ" in line and "%" in line:
            used_indices.add(i)
        if "Date:" in line:
            used_indices.add(i)

    # --- 7. Extract Description & Treatment ---
    remaining_lines = []
    for i, line in enumerate(block_lines):
        if i not in used_indices:
            remaining_lines.append(line)

    full_text = " ".join(remaining_lines).replace("FarMore«", "FarMore").strip()

    # NOISE REMOVAL
    noise_pattern = re.compile(
        r"(Sub Total|TOTAL\b|Total due|Thank you|This invoice is|Page\s*:|syngenta\s*Invoice|Item\s*Material|\d+%?\s*DISCOUNT|DISCOUNT\s*APPLIED)", 
        re.IGNORECASE
    )
    match = noise_pattern.search(full_text)
    if match:
        full_text = full_text[:match.start()].strip()

    if "KS" in full_text:
        parts = full_text.split("KS", 1)
        item_data["VendorItemDescription"] = parts[0].strip() + " KS"
        remainder = parts[1].strip()
        
        pkg_match = re.match(r"^(Pail|Foil|Bag|Carton|Box)\b", remainder, re.IGNORECASE)
        if pkg_match:
            pkg_type = pkg_match.group(0)
            item_data["VendorItemDescription"] += " " + pkg_type
            remainder = remainder[len(pkg_type):].strip()
            
        item_data["VendorTreatment"] = remainder.replace("Pail", "").replace("Foil", "").replace("Bag", "").strip()
        
    elif "Metal-" in full_text:
        parts = full_text.split("Metal-", 1)
        item_data["VendorItemDescription"] = parts[0].strip()
        item_data["VendorTreatment"] = "Metal-" + parts[1].strip()
        
    elif "FarMore" in full_text:
        parts = full_text.split("FarMore", 1)
        item_data["VendorItemDescription"] = parts[0].strip()
        item_data["VendorTreatment"] = "FarMore" + parts[1].strip()
        
    else:
        item_data["VendorItemDescription"] = full_text

    # --- 8. Final Cleanups ---
    if item_data["PurchaseOrder"] is None:
        item_data["PurchaseOrder"] = global_po

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
    Splits invoice text into item blocks using flexible pattern matching.
    """
    text = _INLINE_AMOUNTS.sub(r"\n\1\nEA\n\2\n\3\n", text)
    text = re.sub(r"(?<!\S)(\d{8})[ \t]*(/[A-Z0-9-]{3,})(?=[ \t]*$)", r"\1\n\2", text, flags=re.M)
    lines = [l.strip() for l in text.splitlines() if l.strip()]
    is_kamterter = "KAMTERTER" in text.upper()
    
    item_indices = []
    for i, line in enumerate(lines):
        if _DAMAGED_ITEM_NUMBER.match(line):
            item_indices.append(i)
            continue
        # Flexible Start: Starts with 4 or 5 digits, allows extra text (merged lines)
        if re.match(r"^\d{4,5}\b", line):
            # Confirmation: Must find 8-digit Material Number within 3 lines
            found_material = False
            for offset in range(3):
                if i + offset < len(lines):
                    if re.search(r"\b\d{8}\b", lines[i+offset]):
                        found_material = True
                        break
            if found_material:
                item_indices.append(i)
    
    items = []
    for k, start_idx in enumerate(item_indices):
        end_idx = item_indices[k+1] if k + 1 < len(item_indices) else len(lines)
        block_lines = lines[start_idx:end_idx]
        item = process_item_block(block_lines, global_po, invoice_no, is_kamterter)
        if item:
            items.append(item)
            
    return items

def _is_syngenta_certificate(text: str) -> bool:
    upper = text.upper()
    return bool(
        re.search(r"^\s*Report\s+of\s+Analysis\b", text, re.I | re.M)
        or ("REPORT OF ANALYSIS" in upper and "VIABILITY" in upper)
        or (
            CUSTOMS_RELEASE_STAMP.search(text)
            and not ("INVOICE" in upper and "SYNGENTA" in upper and "STOKES" in upper)
            and not _PACKING_LIST.search(text)
        )
    )


def _ocr_syngenta_page(doc, index: int) -> str:
    with fitz.open() as page_doc:
        page_doc.insert_pdf(doc, from_page=index, to_page=index)
        return "\n".join(extract_text_with_azure_ocr(page_doc.tobytes()))


def _unread_invoice_rows(items):
    return sum(
        not (item.get("VendorItemNumber") and item.get("VendorLotNo") and item.get("VendorBatchNo")
             and item.get("QuantityLine", 0) > 0 and item.get("LineAmountsRead"))
        for item in items
    )


def extract_syngenta_data_from_bytes(
    pdf_files: List[Tuple[str, bytes]], pkg_desc_list: list,
) -> Dict[str, List[Dict]]:
    def note(source, message, *, warning=True):
        logger.log(logging.WARNING if warning else logging.INFO, "Syngenta extraction for %r. %s", source, message)

    analysis_map = {}
    temp_invoice_items = {}
    analysis_files_queue = []
    invoice_sources = {}
    seen_files = set()

    for upload_number, (filename, pdf_bytes) in enumerate(pdf_files, 1):
        if pdf_bytes in seen_files:
            note(filename, "This PDF duplicates an earlier upload and was counted once.", warning=False)
            continue
        seen_files.add(pdf_bytes)
        invoice_pages = {}
        ocr_invoices = set()
        previous_page = None
        try:
            with fitz.open(stream=pdf_bytes, filetype="pdf") as doc:
                for index, page in enumerate(doc):
                    page_num = index + 1
                    source = f"{filename}, page {page_num}"
                    native_text = page.get_text()
                    text = native_text
                    used_ocr = False
                    if not (_PACKING_LIST.search(text) or _is_syngenta_certificate(text)
                            or (_INVOICE_NUMBER.search(text) and _INVOICE_TABLE.search(text))
                            or ("SYNGENTA" in text.upper() and "INVOICE" in text.upper())
                            or (_PAGE_NUMBER.search(text) and _INVOICE_TABLE.search(text) and previous_page)):
                        used_ocr = True
                        try:
                            text = _ocr_syngenta_page(doc, index) or text
                        except Exception:
                            pass

                    if _PACKING_LIST.search(text):
                        previous_page = None
                        continue
                    if _is_syngenta_certificate(text):
                        analysis_files_queue.append((filename, page_num, text, None if used_ocr else pdf_bytes))
                        previous_page = None
                        continue

                    ref = _INVOICE_NUMBER.search(text)
                    invoice_no = ref[1] if ref else None
                    page_match = _PAGE_NUMBER.search(text)
                    numbering = tuple(map(int, page_match.groups())) if page_match else None
                    has_table = bool(_INVOICE_TABLE.search(text))
                    if (invoice_no is None and previous_page and numbering and numbering[0] > 1
                            and previous_page[1] == (numbering[0] - 1, numbering[1]) and has_table):
                        invoice_no = previous_page[0]
                    upper = text.upper()
                    anchored = all(marker in upper for marker in ("SYNGENTA", "INVOICE", "STOKES"))
                    continuation = invoice_no is not None and invoice_no in invoice_pages and (has_table or numbering)
                    if not (anchored or continuation):
                        note(source, "The page could not be identified as an invoice, packing list, or analysis report.")
                        previous_page = None
                        continue

                    line_count = len(_QUANTITY_EA.findall(text))
                    if not used_ocr and (has_table or (ref and page.get_images())):
                        native_rows = parse_invoice_text(text, filename, None, invoice_no)
                        native_unread = _unread_invoice_rows(native_rows)
                        if line_count > len(native_rows) or native_unread or (not native_rows and page.get_images()):
                            try:
                                candidate = _ocr_syngenta_page(doc, index)
                            except Exception:
                                candidate = ""
                            ocr_ref = _INVOICE_NUMBER.search(candidate)
                            if ocr_ref and ocr_ref[1] == invoice_no and _INVOICE_TABLE.search(candidate):
                                ocr_rows = parse_invoice_text(candidate, filename, None, invoice_no)
                                if (len(ocr_rows) >= max(line_count, len(native_rows))
                                        and len(ocr_rows) - _unread_invoice_rows(ocr_rows) > len(native_rows) - native_unread):
                                    text = candidate
                                    line_count = max(line_count, len(_QUANTITY_EA.findall(candidate)))
                                    used_ocr = True

                    subtotal_match = _SUBTOTAL.search(native_text) or _SUBTOTAL.search(text)
                    if used_ocr:
                        ocr_invoices.add(invoice_no)
                    pages = invoice_pages.setdefault(invoice_no, [])
                    if numbering and any(record[0] and record[0][0] == numbering[0] for record in pages):
                        note(source, f"Invoice page {numbering[0]} appears more than once. It was counted once.")
                    else:
                        pages.append((numbering, text, line_count, subtotal_match[1] if subtotal_match else None))
                    previous_page = (invoice_no, numbering)
        except Exception:
            note(filename, "The PDF could not be read completely.")

        for invoice_no, pages in invoice_pages.items():
            label = f"{filename} (invoice {invoice_no or 'unknown'})" if len(invoice_pages) > 1 else filename
            if label in temp_invoice_items:
                label = f"{label} (upload {upload_number})"
            if invoice_no is None:
                note(label, "The invoice number could not be read.")
            elif invoice_sources.setdefault(invoice_no, upload_number) != upload_number:
                note(label, "This invoice number appears in more than one distinct PDF. Check for overlapping pages.")
            if invoice_no in ocr_invoices:
                note(label, "Some invoice text came from OCR. Verify item, lot, and batch numbers against the PDF.", warning=False)

            numbered = [numbering for numbering, *_ in pages if numbering]
            if numbered:
                reported_totals = {total for _, total in numbered}
                expected_pages = max(reported_totals)
                seen_numbers = {number for number, _ in numbered}
                if (len(reported_totals) != 1 or len(seen_numbers) != expected_pages
                        or min(seen_numbers) != 1 or max(seen_numbers) != expected_pages):
                    note(label, f"Read {len(seen_numbers)} numbered invoice pages. The PDF reports {expected_pages}. Check for missing or unreadable pages.")
            current_invoice_text = "\n".join(text for _, text, _, _ in pages)
            po = re.search(r"\bPO\s*:\s*([^\n]+)", current_invoice_text, re.I)
            po_digits = re.search(r"\b(\d{5})\b", po[1]) if po and "DLL" not in po[1].upper() else None
            po_number = f"PO-{po_digits[1]}" if po_digits else None
            items = parse_invoice_text(current_invoice_text, filename, po_number, invoice_no)
            if not items:
                note(label, "No lot rows were readable on the invoice.")
                continue

            expected_rows = sum(count for _, _, count, _ in pages)
            if expected_rows and expected_rows != len(items):
                note(label, f"Read {len(items)} lot rows from {expected_rows} quantity lines. Check the source invoice.")
            if unread_count := _unread_invoice_rows(items):
                note(label, f"Lot details were incomplete on {unread_count} of {len(items)} extracted rows. "
                            "Check the item number, lot number, batch, quantity, and price.")
            subtotals = {int(subtotal.replace(",", "").replace(".", "")) for _, _, _, subtotal in pages if subtotal}
            if len(subtotals) == 1:
                expected_total = next(iter(subtotals))
                line_total = sum(round(item.get("TotalPrice", 0) * 100) for item in items)
                if line_total != expected_total:
                    note(label, f"Line prices total {line_total / 100:,.2f} USD. The printed goods subtotal is {expected_total / 100:,.2f} USD.")
            elif subtotals:
                note(label, "The invoice shows conflicting goods subtotals.")
            else:
                note(label, "The goods subtotal was unavailable for reconciliation.", warning=False)

            temp_invoice_items[label] = items
            try:
                log_processing_event("Syngenta", filename, {"method": "Mixed/Page-Level", "page_count": len(pages)}, po_number)
            except Exception as exc:
                print(f"Syngenta processing log failed for {filename}: {exc}")

    # --- PASS 2: Match Analysis Reports ---
    all_invoice_lots = set()
    for items in temp_invoice_items.values():
        for item in items:
            if item.get("VendorLotNo"):
                all_invoice_lots.add(item.get("VendorLotNo"))
    
    print(f"--- Known Lots from Invoices: {all_invoice_lots} ---")

    conflicting_lots = set()
    for filename, page_num, text, pdf_bytes in analysis_files_queue:
        report_name = f"{filename}_pg{page_num}"
        report = parse_analysis_text(text, report_name, all_invoice_lots)
        if pdf_bytes is not None and (
            not report or any(certificate_fields_missing(fields, _CERTIFICATE_FIELDS) for fields in report.values())
        ):
            try:
                with fitz.open(stream=pdf_bytes, filetype="pdf") as doc:
                    ocr_text = _ocr_syngenta_page(doc, page_num - 1)
                if _is_syngenta_certificate(ocr_text):
                    ocr_report = parse_analysis_text(ocr_text, report_name, all_invoice_lots)
                    if report:
                        for lot, fields in report.items():
                            fill_missing_certificate_fields(fields, ocr_report.get(lot, {}))
                    else:
                        report = ocr_report
            except Exception as exc:
                print(f"Certificate OCR failed for {report_name}: {exc}")

        source = f"{filename}, page {page_num}"
        if not report:
            note(source, "An analysis report was found, but its lot could not be identified.", warning=False)
        for lot, fields in report.items():
            if lot not in all_invoice_lots:
                note(source, f"Analysis lot {lot} did not match an extracted invoice lot.", warning=False)
                continue
            if certificate_fields_missing(fields, _CERTIFICATE_FIELDS):
                note(source, f"Some analysis fields for lot {lot} could not be read.", warning=False)
            if fields.get("Purity") == 100:
                fields["Purity"] = 99.99
                if fields.get("Inert") is not None:
                    fields["Inert"] = 0.01
            previous = analysis_map.setdefault(lot, {})
            if any(field in previous and value != previous[field] for field, value in fields.items()):
                note(source, f"Analysis reports disagree for lot {lot}. Verify the certificate values manually.", warning=False)
                conflicting_lots.add(lot)
            previous.update(fields)

    # --- Link Data ---
    grouped_results = {}
    for filename, items in temp_invoice_items.items():
        final_items = []
        for item in items:
            lot = item.get("VendorLotNo")
            print(f"Linking Invoice Lot: '{lot}' ...")
            if lot and lot in analysis_map and lot not in conflicting_lots:
                print(f"   >>> MATCH FOUND for {lot}")
                ana = analysis_map[lot]
                certificate_germ = ana.get("CertificateGerm")
                certificate_date = ana.get("CertificateGermDate")
                if item.get("CurrentGerm") is None or (certificate_germ is not None and certificate_date):
                    item["CurrentGerm"] = certificate_germ
                    item["CurrentGermDate"] = certificate_date
                item["Purity"] = ana.get("Purity")
                item["Inert"] = ana.get("Inert")
                item["GrowerGerm"] = certificate_germ
                item["GrowerGermDate"] = certificate_date
            else:
                print(f"   >>> NO MATCH in Analysis Data for {lot}")
            final_items.append(item)
        grouped_results[filename] = final_items
    
    print("=== END EXTRACTION ===\n")
    return grouped_results
