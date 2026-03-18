# # vendor_extractors/nunhems.py
# import math
# import os
# import re
# import json
# import fitz  # PyMuPDF
# import requests
# import time
# import pycountry
# from datetime import datetime
# from difflib import get_close_matches
# from typing import List, Dict, Any, Union, Tuple
# from db_logger import log_processing_event

# # Azure credentials from environment variables
# AZURE_ENDPOINT = os.getenv("AZURE_ENDPOINT")
# AZURE_KEY = os.getenv("AZURE_KEY")

# def convert_to_alpha2(country_value: str) -> str:
#     """Converts a country name or code to the ISO Alpha-2 format."""
#     if not country_value: return ""
#     country_value = country_value.strip()
#     if len(country_value) == 2: return country_value.upper()
#     try:
#         return pycountry.countries.lookup(country_value).alpha_2
#     except LookupError:
#         return country_value

# def _extract_text_with_azure_ocr(pdf_content: bytes) -> List[str]:
#     """Sends PDF content (bytes) to Azure Form Recognizer for OCR."""
#     if not AZURE_ENDPOINT or not AZURE_KEY:
#         raise ValueError("Azure OCR credentials are not set in environment variables.")
    
#     headers = {"Ocp-Apim-Subscription-Key": AZURE_KEY, "Content-Type": "application/pdf"}
#     response = requests.post(
#         f"{AZURE_ENDPOINT}formrecognizer/documentModels/prebuilt-layout:analyze?api-version=2023-07-31",
#         headers=headers, data=pdf_content
#     )
#     if response.status_code != 202:
#         raise RuntimeError(f"OCR request failed: {response.text}")

#     op_url = response.headers["Operation-Location"]
#     for _ in range(30):
#         time.sleep(1.5)
#         result = requests.get(op_url, headers={"Ocp-Apim-Subscription-Key": AZURE_KEY}).json()
#         if result.get("status") == "succeeded":
#             return [ln.get("content", "").strip() for page in result["analyzeResult"]["pages"] for ln in page["lines"] if ln.get("content", "").strip()]
#         if result.get("status") == "failed":
#             raise RuntimeError("OCR analysis failed")
#     raise TimeoutError("OCR timed out")

# def _extract_lines_with_info(source: bytes) -> Tuple[List[str], Dict]:
#     """Extracts text lines and metadata (method, page_count)."""
#     info = {'method': 'PyMuPDF', 'page_count': 0}
#     try:
#         doc = fitz.open(stream=source, filetype="pdf")
#         info['page_count'] = doc.page_count
#         if any(page.get_text().strip() for page in doc):
#             lines = [l.strip() for page in doc for l in page.get_text().split("\n") if l.strip()]
#             doc.close()
#             return lines, info
#         doc.close()
#     except Exception:
#         pass

#     # Fallback to OCR
#     info['method'] = 'Azure OCR'
#     lines = _extract_text_with_azure_ocr(source)
#     return lines, info

# def _extract_nunhems_quality_data(pdf_files: List[Tuple[str, bytes]]) -> Dict[str, Dict]:
#     """Extracts data from Nunhems NAL Quality Certificate PDFs."""
#     quality_map = {}
#     for filename, pdf_bytes in pdf_files:
#         lines, _ = _extract_lines_with_info(pdf_bytes)
#         if not any("Quality Certificate" in ln for ln in lines):
#             continue

#         current_lot = None
#         i = 0
#         while i < len(lines):
#             ln = lines[i]
            
#             # Identify Lot
#             found_lot = None
#             if re.search(r"Lot[/ ]*Batch number", ln, re.IGNORECASE):
#                 if m := re.search(r"(\d{11})", ln): found_lot = m.group(1)
#                 elif i + 1 < len(lines) and (m := re.search(r"(\d{11})", lines[i + 1])): found_lot = m.group(1)
#                 elif i + 2 < len(lines) and (m := re.search(r"(\d{11})", lines[i + 2])): found_lot = m.group(1)
            
#             if found_lot:
#                 current_lot = found_lot
#                 quality_map.setdefault(current_lot, {})
            
#             if current_lot:
#                 # Purity & Inert
#                 if re.match(r"Pure\s*seeds?", ln, re.IGNORECASE):
#                     found_floats = []
#                     for j in range(i+1, min(i+10, len(lines))):
#                         if "Lot/Batch" in lines[j] or "Remarks:" in lines[j]: break
#                         matches = re.findall(r"(\d{1,3}(?:\.\d+)?)%?", lines[j])
#                         for m in matches:
#                             try: found_floats.append(float(m))
#                             except: pass
                    
#                     if len(found_floats) >= 1:
#                         pure = found_floats[0]
#                         inert = found_floats[1] if len(found_floats) > 1 else 0.0
#                         if pure == 100.0:
#                             pure = 99.99
#                             inert = 0.01
#                         quality_map[current_lot]["Purity"] = pure
#                         quality_map[current_lot]["Inert"] = inert

#                 # Certificate Germ Date
#                 if m := re.match(r"Date:\s*([A-Za-z]+\s+\d{1,2},\s*\d{4})", ln):
#                     dt = datetime.strptime(m.group(1), "%B %d, %Y")
#                     quality_map[current_lot]["GrowerGermDate"] = dt.strftime("%m/%d/%Y")

#                 # Certificate Germ %
#                 if "Normal seedlings" in ln:
#                     for j in range(i+1, min(i+6, len(lines))):
#                         if "Lot/Batch" in lines[j]: break
#                         if percent_match := re.search(r"(\d+)%", lines[j]):
#                             quality_map[current_lot]["GrowerGerm"] = int(percent_match.group(1))
#                             break
#             i += 1
#     return quality_map

# def _extract_nunhems_germ_data(pdf_files: List[Tuple[str, bytes]]) -> Dict[str, Dict]:
#     """Parses Nunhems Germ Confirmation Letter (Test Date Confirmation)."""
#     germ_data = {}
#     for filename, pdf_bytes in pdf_files:
#         lines, _ = _extract_lines_with_info(pdf_bytes)
#         if not any("Test Date Confirmation" in ln for ln in lines):
#             continue
        
#         for i, line in enumerate(lines):
#             # Look for 11-digit lot numbers in the table
#             if m := re.search(r"\b(\d{11})\b", line):
#                 lot = m.group(1)
#                 context = " ".join(lines[i:i+5]) # Read ahead for values
                
#                 germ, germ_date = None, None
#                 # Extract Germ % (e.g. 92%)
#                 if g := re.search(r"(\d+)%", context): 
#                     germ = int(g.group(1))
                
#                 # Extract Date (e.g. 5/2025 -> 05/01/2025)
#                 # Matches M/YYYY or MM/YYYY
#                 if d := re.search(r"\b(\d{1,2})/(\d{4})\b", context):
#                     month, year = d.groups()
#                     germ_date = f"{int(month):02d}/01/{year}"
                
#                 if germ or germ_date:
#                      germ_data[lot] = {"Germ": germ, "GermDate": germ_date}
#     return germ_data

# def _extract_nunhems_packing_data(pdf_files: List[Tuple[str, bytes]]) -> Dict[str, Dict]:
#     """Extracts Lot, SeedCount, SeedForm, and SeedSize from Packing Lists."""
#     packing_data = {}
#     for filename, pdf_bytes in pdf_files:
#         lines, _ = _extract_lines_with_info(pdf_bytes)
#         if not any("PACKING LIST" in ln.upper() for ln in lines):
#             continue
            
#         for i, line in enumerate(lines):
#             if lot_match := re.search(r"\b(\d{11})\b", line):
#                 lot = lot_match.group(1)
#                 packing_data.setdefault(lot, {})
                
#                 # Search window around the lot number (backward and forward)
#                 start_win = max(0, i - 15)
#                 end_win = min(len(lines), i + 15)
#                 window_lines = lines[start_win:end_win]
#                 window_text = " ".join(window_lines)

#                 # 1. Seed Count (S/C)
#                 if match := re.search(r"S/C\s*([\d,]+)", window_text, re.IGNORECASE):
#                     packing_data[lot]["SeedCount"] = int(match.group(1).replace(",", ""))
#                 elif match := re.search(r"([\d,]+)\s*LBS", window_text): # Fallback
#                      packing_data[lot]["SeedCount"] = int(match.group(1).replace(",", ""))

#                 # 2. Seed Size
#                 # Looks for typical size patterns like "1.75-2.00", "2.0-2.25", "LR", "MED"
#                 size_match = re.search(r"\b(\d\.\d{1,2}\s*-\s*\d\.\d{1,2})\b", window_text)
#                 if not size_match:
#                     size_match = re.search(r"\b(LR|MR|LL|LF|MF|MD|SM|MED|LGE)\b", window_text)
                
#                 if size_match:
#                     packing_data[lot]["SeedSize"] = size_match.group(1)

#                 # 3. Seed Form
#                 # Looks for keywords like PRECISUN, PELLET, INCRUSTED, RAW, PRIMED
#                 form_keywords = ["PRECISUN", "PELLET", "INCRUSTED", "RAW", "PRIMED", "TREATED"]
#                 for keyword in form_keywords:
#                     if keyword in window_text.upper():
#                         packing_data[lot]["ProductForm"] = keyword
#                         break

#     return packing_data

# def _process_customs_invoice(lines: List[str], quality_map: dict, germ_map: dict, packing_map: dict, pkg_desc_list: list[str]) -> List[Dict]:
#     """
#     Parses the 'Customs Invoice' format (identified by 'CONSIGNEE' or 'CUSTOMS INVOICE').
#     Extracts pricing, PO, and links lots to auxiliary data.
#     """
#     text_content = "\n".join(lines)
#     items = []

#     # 1. Extract Header Info
#     vendor_invoice_no = None
#     if m := re.search(r"Invoice\s+(?:No\.?|Number)[:\s]+([\w-]+)", text_content, re.IGNORECASE):
#         vendor_invoice_no = m.group(1)

#     po_number = None
#     # Look for PO in various formats
#     if m := re.search(r"PO\s*[:#]?\s*(\d{5,})", text_content, re.IGNORECASE):
#         po_number = f"PO-{m.group(1)}"
#     elif m := re.search(r"Purchase\s*Order\s*[:#]?\s*(\d{5,})", text_content, re.IGNORECASE):
#         po_number = f"PO-{m.group(1)}"

#     # 2. Find Item Blocks using Lot Number (11 digits) as an anchor
#     # We iterate through the text looking for lines containing a Lot Number.
#     # Then we scan backwards/forwards for Description, Price, etc.
    
#     # Identify lines with a Lot Number
#     lot_indices = [i for i, ln in enumerate(lines) if re.search(r"\b\d{11}\b", ln)]

#     for idx in lot_indices:
#         lot_line = lines[idx]
#         vendor_lot = re.search(r"\b(\d{11})\b", lot_line).group(1)
        
#         # Define a context window around the lot line
#         start_idx = max(0, idx - 10)
#         end_idx = min(len(lines), idx + 10)
#         context_lines = lines[start_idx:end_idx]
#         context_text = "\n".join(context_lines)

#         # A. Description (e.g. "LEEK KRYPTON F1 100,000 SDS")
#         # Look for lines above the lot that contain species/variety names
#         description = ""
#         for i in range(idx - 1, max(0, idx - 5), -1):
#             line = lines[i]
#             # Simple heuristic: Ends with "SDS" or has caps
#             if "SDS" in line.upper() or "SEEDS" in line.upper():
#                 description = line.strip()
#                 break
#         if not description: description = f"Item for Lot {vendor_lot}"

#         # B. Origin
#         origin_country = None
#         if m := re.search(r"\b([A-Z]{2})\s+Origin", context_text, re.IGNORECASE):
#             origin_country = convert_to_alpha2(m.group(1))
#         elif m := re.search(r"Origin\s*[:\.]?\s*([A-Za-z]+)", context_text, re.IGNORECASE):
#             origin_country = convert_to_alpha2(m.group(1))

#         # C. Treatment
#         treatment = "Untreated"
#         if "TREATED" in context_text.upper():
#             treatment = "Treated" # Or try to extract specific chemical if visible
#         elif "UNTREATED" in context_text.upper():
#             treatment = "Untreated"

#         # D. Pricing & Quantity
#         # Strategy: Look for "Price" line or "Amount" line in context
#         # Example line: "3 EA ... 5,523.00"
#         total_price = 0.0
#         line_qty = 0
        
#         # Regex to find: Qty (int) ... Price (float)
#         # Matches: "3 EA ... 5,523.00" or "3 ... 5523.00"
#         price_qty_match = re.search(r"\b(\d+)\s*(?:EA|PC|UN)?\s+.*?([\d,]+\.\d{2})", context_text)
#         if price_qty_match:
#             line_qty_raw = int(price_qty_match.group(1))
#             total_price = float(price_qty_match.group(2).replace(",", ""))
            
#             # E. Calculate Original Received Qty (in Thousands/KS)
#             # Logic: If description says "100,000 SDS" and Qty is 3 => 300,000 seeds => 300 KS
#             pkg_size_seeds = 0
#             if m_pkg := re.search(r"([\d,]+)\s*SDS", description, re.IGNORECASE):
#                 pkg_size_seeds = int(m_pkg.group(1).replace(",", ""))
            
#             if pkg_size_seeds > 0:
#                 total_ks_qty = (line_qty_raw * pkg_size_seeds) / 1000
#                 total_qty = total_ks_qty
#             else:
#                 total_qty = line_qty_raw # Fallback
#         else:
#             total_qty = 0
#             total_price = 0.0

#         # F. Package Description
#         # e.g., "100,000 SEEDS" based on description
#         if pkg_size_seeds > 0:
#             package_description = f"{pkg_size_seeds:,} SEEDS"
#         else:
#             package_description = find_best_nunhems_package_description(description, pkg_desc_list)

#         # G. Cost Calculation
#         # USD Actual Cost = Total Price / Total KS Qty
#         usd_cost = 0.0
#         if total_qty > 0:
#             usd_cost = total_price / total_qty

#         # H. Link Auxiliary Data
#         q_data = quality_map.get(vendor_lot, {})
#         g_data = germ_map.get(vendor_lot, {})
#         p_data = packing_map.get(vendor_lot, {})

#         # I. Consolidate Item
#         item = {
#             "VendorInvoiceNo": vendor_invoice_no,
#             "PurchaseOrder": po_number,
#             "VendorItemDescription": description,
#             "VendorLotNo": vendor_lot,
#             "VendorBatchNo": None, # Nunhems doesn't usually have a separate batch
#             "OriginCountry": origin_country,
#             "VendorTreatment": treatment,
#             "TreatmentsDescription": "", # User input
#             "TreatmentsDescription2": "", # User input
            
#             # Invoice Financials
#             "TotalQuantity": total_qty, # In KS
#             "TotalPrice": total_price,
#             "USD_Actual_Cost_$": "{:.2f}".format(usd_cost),
#             "PackageDescription": package_description,

#             # Germ Letter Data
#             "Germ": g_data.get("Germ"), # Current Germ
#             "GermDate": g_data.get("GermDate"), # Current Germ Date

#             # Packing List Data
#             "ProductForm": p_data.get("ProductForm"),
#             "SeedSize": p_data.get("SeedSize"),
#             "SeedCount": p_data.get("SeedCount"),

#             # Germ Certificate Data
#             "GrowerGerm": q_data.get("GrowerGerm"), # Certificate Germ
#             "GrowerGermDate": q_data.get("GrowerGermDate"),
#             "Purity": q_data.get("Purity"),
#             "Inert": q_data.get("Inert")
#         }
#         items.append(item)

#     return items

# def extract_nunhems_data_from_bytes(pdf_files: List[Tuple[str, bytes]], pkg_desc_list: list[str]) -> Dict[str, List[Dict]]:
#     """
#     Main extraction function for Nunhems.
#     Orchestrates parsing of Customs Invoices, Germ Letters, Packing Lists, and Certificates.
#     """
#     if not pdf_files: return {}

#     # 1. Parse Auxiliary Files first to build lookup maps
#     quality_map = _extract_nunhems_quality_data(pdf_files)
#     germ_map = _extract_nunhems_germ_data(pdf_files)
#     packing_map = _extract_nunhems_packing_data(pdf_files)

#     grouped_results = {}
    
#     # 2. Process Files to find the Invoice(s)
#     for filename, pdf_bytes in pdf_files:
#         lines, info = _extract_lines_with_info(pdf_bytes)
#         text_content = "\n".join(lines).upper()
        
#         # Identify Customs Invoice
#         # Key indicators: "CUSTOMS INVOICE" or "CONSIGNEE" and contains Pricing info
#         is_customs_invoice = (("CUSTOMS INVOICE" in text_content or "CONSIGNEE" in text_content) 
#                               and "TOTAL PRICE" in text_content or "AMOUNT" in text_content)
        
#         # Fallback for standard invoice if Customs Invoice logic doesn't catch it
#         is_standard_invoice = "INVOICE NUMBER" in text_content and "PACKING LIST" not in text_content

#         po_number = None
#         if m := re.search(r"(?:PO|PURCHASE ORDER)\s*[:#]?\s*(\d{5})", text_content, re.IGNORECASE):
#             po_number = f"PO-{m.group(1)}"

#         log_processing_event(
#             vendor='Nunhems',
#             filename=filename,
#             extraction_info=info,
#             po_number=po_number
#         )

#         items = []
#         if is_customs_invoice:
#             items = _process_customs_invoice(lines, quality_map, germ_map, packing_map, pkg_desc_list)
#         elif is_standard_invoice and not is_customs_invoice:
#             # Fallback to old processor if new Customs Invoice format isn't detected
#             # (Keeping the old logic for backward compatibility with older files)
#             items = _process_single_nunhems_invoice(lines, quality_map, germ_map, packing_map, pkg_desc_list)

#         if items:
#             grouped_results[filename] = items
    
#     return grouped_results

# def find_best_nunhems_package_description(vendor_desc: str, pkg_desc_list: list[str]) -> str:
#     """Finds the best matching package description."""
#     if not vendor_desc or not pkg_desc_list: return ""
    
#     if m := re.search(r"([\d,]+)\s+SDS", vendor_desc, re.IGNORECASE):
#         try:
#             qty_num = int(m.group(1).replace(',', ''))
#             candidate = f"{qty_num:,} SEEDS"
#             if candidate in pkg_desc_list:
#                 return candidate
#         except: pass
        
#     matches = get_close_matches(vendor_desc, pkg_desc_list, n=1, cutoff=0.6)
#     return matches[0] if matches else ""

# # -- Retained Old Invoice Processor for Backward Compatibility --
# def _process_single_nunhems_invoice(lines: List[str], quality_map: dict, germ_map: dict, packing_map: dict, pkg_desc_list: list[str]) -> List[Dict]:
#     """Legacy processor for standard Nunhems invoices."""
#     text_content = "\n".join(lines)
#     vendor_invoice_no = None
#     po_number = None
    
#     if m := re.search(r"Invoice\s+Number[:\s]+([\s\S]*?)\b(\d{9})\b", text_content, re.IGNORECASE):
#         vendor_invoice_no = m.group(2)
#     if m := re.search(r"Customer\s+P\.?O\.?\s+Number[:\s]+([\s\S]*?)\b(\d{5})\b", text_content, re.IGNORECASE):
#         po_number = f"PO-{m.group(2)}"

#     items = []
#     item_header_indices = []
#     for i, line in enumerate(lines):
#         if re.search(r"(\d+[\d,]*)\s+SDS(?!\/LB)", line):
#             item_header_indices.append(i)
    
#     for idx, start_i in enumerate(item_header_indices):
#         end_i = item_header_indices[idx+1] if idx + 1 < len(item_header_indices) else len(lines)
#         block_lines = lines[start_i-2 : end_i] 
#         block_text = "\n".join(block_lines)
        
#         sds_line = lines[start_i]
#         packaging_context_match = re.search(r"([\d,]+)\s+SDS", sds_line)
#         packaging_context_str = f"{packaging_context_match.group(1)} SDS" if packaging_context_match else ""

#         desc_part1 = lines[start_i - 1].strip() if start_i > 0 else ""
#         desc_part2 = lines[start_i + 1].strip() if start_i + 1 < len(lines) else ""
#         vendor_item_description = f"{desc_part1} {desc_part2} {sds_line}".strip()

#         treatment = "Untreated"
#         if "TREATED" in block_text.upper(): treatment = "Treated"

#         unit_price = 0.0
#         price_qty_pattern = re.compile(r"([\d,]+(?:\.\d{2})?)\s+Net price\s+([\d,]+\.\d{2})", re.IGNORECASE)
#         pq_match = price_qty_pattern.search(block_text)
#         if pq_match:
#              unit_price = float(pq_match.group(2).replace(",", ""))
        
#         for l_line in block_lines:
#             lot_match = re.search(r"(\d{11})\s*\|\s*([\d,]+)\s*\|", l_line)
#             if lot_match:
#                 vendor_lot = lot_match.group(1)
#                 lot_qty = int(float(lot_match.group(2).replace(",", "")))
                
#                 origin_country = None
#                 if origin_match := re.search(r"\|\s*([A-Za-z\s]+?)\s+ORIGIN", l_line, re.IGNORECASE):
#                     origin_country = convert_to_alpha2(origin_match.group(1))
                
#                 quality_info = quality_map.get(vendor_lot, {})
#                 germ_info = germ_map.get(vendor_lot, {})
#                 packing_info = packing_map.get(vendor_lot, {})
                
#                 package_description = find_best_nunhems_package_description(packaging_context_str, pkg_desc_list)

#                 calculated_cost = unit_price / lot_qty if lot_qty else 0.0

#                 items.append({
#                     "VendorInvoiceNo": vendor_invoice_no,
#                     "PurchaseOrder": po_number,
#                     "VendorLotNo": vendor_lot, # Mapped to VendorLotNo for consistency
#                     "VendorItemDescription": vendor_item_description,
#                     "OriginCountry": origin_country,
#                     "TotalPrice": round(unit_price, 2),
#                     "TotalQuantity": lot_qty,
#                     "USD_Actual_Cost_$": "{:.5f}".format(calculated_cost),
#                     "VendorTreatment": treatment,
#                     "Purity": quality_info.get("Purity"),
#                     "Inert": quality_info.get("Inert"),
#                     "Germ": germ_info.get("Germ"),
#                     "GermDate": germ_info.get("GermDate"),
#                     "SeedCount": packing_info.get("SeedCount"),
#                     "ProductForm": packing_info.get("ProductForm"),
#                     "SeedSize": packing_info.get("SeedSize"),
#                     "GrowerGerm": quality_info.get("GrowerGerm"),
#                     "GrowerGermDate": quality_info.get("GrowerGermDate"),
#                     "PackageDescription": package_description
#                 })
#     return items


# vendor_extractors/nunhems.py
"""
Nunhems / BASF invoice extractor.

Document types handled (by page):
  1. Customs Invoice   – CONSIGNEE + H-S Code blocks  → lot items, qty, price, origin
  2. Packing List      – "PACKING LIST" + Lot Number: → SeedCount (S/C), SeedForm, SeedSize
  3. NAL Quality Cert  – "Quality Certificate"        → Purity, Inert, GrowerGerm, GrowerGermDate
  4. Germ Letter       – "Test Date Confirmation"     → Germ %, GermDate
  5. Standard Invoice  – "INVOICE" + "Net price"      → VendorInvoiceNo, PO (for logging/display)

The page-by-page approach ensures data from one document type never contaminates another.
"""

import os
import re
import fitz          # PyMuPDF
import requests
import time
import pycountry
from datetime import datetime
from difflib import get_close_matches
from typing import Dict, List, Optional, Tuple
from db_logger import log_processing_event

AZURE_ENDPOINT = os.getenv("AZURE_ENDPOINT")
AZURE_KEY      = os.getenv("AZURE_KEY")


# ──────────────────────────────────────────────────────────────────────────────
# HELPERS
# ──────────────────────────────────────────────────────────────────────────────

def convert_to_alpha2(country_value: str) -> str:
    """Convert country name or code to ISO Alpha-2 (e.g. 'ITALY' → 'IT')."""
    if not country_value:
        return ""
    country_value = country_value.strip()
    if len(country_value) == 2:
        return country_value.upper()
    try:
        return pycountry.countries.lookup(country_value).alpha_2
    except LookupError:
        return country_value


def _eu_float(s: str) -> float:
    """
    Convert a European-formatted number string to a Python float.

    Examples
    --------
    '5.523,00'  → 5523.0      (dot = thousands sep, comma = decimal)
    '1.841,0000' → 1841.0
    '344.791'   → 344791.0    (integer with dot as thousands separator)
    '5523.00'   → 5523.0      (plain US-style float – left unchanged)
    """
    if not s:
        return 0.0
    s = s.strip()
    if "," in s:
        # European decimal: remove dots (thousands) and replace comma with dot
        s = s.replace(".", "").replace(",", ".")
    elif re.match(r"^\d{1,3}(\.\d{3})+$", s):
        # European integer like 344.791  →  344791
        s = s.replace(".", "")
    try:
        return float(s)
    except ValueError:
        return 0.0


def _get_pages(pdf_bytes: bytes) -> List[List[str]]:
    """Return a list of pages; each page is a list of non-empty trimmed text lines."""
    pages: List[List[str]] = []
    try:
        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
        for page in doc:
            lines = [l.strip() for l in page.get_text().split("\n") if l.strip()]
            pages.append(lines)
        doc.close()
    except Exception:
        pass
    return pages


def _page_info(pdf_bytes: bytes) -> Dict:
    info = {"method": "PyMuPDF", "page_count": 0}
    try:
        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
        info["page_count"] = doc.page_count
        doc.close()
    except Exception:
        pass
    return info


def _classify_page(lines: List[str]) -> str:
    """
    Determine the functional type of a PDF page from its text content.

    Priority order avoids mis-classifying pages that contain multiple keywords
    (e.g. the Shipping Information cover page ticks 'Packing List' as a checkbox).
    """
    text = " ".join(lines).upper()

    if "QUALITY CERTIFICATE" in text:
        return "quality_cert"

    if "TEST DATE CONFIRMATION" in text:
        return "germ_letter"

    # Require "LOT NUMBER:" (with colon) so we don't trigger on the shipping
    # info cover page that just has "Packing List" as a checkbox label.
    if "PACKING LIST" in text and "LOT NUMBER:" in text:
        return "packing_list"

    # Customs invoice: must have CONSIGNEE *and* the H-S Code label
    if "CONSIGNEE" in text and "H-S CODE" in text:
        return "customs_invoice"

    # Standard Nunhems invoice (INVOICE_012506533 style)
    if "INVOICE" in text and "NET PRICE" in text:
        return "standard_invoice"

    return "other"


# ──────────────────────────────────────────────────────────────────────────────
# AZURE OCR FALLBACK  (unchanged from previous implementation)
# ──────────────────────────────────────────────────────────────────────────────

def _extract_text_with_azure_ocr(pdf_content: bytes) -> List[str]:
    if not AZURE_ENDPOINT or not AZURE_KEY:
        raise ValueError("Azure OCR credentials are not set in environment variables.")
    headers = {"Ocp-Apim-Subscription-Key": AZURE_KEY, "Content-Type": "application/pdf"}
    response = requests.post(
        f"{AZURE_ENDPOINT}formrecognizer/documentModels/prebuilt-layout:analyze?api-version=2023-07-31",
        headers=headers,
        data=pdf_content,
    )
    if response.status_code != 202:
        raise RuntimeError(f"OCR request failed: {response.text}")
    op_url = response.headers["Operation-Location"]
    for _ in range(30):
        time.sleep(1.5)
        result = requests.get(op_url, headers={"Ocp-Apim-Subscription-Key": AZURE_KEY}).json()
        if result.get("status") == "succeeded":
            return [
                ln.get("content", "").strip()
                for page in result["analyzeResult"]["pages"]
                for ln in page["lines"]
                if ln.get("content", "").strip()
            ]
        if result.get("status") == "failed":
            raise RuntimeError("OCR analysis failed")
    raise TimeoutError("OCR timed out")


# ──────────────────────────────────────────────────────────────────────────────
# QUALITY CERTIFICATE PARSER
# ──────────────────────────────────────────────────────────────────────────────

def _parse_quality_cert_page(lines: List[str]) -> Dict[str, Dict]:
    """
    Parse one NAL Quality Certificate page.
    Returns  {lot_no: {GrowerGerm, GrowerGermDate, Purity, Inert, SeedCount}}
    """
    # ── find lot number ──────────────────────────────────────────────────────
    lot_no: Optional[str] = None
    for i, line in enumerate(lines):
        if re.search(r"Lot\s*/?\s*batch\s+number", line, re.IGNORECASE):
            for j in range(i, min(i + 4, len(lines))):
                if m := re.search(r"\b(\d{11})\b", lines[j]):
                    lot_no = m.group(1)
                    break
        if lot_no:
            break

    if not lot_no:
        return {}

    data: Dict = {}

    # ── germination % ── look for first % after "Normal seedlings" heading ──
    for i, line in enumerate(lines):
        if "Normal seedlings" in line:
            for j in range(i + 1, min(i + 5, len(lines))):
                if m := re.search(r"(\d+)\s*%", lines[j]):
                    data["GrowerGerm"] = int(m.group(1))
                    break
            break

    # ── purity & inert ── row of float percentages after "Pure seeds" ───────
    for i, line in enumerate(lines):
        if re.match(r"^Pure\s+seeds?\s*$", line, re.IGNORECASE):
            for j in range(i + 1, min(i + 5, len(lines))):
                floats = re.findall(r"(\d+[.,]\d+)\s*%?", lines[j])
                if floats:
                    try:
                        pure  = float(floats[0].replace(",", "."))
                        inert = float(floats[1].replace(",", ".")) if len(floats) > 1 else 0.0
                        if pure >= 100.0:
                            pure, inert = 99.99, 0.01
                        data["Purity"] = pure
                        data["Inert"]  = inert
                    except ValueError:
                        pass
                    break
            break

    # ── seed count (seeds/kg) ────────────────────────────────────────────────
    for line in lines:
        if m := re.search(r"([\d.,]+)\s*seeds?/kg", line, re.IGNORECASE):
            sc = _eu_float(m.group(1))
            if sc > 0:
                data["SeedCount"] = int(sc)
            break

    # ── certificate date ─────────────────────────────────────────────────────
    for line in lines:
        if m := re.match(r"Date:\s*([A-Za-z]+\s+\d{1,2},\s*\d{4})", line):
            try:
                dt = datetime.strptime(m.group(1), "%B %d, %Y")
                data["GrowerGermDate"] = dt.strftime("%m/%d/%Y")
            except ValueError:
                pass

    return {lot_no: data}


# ──────────────────────────────────────────────────────────────────────────────
# GERM LETTER PARSER
# ──────────────────────────────────────────────────────────────────────────────

def _parse_germ_letter_page(lines: List[str]) -> Dict[str, Dict]:
    """
    Parse a BASF 'Test Date Confirmation' germ letter page.
    Returns  {lot_no: {Germ, GermDate}}
    """
    result: Dict[str, Dict] = {}
    for i, line in enumerate(lines):
        if m := re.search(r"\b(\d{11})\b", line):
            lot = m.group(1)
            context = " ".join(lines[i : i + 5])
            germ = germ_date = None
            if g := re.search(r"(\d+)\s*%", context):
                germ = int(g.group(1))
            if d := re.search(r"\b(\d{1,2})/(\d{4})\b", context):
                month, year = d.groups()
                germ_date = f"{int(month):02d}/01/{year}"
            if germ or germ_date:
                result[lot] = {"Germ": germ, "GermDate": germ_date}
    return result


# ──────────────────────────────────────────────────────────────────────────────
# PACKING LIST PARSER
# ──────────────────────────────────────────────────────────────────────────────

def _parse_packing_list_page(lines: List[str]) -> Dict[str, Dict]:
    """
    Parse a Nunhems Packing List page.
    Returns  {lot_no: {SeedCount, SeedForm, SeedSize}}

    Each item block ends with a 'Lot Number:' line of the form:
        33192501011 | 3 EA | S/C: 344.791 | Net weight: 0,870 KG |
    The Seed Form and Seed Size appear above in labelled rows.
    """
    result: Dict[str, Dict] = {}

    for i, line in enumerate(lines):
        if not re.search(r"(?i)\bLot\s+Number:", line):
            continue

        # The lot number might be inline or on the very next line
        combined = line + (" " + lines[i + 1] if i + 1 < len(lines) else "")

        lot_m = re.search(r"\b(\d{11})\b", combined)
        if not lot_m:
            continue
        lot_no = lot_m.group(1)
        data   = result.setdefault(lot_no, {})

        # S/C is European-formatted (e.g. "344.791" = 344,791 seeds/kg)
        sc_m = re.search(r"S/C\s*:\s*([\d.,]+)", combined)
        if sc_m and "SeedCount" not in data:
            sc = _eu_float(sc_m.group(1))
            if sc > 0:
                data["SeedCount"] = int(sc)

        # Seed Form and Seed Size appear in labelled rows above the lot line
        for j in range(max(0, i - 10), i):
            bl = lines[j]
            if bl.startswith("Seed Form:"):
                sf = bl.replace("Seed Form:", "").strip()
                if not sf and j + 1 < i:
                    sf = lines[j + 1].strip()
                if sf:
                    data["SeedForm"] = sf
            elif bl.startswith("Seed Size:"):
                ss = bl.replace("Seed Size:", "").strip()
                if not ss and j + 1 < i:
                    ss = lines[j + 1].strip()
                if ss:
                    # Convert European decimal separator: "1,75-2,00" → "1.75-2.00"
                    data["SeedSize"] = ss.replace(",", ".")

    return result


# ──────────────────────────────────────────────────────────────────────────────
# STANDARD INVOICE PARSER  (for invoice number / PO only)
# ──────────────────────────────────────────────────────────────────────────────

def _parse_standard_invoice_header(lines: List[str]) -> Tuple[Optional[str], Optional[str]]:
    """
    Extract VendorInvoiceNo and PO from a standard Nunhems INVOICE file.
    Returns  (invoice_no, po_no)
    """
    invoice_no: Optional[str] = None
    po_no:      Optional[str] = None
    text = "\n".join(lines)

    # Invoice Number: 9-digit number near "Invoice Number"
    if m := re.search(r"Invoice\s+Number[:\s]+(\d{9})", text, re.IGNORECASE):
        invoice_no = m.group(1)

    # Customer P.O. Number: 5-digit number
    for i, line in enumerate(lines):
        if re.search(r"Customer\s+P\.?O\.?\s+Number", line, re.IGNORECASE):
            search = " ".join(lines[i : i + 4])
            if m := re.search(r"\b(\d{5})\b", search):
                po_no = f"PO-{m.group(1)}"
            break

    return invoice_no, po_no


# ──────────────────────────────────────────────────────────────────────────────
# CUSTOMS INVOICE PARSER
# ──────────────────────────────────────────────────────────────────────────────

def _extract_po_from_customs_lines(lines: List[str]) -> Optional[str]:
    """
    Find the Customer P.O. number on a customs invoice.
    The label 'Cus. P.O.:' appears in the shipping marks area; the value (5 digits)
    is on the same line or within the next few lines.
    """
    for i, line in enumerate(lines):
        if re.search(r"Cus\.?\s*P\.?O\.?", line):
            search = " ".join(lines[i : i + 5])
            if m := re.search(r"\b(\d{5})\b", search):
                return f"PO-{m.group(1)}"
    return None


def _parse_customs_invoice_pages(
    lines:       List[str],
    quality_map: Dict[str, Dict],
    germ_map:    Dict[str, Dict],
    packing_map: Dict[str, Dict],
    pkg_desc_list: List[str],
) -> Tuple[List[Dict], Optional[str]]:
    """
    Parse the combined customs-invoice lines (all customs-invoice pages concatenated).

    Each item is delimited by an 'H-S Code:' header followed by labelled fields:
        H-S Code:       12099180
        Kind/variety:   LEEK KRYPTON F1
        Package Size:   3 POUCH(ES) of 100.000 SDS
        Treated With:   UNTREATED
        Lot Number:     33192501011 | 3 EA | Net weight: 0,870 KG |
                        ITALY Origin*
                        3 EA    1.841,0000    5.523,00   ← amount has exactly 2 dec places

    Returns  (items, po_number)
    """
    po_number = _extract_po_from_customs_lines(lines)
    items: List[Dict] = []
    n = len(lines)
    i = 0

    while i < n:
        # ── detect start of an H-S Code block ───────────────────────────────
        if not re.match(r"^H-S\s*Code:", lines[i]):
            i += 1
            continue

        block = {
            "variety":        None,
            "pkg_size_seeds": 0,
            "treatment":      "Untreated",
            "lot_no":         None,
            "lot_ea":         0,
            "origin":         None,
            "amount":         0.0,
        }

        j = i + 1
        while j < min(i + 28, n):
            bl = lines[j].strip()

            # Next H-S Code block → stop
            if re.match(r"^H-S\s*Code:", bl):
                break

            # Kind/variety
            if re.match(r"^Kind/variety:", bl):
                val = bl.replace("Kind/variety:", "").strip()
                if not val and j + 1 < n and not re.match(r"^(Package Size:|Treated With:|H-S\s*Code:)", lines[j + 1]):
                    j += 1
                    val = lines[j].strip()
                block["variety"] = val

            # Package Size  →  parse seeds-per-package from European format
            elif re.match(r"^Package Size:", bl):
                val = bl.replace("Package Size:", "").strip()
                if not val and j + 1 < n:
                    j += 1
                    val = lines[j].strip()
                # "3 POUCH(ES) of 100.000 SDS" or "40 NUNHEMS CAN(S) of 25.000 SDS"
                if ps_m := re.search(r"of\s+([\d.,]+)\s*SDS", val, re.IGNORECASE):
                    block["pkg_size_seeds"] = int(_eu_float(ps_m.group(1)))

            # Treated With
            elif re.match(r"^Treated With:", bl):
                val = bl.replace("Treated With:", "").strip()
                if not val and j + 1 < n:
                    j += 1
                    val = lines[j].strip()
                block["treatment"] = val or "Untreated"

            # Lot Number  →  parse lot ID and EA quantity
            elif re.match(r"^Lot Number:", bl):
                val = bl.replace("Lot Number:", "").strip()
                if not val and j + 1 < n:
                    j += 1
                    val = lines[j].strip()
                if lot_m := re.search(r"\b(\d{11})\b", val):
                    block["lot_no"] = lot_m.group(1)
                if ea_m := re.search(r"\b(\d+)\s*EA\b", val):
                    block["lot_ea"] = int(ea_m.group(1))

            # Origin  e.g. "ITALY Origin*"
            elif re.match(r"^[A-Z][A-Za-z]+(\s+[A-Z][A-Za-z]+)?\s+Origin\*?\s*$", bl):
                country = re.sub(r"\s*Origin\*?\s*$", "", bl).strip()
                block["origin"] = convert_to_alpha2(country)

            # Amount line: European float with *exactly 2* decimal places at end
            # (Unit-price lines have 4 decimal places so they are excluded)
            elif block["lot_no"] and block["amount"] == 0.0:
                amt_m = re.search(r"([\d.]+,\d{2})\s*$", bl)
                if amt_m:
                    block["amount"] = _eu_float(amt_m.group(1))

            j += 1

        # ── build item if we have the minimum required fields ─────────────
        if block["lot_no"] and block["variety"]:
            lot_no         = block["lot_no"]
            lot_ea         = block["lot_ea"]
            pkg_size_seeds = block["pkg_size_seeds"]
            variety        = block["variety"]
            treatment      = block["treatment"]
            origin         = block["origin"]
            amount         = block["amount"]

            # Human-readable description: "LEEK KRYPTON F1 100,000 SDS"
            if pkg_size_seeds > 0:
                description = f"{variety.upper()} {pkg_size_seeds:,} SDS"
            else:
                description = variety.upper()

            # Quantity in KS (thousands of seeds)
            if pkg_size_seeds > 0 and lot_ea > 0:
                total_qty = (lot_ea * pkg_size_seeds) / 1000.0
            else:
                total_qty = float(lot_ea)

            usd_cost = round(amount / total_qty, 4) if total_qty > 0 else 0.0

            # Package description for dropdown
            pkg_desc_str = f"{pkg_size_seeds:,} SEEDS" if pkg_size_seeds > 0 else ""

            # Merge auxiliary data
            q_data = quality_map.get(lot_no, {})
            g_data = germ_map.get(lot_no, {})
            p_data = packing_map.get(lot_no, {})

            items.append({
                "VendorInvoiceNo":        None,          # filled later from standard invoice
                "PurchaseOrder":          po_number,
                "VendorItemDescription":  description,
                "VendorLotNo":            lot_no,
                "OriginCountry":          origin,
                "VendorTreatment":        treatment,
                "TreatmentsDescription":  "",
                "TreatmentsDescription2": "",
                "TotalQuantity":          total_qty,
                "TotalPrice":             amount,
                "USD_Actual_Cost_$":      f"{usd_cost:.4f}",
                "PackageDescription":     pkg_desc_str,
                # Germ letter
                "Germ":                   g_data.get("Germ"),
                "GermDate":               g_data.get("GermDate"),
                # Packing list
                "SeedCount":              p_data.get("SeedCount"),
                "SeedForm":               p_data.get("SeedForm"),
                "SeedSize":               p_data.get("SeedSize"),
                # Quality certificate
                "GrowerGerm":             q_data.get("GrowerGerm"),
                "GrowerGermDate":         q_data.get("GrowerGermDate"),
                "Purity":                 q_data.get("Purity"),
                "Inert":                  q_data.get("Inert"),
            })

        i = j  # continue from where inner loop stopped

    return items, po_number


# ──────────────────────────────────────────────────────────────────────────────
# PUBLIC INTERFACE
# ──────────────────────────────────────────────────────────────────────────────

def extract_nunhems_data_from_bytes(
    pdf_files:    List[Tuple[str, bytes]],
    pkg_desc_list: List[str],
) -> Dict[str, List[Dict]]:
    """
    Main entry point for the Nunhems extractor.

    Parameters
    ----------
    pdf_files     : list of (filename, pdf_bytes) tuples uploaded by the user
    pkg_desc_list : package description options loaded from Business Central

    Returns
    -------
    Dict mapping filename → list of extracted item dicts (one per customs-invoice line)
    """
    if not pdf_files:
        return {}

    # ── Step 1: Build auxiliary lookup maps from ALL pages of ALL files ──────
    quality_map: Dict[str, Dict] = {}
    germ_map:    Dict[str, Dict] = {}
    packing_map: Dict[str, Dict] = {}

    for _filename, pdf_bytes in pdf_files:
        for page_lines in _get_pages(pdf_bytes):
            ptype = _classify_page(page_lines)
            if ptype == "quality_cert":
                quality_map.update(_parse_quality_cert_page(page_lines))
            elif ptype == "germ_letter":
                germ_map.update(_parse_germ_letter_page(page_lines))
            elif ptype == "packing_list":
                pack_data = _parse_packing_list_page(page_lines)
                for lot, lot_data in pack_data.items():
                    if lot not in packing_map:
                        packing_map[lot] = lot_data
                    else:
                        packing_map[lot].update(lot_data)

    # ── Step 2: Find the vendor invoice number from the standard invoice ──────
    global_invoice_no: Optional[str] = None
    global_po_no:      Optional[str] = None

    for _filename, pdf_bytes in pdf_files:
        pages = _get_pages(pdf_bytes)
        if any(_classify_page(p) == "standard_invoice" for p in pages):
            all_lines = [l for p in pages for l in p]
            inv_no, po_no = _parse_standard_invoice_header(all_lines)
            if inv_no:
                global_invoice_no = inv_no
            if po_no:
                global_po_no = po_no
            break

    # ── Step 3: Extract items from files that contain customs invoices ────────
    grouped_results: Dict[str, List[Dict]] = {}

    for filename, pdf_bytes in pdf_files:
        info  = _page_info(pdf_bytes)
        pages = _get_pages(pdf_bytes)

        # Collect lines only from customs-invoice classified pages
        customs_lines: List[str] = []
        for page_lines in pages:
            if _classify_page(page_lines) == "customs_invoice":
                customs_lines.extend(page_lines)

        if not customs_lines:
            # Log auxiliary files (germ letters, certs, packing lists, standard invoice)
            log_processing_event(
                vendor="Nunhems",
                filename=filename,
                extraction_info=info,
                po_number=global_po_no,
            )
            continue

        items, po_number = _parse_customs_invoice_pages(
            customs_lines, quality_map, germ_map, packing_map, pkg_desc_list
        )

        # Back-fill the vendor invoice number from the standard invoice file
        if global_invoice_no:
            for item in items:
                item["VendorInvoiceNo"] = global_invoice_no

        # If customs invoice didn't have a PO but we found one elsewhere, use it
        if not po_number and global_po_no:
            po_number = global_po_no
            for item in items:
                if not item.get("PurchaseOrder"):
                    item["PurchaseOrder"] = po_number

        log_processing_event(
            vendor="Nunhems",
            filename=filename,
            extraction_info=info,
            po_number=po_number,
        )

        if items:
            grouped_results[filename] = items

    return grouped_results


def find_best_nunhems_package_description(vendor_desc: str, pkg_desc_list: List[str]) -> str:
    """Find the best matching package description from the BC list."""
    if not vendor_desc or not pkg_desc_list:
        return ""
    if m := re.search(r"([\d,]+)\s+SDS", vendor_desc, re.IGNORECASE):
        try:
            qty_num   = int(m.group(1).replace(",", ""))
            candidate = f"{qty_num:,} SEEDS"
            if candidate in pkg_desc_list:
                return candidate
        except ValueError:
            pass
    matches = get_close_matches(vendor_desc, pkg_desc_list, n=1, cutoff=0.6)
    return matches[0] if matches else ""