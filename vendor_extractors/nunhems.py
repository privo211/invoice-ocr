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
Nunhems / BASF invoice extractor — definitive version with DEBUG logging.

KEY FIX: Azure Form Recognizer returns per-page results. Previously we were
flattening all 1030 lines into a single "page", causing _classify_page() to
see every keyword at once (QUALITY CERTIFICATE, CONSIGNEE, H-S CODE, etc.)
and always pick quality_cert (first check wins). Now we preserve the page
structure from OCR so each page gets its own correct classification.
"""

import os
import re
import fitz
import requests
import time
import pycountry
from datetime import datetime
from difflib import get_close_matches
from typing import Dict, List, Optional, Tuple
from db_logger import log_processing_event

AZURE_ENDPOINT = os.getenv("AZURE_ENDPOINT")
AZURE_KEY      = os.getenv("AZURE_KEY")


# ─────────────────────────────────────────────────────────────────────────────
# EUROPEAN NUMBER PARSING
# ─────────────────────────────────────────────────────────────────────────────

def parse_euro_float(s: str) -> float:
    """
    Robustly parse a European or US formatted number string.
    "5.523,00" -> 5523.0 | "344.791" -> 344791.0 | "1.841,0000" -> 1841.0
    """
    if not s:
        return 0.0
    clean = s.strip().replace(" ", "")

    if "," in clean:
        if "." in clean:
            last_dot   = clean.rfind(".")
            last_comma = clean.rfind(",")
            if last_dot < last_comma:
                clean = clean.replace(".", "").replace(",", ".")
            else:
                clean = clean.replace(",", "")
        else:
            if re.search(r",\d{2,}$", clean):
                clean = clean.replace(",", ".")
            else:
                clean = clean.replace(",", "")
    elif re.match(r"^\d{1,3}(\.\d{3})+$", clean):
        # European integer thousands: "344.791" -> "344791"
        clean = clean.replace(".", "")

    try:
        return float(clean)
    except ValueError:
        return 0.0


# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def convert_to_alpha2(country_value: str) -> str:
    if not country_value:
        return ""
    country_value = country_value.strip()
    if len(country_value) == 2:
        return country_value.upper()
    try:
        return pycountry.countries.lookup(country_value).alpha_2
    except LookupError:
        return country_value


def _extract_text_with_azure_ocr(pdf_content: bytes) -> List[List[str]]:
    """
    Send PDF to Azure Form Recognizer and return PER-PAGE results.
    Returns List[List[str]] — one inner list per PDF page.

    KEY FIX: Previously returned a flat List[str] (all pages merged),
    which destroyed the page structure needed for _classify_page().
    """
    if not AZURE_ENDPOINT or not AZURE_KEY:
        raise ValueError("Azure OCR credentials are not set.")
    headers = {"Ocp-Apim-Subscription-Key": AZURE_KEY, "Content-Type": "application/pdf"}
    response = requests.post(
        f"{AZURE_ENDPOINT}formrecognizer/documentModels/prebuilt-layout:analyze?api-version=2023-07-31",
        headers=headers, data=pdf_content,
    )
    if response.status_code != 202:
        raise RuntimeError(f"OCR request failed: {response.text}")
    op_url = response.headers["Operation-Location"]
    for _ in range(30):
        time.sleep(1.5)
        result = requests.get(op_url, headers={"Ocp-Apim-Subscription-Key": AZURE_KEY}).json()
        if result.get("status") == "succeeded":
            # ── PRESERVE per-page structure ──────────────────────────────────
            pages_out = []
            for page in result["analyzeResult"]["pages"]:
                page_lines = [
                    ln.get("content", "").strip()
                    for ln in page.get("lines", [])
                    if ln.get("content", "").strip()
                ]
                pages_out.append(page_lines)
            return pages_out
        if result.get("status") == "failed":
            raise RuntimeError("OCR analysis failed")
    raise TimeoutError("OCR timed out")


def _get_pages_with_info(pdf_bytes: bytes, filename: str = "") -> Tuple[List[List[str]], Dict]:
    """
    Extract text pages from a PDF, with Azure OCR fallback.
    Returns (pages, info) where pages is List[List[str]] — one list per page.
    """
    info = {"method": "PyMuPDF", "page_count": 0}
    pages: List[List[str]] = []
    try:
        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
        info["page_count"] = doc.page_count
        print(f"\nDEBUG [{filename}]: Opened with PyMuPDF. Page count: {doc.page_count}")
        has_text = False
        for pg_num, page in enumerate(doc):
            txt = page.get_text()
            lines = [l.strip() for l in txt.split("\n") if l.strip()]
            pages.append(lines)
            if txt.strip():
                has_text = True
            print(f"  Page {pg_num + 1}: {len(lines)} lines, has_text={bool(txt.strip())}, "
                  f"first_line={repr(lines[0]) if lines else '(empty)'}")
        doc.close()
        if has_text:
            print(f"DEBUG [{filename}]: PyMuPDF succeeded, returning {len(pages)} pages.")
            return pages, info
        else:
            print(f"DEBUG [{filename}]: PyMuPDF found NO text — falling back to Azure OCR.")
    except Exception as e:
        print(f"DEBUG [{filename}]: PyMuPDF exception: {e}")

    info["method"] = "Azure OCR"
    print(f"DEBUG [{filename}]: Sending to Azure OCR...")
    # Now returns List[List[str]] — per page, not flat
    ocr_pages = _extract_text_with_azure_ocr(pdf_bytes)
    info["page_count"] = len(ocr_pages)
    total_lines = sum(len(p) for p in ocr_pages)
    print(f"DEBUG [{filename}]: Azure OCR returned {len(ocr_pages)} pages, {total_lines} total lines.")
    for pg_num, pg_lines in enumerate(ocr_pages):
        print(f"  OCR Page {pg_num + 1}: {len(pg_lines)} lines, "
              f"first_line={repr(pg_lines[0]) if pg_lines else '(empty)'}")
    return ocr_pages, info


def _read_label_value(lines: List[str], idx: int, label: str) -> Optional[str]:
    """
    Read value for a labelled field that may be inline or on the next line.
      (a) PyMuPDF:  "Kind/variety: LEEK KRYPTON F1"
      (b) Azure OCR:"Kind/variety:"  then  "LEEK KRYPTON F1" on next line
    """
    line = lines[idx]
    after_colon = re.sub(r"^" + re.escape(label) + r"\s*:\s*", "", line, flags=re.IGNORECASE).strip()
    if after_colon:
        return after_colon
    if idx + 1 < len(lines):
        nxt = lines[idx + 1].strip()
        if nxt and not re.match(r"^[A-Za-z /]+:\s*$", nxt):
            return nxt
    return None


# ─────────────────────────────────────────────────────────────────────────────
# PAGE CLASSIFIER
# ─────────────────────────────────────────────────────────────────────────────

def _classify_page(lines: List[str]) -> str:
    text = " ".join(lines).upper()

    if "QUALITY CERTIFICATE" in text:
        return "quality_cert"
    if "TEST DATE CONFIRMATION" in text:
        return "germ_letter"
    # Require "LOT NUMBER:" (with colon) to avoid false-positive on the
    # shipping info cover page that mentions "Packing List" as a checkbox label.
    if "PACKING LIST" in text and "LOT NUMBER:" in text:
        return "packing_list"
    if "CONSIGNEE" in text and "H-S CODE" in text:
        return "customs_invoice"
    if "INVOICE" in text and "NET PRICE" in text:
        return "standard_invoice"
    return "other"


def _classify_page_debug(lines: List[str], page_num: int) -> str:
    text = " ".join(lines).upper()
    result = _classify_page(lines)
    checks = {
        "QUALITY CERTIFICATE": "QUALITY CERTIFICATE" in text,
        "TEST DATE CONFIRMATION": "TEST DATE CONFIRMATION" in text,
        "PACKING LIST": "PACKING LIST" in text,
        "LOT NUMBER:": "LOT NUMBER:" in text,
        "CONSIGNEE": "CONSIGNEE" in text,
        "H-S CODE": "H-S CODE" in text,
        "INVOICE": "INVOICE" in text,
        "NET PRICE": "NET PRICE" in text,
    }
    hits = [k for k, v in checks.items() if v]
    print(f"  Page {page_num}: classified as '{result}'. Keywords found: {hits}")
    if result == "other":
        print(f"    -> First 3 lines: {lines[:3]}")
    return result


# ─────────────────────────────────────────────────────────────────────────────
# QUALITY CERTIFICATE PARSER
# ─────────────────────────────────────────────────────────────────────────────

def _parse_quality_cert_page(lines: List[str]) -> Dict[str, Dict]:
    lot_no: Optional[str] = None
    for i, line in enumerate(lines):
        if re.search(r"Lot\s*/?\s*batch\s+number", line, re.IGNORECASE):
            for offset in range(0, 4):
                if i + offset < len(lines):
                    m = re.search(r"\b(\d{11})\b", lines[i + offset])
                    if m:
                        lot_no = m.group(1)
                        break
        if lot_no:
            break
    if not lot_no:
        return {}

    data: Dict = {}

    for i, line in enumerate(lines):
        if "Normal seedlings" in line:
            for j in range(i + 1, min(i + 5, len(lines))):
                if m := re.search(r"(\d+)\s*%", lines[j]):
                    data["GrowerGerm"] = int(m.group(1))
                    break
            break

    for i, line in enumerate(lines):
        if re.match(r"^Pure\s+seeds?\s*$", line, re.IGNORECASE):
            for j in range(i + 1, min(i + 5, len(lines))):
                floats = re.findall(r"(\d+[,.]?\d*)\s*%", lines[j])
                if not floats:
                    floats = re.findall(r"(\d+[,.]?\d+)", lines[j])
                if floats:
                    try:
                        pure  = parse_euro_float(floats[0])
                        inert = parse_euro_float(floats[1]) if len(floats) > 1 else 0.0
                        if pure >= 99.99:
                            pure, inert = 99.99, 0.01
                        data["Purity"] = pure
                        data["Inert"]  = inert
                    except (ValueError, IndexError):
                        pass
                    break
            break

    for line in lines:
        if m := re.search(r"([\d.,]+)\s*seeds?/kg", line, re.IGNORECASE):
            sc = parse_euro_float(m.group(1))
            if sc > 0:
                data["SeedCount"] = int(sc)
            break

    for line in lines:
        if m := re.match(r"Date:\s*([A-Za-z]+\s+\d{1,2},\s*\d{4})", line):
            try:
                dt = datetime.strptime(m.group(1), "%B %d, %Y")
                data["GrowerGermDate"] = dt.strftime("%m/%d/%Y")
            except ValueError:
                pass

    return {lot_no: data}


# ─────────────────────────────────────────────────────────────────────────────
# GERM LETTER PARSER
# ─────────────────────────────────────────────────────────────────────────────

def _parse_germ_letter_page(lines: List[str]) -> Dict[str, Dict]:
    result: Dict[str, Dict] = {}
    for i, line in enumerate(lines):
        if m := re.search(r"\b(\d{11})\b", line):
            lot     = m.group(1)
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


# ─────────────────────────────────────────────────────────────────────────────
# PACKING LIST PARSER
# ─────────────────────────────────────────────────────────────────────────────

def _parse_packing_list_page(lines: List[str]) -> Dict[str, Dict]:
    """
    FIX for ProductForm bug: only capture the line immediately after "Seed Form:"
    and reject it if it matches a treatment/label keyword.
    """
    result: Dict[str, Dict] = {}

    for i, line in enumerate(lines):
        if not re.search(r"(?i)\bLot\s+Number:", line):
            continue

        combined = line
        if i + 1 < len(lines):
            combined += " " + lines[i + 1]

        lot_m = re.search(r"\b(\d{11})\b", combined)
        if not lot_m:
            continue
        lot_no = lot_m.group(1)
        data   = result.setdefault(lot_no, {})

        # S/C: European integer e.g. "344.791" = 344,791 seeds
        sc_m = re.search(r"S/C\s*:\s*([\d.,]+)", combined)
        if sc_m and "SeedCount" not in data:
            sc = parse_euro_float(sc_m.group(1))
            if sc > 0:
                data["SeedCount"] = int(sc)

        # Backward window for Seed Form / Seed Size labels
        window_start = max(0, i - 20)
        window = lines[window_start : i]

        for j, w_line in enumerate(window):
            # Seed Form — split-line (OCR) or inline (PyMuPDF)
            if re.match(r"^Seed\s+Form\s*:\s*$", w_line, re.IGNORECASE):
                if j + 1 < len(window):
                    val = window[j + 1].strip()
                    if val and not re.match(
                        r"^(Seed\s+(Size|Form|Count)|Treated|Order|Customer)", val, re.IGNORECASE
                    ):
                        data.setdefault("SeedForm", val)
            elif re.match(r"^Seed\s+Form:\s+\S+", w_line, re.IGNORECASE):
                val = re.sub(r"^Seed\s+Form:\s*", "", w_line, flags=re.IGNORECASE).strip()
                if val:
                    data.setdefault("SeedForm", val)

            # Seed Size
            if re.match(r"^Seed\s+Size\s*:\s*$", w_line, re.IGNORECASE):
                if j + 1 < len(window):
                    val = window[j + 1].strip()
                    if val and not re.match(r"^(Seed|Treated)", val, re.IGNORECASE):
                        data.setdefault("SeedSize", val.replace(",", "."))
            elif re.match(r"^Seed\s+Size:\s+\S+", w_line, re.IGNORECASE):
                val = re.sub(r"^Seed\s+Size:\s*", "", w_line, flags=re.IGNORECASE).strip()
                if val:
                    data.setdefault("SeedSize", val.replace(",", "."))

    return result


# ─────────────────────────────────────────────────────────────────────────────
# STANDARD INVOICE HEADER PARSER
# ─────────────────────────────────────────────────────────────────────────────

def _parse_standard_invoice_header(lines: List[str]) -> Tuple[Optional[str], Optional[str]]:
    """
    Extract VendorInvoiceNo and PO from a standard Nunhems INVOICE file.
    Uses broad PO search across the whole document as a fallback since
    PyMuPDF may read table columns in a different order than expected.
    """
    invoice_no: Optional[str] = None
    po_no:      Optional[str] = None
    text = "\n".join(lines)

    # Invoice Number: 9-digit number
    if m := re.search(r"Invoice\s+Number[:\s]+(\d{9})", text, re.IGNORECASE):
        invoice_no = m.group(1)

    # PO: look near the "Customer P.O. Number" label first
    for i, line in enumerate(lines):
        if re.search(r"Customer\s+P\.?O\.?\s+Number", line, re.IGNORECASE):
            search = " ".join(lines[i : i + 6])
            if m := re.search(r"\b(\d{5})\b", search):
                po_no = f"PO-{m.group(1)}"
            break

    # Fallback: "Cus. P.O." label (appears in customs/packing docs) or
    # a standalone 5-digit number that appears right after any PO-related label
    if not po_no:
        for i, line in enumerate(lines):
            if re.search(r"(Cus\.?\s*P\.?O\.?|P\.O\.\s*Number|Purchase Order)", line, re.IGNORECASE):
                search = " ".join(lines[i : i + 4])
                if m := re.search(r"\b(\d{5})\b", search):
                    po_no = f"PO-{m.group(1)}"
                    break

    # Last resort: the PO number also appears in the filename for this invoice
    # pattern: "PO_89811" in the filename — but we don't have the filename here.
    # Instead scan for a 5-digit number that appears on its own line (table cell)
    # near the top of the document (header area only — first 50 lines)
    if not po_no:
        for line in lines[:50]:
            line_stripped = line.strip()
            if re.match(r"^\d{5}$", line_stripped):
                po_no = f"PO-{line_stripped}"
                break

    return invoice_no, po_no


# ─────────────────────────────────────────────────────────────────────────────
# CUSTOMS INVOICE PARSER
# ─────────────────────────────────────────────────────────────────────────────

def _extract_po_from_customs_lines(lines: List[str]) -> Optional[str]:
    for i, line in enumerate(lines):
        if re.search(r"Cus\.?\s*P\.?O\.?", line, re.IGNORECASE):
            search = " ".join(lines[i : i + 4])
            if m := re.search(r"\b(\d{5})\b", search):
                return f"PO-{m.group(1)}"
    return None


def _parse_customs_invoice_pages(
    lines:         List[str],
    quality_map:   Dict[str, Dict],
    germ_map:      Dict[str, Dict],
    packing_map:   Dict[str, Dict],
    pkg_desc_list: List[str],
) -> Tuple[List[Dict], Optional[str]]:
    po_number = _extract_po_from_customs_lines(lines)
    print(f"\nDEBUG [customs_invoice_parser]: Total lines to parse: {len(lines)}")
    print(f"DEBUG [customs_invoice_parser]: PO found: {po_number}")

    hs_hits = [(i, lines[i]) for i in range(len(lines)) if re.match(r"^H-S\s*Code\s*:", lines[i], re.IGNORECASE)]
    print(f"DEBUG [customs_invoice_parser]: H-S Code hits ({len(hs_hits)} found):")
    for idx, ln in hs_hits:
        print(f"  Line {idx}: {repr(ln)}")

    if not hs_hits:
        print("DEBUG [customs_invoice_parser]: *** NO H-S Code blocks found! Dumping first 40 lines: ***")
        for idx, ln in enumerate(lines[:40]):
            print(f"  [{idx}] {repr(ln)}")

    items: List[Dict] = []
    n = len(lines)
    i = 0

    while i < n:
        if not re.match(r"^H-S\s*Code\s*:", lines[i], re.IGNORECASE):
            i += 1
            continue

        print(f"\nDEBUG [customs_invoice_parser]: --- H-S block at line {i} ---")

        variety        = ""
        pkg_size_str   = ""
        pkg_size_seeds = 0
        treatment      = "Untreated"
        lot_no: Optional[str] = None
        lot_ea         = 0
        origin         = ""
        amount         = 0.0

        j = i + 1
        while j < min(i + 35, n):
            bl = lines[j].strip()

            if re.match(r"^H-S\s*Code\s*:", bl, re.IGNORECASE):
                break

            if re.match(r"^Kind/variety\s*:", bl, re.IGNORECASE):
                val = _read_label_value(lines, j, "Kind/variety")
                if val:
                    variety = val.strip()
                print(f"  Kind/variety -> {repr(variety)}")

            elif re.match(r"^Package\s+Size\s*:", bl, re.IGNORECASE):
                val = _read_label_value(lines, j, "Package Size")
                if val:
                    pkg_size_str = val.strip()
                    if ps_m := re.search(r"(?:of\s+)?([\d.,]+)\s*SDS", pkg_size_str, re.IGNORECASE):
                        pkg_size_seeds = int(parse_euro_float(ps_m.group(1)))
                print(f"  Package Size -> {repr(pkg_size_str)} -> seeds={pkg_size_seeds}")

            elif re.match(r"^Treated\s+With\s*:", bl, re.IGNORECASE):
                val = _read_label_value(lines, j, "Treated With")
                if val:
                    treatment = val.strip()
                print(f"  Treated With -> {repr(treatment)}")

            elif re.match(r"^Lot\s+Number\s*:", bl, re.IGNORECASE):
                val = _read_label_value(lines, j, "Lot Number")
                if val:
                    if lot_m := re.search(r"\b(\d{11})\b", val):
                        lot_no = lot_m.group(1)
                    if ea_m := re.search(r"\b(\d+)\s*EA\b", val):
                        lot_ea = int(ea_m.group(1))
                print(f"  Lot Number -> {repr(val)} -> lot={lot_no}, ea={lot_ea}")

            elif re.match(r"^[A-Z][A-Za-z ]+\s+Origin\*?\s*$", bl):
                country = re.sub(r"\s*Origin\*?\s*$", "", bl, flags=re.IGNORECASE).strip()
                origin = convert_to_alpha2(country)
                print(f"  Origin -> {repr(bl)} -> {origin}")

            elif lot_no and not re.match(r"^\d{8}$", bl):
                # Amount: rightmost European number with exactly 2 decimal places
                amt_m = re.search(r"([\d.]+,\d{2})\s*$", bl)
                if amt_m:
                    candidate = parse_euro_float(amt_m.group(1))
                    if candidate > amount:
                        amount = candidate
                        print(f"  Amount candidate -> {repr(amt_m.group(1))} -> {candidate}")

            j += 1

        print(f"  RESULT: lot={lot_no}, variety={repr(variety)}, seeds={pkg_size_seeds}, "
              f"ea={lot_ea}, amount={amount}, origin={origin}")

        if lot_no and (variety or pkg_size_str):
            if pkg_size_seeds > 0:
                description = f"{variety.upper()} {pkg_size_seeds:,} SDS".strip()
            else:
                description = f"{variety.upper()} {pkg_size_str}".strip() or f"Item for Lot {lot_no}"

            if pkg_size_seeds > 0 and lot_ea > 0:
                total_qty = (lot_ea * pkg_size_seeds) / 1000.0
            else:
                total_qty = float(lot_ea)

            usd_cost     = round(amount / total_qty, 4) if total_qty > 0 else 0.0
            pkg_desc_str = f"{pkg_size_seeds:,} SEEDS" if pkg_size_seeds > 0 else \
                           find_best_nunhems_package_description(description, pkg_desc_list)

            q_data = quality_map.get(lot_no, {})
            g_data = germ_map.get(lot_no, {})
            p_data = packing_map.get(lot_no, {})

            print(f"  -> ITEM CREATED: {description}, qty={total_qty}, price={amount}, cost={usd_cost}")
            print(f"     quality={bool(q_data)}, germ={bool(g_data)}, packing={bool(p_data)}, packing_data={p_data}")

            items.append({
                "VendorInvoiceNo":        None,
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
                "Germ":                   g_data.get("Germ"),
                "GermDate":               g_data.get("GermDate"),
                "SeedCount":              p_data.get("SeedCount"),
                "SeedForm":               p_data.get("SeedForm"),
                "SeedSize":               p_data.get("SeedSize"),
                "GrowerGerm":             q_data.get("GrowerGerm"),
                "GrowerGermDate":         q_data.get("GrowerGermDate"),
                "Purity":                 q_data.get("Purity"),
                "Inert":                  q_data.get("Inert"),
            })
        else:
            print(f"  -> ITEM SKIPPED (lot_no={lot_no}, variety={repr(variety)}, pkg={repr(pkg_size_str)})")

        i = j

    print(f"\nDEBUG [customs_invoice_parser]: Extracted {len(items)} items total.")
    return items, po_number


# ─────────────────────────────────────────────────────────────────────────────
# PUBLIC INTERFACE
# ─────────────────────────────────────────────────────────────────────────────

def extract_nunhems_data_from_bytes(
    pdf_files:     List[Tuple[str, bytes]],
    pkg_desc_list: List[str],
) -> Dict[str, List[Dict]]:
    if not pdf_files:
        return {}

    print(f"\n{'='*60}")
    print(f"DEBUG: extract_nunhems_data_from_bytes called with {len(pdf_files)} file(s):")
    for fn, fb in pdf_files:
        print(f"  - {fn} ({len(fb)} bytes)")

    # ── Step 1: Build auxiliary lookup maps ──────────────────────────────────
    print(f"\nDEBUG: === Step 1: Building auxiliary maps ===")
    quality_map: Dict[str, Dict] = {}
    germ_map:    Dict[str, Dict] = {}
    packing_map: Dict[str, Dict] = {}

    for filename, pdf_bytes in pdf_files:
        print(f"\nDEBUG: Classifying pages in '{filename}':")
        pages, _ = _get_pages_with_info(pdf_bytes, filename)
        for pg_num, page_lines in enumerate(pages, 1):
            ptype = _classify_page_debug(page_lines, pg_num)
            if ptype == "quality_cert":
                result = _parse_quality_cert_page(page_lines)
                quality_map.update(result)
                print(f"    -> quality_cert: lots found = {list(result.keys())}")
            elif ptype == "germ_letter":
                result = _parse_germ_letter_page(page_lines)
                germ_map.update(result)
                print(f"    -> germ_letter: lots found = {list(result.keys())}")
            elif ptype == "packing_list":
                result = _parse_packing_list_page(page_lines)
                for lot, lot_data in result.items():
                    existing = packing_map.setdefault(lot, {})
                    for k, v in lot_data.items():
                        if v and k not in existing:
                            existing[k] = v
                print(f"    -> packing_list: lots found = {list(result.keys())}")

    print(f"\nDEBUG: Auxiliary maps built:")
    print(f"  quality_map lots : {list(quality_map.keys())}")
    print(f"  germ_map lots    : {list(germ_map.keys())}")
    print(f"  packing_map lots : {list(packing_map.keys())}")
    for lot, pd in packing_map.items():
        print(f"    {lot}: {pd}")

    # ── Step 2: Find vendor invoice number ───────────────────────────────────
    print(f"\nDEBUG: === Step 2: Searching for standard invoice ===")
    global_invoice_no: Optional[str] = None
    global_po_no:      Optional[str] = None

    for filename, pdf_bytes in pdf_files:
        pages, _ = _get_pages_with_info(pdf_bytes, filename)
        page_types = [_classify_page(p) for p in pages]
        print(f"  '{filename}': page types = {page_types}")
        if "standard_invoice" in page_types:
            flat = [l for p in pages for l in p]
            inv_no, po_no = _parse_standard_invoice_header(flat)
            global_invoice_no = inv_no
            global_po_no      = po_no
            print(f"  -> Found standard invoice: inv_no={inv_no}, po_no={po_no}")
            break

    # ── Step 3: Extract items from customs invoice pages ─────────────────────
    print(f"\nDEBUG: === Step 3: Extracting from customs invoice pages ===")
    grouped_results: Dict[str, List[Dict]] = {}

    for filename, pdf_bytes in pdf_files:
        pages, info = _get_pages_with_info(pdf_bytes, filename)

        customs_lines: List[str] = []
        customs_page_count = 0
        for pg_num, page_lines in enumerate(pages, 1):
            ptype = _classify_page(page_lines)
            if ptype == "customs_invoice":
                customs_lines.extend(page_lines)
                customs_page_count += 1
                print(f"  '{filename}' page {pg_num}: customs_invoice, "
                      f"added {len(page_lines)} lines (total: {len(customs_lines)})")

        print(f"  '{filename}': {customs_page_count} customs pages, "
              f"{len(customs_lines)} total lines")

        if not customs_lines:
            print(f"  -> SKIPPING '{filename}' (no customs invoice pages)")
            log_processing_event(vendor="Nunhems", filename=filename,
                                 extraction_info=info, po_number=global_po_no)
            continue

        items, po_number = _parse_customs_invoice_pages(
            customs_lines, quality_map, germ_map, packing_map, pkg_desc_list
        )

        effective_po = po_number or global_po_no
        for item in items:
            if global_invoice_no and not item.get("VendorInvoiceNo"):
                item["VendorInvoiceNo"] = global_invoice_no
            if effective_po and not item.get("PurchaseOrder"):
                item["PurchaseOrder"] = effective_po

        log_processing_event(vendor="Nunhems", filename=filename,
                             extraction_info=info, po_number=effective_po)

        if items:
            grouped_results[filename] = items
            print(f"  -> '{filename}': {len(items)} items stored.")
        else:
            print(f"  -> '{filename}': customs lines found but 0 items extracted!")

    print(f"\nDEBUG: === Final result: {len(grouped_results)} file(s) with data ===")
    print(f"{'='*60}\n")
    return grouped_results


def find_best_nunhems_package_description(vendor_desc: str, pkg_desc_list: List[str]) -> str:
    if not vendor_desc or not pkg_desc_list:
        return ""
    if m := re.search(r"([\d,]+)\s+SDS", vendor_desc, re.IGNORECASE):
        try:
            qty_num   = int(m.group(1).replace(",", "").replace(".", ""))
            candidate = f"{qty_num:,} SEEDS"
            if candidate in pkg_desc_list:
                return candidate
        except ValueError:
            pass
    matches = get_close_matches(vendor_desc, pkg_desc_list, n=1, cutoff=0.6)
    return matches[0] if matches else ""


# ─────────────────────────────────────────────────────────────────────────────
# LEGACY FALLBACK
# ─────────────────────────────────────────────────────────────────────────────

def _process_single_nunhems_invoice(
    lines:         List[str],
    quality_map:   dict,
    germ_map:      dict,
    packing_map:   dict,
    pkg_desc_list: List[str],
) -> List[Dict]:
    text_content      = "\n".join(lines)
    vendor_invoice_no = None
    po_number         = None

    if m := re.search(r"Invoice\s+Number[:\s]+([\s\S]*?)\b(\d{9})\b", text_content, re.IGNORECASE):
        vendor_invoice_no = m.group(2)
    if m := re.search(r"Customer\s+P\.?O\.?\s+Number[:\s]+([\s\S]*?)\b(\d{5})\b", text_content, re.IGNORECASE):
        po_number = f"PO-{m.group(2)}"

    items = []
    item_header_indices = [
        i for i, line in enumerate(lines)
        if re.search(r"(\d+[\d,]*)\s+SDS(?!\/LB)", line)
    ]

    for idx, start_i in enumerate(item_header_indices):
        end_i       = item_header_indices[idx + 1] if idx + 1 < len(item_header_indices) else len(lines)
        block_lines = lines[start_i - 2 : end_i]
        block_text  = "\n".join(block_lines)
        sds_line    = lines[start_i]

        packaging_context_match = re.search(r"([\d,]+)\s+SDS", sds_line)
        packaging_context_str   = f"{packaging_context_match.group(1)} SDS" if packaging_context_match else ""
        desc_part1 = lines[start_i - 1].strip() if start_i > 0 else ""
        desc_part2 = lines[start_i + 1].strip() if start_i + 1 < len(lines) else ""
        vendor_item_description = f"{desc_part1} {desc_part2} {sds_line}".strip()

        treatment = "Untreated"
        if "TREATED" in block_text.upper():
            treatment = "Treated"

        unit_price = 0.0
        pq_match   = re.compile(
            r"([\d,]+(?:\.\d{2})?)\s+Net price\s+([\d,]+\.\d{2})", re.IGNORECASE
        ).search(block_text)
        if pq_match:
            unit_price = float(pq_match.group(2).replace(",", ""))

        for l_line in block_lines:
            lot_match = re.search(r"(\d{11})\s*\|\s*([\d,]+)\s*\|", l_line)
            if not lot_match:
                continue
            vendor_lot = lot_match.group(1)
            lot_qty    = int(float(lot_match.group(2).replace(",", "")))
            origin_country = None
            if origin_match := re.search(r"\|\s*([A-Za-z\s]+?)\s+ORIGIN", l_line, re.IGNORECASE):
                origin_country = convert_to_alpha2(origin_match.group(1))
            quality_info = quality_map.get(vendor_lot, {})
            germ_info    = germ_map.get(vendor_lot, {})
            packing_info = packing_map.get(vendor_lot, {})
            package_desc = find_best_nunhems_package_description(packaging_context_str, pkg_desc_list)
            calculated_cost = unit_price / lot_qty if lot_qty else 0.0
            seed_size = packing_info.get("SeedSize")
            if seed_size:
                seed_size = seed_size.replace(",", ".")
            items.append({
                "VendorInvoiceNo":        vendor_invoice_no,
                "PurchaseOrder":          po_number,
                "VendorLotNo":            vendor_lot,
                "VendorItemDescription":  vendor_item_description,
                "OriginCountry":          origin_country,
                "TotalPrice":             round(unit_price, 2),
                "TotalQuantity":          lot_qty,
                "USD_Actual_Cost_$":      f"{calculated_cost:.5f}",
                "VendorTreatment":        treatment,
                "Purity":                 quality_info.get("Purity"),
                "Inert":                  quality_info.get("Inert"),
                "Germ":                   germ_info.get("Germ"),
                "GermDate":               germ_info.get("GermDate"),
                "SeedCount":              packing_info.get("SeedCount"),
                "SeedForm":               packing_info.get("SeedForm"),
                "SeedSize":               seed_size,
                "GrowerGerm":             quality_info.get("GrowerGerm"),
                "GrowerGermDate":         quality_info.get("GrowerGermDate"),
                "PackageDescription":     package_desc,
            })
    return items