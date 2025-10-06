# # seminis.py
# import os
# import json
# import fitz  # PyMuPDF
# import re
# from typing import List, Dict, Tuple, Union
# import requests
# import time
# from difflib import get_close_matches
# from collections import defaultdict

# # --- Configuration for Azure OCR (if needed) ---
# AZURE_ENDPOINT = os.getenv("AZURE_ENDPOINT")
# AZURE_KEY = os.getenv("AZURE_KEY")

# # --- OCR and Text Extraction Logic (Modified for In-Memory) ---

# def extract_text_with_azure_ocr(pdf_content: bytes) -> List[str]:
#     """Sends PDF content (bytes) to Azure Form Recognizer for OCR."""
#     headers = {
#         "Ocp-Apim-Subscription-Key": AZURE_KEY,
#         "Content-Type": "application/pdf"
#     }
#     # Post the raw bytes directly
#     response = requests.post(
#         f"{AZURE_ENDPOINT}formrecognizer/documentModels/prebuilt-layout:analyze?api-version=2023-07-31",
#         headers=headers,
#         data=pdf_content
#     )
#     if response.status_code != 202:
#         raise RuntimeError(f"OCR request failed: {response.text}")

#     op_url = response.headers["Operation-Location"]
#     for _ in range(30): # Poll for results
#         time.sleep(1.5)
#         result = requests.get(op_url, headers={"Ocp-Apim-Subscription-Key": AZURE_KEY}).json()
#         if result.get("status") == "succeeded":
#             lines = []
#             for page in result["analyzeResult"]["pages"]:
#                 if "notice to purchaser" in " ".join(line.get("content", "").lower() for line in page.get("lines", [])):
#                     continue
#                 for line in page["lines"]:
#                     txt = line.get("content", "").strip()
#                     if txt:
#                         lines.append(txt)
#             return lines
#         if result.get("status") == "failed":
#             raise RuntimeError("OCR analysis failed")
#     raise TimeoutError("OCR timed out")

# def extract_text_with_fallback(source: Union[str, bytes]) -> List[str]:
#     """
#     Extracts text from a PDF source (path or bytes), falling back to Azure OCR if needed.
#     """
#     lines = []
#     is_scanned = False
#     doc = None
#     try:
#         if isinstance(source, bytes):
#             # If the source is bytes, open it as a stream
#             doc = fitz.open(stream=source, filetype="pdf")
#         else: # Assumes it's a file path
#             doc = fitz.open(source)
#     except Exception:
#         # If PyMuPDF fails, go straight to OCR. Pass bytes if we have them.
#         if isinstance(source, bytes):
#             return extract_text_with_azure_ocr(source)
#         with open(source, "rb") as f: # Otherwise, read the file and pass bytes
#             return extract_text_with_azure_ocr(f.read())

#     # Check for scanned document
#     for page in doc:
#         if "notice to purchaser" in page.get_text().lower():
#             continue
#         if not page.get_text().strip():
#             is_scanned = True
#             break
    
#     # If not scanned, extract text normally
#     if not is_scanned:
#         for page in doc:
#             if "notice to purchaser" in page.get_text().lower():
#                 continue
#             lines.extend([ln.strip() for ln in page.get_text().splitlines() if ln.strip()])
#         return lines
    
#     # If scanned, fall back to OCR. Pass bytes if we have them.
#     if isinstance(source, bytes):
#         return extract_text_with_azure_ocr(source)
#     with open(source, "rb") as f:
#         return extract_text_with_azure_ocr(f.read())

# # --- Data Extraction Logic (Modified for In-Memory) ---

# def _extract_seminis_analysis_data(pdf_files: List[Tuple[str, bytes]]) -> Dict[str, Dict]:
#     """Extracts data from Seminis analysis reports from a list of file bytes."""
#     analysis = {}
#     for filename, pdf_bytes in pdf_files:
#         lines = extract_text_with_fallback(pdf_bytes)
#         if not lines: continue
        
#         text = "\n".join(lines)
#         if "REPORT" not in text.upper() or "ANALYSIS" not in text.upper():
#             continue
        
#         norm = re.sub(r"\s{2,}", " ", text.replace("\n", " ").replace("\r", " "))
#         if not (m_lot := re.search(r"Lot Number[:\s]+(\d{9})", norm)):
#             continue
#         lot = m_lot.group(1)

#         pure_match = re.search(r"Pure Seed\s*%\s*([\d.]+)", norm)
#         inert_match = re.search(r"Inert Matter\s*%\s*([\d.]+)", norm)
#         germ_match = re.search(r"Germination\s*%\s*([\d.]+)", norm)
#         date_match = re.search(r"Date Tested\s*([\d/]{8,10})", norm)

#         pure = float(pure_match.group(1)) if pure_match else None
#         inert = float(inert_match.group(1)) if inert_match else None

#         if pure == 100.0: pure, inert = 99.99, 0.01

#         analysis[lot] = {
#             "PureSeed": pure, "InertMatter": inert,
#             "Germ": int(float(germ_match.group(1))) if germ_match else None,
#             "GermDate": date_match.group(1) if date_match else None
#         }
#     return analysis

# def _extract_seminis_packing_data(pdf_files: List[Tuple[str, bytes]]) -> Dict[str, Dict]:
#     """Extracts data from Seminis packing slips from a list of file bytes."""
#     packing_data = {}
#     for filename, pdf_bytes in pdf_files:
#         lines = extract_text_with_fallback(pdf_bytes)
#         if not lines: continue
        
#         text = "\n".join(lines)
#         if "PACKING" not in text.upper() or "LIST" not in text.upper():
#             continue
        
#         for i, line in enumerate(lines):
#             if "TRT:" not in line: continue
            
#             block = lines[i:i+12]
#             joined = " ".join(block)

#             m_seed_count = re.search(r"\d+\s*/\s*(\d+)", joined)
#             m_vendor_batch = re.search(r"\d{2}/\d{2}/\d{4}.*?\b(\d{10})\b", joined)
#             m_germ_date = re.search(r"(\d{2}/\d{2}/\d{4})", joined)
#             m_germ = re.search(r"(\d{2,3})\s+(?=\d{2}/\d{2}/\d{4})", joined)

#             germ = None
#             if m_germ:
#                 germ_val = int(m_germ.group(1))
#                 germ = 98 if germ_val == 100 else germ_val

#             if m_vendor_batch:
#                 vendor_batch = m_vendor_batch.group(1)
#                 packing_data[vendor_batch] = {
#                     "SeedCountPerLB": int(m_seed_count.group(1)) if m_seed_count else None,
#                     "PackingGerm": germ,
#                     "PackingGermDate": m_germ_date.group(1) if m_germ_date else None
#                 }
#     return packing_data

# def _process_single_seminis_invoice(lines: List[str], analysis_map: dict, packing_map: dict) -> List[Dict]:
#     """Processes the extracted lines from a single Seminis invoice."""
#     text_content = "\n".join(lines)
#     vendor_invoice_no = po_number = None
    
#     if m := re.search(r"Invoice Number\s*:\s*(\S+)", text_content): vendor_invoice_no = m.group(1)
#     if m := re.search(r"PO #\s*:\s*(\S+)", text_content): po_number = f"PO-{m.group(1)}"

#     items = []
#     trt_indices = [i for i, l in enumerate(lines) if "TRT:" in l]
#     amount_idx = next((i for i, l in enumerate(lines) if "Amount" in l), 0)
#     total_item_indices = [i for i, l in enumerate(lines) if "Total Item" in l]
#     block_starts = [amount_idx + 1] + [total_item_indices[i] + 3 for i in range(min(len(trt_indices) - 1, len(total_item_indices)))]

#     for idx, trt_idx in enumerate(trt_indices):
#         desc_lines = lines[block_starts[idx]:trt_idx]
#         filtered = [l for l in desc_lines if not any(x in l for x in ["Invoice Number", "PO #", "Sales Order", "Delivery Nr", "Order Date", "Ship Date", "/", "Page"])]
#         vendor_item_description = " ".join(filtered).strip()
#         treatment_desc = re.sub(r"TRT:\s*", "", lines[trt_idx].strip()).strip()

#         package = vendor_lot = origin_country = vendor_batch = total_price = total_quantity = None
        
#         for j in range(trt_idx + 1, min(trt_idx + 15, len(lines))):
#             line = lines[j]
#             if not package and (m := re.search(r"\d+\s+MK\s+\w+", line)): package = m.group().strip()
#             if not vendor_lot and (m := re.search(r"\b\d{9}(?:/\d{2})?\b", line)): vendor_lot = m.group()
#             if not vendor_batch and (m := re.search(r"\b(\d{10})\b", line)):
#                 vendor_batch = m.group(1)
#                 if j + 1 < len(lines):
#                     next_line = lines[j+1].strip()
#                     if (m_qty := re.search(r'([\d,]+)\s*(?:MK)?', next_line)):
#                         try: total_quantity = int(m_qty.group(1).replace(",", ""))
#                         except ValueError: total_quantity = None
#             if not origin_country:
#                 cc_match = re.findall(r"\b[A-Z]{2}\b", line)
#                 if (filtered_cc := [c for c in cc_match if c != "MK"]): origin_country = filtered_cc[0]
#             if line == "Total Item":
#                 for k in range(j + 1, min(j + 4, len(lines))):
#                     if (m := re.search(r"[\d,]+\.\d{2}", lines[k+1])):
#                         total_price = float(m.group().replace(",", ""))
#                         break
#                 break

#         lot_key = vendor_lot.split("/")[0] if vendor_lot else None
#         item = {
#             "VendorInvoiceNo": vendor_invoice_no, "PurchaseOrder": po_number, "VendorLot": vendor_lot,
#             "VendorItemDescription": f"{vendor_item_description} {package}".strip(), "VendorBatch": vendor_batch,
#             "OriginCountry": origin_country, "TotalPrice": total_price, "TotalQuantity": total_quantity,
#             "Treatment": treatment_desc,
#         }

#         if lot_key and (analysis_data := analysis_map.get(lot_key)):
#             item.update(analysis_data)
#             if "PureSeed" in analysis_data: item["Purity"] = analysis_data["PureSeed"]
#         if vendor_batch and (packing_data := packing_map.get(vendor_batch)):
#             item.update(packing_data)

#         tp = item.get("TotalPrice") or 0.0
#         qty = item.get("TotalQuantity")
#         item["USD_Actual_Cost_$"] = round((tp / qty), 4) if qty and qty > 0 else None
#         items.append(item)
#     return items

# def extract_seminis_data_from_bytes(pdf_files: List[Tuple[str, bytes]]) -> Dict[str, List[Dict]]:
#     """Main in-memory function to extract all item data from a batch of Seminis files."""
#     if not pdf_files:
#         return {}

#     # Pre-process all files to get analysis and packing data first
#     analysis_map = _extract_seminis_analysis_data(pdf_files)
#     packing_map = _extract_seminis_packing_data(pdf_files)

#     grouped_results = {}
#     for filename, pdf_bytes in pdf_files:
#         lines = extract_text_with_fallback(pdf_bytes)
#         if not lines: continue

#         # Identify if the current file is the main invoice
#         text_content = "\n".join(lines)
#         if "INVOICE" in text_content.upper() and "PACKING" not in text_content.upper() and "REPORT" not in text_content.upper():
#             # Process this invoice using the pre-computed maps
#             invoice_items = _process_single_seminis_invoice(lines, analysis_map, packing_map)
#             if invoice_items:
#                 grouped_results[filename] = invoice_items
    
#     return grouped_results

# def find_best_seminis_package_description(vendor_desc: str, pkg_desc_list: list[str]) -> str:
#     """Finds the best matching package description for Seminis items."""
#     if not vendor_desc or not pkg_desc_list:
#         return ""

#     # Seminis specific logic: e.g., "80 MK" -> "80,000 SEEDS"
#     if m := re.search(r"(\d+)\s*(MK)\b", vendor_desc.upper()):
#         seed_count = int(m.group(1)) * 1000
#         candidate = f"{seed_count:,} SEEDS"
#         if candidate in pkg_desc_list:
#             return candidate

#     # Fallback to general fuzzy matching
#     matches = get_close_matches(vendor_desc.upper(), pkg_desc_list, n=1, cutoff=0.6)
#     return matches[0] if matches else ""


# seminis.py
import os
import json
import fitz  # PyMuPDF
import re
from typing import List, Dict, Tuple, Union
import requests
import time
from difflib import get_close_matches
from collections import defaultdict
from db_logger import log_processing_event

# --- Configuration for Azure OCR (if needed) ---
AZURE_ENDPOINT = os.getenv("AZURE_ENDPOINT")
AZURE_KEY = os.getenv("AZURE_KEY")

# --- OCR and Text Extraction Logic (Modified for In-Memory) ---
def extract_text_with_azure_ocr(pdf_content: bytes) -> Tuple[List[str], int]:
    """Sends PDF content to Azure OCR and returns lines and page count."""
    headers = {
        "Ocp-Apim-Subscription-Key": AZURE_KEY,
        "Content-Type": "application/pdf"
    }
    response = requests.post(
        f"{AZURE_ENDPOINT}formrecognizer/documentModels/prebuilt-layout:analyze?api-version=2023-07-31",
        headers=headers,
        data=pdf_content
    )
    if response.status_code != 202:
        raise RuntimeError(f"OCR request failed: {response.text}")

    op_url = response.headers["Operation-Location"]
    for _ in range(30):
        time.sleep(1.5)
        result = requests.get(op_url, headers={"Ocp-Apim-Subscription-Key": AZURE_KEY}).json()
        if result.get("status") == "succeeded":
            lines = []
            analyze_result = result.get("analyzeResult", {})
            pages = analyze_result.get("pages", [])
            page_count = len(pages)  # Get page count from OCR result
            for page in pages:
                # ... existing line processing logic ...
                if "notice to purchaser" in " ".join(line.get("content", "").lower() for line in page.get("lines", [])):
                    continue
                for line in page["lines"]:
                    txt = line.get("content", "").strip()
                    if txt:
                        lines.append(txt)
            return lines, page_count
        if result.get("status") == "failed":
            raise RuntimeError("OCR analysis failed")
    raise TimeoutError("OCR timed out")

# def extract_text_with_fallback(source: Union[str, bytes]) -> List[str]:
#     """
#     Extracts text from a PDF source (path or bytes), falling back to Azure OCR if needed.
#     """
#     lines = []
#     is_scanned = False
#     doc = None
#     try:
#         if isinstance(source, bytes):
#             # If the source is bytes, open it as a stream
#             doc = fitz.open(stream=source, filetype="pdf")
#         else: # Assumes it's a file path
#             doc = fitz.open(source)
#     except Exception:
#         # If PyMuPDF fails, go straight to OCR. Pass bytes if we have them.
#         if isinstance(source, bytes):
#             return extract_text_with_azure_ocr(source)
#         with open(source, "rb") as f: # Otherwise, read the file and pass bytes
#             return extract_text_with_azure_ocr(f.read())

#     # Check for scanned document
#     for page in doc:
#         if "notice to purchaser" in page.get_text().lower():
#             continue
#         if not page.get_text().strip():
#             is_scanned = True
#             break
    
#     # If not scanned, extract text normally
#     if not is_scanned:
#         for page in doc:
#             if "notice to purchaser" in page.get_text().lower():
#                 continue
#             lines.extend([ln.strip() for ln in page.get_text().splitlines() if ln.strip()])
#         return lines
    
#     # If scanned, fall back to OCR. Pass bytes if we have them.
#     if isinstance(source, bytes):
#         return extract_text_with_azure_ocr(source)
#     with open(source, "rb") as f:
#         return extract_text_with_azure_ocr(f.read())

def extract_text_with_fallback(source: Union[str, bytes]) -> Dict:
    """Extracts text and returns a dictionary with metadata for logging."""
    doc = None
    try:
        doc = fitz.open(stream=source, filetype="pdf") if isinstance(source, bytes) else fitz.open(source)
    except Exception:
        # If PyMuPDF fails, go straight to OCR
        pdf_bytes = source if isinstance(source, bytes) else open(source, "rb").read()
        lines, page_count = extract_text_with_azure_ocr(pdf_bytes)
        return {'lines': lines, 'method': 'Azure OCR', 'page_count': page_count}

    page_count = doc.page_count
    is_scanned = not any(page.get_text().strip() for page in doc)
    
    if not is_scanned:
        lines = []
        for page in doc:
            page_text = page.get_text()
            if "notice to purchaser" in page_text.lower():
                continue
            lines.extend([ln.strip() for ln in page_text.splitlines() if ln.strip()])
        doc.close()
        return {'lines': lines, 'method': 'PyMuPDF', 'page_count': page_count}
    
    # If scanned, fall back to OCR
    doc.close()
    pdf_bytes = source if isinstance(source, bytes) else open(source, "rb").read()
    lines, page_count_ocr = extract_text_with_azure_ocr(pdf_bytes)
    return {'lines': lines, 'method': 'Azure OCR', 'page_count': page_count_ocr}

# --- Data Extraction Logic (Modified for In-Memory) ---
def _extract_seminis_analysis_data(pdf_files: List[Tuple[str, bytes]]) -> Dict[str, Dict]:
    """Extracts data from Seminis analysis reports."""
    analysis = {}
    for filename, pdf_bytes in pdf_files:
        extraction_info = extract_text_with_fallback(pdf_bytes)
        lines = extraction_info['lines']
        if not lines: continue
        
        text = "\n".join(lines)
        if "REPORT" not in text.upper() or "ANALYSIS" not in text.upper():
            continue
        
        norm = re.sub(r"\s{2,}", " ", text.replace("\n", " ").replace("\r", " "))
        if not (m_lot := re.search(r"Lot Number[:\s]+(\d{9})", norm)):
            continue
        lot = m_lot.group(1)

        pure_match = re.search(r"Pure Seed\s*%\s*([\d.]+)", norm)
        inert_match = re.search(r"Inert Matter\s*%\s*([\d.]+)", norm)
        germ_match = re.search(r"Germination\s*%\s*([\d.]+)", norm)
        date_match = re.search(r"Date Tested\s*([\d/]{8,10})", norm)

        pure = float(pure_match.group(1)) if pure_match else None
        inert = float(inert_match.group(1)) if inert_match else None

        if pure == 100.0: pure, inert = 99.99, 0.01

        analysis[lot] = {
            "PureSeed": pure, "InertMatter": inert,
            "Germ": int(float(germ_match.group(1))) if germ_match else None,
            "GermDate": date_match.group(1) if date_match else None
        }
    return analysis

def _extract_seminis_packing_data(pdf_files: List[Tuple[str, bytes]]) -> Dict[str, Dict]:
    """Extracts data from Seminis packing slips from a list of file bytes."""
    packing_data = {}
    for filename, pdf_bytes in pdf_files:
        extraction_info = extract_text_with_fallback(pdf_bytes)
        lines = extraction_info['lines']
        if not lines: continue
        
        text = "\n".join(lines)
        if "PACKING" not in text.upper() or "LIST" not in text.upper():
            continue
        
        for i, line in enumerate(lines):
            if "TRT:" not in line: continue
            
            block = lines[i:i+12]
            joined = " ".join(block)

            m_seed_count = re.search(r"\d+\s*/\s*(\d+)", joined)
            m_vendor_batch = re.search(r"\d{2}/\d{2}/\d{4}.*?\b(\d{10})\b", joined)
            m_germ_date = re.search(r"(\d{2}/\d{2}/\d{4})", joined)
            m_germ = re.search(r"(\d{2,3})\s+(?=\d{2}/\d{2}/\d{4})", joined)

            germ = None
            if m_germ:
                germ_val = int(m_germ.group(1))
                germ = 98 if germ_val == 100 else germ_val

            if m_vendor_batch:
                vendor_batch = m_vendor_batch.group(1)
                packing_data[vendor_batch] = {
                    "SeedCountPerLB": int(m_seed_count.group(1)) if m_seed_count else None,
                    "PackingGerm": germ,
                    "PackingGermDate": m_germ_date.group(1) if m_germ_date else None
                }
    return packing_data

def _process_single_seminis_invoice(lines: List[str], analysis_map: dict, packing_map: dict, pkg_desc_list: list[str]) -> List[Dict]:
    """Processes the extracted lines from a single Seminis invoice."""
    text_content = "\n".join(lines)
    text_content_upper = text_content.upper()
    vendor_invoice_no = po_number = None
    
    if m := re.search(r"Invoice Number\s*:\s*(\S+)", text_content): vendor_invoice_no = m.group(1)
    if m := re.search(r"PO #\s*:\s*(\S+)", text_content): po_number = f"PO-{m.group(1)}"

    items = []
    trt_indices = [i for i, l in enumerate(lines) if "TRT:" in l]
    amount_idx = next((i for i, l in enumerate(lines) if "Amount" in l), 0)
    total_item_indices = [i for i, l in enumerate(lines) if "Total Item" in l]
    block_starts = [amount_idx + 1] + [total_item_indices[i] + 3 for i in range(min(len(trt_indices) - 1, len(total_item_indices)))]

    for idx, trt_idx in enumerate(trt_indices):
        desc_lines = lines[block_starts[idx]:trt_idx]
        filtered = [l for l in desc_lines if not any(x in l for x in ["Invoice Number", "PO #", "Sales Order", "Delivery Nr", "Order Date", "Ship Date", "/", "Page"])]
        vendor_item_description = " ".join(filtered).strip()
        treatment_desc = re.sub(r"TRT:\s*", "", lines[trt_idx].strip()).strip()

        package = vendor_lot = origin_country = vendor_batch = total_price = total_quantity = None
        
        for j in range(trt_idx + 1, min(trt_idx + 15, len(lines))):
            line = lines[j]
            if not package and (m := re.search(r"\d+\s+MK\s+\w+", line)): package = m.group().strip()
            if not vendor_lot and (m := re.search(r"\b\d{9}(?:/\d{2})?\b", line)): vendor_lot = m.group()
            if not vendor_batch and (m := re.search(r"\b(\d{10})\b", line)):
                vendor_batch = m.group(1)
                if j + 1 < len(lines):
                    next_line = lines[j+1].strip()
                    if (m_qty := re.search(r'([\d,]+)\s*(?:MK)?', next_line)):
                        try: total_quantity = int(m_qty.group(1).replace(",", ""))
                        except ValueError: total_quantity = None
            if not origin_country:
                cc_match = re.findall(r"\b[A-Z]{2}\b", line)
                if (filtered_cc := [c for c in cc_match if c != "MK"]): origin_country = filtered_cc[0]
            if line == "Total Item":
                for k in range(j + 1, min(j + 4, len(lines))):
                    if (m := re.search(r"[\d,]+\.\d{2}", lines[k+1])):
                        total_price = float(m.group().replace(",", ""))
                        break
                break

        final_vendor_item_desc = f"{vendor_item_description} {package}".strip()
        package_description = ""
        if "KAMTERTER" in text_content_upper:
            package_description = "SUBCON BULK-MS"
        else:
            package_description = find_best_seminis_package_description(final_vendor_item_desc, pkg_desc_list)
        
        lot_key = vendor_lot.split("/")[0] if vendor_lot else None
        item = {
            "VendorInvoiceNo": vendor_invoice_no, "PurchaseOrder": po_number, "VendorLot": vendor_lot,
            "VendorItemDescription": final_vendor_item_desc, "VendorBatch": vendor_batch,
            "OriginCountry": origin_country, "TotalPrice": total_price, "TotalQuantity": total_quantity,
            "Treatment": treatment_desc,
            "PackageDescription": package_description,
        }

        if lot_key and (analysis_data := analysis_map.get(lot_key)):
            item.update(analysis_data)
            if "PureSeed" in analysis_data: item["Purity"] = analysis_data["PureSeed"]
        if vendor_batch and (packing_data := packing_map.get(vendor_batch)):
            item.update(packing_data)

        tp = item.get("TotalPrice") or 0.0
        qty = item.get("TotalQuantity")
        item["USD_Actual_Cost_$"] = round((tp / qty), 4) if qty and qty > 0 else None
        items.append(item)
    return items

# def extract_seminis_data_from_bytes(pdf_files: List[Tuple[str, bytes]], pkg_desc_list: list[str]) -> Dict[str, List[Dict]]:
#     """Main in-memory function to extract all item data from a batch of Seminis files."""
#     if not pdf_files:
#         return {}

#     # Pre-process all files to get analysis and packing data first
#     analysis_map = _extract_seminis_analysis_data(pdf_files)
#     packing_map = _extract_seminis_packing_data(pdf_files)

#     grouped_results = {}
#     for filename, pdf_bytes in pdf_files:
#         lines = extract_text_with_fallback(pdf_bytes)
#         if not lines: continue
        
#         extraction_info = extract_text_with_fallback(pdf_bytes)
#         lines = extraction_info['lines']

#         # Identify if the current file is the main invoice
#         text_content = "\n".join(lines)
#         if "INVOICE" in text_content.upper() and "PACKING" not in text_content.upper() and "REPORT" not in text_content.upper():
#             # Process this invoice using the pre-computed maps
#             invoice_items = _process_single_seminis_invoice(lines, analysis_map, packing_map, pkg_desc_list)
#             if invoice_items:
#                 grouped_results[filename] = invoice_items
    
#     return grouped_results

def extract_seminis_data_from_bytes(pdf_files: List[Tuple[str, bytes]], pkg_desc_list: list[str]) -> Dict[str, List[Dict]]:
    """Main function to extract all data from a batch of Seminis files and log each one."""
    if not pdf_files:
        return {}

    analysis_map = _extract_seminis_analysis_data(pdf_files)
    packing_map = _extract_seminis_packing_data(pdf_files)

    grouped_results = {}
    for filename, pdf_bytes in pdf_files:
        extraction_info = extract_text_with_fallback(pdf_bytes)
        lines = extraction_info['lines']
        
        po_number = None
        is_invoice = False
        
        if lines:
            text_content = "\n".join(lines)
            if m := re.search(r"PO #\s*:\s*(\S+)", text_content):
                po_number = f"PO-{m.group(1)}"
            is_invoice = "INVOICE" in text_content.upper() and "PACKING" not in text_content.upper() and "REPORT" not in text_content.upper()

        # LOG THE EXTRACTION EVENT
        log_processing_event(
            vendor='Seminis',
            po_number=po_number,
            filename=filename,
            extraction_info=extraction_info
        )

        if is_invoice:
            invoice_items = _process_single_seminis_invoice(lines, analysis_map, packing_map, pkg_desc_list)
            if invoice_items:
                # Associate PO with each item if not already present
                for item in invoice_items:
                    if not item.get("PurchaseOrder") and po_number:
                        item["PurchaseOrder"] = po_number
                grouped_results[filename] = invoice_items
    
    return grouped_results

def find_best_seminis_package_description(vendor_desc: str, pkg_desc_list: list[str]) -> str:
    """Finds the best matching package description for Seminis items."""
    if not vendor_desc or not pkg_desc_list:
        return ""

    # Seminis specific logic: e.g., "80 MK" -> "80,000 SEEDS"
    if m := re.search(r"(\d+)\s*(MK)\b", vendor_desc.upper()):
        seed_count = int(m.group(1)) * 1000
        candidate = f"{seed_count:,} SEEDS"
        if candidate in pkg_desc_list:
            return candidate

    # Fallback to general fuzzy matching
    matches = get_close_matches(vendor_desc.upper(), pkg_desc_list, n=1, cutoff=0.6)
    return matches[0] if matches else ""