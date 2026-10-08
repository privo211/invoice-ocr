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
from datetime import datetime
from db_logger import log_processing_event

# --- Configuration for Azure OCR (if needed) ---
AZURE_ENDPOINT = os.getenv("AZURE_ENDPOINT")
AZURE_KEY = os.getenv("AZURE_KEY")

# --- OCR and Text Extraction Logic (Modified for In-Memory) ---
def extract_text_with_azure_ocr(pdf_content: bytes) -> Dict:
    """Keep OCR line positions so certificate columns retain their values."""
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
            return {
                'lines': lines, 'method': 'Azure OCR', 'page_count': page_count,
                'pages': pages,
            }
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
        return extract_text_with_azure_ocr(pdf_bytes)

    page_count = doc.page_count
    page_texts = [page.get_text() for page in doc]
    is_scanned = not any(text.strip() for text in page_texts)
    for page, page_text in zip(doc, page_texts):
        if 'notice to purchaser' in page_text.lower():
            continue
        # Approved certificates can contain only a selectable customs stamp;
        # the report itself is scanned or drawn as outlines. Stamp text alone
        # must not suppress OCR of the lot, purity, and germination results.
        stamp_only = (re.search(r'Seed\s+Shipment\s+Release\s+From\s+Customs', page_text, re.I)
                      and not _is_seminis_analysis_report([page_text]))
        graphical_scan = not page_text.strip() and (page.get_images() or page.get_drawings())
        if stamp_only or graphical_scan:
            is_scanned = True
            break
    
    if not is_scanned:
        lines = []
        pages = []
        for page, page_text in zip(doc, page_texts):
            if "notice to purchaser" in page_text.lower():
                continue
            lines.extend([ln.strip() for ln in page_text.splitlines() if ln.strip()])
            layout_lines = []
            for block in page.get_text('dict').get('blocks', []):
                for line in block.get('lines', []):
                    content = ''.join(span['text'] for span in line['spans']).strip()
                    x0, y0, x1, y1 = line['bbox']
                    if content:
                        layout_lines.append({
                            'content': content,
                            'polygon': [x0, y0, x1, y0, x1, y1, x0, y1],
                        })
            pages.append({'lines': layout_lines})
        doc.close()
        return {'lines': lines, 'method': 'PyMuPDF', 'page_count': page_count, 'pages': pages}
    
    # If scanned, fall back to OCR
    doc.close()
    pdf_bytes = source if isinstance(source, bytes) else open(source, "rb").read()
    return extract_text_with_azure_ocr(pdf_bytes)

# --- Data Extraction Logic (Modified for In-Memory) ---
def _is_seminis_analysis_report(lines: List[str]) -> bool:
    """Use certificate field labels, independent of title spacing or OCR case."""
    text = ' '.join(lines)
    return all(re.search(pattern, text, re.IGNORECASE) for pattern in (
        r'\bLot\s+Number\b', r'\bPure\s+Seed\b', r'\bGermination\b',
    ))


def _analysis_value(extraction_info, text, label_pattern, value_pattern, align_with=None):
    """Read inline fields, or the value in the column beneath an OCR heading."""
    if match := re.search(
        rf'{label_pattern}\s*:?\s*({value_pattern})(?![\d./])', text, re.IGNORECASE
    ):
        return match.group(1)

    for page in extraction_info.get('pages', []):
        positioned = []
        for line in page.get('lines', []):
            polygon = line.get('polygon', [])
            if len(polygon) < 8:
                continue
            xs, ys = polygon[::2], polygon[1::2]
            positioned.append((line.get('content', '').strip(), min(xs), min(ys), max(xs), max(ys)))

        anchors = [line for line in positioned if align_with and re.fullmatch(align_with, line[0], re.I)]
        for label, x0, y0, x1, y1 in positioned:
            if not re.fullmatch(label_pattern + r'\s*:?\s*', label, re.I):
                continue
            height = y1 - y0
            if height <= 0:
                continue
            # Date Tested also occurs in the moisture table. Only use the
            # heading on the germination row when reading a column value.
            if align_with and not any(abs(anchor[2] - y0) <= height for anchor in anchors):
                continue
            candidates = []
            for value, vx0, vy0, vx1, vy1 in positioned:
                if not re.fullmatch(value_pattern + r'\s*%?', value):
                    continue
                same_row = abs((vy0 + vy1) / 2 - (y0 + y1) / 2) <= height / 2
                to_right = 0 <= vx0 - x1 <= height * 3
                below = y1 <= (vy0 + vy1) / 2 <= y1 + height * 3
                in_column = x0 <= (vx0 + vx1) / 2 <= x1
                if same_row and to_right:
                    candidates.append((0, vx0 - x1, value))
                elif below and in_column:
                    candidates.append((1, vy0 - y1, value))
            if candidates:
                return min(candidates)[2].rstrip('%').strip()
    return None


def _extract_seminis_analysis_data(pdf_files: List[Tuple[str, bytes]], extraction_infos=None) -> Dict[str, Dict]:
    """Extracts data from Seminis analysis reports."""
    analysis = {}
    if extraction_infos is None:
        extraction_infos = [extract_text_with_fallback(pdf_bytes) for _, pdf_bytes in pdf_files]
    for extraction_info in extraction_infos:
        lines = extraction_info['lines']
        if not lines: continue
        
        text = "\n".join(lines)
        if not _is_seminis_analysis_report(lines):
            continue
        
        norm = re.sub(r"\s{2,}", " ", text.replace("\n", " ").replace("\r", " "))
        if not (m_lot := re.search(r"\bLot\s+Number\s*:?\s*(\d{9,10})(?:/\d{2,4})?\b", norm, re.I)):
            continue
        lot = m_lot.group(1)

        percent = r'\d{1,3}(?:\.\d+)?'
        pure_value = _analysis_value(extraction_info, norm, r'Pure\s+Seed\s*%', percent)
        inert_value = _analysis_value(extraction_info, norm, r'Inert\s+Matter\s*%', percent)
        germ_value = _analysis_value(extraction_info, norm, r'Germination\s*%', percent)
        germ_start = next((i for i, line in enumerate(lines) if re.fullmatch(r'GERMINATION', line, re.I)), None)
        germ_text = norm
        if germ_start is not None:
            germ_end = next((i for i in range(germ_start + 1, len(lines))
                             if re.fullmatch(r'MOISTURE\s+CONTENT', lines[i], re.I)), len(lines))
            germ_text = ' '.join(lines[germ_start:germ_end])
        date_value = _analysis_value(
            extraction_info, germ_text,
            r'Date\s+Tested', r'\d{1,2}/\d{1,2}/\d{4}', align_with=r'Germination\s*%',
        )

        def percentage(value):
            number = float(value) if value is not None else None
            return number if number is not None and 0 <= number <= 100 else None

        pure, inert, germ = map(percentage, (pure_value, inert_value, germ_value))
        germ_date = None
        if date_value:
            try:
                germ_date = datetime.strptime(date_value, '%m/%d/%Y').strftime('%m/%d/%Y')
            except ValueError:
                pass

        if pure == 100.0: pure, inert = 99.99, 0.01

        analysis[lot] = {
            "PureSeed": pure, "InertMatter": inert,
            "Germ": int(germ) if germ is not None else None,
            "GermDate": germ_date,
        }
    return analysis

def _extract_seminis_packing_data(pdf_files: List[Tuple[str, bytes]], extraction_infos=None) -> Dict[str, Dict]:
    """Extracts data from Seminis packing slips from a list of file bytes."""
    packing_data = {}
    if extraction_infos is None:
        extraction_infos = [extract_text_with_fallback(pdf_bytes) for _, pdf_bytes in pdf_files]
    for extraction_info in extraction_infos:
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
        # desc_lines = lines[block_starts[idx]:trt_idx]
        # filtered = [l for l in desc_lines if not any(x in l for x in ["Invoice Number", "PO #", "Sales Order", "Delivery Nr", "Order Date", "Ship Date", "/", "Page"])]
        # vendor_item_description = " ".join(filtered).strip()
        
        vendor_item_description = lines[trt_idx - 1].strip()

        treatment_desc = re.sub(r"TRT:\s*", "", lines[trt_idx].strip()).strip()

        package = vendor_lot = origin_country = vendor_batch = total_price = total_quantity = None
        
        for j in range(trt_idx + 1, len(lines)):
            line = lines[j]
            batch_search_line = line
            # if not package and (m := re.search(r"\d+\s+MK\s+\w+", line)): package = m.group().strip()
            # Updated to match MK or LB (e.g., "50 LB BAG")
            if not package and (m := re.search(r"\d+\s+(?:MK|LB)\s+\w+", line, re.IGNORECASE)): 
                package = m.group().strip()
            if not vendor_lot and (m := re.search(r"\b(\d{9,10})(?:/(\d{2,4}))?\b", line)):
                lot_base, lot_suffix = m.groups()
                # Seminis sometimes wraps the last two suffix digits onto the
                # next PDF line (for example, 4513632829/03 + 10).
                if lot_suffix and len(lot_suffix) == 2 and j + 1 < len(lines):
                    wrapped_suffix = lines[j + 1].strip()
                    if re.fullmatch(r"\d{2}", wrapped_suffix):
                        lot_suffix += wrapped_suffix
                vendor_lot = lot_base + (f"/{lot_suffix}" if lot_suffix else "")
                # Remove the lot from this line before looking for the batch;
                # some invoices put both ten-digit values on the same line.
                batch_search_line = f"{line[:m.start()]} {line[m.end():]}"
            if not vendor_batch and (m := re.search(r"\b(\d{10})\b", batch_search_line)):
                vendor_batch = m.group(1)
                if j + 1 < len(lines):
                    next_line = lines[j+1].strip()
                    if (m_qty := re.search(r'([\d,]+)\s*(?:MK)?', next_line)):
                        try: total_quantity = int(m_qty.group(1).replace(",", ""))
                        except ValueError: total_quantity = None
            if not origin_country:
                cc_match = re.findall(r"\b[A-Z]{2}\b", line)
                # Filter out MK, LB (pounds), KG, EA, etc. to prevent them from being seen as Country
                filtered_cc = [c for c in cc_match if c not in ["MK", "LB", "KG", "MT", "EA", "OZ"]]
                if filtered_cc: 
                    origin_country = filtered_cc[0]
                
            # if line == "Total Item":
            #     # Try to take the next line with comma first
            #     for k in range(j + 1, min(j + 4, len(lines))):
            #         if (m := re.search(r"[\d,]+\.\d{2}", lines[k+1])) and "," in m.group():
            #             total_price = float(m.group().replace(",", ""))
            #             break
            #     else:
            #         # Fallback: take the first numeric value if no comma found
            #         for k in range(j + 1, min(j + 4, len(lines))):
            #             if (m := re.search(r"[\d,]+\.\d{2}", lines[k])):
            #                 total_price = float(m.group().replace(",", ""))
            #                 break
            #     break
            
            if line.strip() == "Total Item":
                # Scan the next 5 lines for numbers
                for k in range(j + 1, min(j + 6, len(lines))):
                    if m := re.search(r"[\d,]+\.\d{2}", lines[k]):
                        candidate = float(m.group().replace(",", ""))
                        if candidate > 50:  # ignore ratios like 0.67
                            total_price = candidate
                            break
                break

            # if line == "Total Item":
            #     for k in range(j + 1, min(j + 4, len(lines))):
            #         if (m := re.search(r"[\d,]+\.\d{2}", lines[k])):
            #             total_price = float(m.group().replace(",", ""))
            #             break
            #     break

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
        # item["USD_Actual_Cost_$"] = round((tp / qty), 4) if qty and qty > 0 else None
        item["USD_Actual_Cost_$"] = "{:.4f}".format(tp / qty) if qty and qty > 0 else None
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

    extraction_infos = [extract_text_with_fallback(pdf_bytes) for _, pdf_bytes in pdf_files]
    analysis_map = _extract_seminis_analysis_data(pdf_files, extraction_infos)
    packing_map = _extract_seminis_packing_data(pdf_files, extraction_infos)

    grouped_results = {}
    for (filename, _), extraction_info in zip(pdf_files, extraction_infos):
        lines = extraction_info['lines']
        
        po_number = None
        is_invoice = False
        
        if lines:
            text_content = "\n".join(lines)
            if m := re.search(r"PO #\s*:\s*(\S+)", text_content):
                po_number = f"PO-{m.group(1)}"
            is_invoice = ("INVOICE" in text_content.upper() and "PACKING" not in text_content.upper()
                          and "REPORT" not in text_content.upper() and not _is_seminis_analysis_report(lines))

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

def find_best_seminis_package_description(vendor_desc: str, pkg_desc_list: list[str]) -> str:
    """Finds the best matching package description for Seminis items."""
    if not vendor_desc or not pkg_desc_list:
        return ""
    
    normalized_desc = vendor_desc.upper()

    # 1. Seminis logic: "80 MK" -> "80,000 SEEDS"
    if m := re.search(r"(\d+)\s*(MK)\b", normalized_desc):
        seed_count = int(m.group(1)) * 1000
        candidate = f"{seed_count:,} SEEDS"
        if candidate in pkg_desc_list:
            return candidate

    # 2. Seminis logic: "50 LB" -> "50 LB"
    # Matches "50 LB", "50LB", "50 LB BAG"
    if m := re.search(r"(\d+)\s*LB\b", normalized_desc):
        candidate = f"{m.group(1)} LB"
        if candidate in pkg_desc_list:
            return candidate

    # Fallback to general fuzzy matching
    matches = get_close_matches(normalized_desc, pkg_desc_list, n=1, cutoff=0.6)
    return matches[0] if matches else ""
