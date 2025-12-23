import os
import json
import fitz  # PyMuPDF
import re
from typing import List, Dict, Tuple
import requests
from difflib import get_close_matches
import time
from collections import defaultdict
import datetime
from db_logger import log_processing_event

item_usage_counter = defaultdict(int)

AZURE_ENDPOINT = os.getenv("AZURE_ENDPOINT")
AZURE_KEY = os.getenv("AZURE_KEY")

def extract_text_with_azure_ocr(pdf_bytes: bytes) -> List[str]:
    """
    Performs OCR on in-memory PDF bytes using Azure Form Recognizer.
    """
    headers = {
        "Ocp-Apim-Subscription-Key": AZURE_KEY,
        "Content-Type": "application/pdf"
    }
    response = requests.post(
        f"{AZURE_ENDPOINT}formrecognizer/documentModels/prebuilt-layout:analyze?api-version=2023-07-31",
        headers=headers,
        data=pdf_bytes
    )

    if response.status_code != 202:
        raise RuntimeError(f"OCR request failed: {response.text}")

    result_url = response.headers["Operation-Location"]

    for _ in range(30):
        time.sleep(1.5)
        result = requests.get(result_url, headers={"Ocp-Apim-Subscription-Key": AZURE_KEY}).json()
        if result.get("status") == "succeeded":
            lines = []
            for page in result.get("analyzeResult", {}).get("pages", []):
                page_text = " ".join(line.get("content", "").strip() for line in page.get("lines", []) if line.get("content"))
                if page_text.lower().startswith("limitation of warranty and liability") or \
                   "limitation of warranty and liability" in page_text.lower():
                    continue
                for line in page.get("lines", []):
                    content = line.get("content", "").strip()
                    if content:
                        lines.append(content)
                lines.append("--- PAGE BREAK ---")
            return lines

        elif result.get("status") == "failed":
            raise RuntimeError("OCR analysis failed")
    raise TimeoutError("OCR timed out")

def extract_items_from_ocr_lines(lines: List[str]) -> List[Dict]:
    line_items = []
    current = {}
    desc_part1 = ""
    vendor_invoice_no = None
    po_number = None
    
    full_ocr_text = " ".join(lines)

    # ... [Keep the nested extract_discounts_from_ocr_lines function as is] ...
    def extract_discounts_from_ocr_lines(lines: List[str]) -> Dict[str, List[float]]:
        discounts_by_item = defaultdict(list)
        prev_discount = None
        for i, line in enumerate(lines):
            if "discount" in line.lower():
                discount_amount = None
                item_number = None
                for j in range(i - 1, max(i - 6, -1), -1):
                    m = re.search(r"-[\d,]+\.\d{2}", lines[j])
                    if m:
                        discount_amount = abs(float(m.group().replace(",", "")))
                        break
                for j in range(i - 1, max(i - 6, -1), -1):
                    m = re.match(r"^(\d{6})\b", lines[j])
                    if m:
                        item_number = m.group(1)
                        break
                if item_number and discount_amount:
                    current_disc = (item_number, discount_amount)
                    if current_disc != prev_discount:
                        discounts_by_item[item_number].append(discount_amount)
                        prev_discount = current_disc
            if "discount-pack size" in line.lower():
                item_number = None
                for j in range(i - 1, max(i - 6, -1), -1):
                    m = re.match(r"^(\d{6})\b", lines[j])
                    if m:
                        item_number = m.group(1)
                        break
                for j in range(i + 1, min(i + 6, len(lines))):
                    discount_line = lines[j].strip()
                    m_disc = re.search(r"-([\d,]+\.\d{2})\s+N", discount_line)
                    if m_disc and item_number:
                        discount_value = float(m_disc.group(1).replace(",", ""))
                        discounts_by_item[item_number].append(discount_value)
                        break
        return discounts_by_item

    discounts_by_item = extract_discounts_from_ocr_lines(lines)

    for line in lines:
        if m_invoice := re.search(r"Invoice\s*(?:No\.?|#|Number)?\s*[:\.]?\s*(\d{5,})", line, re.IGNORECASE):
            vendor_invoice_no = m_invoice.group(1)
            break
            
    if not vendor_invoice_no:
        if m_invoice := re.search(r"Invoice\s*(?:No\.?|#|Number)?\s*[:\.]?\s*(\d{5,})", full_ocr_text, re.IGNORECASE):
            vendor_invoice_no = m_invoice.group(1)

    for line in lines:
        m_po = re.search(r"Customer PO No\.\s*.*?(\d{5})", line, re.IGNORECASE)
        if m_po:
            po_number = f"PO-{m_po.group(1)}"
            break

    def flush_item():
        nonlocal current
        if "VendorBatchLot" in current:
            line_items.append({
                "VendorInvoiceNo":       vendor_invoice_no,
                "PurchaseOrder":         po_number,
                "VendorItemNumber":      current.get("VendorItemNumber"),
                "VendorItemDescription": current.get("VendorItemDescription", "").strip(),
                "VendorBatchLot":        current.get("VendorBatchLot"),
                "VendorProductLot":      current.get("VendorProductLot"),
                "OriginCountry":         current.get("OriginCountry"),
                "TotalPrice":            current.get("TotalPrice"),
                "TotalUpcharge":         current.get("TotalUpcharge"),
                "TotalDiscount":         None,
                "TotalQuantity":         current.get("TotalQuantity"),
                "USD_Actual_Cost_$":     None,
                "ProductForm":           current.get("ProductForm"),
                "Treatment":             current.get("Treatment"),
                "SeedCount":             current.get("SeedCount"),
                "Purity":                current.get("Purity"),
                "SeedSize":              current.get("SeedSize"),
                "PureSeed":              None,
                "InertMatter":           None,
                "Germ":                  None,
                "GermDate":              None
            })
        current.clear()

    # --- UPDATED GUARD CLAUSE ---
    def is_header_artifact(line_idx: int, matched_number: str) -> bool:
        # 1. Explicitly Block Known Customer Numbers
        if matched_number in {"100996", "100476"}:
            return True
            
        # 2. Context Check (Previous Line)
        if line_idx > 0:
            prev_line = lines[line_idx - 1].lower()
            if any(x in prev_line for x in ["cust", "customer", "account", "inv", "invoice", "page"]):
                return True
                
        # 3. Inline Context Check
        current_line = lines[line_idx].lower()
        if any(x in current_line for x in ["cust", "customer", "account", "inv", "invoice", "page"]):
             return True
             
        return False

    for i, raw in enumerate(lines):
        line = raw.strip()
        if line == "--- PAGE BREAK ---": continue
        
        # --- A. NEW ITEM DETECTION ---
        
        # Pattern 1: "123456 Description"
        m2 = re.match(r"^(\d{6})\s+(.+)$", line)
        if m2:
            potential_item = m2.group(1)
            # Only process if NOT a header artifact
            if not is_header_artifact(i, potential_item):
                flush_item()
                current["VendorItemNumber"] = potential_item
                desc_part1 = m2.group(2).strip()
                continue

        # Pattern 2: "123456" (Standalone)
        m1 = re.fullmatch(r"\d{6}", line)
        if m1:
            potential_item = m1.group()
            # Only process if NOT a header artifact
            if not is_header_artifact(i, potential_item):
                flush_item()
                current["VendorItemNumber"] = potential_item
                desc_part1 = ""
                continue

        # --- B. ATTRIBUTE EXTRACTION ---
        
        # Lot Logic (Fallback included)
        if re.fullmatch(r"[A-Z]\d{5}", line):
            if "VendorBatchLot" not in current and "VendorItemNumber" in current:
                current["VendorBatchLot"] = line
            elif "VendorBatchLot" in current and ("VendorProductLot" not in current or not current["VendorProductLot"].startswith("PL")):
                current["VendorProductLot"] = line
            continue

        if "VendorItemNumber" in current and not desc_part1:
            desc_part1 = line

        if desc_part1 and re.search(r"\b\d+\s*(Ks|MS)\b", line, re.IGNORECASE):
            part2 = re.sub(r"\bHM.*$", "", line, flags=re.IGNORECASE).strip()
            part2 = re.sub(r"^(Flc\.|Plt\.\w+)\s*", "", part2, flags=re.IGNORECASE)
            unit_match = re.search(r"\b\d+\s*(Ks|MS)\b", part2, re.IGNORECASE)
            if unit_match:
                part2 = part2[:unit_match.end()]
            current["VendorItemDescription"] = f"{desc_part1} {part2}"

        if (m_pl := re.search(r"\bPL\d{6}\b", line)):
            current["VendorProductLot"] = m_pl.group()

        if "OriginCountry" not in current and "Country of origin:" in line:
            for o in (1,2):
                if i+o < len(lines) and re.fullmatch(r"[A-Z]{2}", lines[i+o].strip()):
                    current["OriginCountry"] = lines[i+o].strip()
                    break

        if "ProductForm" not in current and (m_pf := re.search(r"Product Form:\s*(\w+)", line)):
            current["ProductForm"] = m_pf.group(1)

        if "Treatment" not in current and (m_tr := re.search(r"Treatment:\s*(.+)", line)):
            current["Treatment"] = m_tr.group(1).strip()

        if "SeedCount" not in current and (m_sc := re.search(r"Seed Count:\s*(\d+)", line)):
            current["SeedCount"] = int(m_sc.group(1))

        if "Purity" not in current and (m_pr := re.search(r"Purity:\s*(\d+\.\d+)", line)):
            current["Purity"] = float(m_pr.group(1))

        if "SeedSize" not in current and (m_sz := re.search(r"Seed Size:\s*([\w\.]+)", line)):
            current["SeedSize"] = m_sz.group(1)
                
        if "TotalPrice" not in current and (m_price := re.search(r"(?<!-)(\d[\d,]*\.\d{2})\s+N", line)):
            current["TotalPrice"] = float(m_price.group(1).replace(",", ""))
        
        # Enhanced Upcharge Logic
        if "TotalUpcharge" not in current and "VendorItemNumber" in current:
            # 1. Inline Search: "915.60 Y" anywhere in the line
            if m_up_inline := re.search(r"(\d[\d,]*\.\d{2})\s+Y\b", line):
                current["TotalUpcharge"] = float(m_up_inline.group(1).replace(",", ""))
            
            # 2. Split Line Search: "915.60" on this line, "Y" on the very next line
            elif m_val_only := re.search(r"^(\d[\d,]*\.\d{2})$", line):
                 for k in range(1, 4):
                    if i + k < len(lines):
                        next_l = lines[i+k].strip()
                        if next_l == "--- PAGE BREAK ---": continue
                        if next_l == "Y":
                            current["TotalUpcharge"] = float(m_val_only.group(1).replace(",", ""))
                            break
                        if re.match(r"[A-Z]\d{5}", next_l) or re.match(r"\d{6}", next_l): break

        if "TotalQuantity" not in current and (m_qty := re.search(r"(\d+)\s*KS\b", line)):
            if (qty := int(m_qty.group(1))) > 0:
                current["TotalQuantity"] = qty

    flush_item()
    
    # ... [Keep Post-Processing: item_counter, discounts logic, USD calc] ...
    item_counter = defaultdict(int)
    for item in line_items:
        if not (item_num := item.get("VendorItemNumber")):
            continue
        occurrence_idx = item_counter[item_num]
        item_counter[item_num] += 1
        if occurrence_idx < len(discounts_by_item.get(item_num, [])):
            item["TotalDiscount"] = discounts_by_item[item_num][occurrence_idx]
        else:
            item["TotalDiscount"] = None
    
    for item in line_items:
        tp = item.get("TotalPrice") or 0.0
        tu = item.get("TotalUpcharge") or 0.0
        td = item.get("TotalDiscount") or 0.0
        qty = item.get("TotalQuantity")
        item["USD_Actual_Cost_$"] = round(((tp + tu - td) / qty), 4) if qty and qty > 0 else None

    return line_items

def _choose_batch_key(report_text_upper: str, filename: str) -> str | None:
    m = re.search(r"(?:LOT|BATCH)\s*(?:#|NO\.?)?\s*[^A-Z0-9]*([A-Z]{1,2}\d{5,})", report_text_upper, re.IGNORECASE)
    if m: return m.group(1).upper()

    m = re.match(r"([A-Z]{1,2}\d{5,})", os.path.basename(filename).upper())
    if m: return m.group(1).upper()

    codes = re.findall(r"\b([A-Z]{1,2}\d{5,})\b", report_text_upper)
    if codes:
        for c in reversed(codes): 
             if c.startswith('K') or c.startswith('PL'):
                 return c.upper()
        return codes[-1].upper()
    return None

def extract_purity_analysis_reports_from_bytes(pdf_files: list[tuple[str, bytes]]) -> Dict[str, Dict]:
    purity_data = defaultdict(dict)
    for filename, pdf_bytes in pdf_files:
        try:
            doc = fitz.open(stream=pdf_bytes, filetype="pdf")
            text = ""
            for page in doc:
                text += page.get_text() + " " 
            doc.close()

            is_valid_report_text = "REPORT" in text.upper() or "ANALYSIS" in text.upper()
            if not text.strip() or not is_valid_report_text:
                try:
                    ocr_lines = extract_text_with_azure_ocr(pdf_bytes)
                    text = " ".join(ocr_lines)
                except Exception as e:
                    print(f"OCR failed for {filename}: {e}")
                    continue

            if "REPORT" not in text.upper() and "ANALYSIS" not in text.upper():
                continue
            
            U = text.upper()
            batch_key = _choose_batch_key(U, filename)
            if not batch_key:
                continue
            
            keys_to_store = [batch_key]
            if len(batch_key) > 6:
                keys_to_store.append(batch_key[:6])

            data_found = {}
            match_pure = re.search(r"Pure Seed[^0-9]*(\d+(?:\.\d+)?)[^0-9%]*%", text, re.IGNORECASE)
            match_inert = re.search(r"Inert Matter[^0-9]*(\d+(?:\.\d+)?)[^0-9%]*%", text, re.IGNORECASE)

            if match_pure and match_inert:
                pure_seed = float(match_pure.group(1))
                data_found["PureSeed"] = 99.99 if pure_seed == 100 else pure_seed
                data_found["InertMatter"] = 0.01 if pure_seed == 100 else float(match_inert.group(1))
                
            # finditer finds ALL occurrences, not just the first one.
            # We loop through them until we find a valid germ > 50.
            germ_matches = re.finditer(r"Germ(?:ination)?(?!\s*Date)[^0-9]*\b(100|\d{2})\b", text, re.IGNORECASE)

            for match in germ_matches:
                val = int(float(match.group(1)))
                
                # Only accept if > 70. If not, the loop continues to the next match.
                if val > 70:
                    data_found["GrowerGerm"] = val
                    data_found["Germ"] = 98 if val == 100 else val
                    break # Stop searching once we find a valid number

            # if match := re.search(r"%\s*Comments:\s*(?:[A-Za-z]+\s+)*(\d{2,3})\b", text, re.IGNORECASE | re.DOTALL):
            #     germ = int(float(match.group(1)))
            #     data_found["GrowerGerm"] = germ
            #     data_found["Germ"] = 98 if germ == 100 else germ
            
            germ_date = _extract_germ_date_from_report(text)
            if germ_date:
                data_found["GrowerGermDate"] = germ_date
                data_found["GermDate"] = germ_date

            if data_found:
                for k in keys_to_store:
                    purity_data[k].update(data_found)

        except Exception as e:
            print(f"ERROR: Could not process {filename} for purity analysis: {e}")
            continue
            
    return purity_data

def _normalize_mdy(s: str) -> str:
    m, d, y = re.match(r"(\d{1,2})[/-](\d{1,2})[/-](\d{2,4})", s).groups()
    y = int(y)
    if y < 100:
        y += 2000 if y < 50 else 1900
    return f"{int(m)}/{int(d)}/{y}"

def _extract_germ_date_from_report(txt: str) -> str | None:
    flat = re.sub(r"\s+", " ", txt)
    def _norm(mdy: str) -> str:
        return _normalize_mdy(mdy)

    date_issued = None
    m_issued = re.search(r"Date\s*Issued[^0-9]{0,300}([0-9]{1,2}[/-][0-9]{1,2}[/-][0-9]{2,4})", flat, re.IGNORECASE)
    if m_issued:
        date_issued = _norm(m_issued.group(1))

    label_match = re.search(r"(?:Test|Germ(?:ination)?)\s*Date", flat, re.IGNORECASE)
    if label_match:
        window_size = 500
        search_window = flat[label_match.end(): label_match.end() + window_size]
        found_dates = re.findall(r"([0-9]{1,2}[/-][0-9]{1,2}[/-][0-9]{2,4})", search_window)
        
        for date_str in found_dates:
            norm_date = _norm(date_str)
            if date_issued and norm_date == date_issued:
                continue
            return norm_date

    if date_issued:
        return date_issued
        
    any_dates = re.findall(r"([0-9]{1,2}[/-][0-9]{1,2}[/-][0-9]{2,4})", flat)
    if any_dates:
        if len(any_dates) > 1:
             return _norm(any_dates[1])
        return _norm(any_dates[0])

    return None

def enrich_invoice_items_with_purity(items: List[Dict], purity_data: Dict[str, Dict]) -> List[Dict]:
    for item in items:
        keys_to_try = []
        if batch := (item.get("VendorBatchLot") or "").upper():
            keys_to_try.append(batch)
            if len(batch) >= 6:
                keys_to_try.append(batch[:6])
        if product := (item.get("VendorProductLot") or "").upper():
            keys_to_try.append(product)
            if len(product) >= 6:
                keys_to_try.append(product[:6])

        matched_data = None
        for key in keys_to_try:
            if key in purity_data:
                matched_data = purity_data[key]
                break
        
        if matched_data:
            item.update(matched_data)
            
    return items

def extract_discounts(blocks: List) -> List[Tuple[str, float]]:
    discounts = []
    prev_discount = None
    
    for i, b in enumerate(blocks):
        block_text = b[4].strip()
        if "discount" in block_text.lower():
            discount_amount = None
            item_number = None
            for j in range(i - 1, max(i - 6, -1), -1):
                prev_text = blocks[j][4].strip()
                for match in re.finditer(r"-[\d,]+\.\d{2}", prev_text):
                    if "/KS" not in prev_text[match.start():match.end()+5]:
                        discount_amount = abs(float(match.group().replace(",", "")))
                        break
                if discount_amount: break
            for j in range(i - 1, max(i - 6, -1), -1):
                prev_text = blocks[j][4].strip()
                if m := re.match(r"^(\d{6})\b", prev_text):
                    item_number = m.group(1)
                    break
            if item_number and discount_amount:
                current_discount = (item_number, discount_amount)
                if current_discount != prev_discount:
                    discounts.append(current_discount)
                    prev_discount = current_discount
    return discounts

# def extract_hm_clause_invoice_data_from_bytes(pdf_bytes: bytes) -> Tuple[List[Dict], Dict]:
#     """
#     Extracts invoice data from in-memory PDF bytes.
#     Returns:
#         - List of extracted items
#         - Dictionary with extraction metadata (page_count, method)
#     """
#     item_usage_counter.clear()
    
#     extraction_info = {
#         'page_count': 0,
#         'method': 'PyMuPDF' # Default
#     }
    
#     doc = fitz.open(stream=pdf_bytes, filetype="pdf")
#     extraction_info['page_count'] = doc.page_count
    
#     all_blocks = []
#     vendor_invoice_no = None
#     po_number = None
#     full_text = ""
    
#     ocr_triggered = False

#     for page in doc:
#         if "limitation of warranty and liability" in page.get_text("text").lower():
#             continue
#         blocks = page.get_text("blocks")
        
#         # Fallback to OCR if page is blank or image-based
#         if not blocks or not any(b[4].strip() for b in blocks):
#             ocr_triggered = True
#             doc.close() # Close doc before handing off bytes
#             ocr_lines = extract_text_with_azure_ocr(pdf_bytes)
#             extraction_info['method'] = 'Azure OCR'
#             return extract_items_from_ocr_lines(ocr_lines), extraction_info
            
#         sorted_blocks = sorted(blocks, key=lambda b: (b[1], b[0]))
#         all_blocks.extend(sorted_blocks)
#         for b in sorted_blocks:
#             full_text += b[4] + " "
#     doc.close()
    
#     # ... Standard PyMuPDF Extraction Logic ...
#     for block in all_blocks:
#         text = block[4].strip()
#         if m_invoice := re.search(r"Invoice\s*(?:No\.?|#|Number)?\s*[:\.]?\s*(\d{5,})", text, re.IGNORECASE):
#             vendor_invoice_no = m_invoice.group(1)
#             break
    
#     if not vendor_invoice_no:
#         if m_invoice := re.search(r"Invoice\s*(?:No\.?|#|Number)?\s*[:\.]?\s*(\d{5,})", full_text, re.IGNORECASE):
#             vendor_invoice_no = m_invoice.group(1)

#     if m_po := re.search(r"Customer PO No\.\s*.*?(\d{5})", full_text, re.IGNORECASE):
#         po_number = f"PO-{m_po.group(1)}"

#     discount_amounts = extract_discounts(all_blocks)
#     line_items = []
#     current_item_data = {}
#     desc_part1 = ""

#     def flush_item():
#         nonlocal current_item_data, desc_part1
#         if "VendorBatchLot" in current_item_data and "VendorItemNumber" in current_item_data:
#             line_items.append({
#                 "VendorInvoiceNo":       vendor_invoice_no,
#                 "PurchaseOrder":         po_number,
#                 "VendorItemNumber":      current_item_data.get("VendorItemNumber"),
#                 "VendorItemDescription": current_item_data.get("VendorItemDescription", "").strip() or desc_part1,
#                 "VendorBatchLot":        current_item_data.get("VendorBatchLot"),
#                 "VendorProductLot":      current_item_data.get("VendorProductLot"),
#                 "OriginCountry":         current_item_data.get("OriginCountry"),
#                 "TotalPrice":            current_item_data.get("TotalPrice"),
#                 "TotalUpcharge":         current_item_data.get("TotalUpcharge"),
#                 "TotalDiscount":         None,
#                 "TotalQuantity":         current_item_data.get("TotalQuantity"),
#                 "USD_Actual_Cost_$":     None,
#                 "ProductForm":           current_item_data.get("ProductForm"),
#                 "Treatment":             current_item_data.get("Treatment"),
#                 "SeedCount":             current_item_data.get("SeedCount"),
#                 "Purity":                current_item_data.get("Purity"),
#                 "SeedSize":              current_item_data.get("SeedSize"),
#                 "PureSeed":              None, "InertMatter": None,
#                 "Germ":                  None, "GermDate":    None
#             })
#         current_item_data = {}
#         desc_part1 = ""
        
#     def is_disqualified(block_idx: int, all_blocks: List) -> bool:
#         block_text = all_blocks[block_idx][4].strip().lower()
#         disqualifiers = ["cust no.", "cust no", "freight charges", "freight"]
#         if any(term in block_text for term in disqualifiers): return True
#         nearby_texts = [
#             all_blocks[j][4].strip().lower()
#             for j in range(max(0, block_idx - 2), min(len(all_blocks), block_idx + 3))
#             if j != block_idx
#         ]
#         return any(any(term in text for term in disqualifiers) for text in nearby_texts)

#     for idx, b in enumerate(all_blocks):
#         block_text = b[4].strip()

#         if re.fullmatch(r"[A-Z]\d{5}", block_text):
#             if "VendorBatchLot" not in current_item_data:
#                 current_item_data["VendorBatchLot"] = block_text
#             else:
#                 if current_item_data.get("VendorItemNumber") or current_item_data.get("VendorItemDescription"):
#                     flush_item()
#                 current_item_data["VendorBatchLot"] = block_text
#             continue

#         if not current_item_data: continue
        
#         if m := re.match(r"^(\d{6})\s+(.+)", block_text):
#             if is_disqualified(idx, all_blocks): continue
#             current_item_data["VendorItemNumber"] = m.group(1)
#             desc_part1 = m.group(2).strip()
#             continue

#         if re.fullmatch(r"\d{6}", block_text):
#             if is_disqualified(idx, all_blocks): continue
#             current_item_data["VendorItemNumber"] = block_text
#             desc_part1 = ""
#             continue
        
#         if desc_part1 and re.search(r"\b\d+\s*(Ks|MS)\b", block_text, re.IGNORECASE):
#             part2 = re.sub(r"\bHM.*$", "", block_text, flags=re.IGNORECASE).strip()
#             part2 = re.sub(r"^(Flc\.|Plt\.\w+)\s*", "", part2, flags=re.IGNORECASE)
            
#             unit_match = re.search(r"\b\d+\s*(Ks|MS)\b", part2, re.IGNORECASE)
#             if unit_match:
#                 part2 = part2[:unit_match.end()]
            
#             current_item_data["VendorItemDescription"] = f"{desc_part1} {part2}"
#             continue

#         if "VendorItemDescription" not in current_item_data and desc_part1:
#             current_item_data["VendorItemDescription"] = desc_part1
            
#         if "TotalPrice" not in current_item_data and "TotalUpcharge" not in current_item_data:
#             if m_price := re.search(r"(?<!-)(\d[\d,]*\.\d{2})\s+N", block_text):
#                 current_item_data["TotalPrice"] = float(m_price.group(1).replace(",", ""))
#             elif m_upcharge := re.search(r"(\d[\d,]*\.\d{2})\s+Y", block_text):
#                 current_item_data["TotalUpcharge"] = float(m_upcharge.group(1).replace(",", ""))
#             elif (idx + 1 < len(all_blocks) and all_blocks[idx + 1][4].strip() == "Y"):
#                 if m_val := re.search(r"(\d[\d,]*\.\d{2})", block_text):
#                     current_item_data["TotalUpcharge"] = float(m_val.group(1).replace(",", ""))
#             elif (idx + 1 < len(all_blocks) and all_blocks[idx + 1][4].strip() == "N"):
#                 if m_val := re.search(r"(\d[\d,]*\.\d{2})", block_text):
#                     current_item_data["TotalPrice"] = float(m_val.group(1).replace(",", ""))

#         if "TotalQuantity" not in current_item_data and (m_qty := re.search(r"(\d+)\s*KS\b", block_text)):
#             if (qty := int(m_qty.group(1))) > 0: current_item_data["TotalQuantity"] = qty

#         if not current_item_data.get("VendorProductLot") and (m_pl := re.search(r"\bPL\d{6}\b", block_text)):
#             current_item_data["VendorProductLot"] = m_pl.group()
         
#         if m_oc := re.search(r"Country of origin:\s*([A-Z]{2})", block_text):
#             current_item_data["OriginCountry"] = m_oc.group(1)
            
#         if m_pf := re.search(r"Product Form:\s*(\w+)", block_text):
#             current_item_data["ProductForm"] = m_pf.group(1)
            
#         if m_tr := re.search(r"Treatment:\s*(.+)", block_text):
#             current_item_data["Treatment"] = m_tr.group(1).strip()
        
#         if m_sc := re.search(r"(?<!Approx\.\s)Seed Count:\s*(\d+)", block_text):
#             current_item_data["SeedCount"] = int(m_sc.group(1))

#         if m_pr := re.search(r"Purity:\s*(\d+\.\d+)", block_text):
#             current_item_data["Purity"] = float(m_pr.group(1))
            
#         if m_sz := re.search(r"Seed Size:\s*([\w\.]+)", block_text):
#             current_item_data["SeedSize"] = m_sz.group(1)

#     flush_item()
    
#     item_counter = defaultdict(int)
#     discounts_by_item = defaultdict(list)
#     for item_num, amount in discount_amounts:
#         discounts_by_item[item_num].append(amount)

#     for item in line_items:
#         if item_num := item.get("VendorItemNumber"):
#             occurrence_idx = item_counter[item_num]
#             item_counter[item_num] += 1
#             if occurrence_idx < len(discounts_by_item.get(item_num, [])):
#                 item["TotalDiscount"] = discounts_by_item[item_num][occurrence_idx]
#             else:
#                 item["TotalDiscount"] = None
    
#     for item in line_items:
#         tp = item.get("TotalPrice") or 0.0
#         tu = item.get("TotalUpcharge") or 0.0
#         td = item.get("TotalDiscount") or 0.0
#         qty = item.get("TotalQuantity")
#         item["USD_Actual_Cost_$"] = round(((tp + tu - td) / qty), 4) if qty and qty > 0 else None

#     return line_items, extraction_info

def extract_hm_clause_invoice_data_from_bytes(pdf_bytes: bytes) -> Tuple[List[Dict], Dict]:
    # ... [Keep initial setup, fitz open, blocks extraction, OCR fallback logic] ...
    item_usage_counter.clear()
    
    extraction_info = {
        'page_count': 0,
        'method': 'PyMuPDF'
    }
    
    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    extraction_info['page_count'] = doc.page_count
    
    all_blocks = []
    vendor_invoice_no = None
    po_number = None
    full_text = ""
    
    ocr_triggered = False

    for page in doc:
        if "limitation of warranty and liability" in page.get_text("text").lower():
            continue
        blocks = page.get_text("blocks")
        
        if not blocks or not any(b[4].strip() for b in blocks):
            ocr_triggered = True
            doc.close()
            ocr_lines = extract_text_with_azure_ocr(pdf_bytes)
            extraction_info['method'] = 'Azure OCR'
            return extract_items_from_ocr_lines(ocr_lines), extraction_info
            
        sorted_blocks = sorted(blocks, key=lambda b: (b[1], b[0]))
        all_blocks.extend(sorted_blocks)
        for b in sorted_blocks:
            full_text += b[4] + " "
    doc.close()
    
    for block in all_blocks:
        text = block[4].strip()
        if m_invoice := re.search(r"Invoice\s*(?:No\.?|#|Number)?\s*[:\.]?\s*(\d{5,})", text, re.IGNORECASE):
            vendor_invoice_no = m_invoice.group(1)
            break
    
    if not vendor_invoice_no:
        if m_invoice := re.search(r"Invoice\s*(?:No\.?|#|Number)?\s*[:\.]?\s*(\d{5,})", full_text, re.IGNORECASE):
            vendor_invoice_no = m_invoice.group(1)

    if m_po := re.search(r"Customer PO No\.\s*.*?(\d{5})", full_text, re.IGNORECASE):
        po_number = f"PO-{m_po.group(1)}"

    discount_amounts = extract_discounts(all_blocks)
    line_items = []
    current_item_data = {}
    desc_part1 = ""

    def flush_item():
        nonlocal current_item_data, desc_part1
        if "VendorBatchLot" in current_item_data and "VendorItemNumber" in current_item_data:
            line_items.append({
                "VendorInvoiceNo":       vendor_invoice_no,
                "PurchaseOrder":         po_number,
                "VendorItemNumber":      current_item_data.get("VendorItemNumber"),
                "VendorItemDescription": current_item_data.get("VendorItemDescription", "").strip() or desc_part1,
                "VendorBatchLot":        current_item_data.get("VendorBatchLot"),
                "VendorProductLot":      current_item_data.get("VendorProductLot"),
                "OriginCountry":         current_item_data.get("OriginCountry"),
                "TotalPrice":            current_item_data.get("TotalPrice"),
                "TotalUpcharge":         current_item_data.get("TotalUpcharge"),
                "TotalDiscount":         None,
                "TotalQuantity":         current_item_data.get("TotalQuantity"),
                "USD_Actual_Cost_$":     None,
                "ProductForm":           current_item_data.get("ProductForm"),
                "Treatment":             current_item_data.get("Treatment"),
                "SeedCount":             current_item_data.get("SeedCount"),
                "Purity":                current_item_data.get("Purity"),
                "SeedSize":              current_item_data.get("SeedSize"),
                "PureSeed":              None, "InertMatter": None,
                "Germ":                  None, "GermDate":    None
            })
        current_item_data = {}
        desc_part1 = ""
        
    def is_disqualified(block_idx: int, all_blocks: List, potential_val: str = "") -> bool:
        # 1. Block specific Customer Numbers immediately
        if potential_val in {"100996", "100476"}:
            return True

        block_text = all_blocks[block_idx][4].strip().lower()
        disqualifiers = ["cust no.", "cust no", "freight charges", "freight"]
        if any(term in block_text for term in disqualifiers): return True
        
        # 2. Check context in nearby blocks
        nearby_texts = [
            all_blocks[j][4].strip().lower()
            for j in range(max(0, block_idx - 2), min(len(all_blocks), block_idx + 3))
            if j != block_idx
        ]
        return any(any(term in text for term in disqualifiers) for text in nearby_texts)

    for idx, b in enumerate(all_blocks):
        block_text = b[4].strip()

        # 1. LOT LOGIC (With Fallback)
        if re.fullmatch(r"[A-Z]\d{5}", block_text):
            if "VendorBatchLot" not in current_item_data:
                current_item_data["VendorBatchLot"] = block_text
            # Fallback: If BatchLot found, and we find another 5-digit code, treat as ProductLot
            # unless we already found a 'PL' code.
            elif "VendorProductLot" not in current_item_data or not current_item_data["VendorProductLot"].startswith("PL"):
                # Ensure we are inside an item context so we don't accidentally grab a random header code
                if current_item_data.get("VendorItemNumber") or current_item_data.get("VendorItemDescription"):
                    current_item_data["VendorProductLot"] = block_text
                else:
                     # If we aren't deep in an item, but have a batch lot, this MIGHT be a new item's batch lot.
                     # However, given the fallback logic, we prefer to keep the current item open if possible.
                     # Only flush if the previous item is "complete" enough.
                     if "VendorBatchLot" in current_item_data:
                         flush_item()
                         current_item_data["VendorBatchLot"] = block_text
            continue

        if not current_item_data: continue
        
        # New Item Pattern 1: "123456 Description"
        if m := re.match(r"^(\d{6})\s+(.+)", block_text):
            if is_disqualified(idx, all_blocks, m.group(1)): continue
            current_item_data["VendorItemNumber"] = m.group(1)
            desc_part1 = m.group(2).strip()
            continue

        # New Item Pattern 2: "123456" (Standalone)
        if re.fullmatch(r"\d{6}", block_text):
            if is_disqualified(idx, all_blocks, block_text): continue
            current_item_data["VendorItemNumber"] = block_text
            desc_part1 = ""
            continue
        
        if desc_part1 and re.search(r"\b\d+\s*(Ks|MS)\b", block_text, re.IGNORECASE):
            part2 = re.sub(r"\bHM.*$", "", block_text, flags=re.IGNORECASE).strip()
            part2 = re.sub(r"^(Flc\.|Plt\.\w+)\s*", "", block_text, flags=re.IGNORECASE)
            
            unit_match = re.search(r"\b\d+\s*(Ks|MS)\b", part2, re.IGNORECASE)
            if unit_match:
                part2 = part2[:unit_match.end()]
            
            current_item_data["VendorItemDescription"] = f"{desc_part1} {part2}"
            continue

        if "VendorItemDescription" not in current_item_data and desc_part1:
            current_item_data["VendorItemDescription"] = desc_part1
            
        # 2. PRICE / UPCHARGE LOGIC (Extended Lookahead)
        if "TotalPrice" not in current_item_data and "TotalUpcharge" not in current_item_data:
            if m_price := re.search(r"(?<!-)(\d[\d,]*\.\d{2})\s+N", block_text):
                current_item_data["TotalPrice"] = float(m_price.group(1).replace(",", ""))
            elif m_upcharge := re.search(r"(\d[\d,]*\.\d{2})\s+Y", block_text):
                current_item_data["TotalUpcharge"] = float(m_upcharge.group(1).replace(",", ""))
            else:
                # Lookahead for "Y" or "N" in subsequent blocks (up to 3 blocks ahead)
                found_flag = False
                for k in range(1, 4):
                    if idx + k < len(all_blocks):
                        next_block_val = all_blocks[idx + k][4].strip()
                        if next_block_val == "Y":
                            if m_val := re.search(r"(\d[\d,]*\.\d{2})", block_text):
                                current_item_data["TotalUpcharge"] = float(m_val.group(1).replace(",", ""))
                                found_flag = True
                            break
                        elif next_block_val == "N":
                            if m_val := re.search(r"(\d[\d,]*\.\d{2})", block_text):
                                current_item_data["TotalPrice"] = float(m_val.group(1).replace(",", ""))
                                found_flag = True
                            break
                        # Stop if we hit something that looks like a new item or lot
                        if re.match(r"[A-Z]\d{5}", next_block_val) or re.match(r"\d{6}", next_block_val):
                            break
                
        # Fallback: Floating Upcharge "123.45 Y" (Late capture if missed above)
        if "TotalUpcharge" not in current_item_data and "VendorItemNumber" in current_item_data:
             if m_up_loose := re.search(r"(\d[\d,]*\.\d{2})\s+Y", block_text):
                 current_item_data["TotalUpcharge"] = float(m_up_loose.group(1).replace(",", ""))

        if "TotalQuantity" not in current_item_data and (m_qty := re.search(r"(\d+)\s*KS\b", block_text)):
            if (qty := int(m_qty.group(1))) > 0: current_item_data["TotalQuantity"] = qty

        # 3. Explicit PL check (Takes priority)
        if (m_pl := re.search(r"\bPL\d{6}\b", block_text)):
            current_item_data["VendorProductLot"] = m_pl.group()
         
        # ... [Keep remaining field extractions] ...
        if m_oc := re.search(r"Country of origin:\s*([A-Z]{2})", block_text):
            current_item_data["OriginCountry"] = m_oc.group(1)
            
        if m_pf := re.search(r"Product Form:\s*(\w+)", block_text):
            current_item_data["ProductForm"] = m_pf.group(1)
            
        if m_tr := re.search(r"Treatment:\s*(.+)", block_text):
            current_item_data["Treatment"] = m_tr.group(1).strip()
        
        if m_sc := re.search(r"(?<!Approx\.\s)Seed Count:\s*(\d+)", block_text):
            current_item_data["SeedCount"] = int(m_sc.group(1))

        if m_pr := re.search(r"Purity:\s*(\d+\.\d+)", block_text):
            current_item_data["Purity"] = float(m_pr.group(1))
            
        if m_sz := re.search(r"Seed Size:\s*([\w\.]+)", block_text):
            current_item_data["SeedSize"] = m_sz.group(1)

    flush_item()
    
    # ... [Keep remainder: item_counter, discounts, return info] ...
    item_counter = defaultdict(int)
    discounts_by_item = defaultdict(list)
    for item_num, amount in discount_amounts:
        discounts_by_item[item_num].append(amount)

    for item in line_items:
        if item_num := item.get("VendorItemNumber"):
            occurrence_idx = item_counter[item_num]
            item_counter[item_num] += 1
            if occurrence_idx < len(discounts_by_item.get(item_num, [])):
                item["TotalDiscount"] = discounts_by_item[item_num][occurrence_idx]
            else:
                item["TotalDiscount"] = None
    
    for item in line_items:
        tp = item.get("TotalPrice") or 0.0
        tu = item.get("TotalUpcharge") or 0.0
        td = item.get("TotalDiscount") or 0.0
        qty = item.get("TotalQuantity")
        item["USD_Actual_Cost_$"] = round(((tp + tu - td) / qty), 4) if qty and qty > 0 else None

    return line_items, extraction_info

# def extract_hm_clause_invoice_data_from_bytes(pdf_bytes: bytes) -> Tuple[List[Dict], Dict]:
#     # ... [Keep initial setup, fitz open, blocks extraction, OCR fallback logic] ...
#     item_usage_counter.clear()
    
#     extraction_info = {
#         'page_count': 0,
#         'method': 'PyMuPDF'
#     }
    
#     doc = fitz.open(stream=pdf_bytes, filetype="pdf")
#     extraction_info['page_count'] = doc.page_count
    
#     all_blocks = []
#     vendor_invoice_no = None
#     po_number = None
#     full_text = ""
    
#     ocr_triggered = False

#     for page in doc:
#         if "limitation of warranty and liability" in page.get_text("text").lower():
#             continue
#         blocks = page.get_text("blocks")
        
#         if not blocks or not any(b[4].strip() for b in blocks):
#             ocr_triggered = True
#             doc.close()
#             ocr_lines = extract_text_with_azure_ocr(pdf_bytes)
#             extraction_info['method'] = 'Azure OCR'
#             return extract_items_from_ocr_lines(ocr_lines), extraction_info
            
#         sorted_blocks = sorted(blocks, key=lambda b: (b[1], b[0]))
#         all_blocks.extend(sorted_blocks)
#         for b in sorted_blocks:
#             full_text += b[4] + " "
#     doc.close()
    
#     for block in all_blocks:
#         text = block[4].strip()
#         if m_invoice := re.search(r"Invoice\s*(?:No\.?|#|Number)?\s*[:\.]?\s*(\d{5,})", text, re.IGNORECASE):
#             vendor_invoice_no = m_invoice.group(1)
#             break
    
#     if not vendor_invoice_no:
#         if m_invoice := re.search(r"Invoice\s*(?:No\.?|#|Number)?\s*[:\.]?\s*(\d{5,})", full_text, re.IGNORECASE):
#             vendor_invoice_no = m_invoice.group(1)

#     if m_po := re.search(r"Customer PO No\.\s*.*?(\d{5})", full_text, re.IGNORECASE):
#         po_number = f"PO-{m_po.group(1)}"

#     discount_amounts = extract_discounts(all_blocks)
#     line_items = []
#     current_item_data = {}
#     desc_part1 = ""

#     def flush_item():
#         nonlocal current_item_data, desc_part1
#         if "VendorBatchLot" in current_item_data and "VendorItemNumber" in current_item_data:
#             line_items.append({
#                 "VendorInvoiceNo":       vendor_invoice_no,
#                 "PurchaseOrder":         po_number,
#                 "VendorItemNumber":      current_item_data.get("VendorItemNumber"),
#                 "VendorItemDescription": current_item_data.get("VendorItemDescription", "").strip() or desc_part1,
#                 "VendorBatchLot":        current_item_data.get("VendorBatchLot"),
#                 "VendorProductLot":      current_item_data.get("VendorProductLot"),
#                 "OriginCountry":         current_item_data.get("OriginCountry"),
#                 "TotalPrice":            current_item_data.get("TotalPrice"),
#                 "TotalUpcharge":         current_item_data.get("TotalUpcharge"),
#                 "TotalDiscount":         None,
#                 "TotalQuantity":         current_item_data.get("TotalQuantity"),
#                 "USD_Actual_Cost_$":     None,
#                 "ProductForm":           current_item_data.get("ProductForm"),
#                 "Treatment":             current_item_data.get("Treatment"),
#                 "SeedCount":             current_item_data.get("SeedCount"),
#                 "Purity":                current_item_data.get("Purity"),
#                 "SeedSize":              current_item_data.get("SeedSize"),
#                 "PureSeed":              None, "InertMatter": None,
#                 "Germ":                  None, "GermDate":    None
#             })
#         current_item_data = {}
#         desc_part1 = ""
        
#     def is_disqualified(block_idx: int, all_blocks: List) -> bool:
#         block_text = all_blocks[block_idx][4].strip().lower()
#         disqualifiers = ["cust no.", "cust no", "freight charges", "freight"]
#         if any(term in block_text for term in disqualifiers): return True
#         nearby_texts = [
#             all_blocks[j][4].strip().lower()
#             for j in range(max(0, block_idx - 2), min(len(all_blocks), block_idx + 3))
#             if j != block_idx
#         ]
#         return any(any(term in text for term in disqualifiers) for text in nearby_texts)

#     for idx, b in enumerate(all_blocks):
#         block_text = b[4].strip()

#         # 1. LOT LOGIC (Modified for Fallback)
#         if re.fullmatch(r"[A-Z]\d{5}", block_text):
#             if "VendorBatchLot" not in current_item_data:
#                 current_item_data["VendorBatchLot"] = block_text
#             # Fallback: If BatchLot found, and we find another 5-digit code, treat as ProductLot
#             # unless we already found a 'PL' code.
#             elif "VendorProductLot" not in current_item_data or not current_item_data["VendorProductLot"].startswith("PL"):
#                 if current_item_data.get("VendorItemNumber") or current_item_data.get("VendorItemDescription"):
#                     # Only assign if we are inside an item, otherwise it might be a weird header
#                     current_item_data["VendorProductLot"] = block_text
#                 else:
#                     # If we aren't deep in an item, this might be a new batch lot for a new item?
#                     # Original logic suggests flush here, but for fallback assignment we generally assume inside item
#                     if "VendorBatchLot" in current_item_data:
#                          flush_item()
#                          current_item_data["VendorBatchLot"] = block_text
#             continue

#         if not current_item_data: continue
        
#         if m := re.match(r"^(\d{6})\s+(.+)", block_text):
#             if is_disqualified(idx, all_blocks): continue
#             current_item_data["VendorItemNumber"] = m.group(1)
#             desc_part1 = m.group(2).strip()
#             continue

#         if re.fullmatch(r"\d{6}", block_text):
#             if is_disqualified(idx, all_blocks): continue
#             current_item_data["VendorItemNumber"] = block_text
#             desc_part1 = ""
#             continue
        
#         if desc_part1 and re.search(r"\b\d+\s*(Ks|MS)\b", block_text, re.IGNORECASE):
#             part2 = re.sub(r"\bHM.*$", "", block_text, flags=re.IGNORECASE).strip()
#             part2 = re.sub(r"^(Flc\.|Plt\.\w+)\s*", "", block_text, flags=re.IGNORECASE)
            
#             unit_match = re.search(r"\b\d+\s*(Ks|MS)\b", part2, re.IGNORECASE)
#             if unit_match:
#                 part2 = part2[:unit_match.end()]
            
#             current_item_data["VendorItemDescription"] = f"{desc_part1} {part2}"
#             continue

#         if "VendorItemDescription" not in current_item_data and desc_part1:
#             current_item_data["VendorItemDescription"] = desc_part1
            
#         if "TotalPrice" not in current_item_data and "TotalUpcharge" not in current_item_data:
#             if m_price := re.search(r"(?<!-)(\d[\d,]*\.\d{2})\s+N", block_text):
#                 current_item_data["TotalPrice"] = float(m_price.group(1).replace(",", ""))
#             elif m_upcharge := re.search(r"(\d[\d,]*\.\d{2})\s+Y", block_text):
#                 current_item_data["TotalUpcharge"] = float(m_upcharge.group(1).replace(",", ""))
#             elif (idx + 1 < len(all_blocks) and all_blocks[idx + 1][4].strip() == "Y"):
#                 if m_val := re.search(r"(\d[\d,]*\.\d{2})", block_text):
#                     current_item_data["TotalUpcharge"] = float(m_val.group(1).replace(",", ""))
#             elif (idx + 1 < len(all_blocks) and all_blocks[idx + 1][4].strip() == "N"):
#                 if m_val := re.search(r"(\d[\d,]*\.\d{2})", block_text):
#                     current_item_data["TotalPrice"] = float(m_val.group(1).replace(",", ""))

#         # 2. Total Upcharge Next-Page Fallback logic
#         # If we have an active item, but no upcharge yet, allow catching "123.45 Y" in any subsequent block
#         if "TotalUpcharge" not in current_item_data and "VendorItemNumber" in current_item_data:
#              if m_up_loose := re.search(r"(\d[\d,]*\.\d{2})\s+Y", block_text):
#                  current_item_data["TotalUpcharge"] = float(m_up_loose.group(1).replace(",", ""))

#         if "TotalQuantity" not in current_item_data and (m_qty := re.search(r"(\d+)\s*KS\b", block_text)):
#             if (qty := int(m_qty.group(1))) > 0: current_item_data["TotalQuantity"] = qty

#         # 3. Explicit PL check (Takes priority or fills gap)
#         if (m_pl := re.search(r"\bPL\d{6}\b", block_text)):
#             current_item_data["VendorProductLot"] = m_pl.group()
         
#         # ... [Keep remaining field extractions: Origin, Form, Treatment, SeedCount, Purity, SeedSize] ...
#         if m_oc := re.search(r"Country of origin:\s*([A-Z]{2})", block_text):
#             current_item_data["OriginCountry"] = m_oc.group(1)
            
#         if m_pf := re.search(r"Product Form:\s*(\w+)", block_text):
#             current_item_data["ProductForm"] = m_pf.group(1)
            
#         if m_tr := re.search(r"Treatment:\s*(.+)", block_text):
#             current_item_data["Treatment"] = m_tr.group(1).strip()
        
#         if m_sc := re.search(r"(?<!Approx\.\s)Seed Count:\s*(\d+)", block_text):
#             current_item_data["SeedCount"] = int(m_sc.group(1))

#         if m_pr := re.search(r"Purity:\s*(\d+\.\d+)", block_text):
#             current_item_data["Purity"] = float(m_pr.group(1))
            
#         if m_sz := re.search(r"Seed Size:\s*([\w\.]+)", block_text):
#             current_item_data["SeedSize"] = m_sz.group(1)

#     flush_item()
    
#     # ... [Keep remainder: item_counter, discounts, return info] ...
#     item_counter = defaultdict(int)
#     discounts_by_item = defaultdict(list)
#     for item_num, amount in discount_amounts:
#         discounts_by_item[item_num].append(amount)

#     for item in line_items:
#         if item_num := item.get("VendorItemNumber"):
#             occurrence_idx = item_counter[item_num]
#             item_counter[item_num] += 1
#             if occurrence_idx < len(discounts_by_item.get(item_num, [])):
#                 item["TotalDiscount"] = discounts_by_item[item_num][occurrence_idx]
#             else:
#                 item["TotalDiscount"] = None
    
#     for item in line_items:
#         tp = item.get("TotalPrice") or 0.0
#         tu = item.get("TotalUpcharge") or 0.0
#         td = item.get("TotalDiscount") or 0.0
#         qty = item.get("TotalQuantity")
#         item["USD_Actual_Cost_$"] = round(((tp + tu - td) / qty), 4) if qty and qty > 0 else None

#     return line_items, extraction_info

# def extract_hm_clause_data_from_bytes(pdf_files: list[tuple[str, bytes]]) -> dict[str, list[dict]]:
#     if not pdf_files:
#         return {}

#     purity_data = extract_purity_analysis_reports_from_bytes(pdf_files)

#     grouped_results = {}
#     for filename, pdf_bytes in pdf_files:
#         if re.match(r"^[A-Z]\d{5}", os.path.basename(filename), re.IGNORECASE):
#             continue

#         try:
#             items, info = extract_hm_clause_invoice_data_from_bytes(pdf_bytes)
            
#             # --- LOGGING ---
#             po_number = items[0].get("PurchaseOrder") if items else None
#             log_processing_event(
#                 vendor='HM Clause',
#                 filename=filename,
#                 extraction_info=info,
#                 po_number=po_number
#             )
            
#             if items:
#                 enriched_items = enrich_invoice_items_with_purity(items, purity_data)
#                 grouped_results[filename] = enriched_items
#         except Exception as e:
#             print(f"Error processing invoice {filename}: {e}")
#             continue

#     return grouped_results

def find_best_hm_clause_package_description(vendor_desc: str, pkg_desc_list: list[str]) -> str:
    if not vendor_desc or not pkg_desc_list:
        return ""

    normalized_desc = vendor_desc.upper()
    candidate = ""

    m = re.search(r"(\d+)\s*(KS|MS)\b", normalized_desc)
    if m:
        qty = int(m.group(1))
        unit = m.group(2)

        if unit == "KS":
            seed_count = qty * 1000
            candidate = f"{seed_count:,} SEEDS"
        elif unit == "MS":
            seed_count = qty * 1000000
            candidate = f"{seed_count:,} SEEDS"

        if candidate in pkg_desc_list:
            return candidate

    matches = get_close_matches(normalized_desc, pkg_desc_list, n=1, cutoff=0.6)
    return matches[0] if matches else ""