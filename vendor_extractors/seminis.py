# seminis.py
import os
import json
import fitz  # PyMuPDF
import re
from typing import List, Dict, Tuple
import requests
from difflib import get_close_matches
import time
from collections import defaultdict
item_usage_counter = defaultdict(int)

AZURE_ENDPOINT = os.getenv("AZURE_ENDPOINT")
AZURE_KEY = os.getenv("AZURE_KEY")

def extract_text_with_azure_ocr(pdf_path: str) -> List[str]:
    """Sends a PDF to Azure Form Recognizer for OCR and returns extracted lines."""
    with open(pdf_path, "rb") as f:
        headers = {
            "Ocp-Apim-Subscription-Key": AZURE_KEY,
            "Content-Type": "application/pdf"
        }
        response = requests.post(
            f"{AZURE_ENDPOINT}formrecognizer/documentModels/prebuilt-layout:analyze?api-version=2023-07-31",
            headers=headers,
            data=f
        )
    if response.status_code != 202:
        raise RuntimeError(f"OCR request failed: {response.text}")

    op_url = response.headers["Operation-Location"]

    for _ in range(30):
        time.sleep(1.5)
        result = requests.get(op_url, headers={"Ocp-Apim-Subscription-Key": AZURE_KEY}).json()
        if result.get("status") == "succeeded":
            lines = []
            for page in result["analyzeResult"]["pages"]:
                page_text = " ".join(line.get("content", "").strip() for line in page.get("lines", []) if line.get("content"))
                # Skip pages that are just legal notices
                if page_text.lower().startswith("notice to purchaser") or "notice to purchaser" in page_text.lower():
                    continue

                for line in page["lines"]:
                    txt = line.get("content", "").strip()
                    if txt:
                        lines.append(txt)
                lines.append("--- PAGE BREAK ---")
            return lines
        if result.get("status") == "failed":
            raise RuntimeError("OCR analysis failed")
    raise TimeoutError("OCR timed out")

def extract_text_with_fallback(pdf_path: str) -> List[str]:
    """Extracts text from a PDF, using PyMuPDF first and falling back to Azure OCR if needed."""
    lines = []
    azure_lines = None
    doc = fitz.open(pdf_path)

    for page in doc:
        page_text = page.get_text()
        if "notice to purchaser" in page_text.lower():
            continue
        if page_text.strip():
            for ln in page_text.splitlines():
                ln = ln.strip()
                if ln:
                    lines.append(ln)
        else: # If a page is blank (likely a scanned image), use Azure OCR
            if azure_lines is None:
                azure_lines = extract_text_with_azure_ocr(pdf_path)
            lines.extend(azure_lines)
    return lines

def extract_seminis_analysis_data(folder: str) -> Dict[str, Dict]:
    """Extracts data from Seminis analysis report PDFs in the same folder."""
    analysis = {}
    for fn in os.listdir(folder):
        if not fn.lower().endswith(".pdf") or "L_" not in fn:
            continue
        path = os.path.join(folder, fn)
        text = "".join(page.get_text() for page in fitz.open(path))
        norm = re.sub(r"\s{2,}", " ", text.replace("\n", " ").replace("\r", " "))

        m_lot = re.search(r"Lot Number[:\s]+(\d{9})", norm)
        if not m_lot:
            continue
        lot = m_lot.group(1)

        analysis[lot] = {
            "PureSeed": float(re.search(r"Pure Seed\s*%\s*([\d.]+)", norm).group(1)) if re.search(r"Pure Seed\s*%\s*([\d.]+)", norm) else None,
            "InertMatter": float(re.search(r"Inert Matter\s*%\s*([\d.]+)", norm).group(1)) if re.search(r"Inert Matter\s*%\s*([\d.]+)", norm) else None,
            "Germ": int(float(re.search(r"Germination\s*%\s*([\d.]+)", norm).group(1))) if re.search(r"Germination\s*%\s*([\d.]+)", norm) else None,
            "GermDate": re.search(r"Date Tested\s*([\d/]{8,10})", norm).group(1) if re.search(r"Date Tested\s*([\d/]{8,10})", norm) else None,
        }
    return analysis

def extract_seminis_packing_data(folder: str) -> Dict[str, Dict]:
    """Extracts data from Seminis packing slip PDFs in the same folder."""
    packing_data = {}
    for fn in os.listdir(folder):
        if not fn.lower().endswith(".pdf") or "packing" not in fn.lower():
            continue
        path = os.path.join(folder, fn)
        lines = [ln.strip() for ln in fitz.open(path).get_page_text(0).split("\n") if ln.strip()]
        for i, line in enumerate(lines):
            if "TRT:" not in line:
                continue
            block = lines[i:i+12]
            joined = " ".join(block)

            seed_count = vendor_batch = germ = germ_date = None
            if (m := re.search(r"\d+\s*/\s*(\d+)", joined)): seed_count = int(m.group(1))
            if (m := re.search(r"\d{2}/\d{2}/\d{4}.*?\b(\d{10})\b", joined)): vendor_batch = m.group(1)
            if (m := re.search(r"(\d{2,3})\s+(?=\d{2}/\d{2}/\d{4})", joined)): germ = int(m.group(1))
            if (m := re.search(r"(\d{2}/\d{2}/\d{4})", joined)): germ_date = m.group(1)

            if vendor_batch:
                packing_data[vendor_batch] = {
                    "SeedCountPerLB": seed_count,
                    "PackingGerm": germ,
                    "PackingGermDate": germ_date
                }
            print(f"Extracted packing data for batch {vendor_batch}: {packing_data[vendor_batch]}")
    return packing_data

def extract_seminis_invoice_data(pdf_path: str) -> List[Dict]:
    """
    Main function to extract all item data from a Seminis invoice PDF.
    It aligns the output with the standard data model for the Flask web application.
    """
    folder = os.path.dirname(pdf_path)
    lines = extract_text_with_fallback(pdf_path)

    # Pre-load data from supplementary analysis and packing slip PDFs
    analysis_map = extract_seminis_analysis_data(folder)
    packing_map = extract_seminis_packing_data(folder)

    # Extract top-level invoice data that applies to all items
    text_content = "\n".join(lines)
    #print(text_content)  # Debugging output
    vendor_invoice_no = po_number = None
    
    if m := re.search(r"Invoice Number\s*:\s*(\S+)", text_content):
        vendor_invoice_no = m.group(1)
    if m := re.search(r"PO #\s*:\s*(\S+)", text_content):
        po_number = f"PO-{m.group(1)}"

    items = []
    # Find all line items, which are identified by a "TRT:" line
    trt_indices = [i for i, l in enumerate(lines) if "TRT:" in l]
    amount_idx = next((i for i, l in enumerate(lines) if "Amount" in l), 0)
    total_item_indices = [i for i, l in enumerate(lines) if "Total Item" in l]

    # Define the start of each item's text block
    block_starts = [amount_idx + 1] + [total_item_indices[i] + 3 for i in range(min(len(trt_indices) - 1, len(total_item_indices)))]

    for idx, trt_idx in enumerate(trt_indices):
        # Extract item description
        desc_lines = lines[block_starts[idx]:trt_idx]
        filtered = [l for l in desc_lines if not any(x in l for x in ["Invoice Number", "PO #", "Sales Order", "Delivery Nr", "Order Date", "Ship Date", "/", "Page"])]
        vendor_item_description = " ".join(filtered).strip()

        # Parse Treatment and Quantity
        treatment_line = lines[trt_idx].strip()
        treatment_desc = re.sub(r"TRT:\s*", "", treatment_line).strip()

        # Extract other details from the lines following the treatment
        package = vendor_lot = origin_country = vendor_batch = total_price = None
        total_quantity = None
        
        for j in range(trt_idx + 1, min(trt_idx + 15, len(lines))):
            line = lines[j]
            if not package and (m := re.search(r"\d+\s+MK\s+\w+", line)): package = m.group().strip()
            if not vendor_lot and (m := re.search(r"\b\d{9}(?:/\d{2})?\b", line)): vendor_lot = m.group()
            #if not vendor_batch and (m := re.search(r"\b\d{10}\b", line)): vendor_batch = m.group()
            if not vendor_batch and (m := re.search(r"\b(\d{10})\b", line)):
                vendor_batch = m.group(1)
                # Check the next line for TotalQuantity
                if j + 1 < len(lines):
                    next_line = lines[j+1].strip()
                    # Pattern for 28,960 MK or 7,920
                    if (m_qty := re.search(r'([\d,]+)\s*(?:MK)?', next_line)):
                        try:
                            total_quantity = int(m_qty.group(1).replace(",", ""))
                        except ValueError:
                            total_quantity = None
            if not origin_country:
                cc_match = re.findall(r"\b[A-Z]{2}\b", line)
                filtered_cc = [c for c in cc_match if c != "MK"]
                if filtered_cc: origin_country = filtered_cc[0]
            if line == "Total Item":
                for k in range(j + 1, min(j + 4, len(lines))):
                    if (m := re.search(r"[\d,]+\.\d{2}", lines[k+1])):
                        total_price = float(m.group().replace(",", ""))
                        break
                break

        lot_key = vendor_lot.split("/")[0] if vendor_lot else None

        # Build the standardized item dictionary
        item = {
            "VendorInvoiceNo": vendor_invoice_no,
            "PurchaseOrder": po_number,
            "VendorLot": vendor_lot,
            "VendorItemDescription": f"{vendor_item_description} {package}".strip(),
            "VendorBatch": vendor_batch,
            "OriginCountry": origin_country,
            "TotalPrice": total_price,
            "TotalQuantity": total_quantity,
            "USD_Actual_Cost_$": None, # Calculated below
            "Treatment": treatment_desc,
            "Purity": None, # From analysis map
            "InertMatter": None, # From analysis map
            "Germ": None, # From analysis map
            "GermDate": None, # From analysis map
            "SeedCountPerLB": None, # From packing map
            "PackingGerm": None, # From packing map
            "PackingGermDate": None, # From packing map
        }

        # Enrich item with data from analysis and packing slips
        if lot_key:
            analysis_data = analysis_map.get(lot_key, {})
            item.update(analysis_data)
            # Map PureSeed to Purity for consistency with the other vendor module
            if analysis_data.get("PureSeed"):
                item["Purity"] = analysis_data["PureSeed"]

        if vendor_batch:
            packing_data = packing_map.get(vendor_batch, {})
            item.update(packing_data)

        # Final cost calculation
        tp = item.get("TotalPrice") or 0.0
        qty = item.get("TotalQuantity")
        item["USD_Actual_Cost_$"] = round((tp / qty), 4) if qty and qty > 0 else None

        items.append(item)

    return items

def find_best_seminis_package_description(vendor_desc: str, pkg_desc_list: list[str]) -> str:
    """
    Given a Seminis vendor description, find the best match from the BC Package Descriptions.
    Logic: 80 MK -> 80,000 SEEDS.
    """
    if not vendor_desc or not pkg_desc_list:
        return ""

    normalized_desc = vendor_desc.upper()
    candidate = ""

    # Search for patterns like "80 MK"
    m = re.search(r"(\d+)\s*(MK)\b", normalized_desc)
    if m:
        qty = int(m.group(1))
        unit = m.group(2)

        if unit == "MK":
            seed_count = qty * 1000
            candidate = f"{seed_count:,} SEEDS"

        # Check if the generated candidate exists in the BC list
        if candidate in pkg_desc_list:
            return candidate

    # Fallback to fuzzy matching against all package descriptions
    matches = get_close_matches(normalized_desc, pkg_desc_list, n=1, cutoff=0.6)
    return matches[0] if matches else ""
