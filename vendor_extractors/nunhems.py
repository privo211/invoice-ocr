# vendor_extractors/nunhems.py
import os
import re
import json
import fitz  # PyMuPDF
import requests
import time
import pycountry
from datetime import datetime
from typing import List, Dict, Any
from difflib import get_close_matches

# Azure credentials from environment variables, consistent with other modules
AZURE_ENDPOINT = os.getenv("AZURE_ENDPOINT")
AZURE_KEY = os.getenv("AZURE_KEY")

def convert_to_alpha2(country_value: str) -> str:
    """
    Converts a country representation (full name, alpha-3, or alpha-2) to ISO Alpha-2 code.
    Examples:
        "UNITED STATES" -> "US"
        "USA" -> "US"
        "US" -> "US"
    Returns original value if not found.
    """
    if not country_value:
        return ""
    
    country_value = country_value.strip()
    
    # If already 2 letters, assume it's Alpha-2 and return uppercase
    if len(country_value) == 2:
        return country_value.upper()
    
    try:
        # Try lookup (works for names, alpha-2, alpha-3)
        country = pycountry.countries.lookup(country_value)
        return country.alpha_2
    except LookupError:
        return country_value  # fallback


def extract_text_with_azure_ocr(pdf_path: str) -> List[str]:
    """Sends a PDF to Azure Form Recognizer for OCR and returns extracted lines."""
    if not AZURE_ENDPOINT or not AZURE_KEY:
        raise ValueError("Azure OCR credentials (AZURE_ENDPOINT, AZURE_KEY) are not set in environment variables.")
    
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
                for ln in page["lines"]:
                    txt = ln.get("content", "").strip()
                    if txt:
                        lines.append(txt)
            return lines
        if result.get("status") == "failed":
            raise RuntimeError("OCR analysis failed")
    raise TimeoutError("OCR timed out")

def extract_lines(pdf_path: str) -> List[str]:
    """
    Tries to read text natively via PyMuPDF; if no searchable text is found,
    it falls back to Azure OCR. This is the standard text extraction approach.
    """
    lines = []
    try:
        doc = fitz.open(pdf_path)
        for page in doc:
            txt = page.get_text()
            if txt and txt.strip():
                lines.extend([l.strip() for l in txt.split("\n") if l.strip()])
        doc.close()
        if lines:
            return lines
    except Exception:
        # If PyMuPDF fails for any reason, fall back to OCR
        pass
    
    # If no native text was found or PyMuPDF failed, use OCR
    return extract_text_with_azure_ocr(pdf_path)

def parse_percent(s: str):
    """Parses a string like '99.1%' or '100,0%' into a float."""
    s = str(s).strip()
    if s.endswith("%"):
        core = s[:-1].strip().replace(",", ".")
        try:
            return float(core)
        except ValueError:
            return core
    return s

def extract_nunhems_quality_data(folder: str) -> Dict[str, Dict]:
    """Extracts data from Nunhems Quality Certificate PDFs."""
    quality_map = {}
    for fn in os.listdir(folder):
        if not fn.lower().endswith(".pdf"):
            continue
        
        path = os.path.join(folder, fn)
        # We must use OCR for QC files as they are often image-based
        try:
            lines = extract_text_with_azure_ocr(path)
            if not any("Quality Certificate" in ln for ln in lines):
                continue
        except (RuntimeError, TimeoutError, ValueError):
            # Skip files that fail OCR or aren't QCs
            continue

        current_lot = None
        i = 0
        while i < len(lines):
            ln = lines[i]

            if re.match(r"Lot[/ ]*Batch number", ln, re.IGNORECASE) and i + 1 < len(lines):
                if m := re.search(r"(\d{11})", lines[i + 1]):
                    current_lot = m.group(1)
                    quality_map.setdefault(current_lot, {})
                i += 2
                continue

            if current_lot and re.match(r"Pure\s*seeds?", ln, re.IGNORECASE) and i + 5 < len(lines):
                vals = lines[i+3 : i+6]
                quality_map[current_lot]["PureSeeds"] = parse_percent(vals[0])
                quality_map[current_lot]["Inert"] = parse_percent(vals[1])
                #quality_map[current_lot]["OtherSeeds"] = parse_percent(vals[2])
                i += 6
                continue

            if current_lot and (m := re.match(r"Date:\s*([A-Za-z]+\s+\d{1,2},\s*\d{4})", ln)):
                dt = datetime.strptime(m.group(1), "%B %d, %Y")
                quality_map[current_lot]["GrowerGermDate"] = dt.strftime("%m/%d/%Y")
                
            if current_lot and "Normal seedlings" in ln and i + 1 < len(lines):
                # Look ahead up to 5 lines to find the first percentage
                for j in range(i+1, min(i+6, len(lines))):
                    percent_match = re.search(r"(\d+)%", lines[j])
                    if percent_match:
                        quality_map[current_lot]["GrowerGerm"] = int(percent_match.group(1))
                        break
            
            i += 1
    return quality_map

def extract_nunhems_germ_data(folder: str) -> Dict[str, Dict]:
    """Parses Nunhems Germ Confirmation PDFs for germination data based on document text."""
    germ_data = {}
    for fn in os.listdir(folder):
        if not fn.lower().endswith(".pdf"):
            continue

        path = os.path.join(folder, fn)
        lines = extract_lines(path)

        # Check if this is a germination confirmation document by content
        if not any("Test Date Confirmation" in ln for ln in lines):
            continue

        for i, line in enumerate(lines):
            if re.match(r"^\d{11}$", line):  # Lot number is 11 digits
                lot = line
                germ = germ_date = None

                # Extract Germ %
                if i + 1 < len(lines) and (g := re.search(r"(\d+)%", lines[i + 1])):
                    germ = int(g.group(1))
                    
                # Extract Germ Date (Normalize MM/YYYY → MM/DD/YYYY)
                if i + 2 < len(lines) and (d := re.search(r"(\d{1,2})/(\d{4})", lines[i + 2])):
                    month, year = d.groups()
                    germ_date = f"{int(month):02d}/01/{year}"  # Ensure two-digit month


                germ_data[lot] = {"Germ": germ, "GermDate": germ_date}
    return germ_data


# def extract_nunhems_packing_data(folder: str) -> Dict[str, Dict]:
#     """Parses Nunhems Packing List PDFs for seed count data based on content."""
#     packing_data = {}
#     for fn in os.listdir(folder):
#         if not fn.lower().endswith(".pdf"):
#             continue

#         path = os.path.join(folder, fn)
#         lines = extract_lines(path)

#         # Check if this is a packing list by keywords
#         if not any("Packing List" in ln or "S/C" in ln for ln in lines):
#             continue

#         current_seed_count = None
#         for line in lines:
#             line = line.strip()
#             print(line)  # Debugging output

#             # Detect Seed Count per LB
#             if "S/C" in line and (m := re.search(r"(\d{1,3}(?:,\d{3})*)\s+LBS", line)):
#                 current_seed_count = f"{m.group(1)}/LB"

#             # Detect Lot Number after seed count
#             elif current_seed_count and (m := re.search(r"\b(\d{11})\b", line)):
#                 lot = m.group(0)
#                 if lot not in packing_data:
#                     packing_data[lot] = {"SeedCount": current_seed_count}
#                 current_seed_count = None
#     return packing_data

def extract_nunhems_packing_data(folder: str) -> Dict[str, Dict]:
    """Extracts Lot and SeedCount from Nunhems Packing Lists."""
    packing_data = {}

    for fn in os.listdir(folder):
        if not fn.lower().endswith(".pdf"):
            continue

        path = os.path.join(folder, fn)
        lines = extract_lines(path)

        # Confirm this is a packing list
        if not any("PACKING LIST" in ln.upper() for ln in lines):
            continue

        for i, line in enumerate(lines):
            # Find Lot Number
            lot_match = re.search(r"\b(\d{11})\b", line)
            if lot_match:
                lot = lot_match.group(1)

                # Look ahead for up to 5 lines for S/C info
                sc_line = ""
                for j in range(i, min(i + 6, len(lines))):
                    if "S/C" in lines[j]:
                        sc_line = lines[j]
                        break

                if sc_line:
                    # Extract last numeric group before LBS
                    match = re.findall(r"([\d,]+)\s*LBS", sc_line)
                    if match:
                        seed_count = int(match[-1].replace(",", ""))
                        packing_data[lot] = {"SeedCount": seed_count}

    return packing_data

# def extract_nunhems_germ_data(folder: str) -> Dict[str, Dict]:
#     """Parses BASF Germ Confirmation PDFs for germ data."""
#     germ_data = {}
#     for fn in os.listdir(folder):
#         if not fn.lower().endswith(".pdf") or "basf" not in fn.lower():
#             continue

#         path = os.path.join(folder, fn)
#         lines = extract_lines(path)

#         for i, line in enumerate(lines):
#             if re.match(r"^\d{11}$", line):
#                 lot = line
#                 germ = germ_date = None
#                 if i + 1 < len(lines) and (g := re.search(r"(\d+)%", lines[i + 1])):
#                     germ = int(g.group(1))
#                 if i + 2 < len(lines) and (d := re.search(r"\d{1,2}/\d{4}", lines[i + 2])):
#                     germ_date = d.group(0)
                
#                 germ_data[lot] = {"Germ": germ, "GermDate": germ_date}
#     return germ_data

# def extract_nunhems_packing_data(folder: str) -> Dict[str, Dict]:
#     """Parses Nunhems packing lists for seed count data."""
#     packing_data = {}
#     for fn in os.listdir(folder):
#         if not fn.lower().endswith(".pdf") or "packing" not in fn.lower():
#             continue

#         path = os.path.join(folder, fn)
#         lines = extract_lines(path)
#         current_seed_count = None

#         for line in lines:
#             line = line.strip()
#             print(line)
#             if "S/C" in line and (m := re.search(r"(\d{1,3}(?:,\d{3})*)\s+LBS", line)):
#                 current_seed_count = f"{m.group(1)}/LB"
#             elif current_seed_count and (m := re.search(r"\b(\d{11})\b", line)):
#                 lot = m.group(0)
#                 if lot not in packing_data:
#                     packing_data[lot] = {"SeedCount": current_seed_count}
#                 current_seed_count = None
#     return packing_data

def extract_nunhems_invoice_data(pdf_path: str) -> List[Dict]:
    """
    Main function to extract all item data from a Nunhems invoice PDF,
    aligning the output with the standard data model for the application.
    """
    folder = os.path.dirname(pdf_path)
    lines = extract_lines(pdf_path)
    
    text_content = "\n".join(lines)
    

    # Pre-load data from supplementary PDFs in the same folder
    quality_map = extract_nunhems_quality_data(folder)
    germ_map = extract_nunhems_germ_data(folder)
    packing_map = extract_nunhems_packing_data(folder)

    # Extract top-level data applicable to all items
    vendor_invoice_no = po_number = None
    m = re.search(r"Invoice\s+Number[:\s]+([\s\S]*?)\b(\d{9})\b", text_content, re.IGNORECASE)
    if m and m.group(2):
        vendor_invoice_no = m.group(2)

    m = re.search(r"Customer\s+P\.?O\.?\s+Number[:\s]+([\s\S]*?)\b(\d{5})\b", text_content, re.IGNORECASE)
    if m and m.group(2):
        po_number = f"PO-{m.group(2)}"

    items = []
    # Find all line items, identified by a line containing "SDS" (seeds)
    sds_indices = [i for i, l in enumerate(lines) if re.search(r"\d{1,3}(?:,\d{3})*\s+SDS", l)]

    for idx in sds_indices:
        # --- Extract Details for Each Item ---
        sds_line = lines[idx]
        sds_match = re.search(r"(\d{1,3}(?:,\d{3})*)\s+SDS", sds_line)
        
        # Description (built from surrounding lines)
        part1 = lines[idx + 1] if idx + 1 < len(lines) else ""
        part2 = lines[idx - 1] if idx - 1 >= 0 else ""
        part3 = sds_match.group(0).strip() if sds_match else ""
        vendor_item_description = f"{part1} {part2} {part3}".strip()

        # Treatment (line after description part 1)
        treatment = lines[idx + 2].strip() if idx + 2 < len(lines) else None

        # Vendor Lot, Origin Country, and Price (search in a block after the item)
        vendor_lot = origin_country = net_price = None
        lot_line_idx = -1
        for i in range(idx, min(len(lines), idx + 30)):
            if "Lot Number:" in lines[i] and i + 1 < len(lines):
                if m := re.search(r"\b(\d{11})\b", lines[i+1]):
                    vendor_lot = m.group(0)
                    lot_line_idx = i
                    break
        
        if lot_line_idx != -1:
            for i in range(lot_line_idx, min(len(lines), lot_line_idx + 20)):
                if "ORIGIN" in lines[i]:
                    if len(split := lines[i].rsplit("|", 1)) == 2:
                        origin_country_name = split[-1].replace("ORIGIN", "").strip()
                        origin_country = convert_to_alpha2(origin_country_name)
                    break
                
        net_price = None
        for i, line in enumerate(lines):
            if "Net price" in line:
                # Look ahead for the next numeric value
                for j in range(i+1, min(i+4, len(lines))):
                    if re.search(r"[\d,]+\.\d{2}", lines[j]):
                        net_price = float(re.search(r"[\d,]+\.\d{2}", lines[j]).group(0).replace(",", ""))
                        break
                break
            
        total_qty = None
        for i, line in enumerate(lines):
            if "Net price" in line:
                # Look behind for up to 3 previous lines
                for j in range(i-1, max(i-4, -1), -1):  # go backwards
                    qty_match = re.search(r"([\d,]+\.\d{2})", lines[j])
                    if qty_match:
                        # Convert "3,000.00" → 3000
                        total_qty = int(float(qty_match.group(1).replace(",", "")))
                        break
                break

        # --- Assemble Standardized Dictionary ---
        quality_info = quality_map.get(vendor_lot, {})
        germ_info = germ_map.get(vendor_lot, {})
        packing_info = packing_map.get(vendor_lot, {})
        
        qty = int(sds_match.group(1).replace(",", "")) if sds_match else None
        cost = round((net_price / total_qty), 4) if net_price and total_qty and total_qty > 0 else None

        item = {
            "VendorInvoiceNo": vendor_invoice_no,
            "PurchaseOrder": po_number,
            "VendorLot": vendor_lot,
            "VendorItemDescription": vendor_item_description,
            "OriginCountry": origin_country,
            "TotalPrice": net_price,
            "TotalQuantity": total_qty,
            "USD_Actual_Cost_$": cost,
            "Treatment": treatment,
            "Purity": quality_info.get("PureSeeds"),
            "InertMatter": quality_info.get("Inert"),
            "Germ": germ_info.get("Germ"),
            "GermDate": germ_info.get("GermDate"),
            "SeedCountPerLB": packing_info.get("SeedCount"),
            "GrowerGerm": quality_info.get("GrowerGerm"),
            "GrowerGermDate": quality_info.get("GrowerGermDate"),
        }
        items.append(item)

    return items

def find_best_nunhems_package_description(vendor_desc: str, pkg_desc_list: list[str]) -> str:
    """
    Given a Nunhems vendor description, find the best match from BC Package Descriptions.
    Logic: "10,000 SDS" -> "10,000 SEEDS".
    """
    if not vendor_desc or not pkg_desc_list:
        return ""

    # Search for patterns like "10,000 SDS"
    if m := re.search(r"([\d,]+)\s+SDS", vendor_desc):
        qty_str = m.group(1).replace(",", "")
        candidate = f"{int(qty_str):,} SEEDS"
        # Check if the generated candidate exists in the BC list
        if candidate in pkg_desc_list:
            return candidate

    # Fallback to fuzzy matching if direct logic fails
    matches = get_close_matches(vendor_desc, pkg_desc_list, n=1, cutoff=0.6)
    return matches[0] if matches else ""


