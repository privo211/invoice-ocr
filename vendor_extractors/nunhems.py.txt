# vendor_extractors/nunhems.py
import math
import os
import re
import json
import fitz  # PyMuPDF
import requests
import time
import pycountry
from datetime import datetime
from difflib import get_close_matches
from typing import List, Dict, Any, Union, Tuple
from db_logger import log_processing_event

# Azure credentials from environment variables
AZURE_ENDPOINT = os.getenv("AZURE_ENDPOINT")
AZURE_KEY = os.getenv("AZURE_KEY")

def convert_to_alpha2(country_value: str) -> str:
    """Converts a country name or code to the ISO Alpha-2 format."""
    if not country_value: return ""
    country_value = country_value.strip()
    if len(country_value) == 2: return country_value.upper()
    try:
        return pycountry.countries.lookup(country_value).alpha_2
    except LookupError:
        return country_value

def _extract_text_with_azure_ocr(pdf_content: bytes) -> List[str]:
    """Sends PDF content (bytes) to Azure Form Recognizer for OCR."""
    if not AZURE_ENDPOINT or not AZURE_KEY:
        raise ValueError("Azure OCR credentials are not set in environment variables.")
    
    headers = {"Ocp-Apim-Subscription-Key": AZURE_KEY, "Content-Type": "application/pdf"}
    response = requests.post(
        f"{AZURE_ENDPOINT}formrecognizer/documentModels/prebuilt-layout:analyze?api-version=2023-07-31",
        headers=headers, data=pdf_content
    )
    if response.status_code != 202:
        raise RuntimeError(f"OCR request failed: {response.text}")

    op_url = response.headers["Operation-Location"]
    for _ in range(30):
        time.sleep(1.5)
        result = requests.get(op_url, headers={"Ocp-Apim-Subscription-Key": AZURE_KEY}).json()
        if result.get("status") == "succeeded":
            return [ln.get("content", "").strip() for page in result["analyzeResult"]["pages"] for ln in page["lines"] if ln.get("content", "").strip()]
        if result.get("status") == "failed":
            raise RuntimeError("OCR analysis failed")
    raise TimeoutError("OCR timed out")

def _extract_lines(source: Union[bytes, str]) -> List[str]:
    """Extracts text lines from a PDF source (bytes or path), with OCR fallback."""
    try:
        doc = fitz.open(stream=source, filetype="pdf") if isinstance(source, bytes) else fitz.open(source)
        if any(page.get_text().strip() for page in doc):
            lines = [l.strip() for page in doc for l in page.get_text().split("\n") if l.strip()]
            doc.close()
            return lines
        doc.close()
    except Exception:
        pass  # Fall through to OCR if PyMuPDF fails

    pdf_bytes = source if isinstance(source, bytes) else open(source, "rb").read()
    return _extract_text_with_azure_ocr(pdf_bytes)

def _extract_lines_with_info(source: bytes) -> Tuple[List[str], Dict]:
    """Extracts text lines and metadata (method, page_count)."""
    info = {'method': 'PyMuPDF', 'page_count': 0}
    try:
        doc = fitz.open(stream=source, filetype="pdf")
        info['page_count'] = doc.page_count
        if any(page.get_text().strip() for page in doc):
            lines = [l.strip() for page in doc for l in page.get_text().split("\n") if l.strip()]
            doc.close()
            return lines, info
        doc.close()
    except Exception:
        pass

    # Fallback to OCR
    info['method'] = 'Azure OCR'
    lines = _extract_text_with_azure_ocr(source)
    return lines, info

def _extract_nunhems_quality_data(pdf_files: List[Tuple[str, bytes]]) -> Dict[str, Dict]:
    """Extracts data from Nunhems Quality Certificate PDFs."""
    quality_map = {}
    for filename, pdf_bytes in pdf_files:
        lines = _extract_lines(pdf_bytes)
        if not any("Quality Certificate" in ln for ln in lines):
            continue

        current_lot = None
        i = 0
        while i < len(lines):
            ln = lines[i]
            
            # Identify Lot (Check current line, next line, and 2 lines down)
            found_lot = None
            if re.search(r"Lot[/ ]*Batch number", ln, re.IGNORECASE):
                if m := re.search(r"(\d{11})", ln): found_lot = m.group(1)
                elif i + 1 < len(lines) and (m := re.search(r"(\d{11})", lines[i + 1])): found_lot = m.group(1)
                elif i + 2 < len(lines) and (m := re.search(r"(\d{11})", lines[i + 2])): found_lot = m.group(1)
            
            if found_lot:
                current_lot = found_lot
                quality_map.setdefault(current_lot, {})
            
            if current_lot:
                # Stop if we hit a new lot header unexpectedly
                if "Lot/Batch" in ln and not found_lot: 
                    current_lot = None
                    i += 1
                    continue

                # Purity
                if re.match(r"Pure\s*seeds?", ln, re.IGNORECASE):
                    found_floats = []
                    # Scan next 10 lines.
                    for j in range(i+1, min(i+10, len(lines))):
                        # Only break if we hit a new section unrelated to the table columns
                        if "Lot/Batch" in lines[j] or "Remarks:" in lines[j] or "SEED COUNT" in lines[j]: break
                        
                        matches = re.findall(r"(\d{1,3}(?:\.\d+)?)%?", lines[j])
                        for m in matches:
                            try: found_floats.append(float(m))
                            except: pass
                    
                    if len(found_floats) >= 1:
                        pure = found_floats[0]
                        # If a second number exists, assume it's inert; otherwise default 0.0
                        inert = found_floats[1] if len(found_floats) > 1 else 0.0
                        
                        # Apply 100% logic
                        if pure == 100.0:
                            pure = 99.99
                            inert = 0.01
                        
                        quality_map[current_lot]["PureSeeds"] = pure
                        quality_map[current_lot]["Inert"] = inert

                # Germ Date
                if m := re.match(r"Date:\s*([A-Za-z]+\s+\d{1,2},\s*\d{4})", ln):
                    dt = datetime.strptime(m.group(1), "%B %d, %Y")
                    quality_map[current_lot]["GrowerGermDate"] = dt.strftime("%m/%d/%Y")

                # Germ %
                if "Normal seedlings" in ln:
                    for j in range(i+1, min(i+6, len(lines))):
                        if "Lot/Batch" in lines[j]: break
                        if percent_match := re.search(r"(\d+)%", lines[j]):
                            quality_map[current_lot]["GrowerGerm"] = int(percent_match.group(1))
                            break
            i += 1
    return quality_map

def _extract_nunhems_germ_data(pdf_files: List[Tuple[str, bytes]]) -> Dict[str, Dict]:
    """Parses Nunhems Germ Confirmation PDFs."""
    germ_data = {}
    for filename, pdf_bytes in pdf_files:
        lines = _extract_lines(pdf_bytes)
        if not any("Test Date Confirmation" in ln for ln in lines):
            continue
        for i, line in enumerate(lines):
            if m := re.search(r"\b(\d{11})\b", line):
                lot = m.group(1)
                context = " ".join(lines[i:i+3])
                
                germ, germ_date = None, None
                if g := re.search(r"(\d+)%", context): germ = int(g.group(1))
                if d := re.search(r"(\d{1,2})/(\d{4})", context):
                    month, year = d.groups()
                    germ_date = f"{int(month):02d}/01/{year}"
                
                if germ or germ_date:
                     germ_data[lot] = {"Germ": germ, "GermDate": germ_date}
    return germ_data

def _extract_nunhems_packing_data(pdf_files: List[Tuple[str, bytes]]) -> Dict[str, Dict]:
    """Extracts Lot and SeedCount from Nunhems Packing Lists (Backward & Forward scan)."""
    packing_data = {}
    for filename, pdf_bytes in pdf_files:
        lines = _extract_lines(pdf_bytes)
        if not any("PACKING LIST" in ln.upper() for ln in lines):
            continue
        for i, line in enumerate(lines):
            # Check for Lot Number
            if lot_match := re.search(r"\b(\d{11})\b", line):
                lot = lot_match.group(1)
                
                found_sc = False
                # 1. Look Backward (up to 10 lines)
                start_back = max(0, i - 10)
                for j in range(i, start_back, -1):
                    if j < i and re.search(r"\b\d{11}\b", lines[j]): break # Stop if we hit previous lot
                    
                    if "S/C" in lines[j]:
                        if match := re.search(r"([\d,]+)\s*LBS", lines[j]):
                            packing_data[lot] = {"SeedCount": int(match.group(1).replace(",", ""))}
                            found_sc = True
                            break
                        if match := re.search(r"S/C\s*([\d,]+)", lines[j]):
                             nums = re.findall(r"([\d,]+)", lines[j].split("S/C")[1])
                             if nums:
                                 packing_data[lot] = {"SeedCount": int(nums[-1].replace(",", ""))}
                                 found_sc = True
                                 break
                
                if found_sc: continue

                # 2. Look Forward (up to 12 lines)
                for j in range(i, min(i + 12, len(lines))):
                    if j > i and re.search(r"\b\d{11}\b", lines[j]): break # Stop at next lot

                    if "S/C" in lines[j]:
                        if match := re.search(r"([\d,]+)\s*LBS", lines[j]):
                            packing_data[lot] = {"SeedCount": int(match.group(1).replace(",", ""))}
                            break
                        if match := re.search(r"S/C\s*([\d,]+)", lines[j]):
                             nums = re.findall(r"([\d,]+)", lines[j].split("S/C")[1])
                             if nums:
                                 packing_data[lot] = {"SeedCount": int(nums[-1].replace(",", ""))}
                                 break
    return packing_data

def _process_single_nunhems_invoice(lines: List[str], quality_map: dict, germ_map: dict, packing_map: dict, pkg_desc_list: list[str]) -> List[Dict]:
    """Processes Nunhems invoice lines."""
    text_content = "\n".join(lines)
    vendor_invoice_no = None
    po_number = None
    
    if m := re.search(r"Invoice\s+Number[:\s]+([\s\S]*?)\b(\d{9})\b", text_content, re.IGNORECASE):
        vendor_invoice_no = m.group(2)
    if m := re.search(r"Customer\s+P\.?O\.?\s+Number[:\s]+([\s\S]*?)\b(\d{5})\b", text_content, re.IGNORECASE):
        po_number = f"PO-{m.group(2)}"

    # Check for Kamterter address to force specific package description
    is_kamterter = "KAMTERTER" in text_content.upper()

    items = []
    
    # Identify Item Headers by SDS quantity
    # Negative lookahead (?!\/LB) to ensure we don't pick up "SDS/LB" from lot lines
    item_header_indices = []
    for i, line in enumerate(lines):
        if re.search(r"(\d+[\d,]*)\s+SDS(?!\/LB)", line):
            item_header_indices.append(i)
    
    for idx, start_i in enumerate(item_header_indices):
        end_i = item_header_indices[idx+1] if idx + 1 < len(item_header_indices) else len(lines)
        
        block_lines = lines[start_i-2 : end_i] 
        block_text = "\n".join(block_lines)
        
        # Packaging Context from Header (e.g. 6,000,000 SDS)
        sds_line = lines[start_i]
        packaging_context_match = re.search(r"([\d,]+)\s+SDS", sds_line)
        packaging_context_str = f"{packaging_context_match.group(1)} SDS" if packaging_context_match else ""

        # Description
        desc_part1 = lines[start_i - 1].strip() if start_i > 0 else ""
        desc_part2 = lines[start_i + 1].strip() if start_i + 1 < len(lines) else ""
        if re.match(r"^\d+$", desc_part1): desc_part1 = ""
        if re.match(r"^\d+$", desc_part2): desc_part2 = ""
        
        vendor_item_description = f"{desc_part1} {desc_part2} {sds_line}".strip()
        vendor_item_description = re.sub(r"\d+\s+BUCKET.*?SDS", packaging_context_str, vendor_item_description)
        vendor_item_description = re.sub(r"\s+", " ", vendor_item_description).strip()

        # Treatment
        treatment = "Untreated"
        if "TREATED" in block_text.upper() or "THIRAM" in block_text.upper():
            for l in block_lines[:10]:
                if "THIRAM" in l.upper() or "MEFENOXAM" in l.upper():
                    treatment = l.strip()
                    break

        # --- PRICE & QTY EXTRACTION LOGIC (Merged) ---
        unit_price = 0.0
        
        # 1. Borrowed Logic: Specific pattern "Qty ... Net price ... Price"
        # Matches: "180,000.00 Net price 666.07"
        price_qty_pattern = re.compile(r"([\d,]+(?:\.\d{2})?)\s+Net price\s+([\d,]+\.\d{2})", re.IGNORECASE)
        pq_match = price_qty_pattern.search(block_text)
        
        if pq_match:
             # We use the price captured here as the definitive Unit Price (Net Price)
             price_str = pq_match.group(2)
             unit_price = float(price_str.replace(",", ""))
        else:
             # 2. Fallback: Search for "Net price" label explicitly
             net_price_match = re.search(r"Net price\s+.*?(\d{1,3}(?:,\d{3})*\.\d{2})", block_text, re.IGNORECASE | re.DOTALL)
             if net_price_match:
                 unit_price = float(net_price_match.group(1).replace(",", ""))
             else:
                 # 3. Last Resort: Search for "Price ... /TH"
                 price_match_th = re.search(r"(\d{1,3}(?:,\d{3})*\.\d{2})\s+.*?(?:1000|TH|M)", block_text, re.DOTALL)
                 if price_match_th:
                     unit_price = float(price_match_th.group(1).replace(",", ""))
        
        # Extract Lots
        for l_line in block_lines:
            lot_match = re.search(r"(\d{11})\s*\|\s*([\d,]+)\s*\|", l_line)
            
            if lot_match:
                vendor_lot = lot_match.group(1)
                lot_qty = int(float(lot_match.group(2).replace(",", "")))
                
                origin_country = None
                if origin_match := re.search(r"\|\s*([A-Za-z\s]+?)\s+ORIGIN", l_line, re.IGNORECASE):
                    origin_country = convert_to_alpha2(origin_match.group(1))
                
                quality_info = quality_map.get(vendor_lot, {})
                germ_info = germ_map.get(vendor_lot, {})
                packing_info = packing_map.get(vendor_lot, {})
                
                # Package Description Logic
                if is_kamterter:
                    package_description = "SUBCON BULK-MS"
                elif packaging_context_str:
                    pkg_candidate = packaging_context_str.replace("SDS", "SEEDS").replace(",", "")
                    try:
                        formatted_pkg = f"{int(pkg_candidate.split()[0]):,} SEEDS"
                        if formatted_pkg in pkg_desc_list:
                            package_description = formatted_pkg
                        else:
                            package_description = find_best_nunhems_package_description(packaging_context_str, pkg_desc_list)
                    except:
                        package_description = find_best_nunhems_package_description(packaging_context_str, pkg_desc_list)
                else:
                    package_description = find_best_nunhems_package_description(vendor_item_description, pkg_desc_list)

                # Cost & Price Calculations
                # Nunhems unit price is usually Per 1000 Seeds (TH)
                total_line_price = 0.0
                calculated_cost = 0.0
                if unit_price:
                    total_line_price = unit_price
                    calculated_cost = unit_price / lot_qty

                items.append({
                    "VendorInvoiceNo": vendor_invoice_no,
                    "PurchaseOrder": po_number,
                    "VendorLot": vendor_lot,
                    "VendorItemDescription": vendor_item_description,
                    "OriginCountry": origin_country,
                    "TotalPrice": round(total_line_price, 2),
                    "TotalQuantity": lot_qty,
                    "USD_Actual_Cost_$": "{:.5f}".format(calculated_cost),
                    "Treatment": treatment,
                    "Purity": quality_info.get("PureSeeds"),
                    "InertMatter": quality_info.get("Inert"),
                    "Germ": germ_info.get("Germ"),
                    "GermDate": germ_info.get("GermDate"),
                    "SeedCount": packing_info.get("SeedCount"),
                    "GrowerGerm": quality_info.get("GrowerGerm"),
                    "GrowerGermDate": quality_info.get("GrowerGermDate"),
                    "PackageDescription": package_description
                })

    return items

def extract_nunhems_data_from_bytes(pdf_files: List[Tuple[str, bytes]], pkg_desc_list: list[str]) -> Dict[str, List[Dict]]:
    """Main extraction function."""
    if not pdf_files: return {}

    quality_map = _extract_nunhems_quality_data(pdf_files)
    germ_map = _extract_nunhems_germ_data(pdf_files)
    packing_map = _extract_nunhems_packing_data(pdf_files)

    # grouped_results = {}
    # for filename, pdf_bytes in pdf_files:
    #     lines = _extract_lines(pdf_bytes)
    #     text_content = "\n".join(lines).upper()
    #     if "INVOICE NUMBER" in text_content and "PACKING LIST" not in text_content and "QUALITY CERTIFICATE" not in text_content:
    #         invoice_items = _process_single_nunhems_invoice(lines, quality_map, germ_map, packing_map, pkg_desc_list)
    #         if invoice_items:
    #             grouped_results[filename] = invoice_items
                
    grouped_results = {}
    for filename, pdf_bytes in pdf_files:
        lines, info = _extract_lines_with_info(pdf_bytes)
        
        text_content = "\n".join(lines).upper()
        is_invoice = "INVOICE NUMBER" in text_content and "PACKING LIST" not in text_content and "QUALITY CERTIFICATE" not in text_content
        
        po_number = None
        if m := re.search(r"Customer\s+P\.?O\.?\s+Number[:\s]+([\s\S]*?)\b(\d{5})\b", text_content, re.IGNORECASE):
            po_number = f"PO-{m.group(2)}"

        log_processing_event(
            vendor='Nunhems',
            filename=filename,
            extraction_info=info,
            po_number=po_number
        )

        if is_invoice:
            invoice_items = _process_single_nunhems_invoice(lines, quality_map, germ_map, packing_map, pkg_desc_list)
            if invoice_items:
                grouped_results[filename] = invoice_items
    
    return grouped_results

def find_best_nunhems_package_description(vendor_desc: str, pkg_desc_list: list[str]) -> str:
    """Finds the best matching package description."""
    if not vendor_desc or not pkg_desc_list: return ""
    
    if m := re.search(r"([\d,]+)\s+SDS", vendor_desc):
        try:
            qty_num = int(m.group(1).replace(',', ''))
            candidate = f"{qty_num:,} SEEDS"
            if candidate in pkg_desc_list:
                return candidate
        except: pass
        
    matches = get_close_matches(vendor_desc, pkg_desc_list, n=1, cutoff=0.6)
    return matches[0] if matches else ""