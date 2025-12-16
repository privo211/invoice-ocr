# vendor_extractors/nunhems.py
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

def _parse_percent(s: str) -> Union[float, str]:
    """Parses a string like '99.1%' into a float."""
    s = str(s).strip().replace(",", ".")
    if s.endswith("%"):
        try:
            return float(s[:-1].strip())
        except ValueError:
            return s[:-1].strip()
    return s

def _extract_nunhems_quality_data(pdf_files: List[Tuple[str, bytes]]) -> Dict[str, Dict]:
    """Extracts data from Nunhems Quality Certificate PDFs from a list of file bytes."""
    quality_map = {}
    for filename, pdf_bytes in pdf_files:
        lines = _extract_lines(pdf_bytes)
        if not any("Quality Certificate" in ln for ln in lines):
            continue

        current_lot = None
        i = 0
        while i < len(lines):
            ln = lines[i]
            if re.search(r"Lot[/ ]*Batch number", ln, re.IGNORECASE) and i + 1 < len(lines):
                if m := re.search(r"(\d{11})", lines[i + 1]):
                    current_lot = m.group(1)
                    quality_map.setdefault(current_lot, {})
                i += 2
                continue
            if current_lot and re.match(r"Pure\s*seeds?", ln, re.IGNORECASE) and i + 5 < len(lines):
                vals = lines[i+3 : i+6]
                quality_map[current_lot]["PureSeeds"] = _parse_percent(vals[0])
                quality_map[current_lot]["Inert"] = _parse_percent(vals[1])
                i += 6
                continue
            if current_lot and (m := re.match(r"Date:\s*([A-Za-z]+\s+\d{1,2},\s*\d{4})", ln)):
                dt = datetime.strptime(m.group(1), "%B %d, %Y")
                quality_map[current_lot]["GrowerGermDate"] = dt.strftime("%m/%d/%Y")
            if current_lot and "Normal seedlings" in ln and i + 1 < len(lines):
                for j in range(i+1, min(i+6, len(lines))):
                    if percent_match := re.search(r"(\d+)%", lines[j]):
                        quality_map[current_lot]["GrowerGerm"] = int(percent_match.group(1))
                        break
            i += 1
    return quality_map

# def _extract_nunhems_quality_data(pdf_files: List[Tuple[str, bytes]]) -> Dict[str, Dict]:
#     """Extracts data from Nunhems Quality Certificate PDFs from a list of file bytes."""
#     quality_map = {}
#     for filename, pdf_bytes in pdf_files:
#         lines = _extract_lines(pdf_bytes)
#         if not any("Quality Certificate" in ln for ln in lines):
#             continue
#         current_lot = None
#         i = 0
#         while i < len(lines):
#             ln = lines[i]
#             if re.match(r"Lot[/ ]*Batch number", ln, re.IGNORECASE) and i + 1 < len(lines):
#                 if m := re.search(r"(\d{11})", lines[i + 1]):
#                     current_lot = m.group(1)
#                     quality_map.setdefault(current_lot, {})
#                 i += 2
#                 continue
            
#             # Find Purity and apply business rule
#             if current_lot and re.match(r"Pure\s*seeds?", ln, re.IGNORECASE):
#                 # scan next 10 lines for percentages
#                 window_text = " ".join(lines[i : i+5])  # combine next 5 lines
#                 percent_vals = re.findall(r"(\d+\.?\d*|TR)%", window_text)
#                 print(percent_vals)
#                 if percent_vals:
#                     pure_val_str = percent_vals[0]
#                     inert_val_str = percent_vals[1] if len(percent_vals) > 1 else "0.0"
#                     other_val_str = percent_vals[2] if len(percent_vals) > 2 else "0.0"
#                     pure_val = 0.0 if pure_val_str == "TR" else float(pure_val_str)
#                     inert_val = 0.0 if inert_val_str == "TR" else float(inert_val_str)
#                     # Apply business rule
#                     if pure_val == 100.0:
#                         pure_val = 99.99
#                         inert_val = 0.01
#                     quality_map[current_lot]["PureSeeds"] = pure_val
#                     quality_map[current_lot]["Inert"] = inert_val
#                     quality_map[current_lot]["OtherSeeds"] = 0.0 if other_val_str == "TR" else float(other_val_str)

#             if current_lot and (m := re.match(r"Date:\s*([A-Za-z]+\s+\d{1,2},\s*\d{4})", ln)):
#                 try:
#                     dt = datetime.strptime(m.group(1), "%B %d, %Y")
#                     quality_map[current_lot]["GrowerGermDate"] = dt.strftime("%m/%d/%Y")
#                 except ValueError:
#                     pass
#             if current_lot and "Normal seedlings" in ln:
#                 for j in range(i + 1, min(i + 4, len(lines))):
#                     if percent_match := re.search(r"(\d+)%", lines[j]):
#                         quality_map[current_lot]["GrowerGerm"] = int(percent_match.group(1))
#                         break
#             i += 1
#     return quality_map

def _extract_nunhems_germ_data(pdf_files: List[Tuple[str, bytes]]) -> Dict[str, Dict]:
    """Parses Nunhems Germ Confirmation PDFs from a list of file bytes."""
    germ_data = {}
    for filename, pdf_bytes in pdf_files:
        lines = _extract_lines(pdf_bytes)
        if not any("Test Date Confirmation" in ln for ln in lines):
            continue
        for i, line in enumerate(lines):
            if re.match(r"^\d{11}$", line):
                lot, germ, germ_date = line, None, None
                if i + 1 < len(lines) and (g := re.search(r"(\d+)%", lines[i + 1])): germ = int(g.group(1))
                if i + 2 < len(lines) and (d := re.search(r"(\d{1,2})/(\d{4})", lines[i + 2])):
                    month, year = d.groups()
                    germ_date = f"{int(month):02d}/01/{year}"
                germ_data[lot] = {"Germ": germ, "GermDate": germ_date}
    return germ_data

def _extract_nunhems_packing_data(pdf_files: List[Tuple[str, bytes]]) -> Dict[str, Dict]:
    """Extracts Lot and SeedCount from Nunhems Packing Lists from a list of file bytes."""
    packing_data = {}
    for filename, pdf_bytes in pdf_files:
        lines = _extract_lines(pdf_bytes)
        if not any("PACKING LIST" in ln.upper() for ln in lines):
            continue
        for i, line in enumerate(lines):
            if lot_match := re.search(r"\b(\d{11})\b", line):
                lot = lot_match.group(1)
                sc_line = ""
                for j in range(i, min(i + 6, len(lines))):
                    if "S/C" in lines[j]:
                        sc_line = lines[j]
                        break
                if sc_line and (match := re.findall(r"([\d,]+)\s*LBS", sc_line)):
                    packing_data[lot] = {"SeedCount": int(match[-1].replace(",", ""))}
    return packing_data

# def _process_single_nunhems_invoice(lines: List[str], quality_map: dict, germ_map: dict, packing_map: dict, pkg_desc_list: list[str]) -> List[Dict]:
# #def _process_single_nunhems_invoice(lines: List[str], quality_map: dict, germ_map: dict, packing_map: dict) -> List[Dict]:
#     """Processes the extracted lines from a single Nunhems invoice."""
#     text_content = "\n".join(lines)
#     print(text_content)
#     vendor_invoice_no = po_number = None
#     if m := re.search(r"Invoice\s+Number[:\s]+([\s\S]*?)\b(\d{9})\b", text_content, re.IGNORECASE): vendor_invoice_no = m.group(2)
#     if m := re.search(r"Customer\s+P\.?O\.?\s+Number[:\s]+([\s\S]*?)\b(\d{5})\b", text_content, re.IGNORECASE): po_number = f"PO-{m.group(2)}"

#     items = []
#     sds_lines = [(i, re.search(r"(\d{1,3}(?:,\d{3})*)\s+SDS", l)) for i, l in enumerate(lines)]
#     sds_item_markers = [(i, m) for i, m in sds_lines if m]

#     for i, (sds_line_idx, sds_match) in enumerate(sds_item_markers):
        
#         start_idx = sds_item_markers[i-1][0] + 1 if i > 0 else 0
#         end_idx = sds_item_markers[i+1][0] if i + 1 < len(sds_item_markers) else len(lines)
#         item_block_text = "\n".join(lines[start_idx:end_idx])

#         desc_part1 = lines[sds_line_idx - 1].strip() if sds_line_idx > 0 else ""
#         desc_part2 = lines[sds_line_idx - 2].strip() if sds_line_idx > 1 else ""
#         vendor_item_description = f"{desc_part2} {desc_part1} {sds_match.group(0)}".strip()
        
#         if "UNTREATED" in item_block_text.upper():
#             treatment = "Untreated"
#         else:
#             treatment = lines[sds_line_idx + 1].strip() if sds_line_idx + 1 < len(lines) else ""
#             if re.match(r'^\d{11}', treatment):
#                 treatment = ""
        
#     # sds_indices = [i for i, l in enumerate(lines) if re.search(r"\d{1,3}(?:,\d{3})*\s+SDS", l)]
#     # for idx in sds_indices:
#     #     sds_match = re.search(r"(\d{1,3}(?:,\d{3})*)\s+SDS", lines[idx])
#     #     part1 = lines[idx + 1] if idx + 1 < len(lines) else ""
#     #     part2 = lines[idx - 1] if idx - 1 >= 0 else ""
#     #     part3 = sds_match.group(0).strip() if sds_match else ""
#     #     vendor_item_description = f"{part1} {part2} {part3}".strip()

#         vendor_lot = origin_country = net_price = total_qty = None
#         for i in range(sds_line_idx, min(len(lines), sds_line_idx + 30)):
#             if "Lot Number:" in lines[i] and i + 1 < len(lines) and (m := re.search(r"\b(\d{11})\b", lines[i+1])):
#                 vendor_lot = m.group(0)
#                 for j in range(i, min(len(lines), i + 20)):
#                     if "ORIGIN" in lines[j] and len(split := lines[j].rsplit("|", 1)) == 2:
#                         origin_country = convert_to_alpha2(split[-1].replace("ORIGIN", "").strip())
#                         break
#                 break
        
#         # for i, line in enumerate(lines):
#         #     if "Net price" in line:
#         #         for j in range(i+1, min(i+4, len(lines))):
#         #             if m := re.search(r"[\d,]+\.\d{2}", lines[j]):
#         #                 net_price = float(m.group(0).replace(",", ""))
#         #                 break
#         #         for j in range(i-1, max(i-4, -1), -1):
#         #             if m := re.search(r"([\d,]+\.\d{2})", lines[j]):
#         #                 total_qty = int(float(m.group(1).replace(",", "")))
#         #                 break
#         #         break
        
#         # Define the robust pattern for finding quantity and price
#         price_qty_pattern = re.compile(r"([\d,]+(?:\.\d{2})?)\s+Net price\s+([\d,]+\.\d{2})", re.IGNORECASE)
    
#         # Define the text block for the current item to search within
#         start_line_index = sds_line_idx
#         end_line_index = sds_indices[i + 1] if i + 1 < len(sds_indices) else len(lines)
#         item_block_text = "\n".join(lines[start_line_index:end_line_index])
        
#         # Search for the quantity and price pattern within the item's text block
#         match = price_qty_pattern.search(item_block_text)
#         if match:
#             qty_str, price_str = match.groups()
#             total_qty = int(float(qty_str.replace(",", "")))
#             net_price = float(price_str.replace(",", ""))
            
#         package_description = ""
#         if "KAMTERTER PRODUCTS INC" in text_content.upper():
#             package_description = "SUBCON BULK-MS"
#         else:
#             package_description = find_best_nunhems_package_description(vendor_item_description, pkg_desc_list)

#         quality_info, germ_info, packing_info = quality_map.get(vendor_lot, {}), germ_map.get(vendor_lot, {}), packing_map.get(vendor_lot, {})
#         cost = round((net_price / total_qty), 4) if net_price and total_qty and total_qty > 0 else None
#         item = {
#             "VendorInvoiceNo": vendor_invoice_no, "PurchaseOrder": po_number, "VendorLot": vendor_lot,
#             "VendorItemDescription": vendor_item_description, "OriginCountry": origin_country,
#             "TotalPrice": net_price, "TotalQuantity": total_qty, "USD_Actual_Cost_$": cost,
#             "Treatment": treatment, "Purity": quality_info.get("PureSeeds"),
#             "InertMatter": quality_info.get("Inert"), "Germ": germ_info.get("Germ"),
#             "GermDate": germ_info.get("GermDate"), "SeedCount": packing_info.get("SeedCount"),
#             "GrowerGerm": quality_info.get("GrowerGerm"), "GrowerGermDate": quality_info.get("GrowerGermDate"),
#             "PackageDescription": package_description
#         }
#         items.append(item)
#     return items

def _process_single_nunhems_invoice(lines: List[str], quality_map: dict, germ_map: dict, packing_map: dict, pkg_desc_list: list[str]) -> List[Dict]:
    """Processes Nunhems invoice lines and combines entries by VendorLot."""
    text_content = "\n".join(lines)
    vendor_invoice_no = po_number = None
    if m := re.search(r"Invoice\s+Number[:\s]+([\s\S]*?)\b(\d{9})\b", text_content, re.IGNORECASE):
        vendor_invoice_no = m.group(2)
    if m := re.search(r"Customer\s+P\.?O\.?\s+Number[:\s]+([\s\S]*?)\b(\d{5})\b", text_content, re.IGNORECASE):
        po_number = f"PO-{m.group(2)}"

    items = []
    sds_lines = [(i, re.search(r"(\d{1,3}(?:,\d{3})*)\s+SDS", l)) for i, l in enumerate(lines)]
    sds_item_markers = [(i, m) for i, m in sds_lines if m]

    for i, (sds_line_idx, sds_match) in enumerate(sds_item_markers):
        # capture item text block between SDS lines
        start_idx = sds_item_markers[i - 1][0] + 1 if i > 0 else 0
        end_idx = sds_item_markers[i + 1][0] if i + 1 < len(sds_item_markers) else len(lines)
        item_block_text = "\n".join(lines[start_idx:end_idx])

        # description
        desc_part1 = lines[sds_line_idx - 1].strip() if sds_line_idx > 0 else ""
        desc_part2 = lines[sds_line_idx + 1].strip() if sds_line_idx > 1 else ""
        vendor_item_description = f"{desc_part2} {desc_part1} {sds_match.group(0)}".strip()

        # treatment normalization
        if "UNTREATED" in item_block_text.upper():
            treatment = "Untreated"
        else:
            treatment = lines[sds_line_idx + 1].strip() if sds_line_idx + 1 < len(lines) else ""
            if re.match(r"^\d{11}", treatment):
                treatment = ""

        # extract vendor lot and qty from "Lot Number" section
        vendor_lot = None
        total_qty = None
        origin_country = None
        for j in range(sds_line_idx, min(len(lines), sds_line_idx + 25)):
            if "LOT NUMBER" in lines[j].upper():
                if (m := re.search(r"(\d{11})", lines[j + 1] if j + 1 < len(lines) else "")):
                    vendor_lot = m.group(1)
                qty_match = re.search(r"\|\s*([\d,]+)\s*\|", lines[j])
                if qty_match:
                    total_qty = int(float(qty_match.group(1).replace(",", "")))
                if "ORIGIN" in lines[j]:
                    print(lines[j])
                    origin_country = convert_to_alpha2(lines[j].split("ORIGIN")[-1].strip())
                    print(origin_country)
                break

        # fallback price/qty pattern search
        price_qty_pattern = re.compile(r"([\d,]+(?:\.\d{2})?)\s+Net price\s+([\d,]+\.\d{2})", re.IGNORECASE)
        match = price_qty_pattern.search(item_block_text)
        net_price = None
        if match:
            qty_str, price_str = match.groups()
            if not total_qty:
                total_qty = int(float(qty_str.replace(",", "")))
            net_price = float(price_str.replace(",", ""))

        package_description = "SUBCON BULK-MS" if "KAMTERTER PRODUCTS INC" in text_content.upper() \
            else find_best_nunhems_package_description(vendor_item_description, pkg_desc_list)

        quality_info = quality_map.get(vendor_lot, {})
        germ_info = germ_map.get(vendor_lot, {})
        packing_info = packing_map.get(vendor_lot, {})

        cost = round((net_price / total_qty), 4) if net_price and total_qty else None
        items.append({
            "VendorInvoiceNo": vendor_invoice_no, "PurchaseOrder": po_number,
            "VendorLot": vendor_lot, "VendorItemDescription": vendor_item_description,
            "OriginCountry": origin_country, "TotalPrice": net_price, "TotalQuantity": total_qty,
            "USD_Actual_Cost_$": cost, "Treatment": treatment,
            "Purity": quality_info.get("PureSeeds"), "InertMatter": quality_info.get("Inert"),
            "Germ": germ_info.get("Germ"), "GermDate": germ_info.get("GermDate"),
            "SeedCount": packing_info.get("SeedCount"),
            "GrowerGerm": quality_info.get("GrowerGerm"),
            "GrowerGermDate": quality_info.get("GrowerGermDate"),
            "PackageDescription": package_description
        })

    # 🔹 Combine by VendorLot (fixes 7→4 lots issue)
    combined = {}
    for it in items:
        key = it["VendorLot"]
        if not key:
            continue
        if key not in combined:
            combined[key] = it.copy()
        else:
            # merge numeric fields
            for f in ("TotalQuantity", "TotalPrice"):
                if it.get(f):
                    combined[key][f] = (combined[key].get(f) or 0) + it[f]
            # ensure combined tag
            if "[COMBINED]" not in combined[key]["VendorItemDescription"].upper():
                combined[key]["VendorItemDescription"] += " [COMBINED]"
    # recompute cost and normalize treatment
    for c in combined.values():
        if c.get("TotalPrice") and c.get("TotalQuantity"):
            # c["USD_Actual_Cost_$"] = round(c["TotalPrice"] / c["TotalQuantity"], 4)
            c["USD_Actual_Cost_$"] = "{:.4f}".format(c["TotalPrice"] / c["TotalQuantity"])
        c["Treatment"] = "Untreated"
    return list(combined.values())


# def _process_single_nunhems_invoice(lines: List[str], quality_map: dict, germ_map: dict, packing_map: dict, pkg_desc_list: list[str]) -> List[Dict]:
#     """
#     Processes a single Nunhems invoice with a more resilient anchor to fix the
#     "No data was extracted" error.
#     """
#     text_content = "\n".join(lines)
    
#     # # --- ADDED FOR DEBUGGING ---
#     # print("--- EXTRACTED NUNHEMS INVOICE TEXT ---")
#     # print(text_content)
#     # print("--------------------------------------")
#     # # ---------------------------

#     po_number = None
#     if m := re.search(r"Customer\s+P\.?O\.?\s+Number[:\s]+([\s\S]*?)\b(\d{5})\b", text_content, re.IGNORECASE): po_number = f"PO-{m.group(2)}"
    
#     vendor_invoice_no = None
#     if m := re.search(r"Invoice\s+Number[:\s]+.*?(\d{9})\b", text_content, re.IGNORECASE | re.DOTALL):
#         vendor_invoice_no = m.group(1)

#     items = []

#     # Find lines with SDS information
#     sds_indices = [i for i, l in enumerate(lines) if re.search(r"\d{1,3}(?:,\d{3})*\s+SDS", l)]

#     # Precompute vendor item descriptions for each SDS line
#     vendor_item_descriptions = {}
#     treatments = {}
#     for idx in sds_indices:
#         sds_match = re.search(r"(\d{1,3}(?:,\d{3})*)\s+SDS", lines[idx])
#         part1 = lines[idx + 1] if idx + 1 < len(lines) else ""
#         part2 = lines[idx - 1] if idx - 1 >= 0 else ""
#         part3 = sds_match.group(0).strip() if sds_match else ""
#         vendor_item_descriptions[idx] = f"{part1} {part2} {part3}".strip()
#         treatments[idx] = lines[idx + 2].strip() if idx + 2 < len(lines) else None

#     # Find item start indices
#     item_start_indices = [i for i, l in enumerate(lines) if re.match(r"^\d{4}\s+[A-Z\s]+", l)]

#     for i, start_idx in enumerate(item_start_indices):
#         end_idx = item_start_indices[i + 1] if i + 1 < len(item_start_indices) else len(lines)
#         item_block_lines = lines[start_idx:end_idx]
#         item_block_text = "\n".join(item_block_lines)

#         # Attempt to match SDS line within this block to get vendor item description and treatment
#         sds_line_idx = next((idx for idx in sds_indices if start_idx <= idx < end_idx), None)
#         vendor_item_description = vendor_item_descriptions.get(sds_line_idx, "")
#         treatment = treatments.get(sds_line_idx, None)

#         # Extract price
#         price_match = re.search(r"Net price\s+([\d,]+\.\d{2})", item_block_text)
#         net_price_per_1000 = float(price_match.group(1).replace(",", "")) if price_match else None

#         # Extract lot details
#         lot_details = re.findall(r"Lot Number:\s*\n?(\d{11})\s*\|\s*([\d,]+)", item_block_text)

#         for vendor_lot, qty_str in lot_details:
#             total_qty = int(qty_str.replace(",", ""))
            
#             origin_country = None
#             seed_count_per_lb = None
#             for line in item_block_text.split('\n'):
#                 if vendor_lot in line:
#                     if "ORIGIN" in line:
#                         if origin_match := re.search(r"\|\s*([A-Z\s]+?)\s+ORIGIN", line, re.IGNORECASE):
#                             origin_country = convert_to_alpha2(origin_match.group(1).strip())
                    
#                     if "SDS/LB" in line:
#                         if sc_match := re.search(r"([\d,]+)\s+SDS/LB", line):
#                             seed_count_per_lb = int(sc_match.group(1).replace(",", ""))
                    
#                     if origin_country and seed_count_per_lb is not None:
#                         break
            
#             total_price = (total_qty / 1000) * net_price_per_1000 if net_price_per_1000 and total_qty else None
#             cost_per_seed = round(net_price_per_1000 / 1000, 4) if net_price_per_1000 else None
            
#             quality_info = quality_map.get(vendor_lot, {})
#             germ_info = germ_map.get(vendor_lot, {})
#             final_seed_count = seed_count_per_lb if seed_count_per_lb is not None else packing_map.get(vendor_lot, {}).get("SeedCount")
#             package_description = find_best_nunhems_package_description(vendor_item_description, pkg_desc_list)

#             item = {
#                 "VendorInvoiceNo": vendor_invoice_no,
#                 "PurchaseOrder": po_number,
#                 "VendorLot": vendor_lot,
#                 "VendorItemDescription": vendor_item_description,
#                 "OriginCountry": origin_country,
#                 "TotalPrice": total_price,
#                 "TotalQuantity": total_qty,
#                 "USD_Actual_Cost_$": cost_per_seed,
#                 "Treatment": treatment,
#                 "Purity": quality_info.get("PureSeeds"),
#                 "InertMatter": quality_info.get("Inert"),
#                 "Germ": germ_info.get("Germ"),
#                 "GermDate": germ_info.get("GermDate"),
#                 "SeedCount": final_seed_count,
#                 "GrowerGerm": quality_info.get("GrowerGerm"),
#                 "GrowerGermDate": quality_info.get("GrowerGermDate"),
#                 "PackageDescription": package_description
#             }
#             items.append(item)

#     return items


# def _process_single_nunhems_invoice(lines: List[str], quality_map: dict, germ_map: dict, packing_map: dict, pkg_desc_list: list[str]) -> List[Dict]:
#     """
#     Processes a single Nunhems invoice using the robust 'SDS'-based logic,
#     with the corrected treatment rule as specified.
#     """
#     text_content = "\n".join(lines)
    
#     # --- ADDED FOR DEBUGGING ---
#     # This will print the full, raw text of the invoice to the terminal.
#     print("--- EXTRACTED NUNHEMS INVOICE TEXT ---")
#     print(text_content)
#     print("--------------------------------------")
#     # ---------------------------

#     vendor_invoice_no = po_number = None
#     if m := re.search(r"Invoice\s+Number[:\s]+.*?(\d{9})\b", text_content, re.IGNORECASE | re.DOTALL):
#         vendor_invoice_no = m.group(1)
#     if m := re.search(r"Customer\s+P\.?O\.?\s+Number[:\s]+.*?(\d{5})\b", text_content, re.IGNORECASE | re.DOTALL):
#         po_number = f"PO-{m.group(1)}"

#     items = []
#     is_kamterter_shipment = "KAMTERTER PRODUCTS INC" in text_content.upper()

#     sds_lines = [(i, re.search(r"(\d{1,3}(?:,\d{3})*)\s+SDS", l)) for i, l in enumerate(lines)]
#     sds_item_markers = [(i, m) for i, m in sds_lines if m]

#     for i, (sds_line_idx, sds_match) in enumerate(sds_item_markers):
        
#         start_idx = sds_item_markers[i-1][0] + 1 if i > 0 else 0
#         end_idx = sds_item_markers[i+1][0] if i + 1 < len(sds_item_markers) else len(lines)
#         item_block_text = "\n".join(lines[start_idx:end_idx])

#         desc_part1 = lines[sds_line_idx - 1].strip() if sds_line_idx > 0 else ""
#         desc_part2 = lines[sds_line_idx - 2].strip() if sds_line_idx > 1 else ""
#         vendor_item_description = f"{desc_part2} {desc_part1} {sds_match.group(0)}".strip()
        
#         if "UNTREATED" in item_block_text.upper():
#             treatment = "Untreated"
#         else:
#             treatment = lines[sds_line_idx + 1].strip() if sds_line_idx + 1 < len(lines) else ""
#             if re.match(r'^\d{11}', treatment):
#                 treatment = ""

#         package_description = "SUBCON BULK-MS" if is_kamterter_shipment else find_best_nunhems_package_description(vendor_item_description, pkg_desc_list)
        
#         price_match = re.search(r"Net price\s+([\d,]+\.\d{2})", item_block_text)
#         net_price_per_1000 = float(price_match.group(1).replace(",", "")) if price_match else None

#         lot_details = re.findall(r"(\d{11})\s*\|\s*([\d,]+)\s*\|.*?ORIGIN", item_block_text)
#         if not lot_details:
#             continue

#         for lot_tuple in lot_details:
#             vendor_lot, qty_str = lot_tuple
#             total_qty = int(qty_str.replace(",", ""))
            
#             origin_country = None
#             for line in item_block_text.split('\n'):
#                 if vendor_lot in line:
#                     if origin_match := re.search(r"\|\s*([A-Z\s]+?)\s+ORIGIN", line, re.IGNORECASE):
#                         origin_country = convert_to_alpha2(origin_match.group(1).strip())
#                     break

#             total_price = (total_qty / 1000) * net_price_per_1000 if net_price_per_1000 else None
#             cost_per_seed = round(net_price_per_1000 / 1000, 4) if net_price_per_1000 else None
            
#             quality_info = quality_map.get(vendor_lot, {})
#             germ_info = germ_map.get(vendor_lot, {})
#             packing_info = packing_map.get(vendor_lot, {})

#             item = {
#                 "VendorInvoiceNo": vendor_invoice_no, "PurchaseOrder": po_number, "VendorLot": vendor_lot,
#                 "VendorItemDescription": vendor_item_description, "OriginCountry": origin_country,
#                 "TotalPrice": total_price, "TotalQuantity": total_qty, "USD_Actual_Cost_$": cost_per_seed,
#                 "Treatment": treatment, "Purity": quality_info.get("PureSeeds"),
#                 "InertMatter": quality_info.get("Inert"), "Germ": germ_info.get("Germ"),
#                 "GermDate": germ_info.get("GermDate"), "SeedCount": packing_info.get("SeedCount"),
#                 "GrowerGerm": quality_info.get("GrowerGerm"), "GrowerGermDate": quality_info.get("GrowerGermDate"),
#                 "PackageDescription": package_description
#             }
#             items.append(item)
            
#     return items

# def _process_single_nunhems_invoice(lines: List[str], quality_map: dict, germ_map: dict, packing_map: dict, pkg_desc_list: list[str]) -> List[Dict]:
#     """Processes the extracted lines from a single Nunhems invoice."""
#     text_content = "\n".join(lines)
#     vendor_invoice_no = po_number = None
#     if m := re.search(r"Invoice\s+Number[:\s]+.*?(\d{9})\b", text_content, re.IGNORECASE | re.DOTALL): vendor_invoice_no = m.group(1)
#     if m := re.search(r"Customer\s+P\.?O\.?\s+Number[:\s]+.*?(\d{5})\b", text_content, re.IGNORECASE | re.DOTALL): po_number = f"PO-{m.group(1)}"

#     items = []
    
#     # Find all line item sections, typically marked by "SDS" or package type
#     item_start_indices = [i for i, l in enumerate(lines) if re.search(r"\d{1,3}(?:,\d{3})*\s+SDS", l) or "BUCKET(S)/PAIL(S)" in l]

#     for i, start_idx in enumerate(item_start_indices):
#         end_idx = item_start_indices[i+1] if i + 1 < len(item_start_indices) else len(lines)
#         item_block = lines[start_idx:end_idx]
#         item_block_text = "\n".join(item_block)

#         desc_match = re.search(r"HYBRID\s+([A-Z\s]+)\n.*?NUNCOTE", item_block_text, re.DOTALL)
#         vendor_item_description = f"HYBRID {desc_match.group(1).strip()}" if desc_match else ""
#         treatment_match = re.search(r"THIRAM/IPRODIONE/METALAXYL-M\(MEFENOXAM\)", item_block_text)
#         treatment = treatment_match.group(0) if treatment_match else ""
        
#         price_match = re.search(r"Net price\s+([\d,]+\.\d{2})", item_block_text)
#         net_price_per_th = float(price_match.group(1).replace(",", "")) if price_match else None

#         # Find ALL lot number lines and their quantities within this item's block
#         lot_details = re.findall(r"(\d{11})\s*\|\s*([\d,]+)\s*\|.*?ORIGIN", item_block_text)

#         for lot_tuple in lot_details:
#             vendor_lot, qty_str = lot_tuple
#             total_qty = int(qty_str.replace(",", ""))
            
#             # Re-find the full line to get the origin country
#             origin_country = None
#             for line in item_block:
#                 if vendor_lot in line:
#                     if origin_match := re.search(r"\|\s*([A-Z\s]+)\s+ORIGIN", line, re.IGNORECASE):
#                         origin_country = convert_to_alpha2(origin_match.group(1).strip())
#                     break

#             package_description = ""
#             if "KAMTERTER PRODUCTS INC" in text_content.upper():
#                 package_description = "SUBCON BULK-MS"
#             else:
#                 package_description = find_best_nunhems_package_description(vendor_item_description, pkg_desc_list)
            
#             # The price is per 1000 seeds (TH), so calculate total price for this lot
#             total_price = (total_qty / 1000) * net_price_per_th if net_price_per_th and total_qty else None
#             cost = round(net_price_per_th / 1000, 4) if net_price_per_th else None # Cost per seed
            
#             quality_info = quality_map.get(vendor_lot, {})
#             germ_info = germ_map.get(vendor_lot, {})
#             packing_info = packing_map.get(vendor_lot, {})

#             item = {
#                 "VendorInvoiceNo": vendor_invoice_no, "PurchaseOrder": po_number, "VendorLot": vendor_lot,
#                 "VendorItemDescription": vendor_item_description, "OriginCountry": origin_country,
#                 "TotalPrice": total_price, "TotalQuantity": total_qty, "USD_Actual_Cost_$": cost,
#                 "Treatment": treatment, "Purity": quality_info.get("PureSeeds"),
#                 "InertMatter": quality_info.get("Inert"), "Germ": germ_info.get("Germ"),
#                 "GermDate": germ_info.get("GermDate"), "SeedCount": packing_info.get("SeedCount"),
#                 "GrowerGerm": quality_info.get("GrowerGerm"), "GrowerGermDate": quality_info.get("GrowerGermDate"),
#                 "PackageDescription": package_description
#             }
#             items.append(item)
            
#     return items

def extract_nunhems_data_from_bytes(pdf_files: List[Tuple[str, bytes]], pkg_desc_list: list[str]) -> Dict[str, List[Dict]]:
# def extract_nunhems_data_from_bytes(pdf_files: List[Tuple[str, bytes]]) -> Dict[str, List[Dict]]:
    """Main in-memory function to extract all item data from a batch of Nunhems files."""
    if not pdf_files: return {}

    quality_map = _extract_nunhems_quality_data(pdf_files)
    germ_map = _extract_nunhems_germ_data(pdf_files)
    packing_map = _extract_nunhems_packing_data(pdf_files)

    grouped_results = {}
    for filename, pdf_bytes in pdf_files:
        lines = _extract_lines(pdf_bytes)
        text_content = "\n".join(lines).upper()
        if "INVOICE NUMBER" in text_content and "PACKING LIST" not in text_content and "QUALITY CERTIFICATE" not in text_content:
            invoice_items = _process_single_nunhems_invoice(lines, quality_map, germ_map, packing_map, pkg_desc_list)
            if invoice_items:
                grouped_results[filename] = invoice_items
    return grouped_results

def find_best_nunhems_package_description(vendor_desc: str, pkg_desc_list: list[str]) -> str:
    """Finds the best matching package description for Nunhems items."""
    if not vendor_desc or not pkg_desc_list: return ""
    if m := re.search(r"([\d,]+)\s+SDS", vendor_desc):
        candidate = f"{int(m.group(1).replace(',', '')):,} SEEDS"
        if candidate in pkg_desc_list:
            return candidate
    matches = get_close_matches(vendor_desc, pkg_desc_list, n=1, cutoff=0.6)
    return matches[0] if matches else ""