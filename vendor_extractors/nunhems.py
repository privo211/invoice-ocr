# vendor_extractors/nunhems.py
import os
import re
import json
import fitz  # PyMuPDF
import requests
import time
import pycountry
from datetime import datetime
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
            if re.match(r"Lot[/ ]*Batch number", ln, re.IGNORECASE) and i + 1 < len(lines):
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

def _process_single_nunhems_invoice(lines: List[str], quality_map: dict, germ_map: dict, packing_map: dict) -> List[Dict]:
    """Processes the extracted lines from a single Nunhems invoice."""
    text_content = "\n".join(lines)
    vendor_invoice_no = po_number = None
    if m := re.search(r"Invoice\s+Number[:\s]+([\s\S]*?)\b(\d{9})\b", text_content, re.IGNORECASE): vendor_invoice_no = m.group(2)
    if m := re.search(r"Customer\s+P\.?O\.?\s+Number[:\s]+([\s\S]*?)\b(\d{5})\b", text_content, re.IGNORECASE): po_number = f"PO-{m.group(2)}"

    items = []
    sds_indices = [i for i, l in enumerate(lines) if re.search(r"\d{1,3}(?:,\d{3})*\s+SDS", l)]
    for idx in sds_indices:
        sds_match = re.search(r"(\d{1,3}(?:,\d{3})*)\s+SDS", lines[idx])
        part1 = lines[idx + 1] if idx + 1 < len(lines) else ""
        part2 = lines[idx - 1] if idx - 1 >= 0 else ""
        part3 = sds_match.group(0).strip() if sds_match else ""
        vendor_item_description = f"{part1} {part2} {part3}".strip()
        treatment = lines[idx + 2].strip() if idx + 2 < len(lines) else None

        vendor_lot = origin_country = net_price = total_qty = None
        for i in range(idx, min(len(lines), idx + 30)):
            if "Lot Number:" in lines[i] and i + 1 < len(lines) and (m := re.search(r"\b(\d{11})\b", lines[i+1])):
                vendor_lot = m.group(0)
                for j in range(i, min(len(lines), i + 20)):
                    if "ORIGIN" in lines[j] and len(split := lines[j].rsplit("|", 1)) == 2:
                        origin_country = convert_to_alpha2(split[-1].replace("ORIGIN", "").strip())
                        break
                break
        
        for i, line in enumerate(lines):
            if "Net price" in line:
                for j in range(i+1, min(i+4, len(lines))):
                    if m := re.search(r"[\d,]+\.\d{2}", lines[j]):
                        net_price = float(m.group(0).replace(",", ""))
                        break
                for j in range(i-1, max(i-4, -1), -1):
                    if m := re.search(r"([\d,]+\.\d{2})", lines[j]):
                        total_qty = int(float(m.group(1).replace(",", "")))
                        break
                break

        quality_info, germ_info, packing_info = quality_map.get(vendor_lot, {}), germ_map.get(vendor_lot, {}), packing_map.get(vendor_lot, {})
        cost = round((net_price / total_qty), 4) if net_price and total_qty and total_qty > 0 else None
        item = {
            "VendorInvoiceNo": vendor_invoice_no, "PurchaseOrder": po_number, "VendorLot": vendor_lot,
            "VendorItemDescription": vendor_item_description, "OriginCountry": origin_country,
            "TotalPrice": net_price, "TotalQuantity": total_qty, "USD_Actual_Cost_$": cost,
            "Treatment": treatment, "Purity": quality_info.get("PureSeeds"),
            "InertMatter": quality_info.get("Inert"), "Germ": germ_info.get("Germ"),
            "GermDate": germ_info.get("GermDate"), "SeedCountPerLB": packing_info.get("SeedCount"),
            "GrowerGerm": quality_info.get("GrowerGerm"), "GrowerGermDate": quality_info.get("GrowerGermDate"),
        }
        items.append(item)
    return items

def extract_nunhems_data_from_bytes(pdf_files: List[Tuple[str, bytes]]) -> Dict[str, List[Dict]]:
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
            invoice_items = _process_single_nunhems_invoice(lines, quality_map, germ_map, packing_map)
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