import os
import re
import fitz  # PyMuPDF
from typing import List, Dict, TypedDict, Union
import requests
from difflib import get_close_matches
import time
import pycountry
import logging
from functools import wraps
from dotenv import load_dotenv
from db_logger import log_processing_event

load_dotenv()
BC_TENANT  = os.getenv("AZURE_TENANT_ID")
BC_ENV      = "Production"
BC_COMPANY = os.getenv("BC_COMPANY")
CLIENT_ID     = os.getenv("AZURE_CLIENT_ID")
CLIENT_SECRET = os.getenv("AZURE_CLIENT_SECRET")

# ── OCR SUPPORT ────────────────────────────────────────────────────────────────
AZURE_ENDPOINT = os.getenv("AZURE_ENDPOINT")
AZURE_KEY      = os.getenv("AZURE_KEY")
# ── END OCR SUPPORT ────────────────────────────────────────────────────────────

logger = logging.getLogger("invoice-ocr")
logger.setLevel(logging.INFO)

def timed_func(label: str):
    def decorator(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            start = time.perf_counter()
            result = fn(*args, **kwargs)
            elapsed = time.perf_counter() - start
            logger.info(f"[TIMING] {label} took {elapsed:.2f}s")
            return result
        return wrapper
    return decorator

class PurityData(TypedDict):
    Purity: Union[float, str, None]
    Inert: Union[float, str, None]
    GrowerGerm: Union[int, float, None]
    GrowerGermDate: Union[str, None]

# ── OCR HELPERS ────────────────────────────────────────────────────────────────

def _extract_text_with_azure_ocr(pdf_content: bytes) -> str:
    if not AZURE_ENDPOINT or not AZURE_KEY:
        raise ValueError("Azure OCR credentials (AZURE_ENDPOINT / AZURE_KEY) are not set.")

    headers = {
        "Ocp-Apim-Subscription-Key": AZURE_KEY,
        "Content-Type": "application/pdf",
    }
    response = requests.post(
        f"{AZURE_ENDPOINT}formrecognizer/documentModels/prebuilt-layout:analyze?api-version=2023-07-31",
        headers=headers,
        data=pdf_content,
    )
    if response.status_code != 202:
        raise RuntimeError(f"Azure OCR request failed: {response.text}")

    op_url = response.headers["Operation-Location"]
    for _ in range(30):
        time.sleep(1.5)
        result = requests.get(op_url, headers={"Ocp-Apim-Subscription-Key": AZURE_KEY}).json()
        if result.get("status") == "succeeded":
            lines = [
                ln.get("content", "").strip()
                for page in result["analyzeResult"]["pages"]
                for ln in page["lines"]
                if ln.get("content", "").strip()
            ]
            return "\n".join(lines)
        if result.get("status") == "failed":
            raise RuntimeError("Azure OCR analysis failed.")
    raise TimeoutError("Azure OCR timed out.")

def _parse_ocr_lot_line(line: str) -> Dict:
    """Robust parser for flattened OCR lot strings using targeted regex to prevent column shifting."""
    lot = {
        "VendorLotNo": None, "CurrentGerm": None, "GermDate": None,
        "SeedCount": None, "SeedSize": None, "GrowerGerm": None,
        "GrowerGermDate": None, "Purity": None, "OriginCountry": "",
        "SproutCount": None, "Inert": None
    }

    lot_m = re.search(r"^(\d{6}-[\d-]+)", line.strip())
    if lot_m: lot["VendorLotNo"] = lot_m.group(1)

    # Date
    date_m = re.search(r"\b(\d{1,2}/\d{1,2}/\d{4})\b", line)
    if date_m:
        lot["GermDate"] = date_m.group(1)
        # Germ is usually the 2 digits right before the date
        before_date = line[:date_m.start()]
        germ_m = re.findall(r"\b(\d{2})\b", before_date)
        if germ_m: lot["CurrentGerm"] = int(germ_m[-1])

    # Seed Count (comma separated number > 1000)
    sc_m = re.search(r"\b(\d{2,3},\d{3})\b", line)
    if sc_m:
        lot["SeedCount"] = int(sc_m.group(1).replace(',', ''))
        # Seed size is usually a small number right after the seed count
        after_sc = line[sc_m.end():]
        size_m = re.search(r"^\s*([\d\.]+)", after_sc)
        if size_m and float(size_m.group(1)) < 50:
            lot["SeedSize"] = size_m.group(1)

    # Country (Look at the very end of the string)
    country_m = re.search(r"\b(USA|US|CAN|MEX|[A-Z]{2,3})\b$", line.strip(), re.IGNORECASE)
    if country_m:
        lot["OriginCountry"] = convert_to_alpha2(country_m.group(1).upper())

    return lot

def _extract_invoice_from_ocr_text(ocr_text: str, fallback_po: str, token: str) -> List[Dict]:
    """Extracts items from flat text, immune to arbitrary OCR line breaks."""
    items = []
    flat_text = ocr_text.replace("\n", " ")

    # Improved PO Search
    m_po = re.search(r"(?:PO|Purchase\s+order)[-\s#:]*(\d{5})\b", flat_text, re.IGNORECASE)
    header_po = f"PO-{m_po.group(1)}" if m_po else fallback_po

    item_pattern = re.compile(
        r"(?:(?:\b\d+\s+)?\b(?P<item_no>\d{8})\b\s+"
        r"(?P<desc>.*?)\s+"
        r"(?P<unit>EA|M|LB|KG|G)\s+"
        r"(?P<ordered>[\d\.,]+)\s+"
        r"(?P<shipped>[\d\.,]+)\s+"
        r"(?P<unit_price>[\d\.,]+)\s+"
        r"(?:[\d\.,]+\s+)?"
        r"(?P<total_price>[\d\.,]{4,})"
        r")",
        re.IGNORECASE
    )

    matches = list(item_pattern.finditer(flat_text))
    for i, match in enumerate(matches):
        item_no = match.group("item_no")
        desc = match.group("desc").strip()
        shipped_str = match.group("shipped").replace(",", "")
        total_str = match.group("total_price").replace(",", "")

        shipped = float(shipped_str) if shipped_str else None
        total_price = float(total_str) if total_str else None

        m_pkg = re.search(r"(\d+)(?=\s*[Mm]\b|\s*[Ll][Bb]\b|M$|LB$)", desc)
        pkg_qty = int(m_pkg.group(1)) if m_pkg else None

        usd_actual_cost = None
        if pkg_qty and shipped and total_price:
            usd_actual_cost = "{:.4f}".format(total_price / (shipped * pkg_qty))
            
        # Add the calculation logic here (136 * 100)
        original_qty = int(shipped * pkg_qty) if shipped and pkg_qty else shipped

        start_idx = match.end()
        end_idx = matches[i+1].start() if i + 1 < len(matches) else len(flat_text)
        chunk = flat_text[start_idx:end_idx]

        lot_pattern = re.compile(r"\b(\d{6}-[\d-]+)\b")
        lot_matches = list(lot_pattern.finditer(chunk))
        lots_raw = []
        for j, lm in enumerate(lot_matches):
            l_start = lm.start()
            l_end = lot_matches[j+1].start() if j + 1 < len(lot_matches) else len(chunk)
            lot_chunk = chunk[l_start:l_end].strip()
            
            # CRITICAL FIX: Only accept lot chunks that contain a date to prevent false positives (like 280205-70)
            if re.search(r"\d{1,2}/\d{1,2}/\d{4}", lot_chunk):
                lots_raw.append(lot_chunk)

        treatment_name = None
        tn_match = re.search(r"Treatment name:\s*([A-Za-z]+,?\s*[A-Za-z]+)", chunk)
        if tn_match:
            treatment_name = tn_match.group(1).strip()

        current = {
            "VendorItemNumber": item_no,
            "VendorDescription": desc,
            "QtyShipped": shipped,
            "OriginalReceivedQty": original_qty, 
            "USD_Actual_Cost_$": usd_actual_cost,
            "PackageDescription": find_best_package_description(desc),
            "TreatmentName": treatment_name,
            "PurchaseOrder": header_po,
            "TotalPrice": total_price,
            "Lots": lots_raw
        }

        try:
            current["BCOptions"] = get_po_items(current["PurchaseOrder"], token) if current["PurchaseOrder"] else []
        except Exception as e:
            current["BCOptions"] = [{"No": "ERROR", "Description": str(e)}]

        items.append(current)

    return items
# ── END OCR HELPERS ────────────────────────────────────────────────────────────

@timed_func("get_bc_token")
def get_bc_token(client_id, client_secret, tenant_id):
    token_url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
    data = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,
        "scope": "https://api.businesscentral.dynamics.com/.default"
    }
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    resp = requests.post(token_url, data=data, headers=headers)
    resp.raise_for_status()
    return resp.json()["access_token"]

token = get_bc_token(
    client_id=CLIENT_ID,
    client_secret=CLIENT_SECRET,
    tenant_id=BC_TENANT
)

_items_cache = None

@timed_func("load_all_items")
def load_all_items(force: bool = False) -> list[dict]:
    global _items_cache
    if _items_cache is not None and not force:
        return _items_cache

    base_url = (
        f"https://api.businesscentral.dynamics.com/v2.0/"
        f"{BC_TENANT}/{BC_ENV}/ODataV4/"
        f"Company('{BC_COMPANY}')/FilteredItems"
    )

    params = {
        "$select": "No,Description",
        "$orderby": "No"
    }
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json;odata.metadata=none"
    }
    resp = requests.get(base_url, params=params, headers=headers)
    resp.raise_for_status()
    _items_cache = resp.json().get("value", [])
    return _items_cache

_pkg_desc_list = None

@timed_func("load_package_descriptions")
def load_package_descriptions(token: str) -> list[str]:
    global _pkg_desc_list
    if _pkg_desc_list is not None:
        return _pkg_desc_list

    odata_url = (
        f"https://api.businesscentral.dynamics.com/v2.0/"
        f"{BC_TENANT}/{BC_ENV}/ODataV4/"
        f"Company('{BC_COMPANY}')/Package_Descriptions_List_Excel"
    )
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json"
    }
    try:
        resp = requests.get(odata_url, headers=headers)
        resp.raise_for_status()
        payload = resp.json()
        rows = payload.get("value", [])
        desc_set = set()
        for row in rows:
            pkg_desc = row.get("Package_Description")
            if pkg_desc:
                desc_set.add(pkg_desc.strip().upper())
        _pkg_desc_list = sorted(desc_set)
        return _pkg_desc_list
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to load package descriptions: {e}")
        return []

def normalize_text(s: str) -> str:
    return re.sub(r"[,]", "", s).strip().upper()

@timed_func("find_best_package_description")
def find_best_package_description(vendor_desc: str) -> str:
    global _pkg_desc_list
    if _pkg_desc_list is None:
        raise RuntimeError("Package descriptions not loaded.")
    pkg_desc_list = _pkg_desc_list

    normalized = normalize_text(vendor_desc)
    m = re.search(r"(\d+)\s*(M|LB)\b", normalized)
    if m:
        qty = int(m.group(1))
        unit = m.group(2)
        if unit == "M":
            seed_count = qty * 1000
            candidate = f"{seed_count:,} SEEDS"
        else:
            candidate = f"{qty} LB"
        candidate = candidate.strip().upper()
        if candidate in pkg_desc_list:
            return candidate

    matches = get_close_matches(normalized, pkg_desc_list, n=1, cutoff=0.6)
    return matches[0] if matches else ""

_po_cache = {}

@timed_func("get_po_items")
def get_po_items(po_number, token):
    if po_number in _po_cache:
        return _po_cache[po_number]

    po_numbers = [po.strip() for po in po_number.split("|") if po.strip()]
    if not po_numbers: return []

    if len(po_numbers) == 1:
        filter_clause = f"PurchaseOrderNo eq '{po_numbers[0]}'"
    else:
        filter_clause = " or ".join(f"PurchaseOrderNo eq '{po}'" for po in po_numbers)

    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    
    url_main = (
        f"https://api.businesscentral.dynamics.com/v2.0/{BC_TENANT}/{BC_ENV}"
        f"/ODataV4/Company('Stokes%20Seeds%20Limited')/PurchaseOrderQuery?$filter={filter_clause}"
    )
    
    data = []
    seen = set()

    try:
        response = requests.get(url_main, headers=headers)
        if response.status_code == 200:
            for item in response.json().get("value", []):
                no = item.get("ItemNumber")
                if not no or no in seen: continue
                seen.add(no)
                data.append({"No": no, "Description": item.get("ItemDescription", "")})
    except Exception as e: pass

    if not data:
        url_archive = (
            f"https://api.businesscentral.dynamics.com/v2.0/{BC_TENANT}/{BC_ENV}"
            f"/ODataV4/Company('Stokes%20Seeds%20Limited')/ArchivePurchaseOrderQuery?$filter={filter_clause}"
        )
        try:
            response = requests.get(url_archive, headers=headers)
            if response.status_code == 200:
                seen = set()
                for item in response.json().get("value", []):
                    no = item.get("ItemNumber")
                    if not no or no in seen: continue
                    seen.add(no)
                    data.append({"No": no, "Description": item.get("ItemDescription", "")})
        except Exception as e: pass

    _po_cache[po_number] = data
    return data

def convert_to_alpha2(country_value: str) -> str:
    if not country_value: return ""
    country_value = country_value.strip()
    if len(country_value) == 2: return country_value.upper()
    try:
        country = pycountry.countries.lookup(country_value)
        return country.alpha_2
    except LookupError:
        return country_value

@timed_func("parse_lot_block")
def parse_lot_block(raw_text: str) -> Dict:
    parts = raw_text.replace(",", "").split("\n")
    try:
        lot_no = parts[0].strip()
        ship_qty = None
        if len(parts) > 1:
            try: ship_qty = float(parts[1].replace(",", "").strip())
            except ValueError: pass

        current_germ = None
        if len(parts) > 2 and parts[2].isdigit():
            current_germ = 98 if int(parts[2]) == 100 else int(parts[2])

        germ_date, seed_count = None, None
        if len(parts) > 3:
            combined = parts[3]
            germ_date = combined[:10]
            sc = combined[10:].strip()
            seed_count = int(sc) if sc.isdigit() else None

        origin = next((p for p in reversed(parts) if re.fullmatch(r"[A-Z]{3}", p)), None)
        bc_origin = convert_to_alpha2(origin.strip()) if origin else ""

        sprout = float(parts[8]) if len(parts) > 8 and re.match(r"^\d+(\.\d+)?$", parts[8]) else None
        seed_size, purity = None, None
        
        if len(parts) > 4:
            text5 = parts[4].strip()
            if re.fullmatch(r"\d+\.\d{2}", text5):
                val = float(text5)
                if val > 50: purity = val
                else: seed_size = text5
            else:
                seed_size = text5

        if seed_size:
            m = re.search(r"(\d+\.\d{2})", seed_size)
            if m:
                val = float(m.group(1))
                if val > 50:
                    purity = val
                    seed_size = re.sub(r"\d+\.\d{2}", "", seed_size).strip() or None

        if purity is None and len(parts) > 5:
            text6 = parts[5].strip()
            if re.fullmatch(r"\d+\.\d{2}", text6):
                purity = float(text6)

        return {
            "VendorLotNo": lot_no, "CurrentGerm": current_germ, "GermDate": germ_date,
            "SeedCount": seed_count, "SeedSize": seed_size, "GrowerGerm": None,
            "GrowerGermDate": None, "Purity": None, "OriginCountry": bc_origin,
            "SproutCount": sprout, "Inert": None
        }
    except Exception as e:
        return {"error": str(e), "raw": raw_text}

@timed_func("extract_seed_analysis_reports_from_bytes")
def extract_seed_analysis_reports_from_bytes(pdf_files: list[tuple[str, bytes]]) -> Dict[str, PurityData]:
    """Extracts Pure Seed %, Inert %, Normal Seedling % and Date Issued dynamically from whole text."""
    report_map: Dict[str, PurityData] = {}
    for fname, bts in pdf_files:
        try:
            with fitz.open(stream=bts, filetype="pdf") as doc:
                text = "".join(page.get_text() for page in doc)
            
            if "Report of Seed Analysis" not in text and "Purity Analysis" not in text:
                continue

            # 1. Vendor Lot Number
            lot_match = re.search(r"Lot Number:[\s\n]*([\d-]+)", text, re.IGNORECASE)
            if not lot_match: continue
            lot_no = lot_match.group(1).strip()

            # 2. Date Issued (Maps to GrowerGermDate/Certificate Date)
            date_match = re.search(r"Date Issued:[\s\n]*(\d{1,2}/\d{1,2}/\d{4})", text, re.IGNORECASE)
            date_issued = date_match.group(1) if date_match else None

            # 3. Table Values 
            # PyMuPDF flattens tables inconsistently. We search for the end of the headers 
            # ("Days Tested") and grab the sequence of numbers right after it.
            pure, inert, normal = None, None, None
            
            header_end_match = re.search(r"Days[\s\n]*Tested", text, re.IGNORECASE)
            if header_end_match:
                subtext = text[header_end_match.end():]
                # Finds the row of numbers e.g., 99.99, 0.01, 0.00, 0.00, 99, X...
                tokens = re.findall(r"\b(?:\d+(?:\.\d+)?|X|TR|-TR-)\b", subtext)
                
                if len(tokens) >= 5:
                    p_val = tokens[0]  # Pure Seed %
                    i_val = tokens[1]  # Inert Matter %
                    n_val = tokens[4]  # Normal Seedling % (GrowerGerm)
                    
                    if p_val.replace('.', '', 1).isdigit(): pure = float(p_val)
                    if i_val.replace('.', '', 1).isdigit(): inert = float(i_val)
                    if n_val.isdigit(): normal = int(n_val)

            report_map[lot_no] = {
                "Purity": pure,
                "Inert": inert,
                "GrowerGerm": normal,
                "GrowerGermDate": date_issued
            }
        except Exception as e:
            logger.warning(f"Could not process {fname} for seed analysis. Error: {e}")
            
    return report_map

@timed_func("extract_invoice_from_pdf")
def extract_invoice_from_pdf(
    source: Union[str, bytes],
    fallback_po: str = "",
    token: str = ""
) -> List[Dict]:
    doc = None
    try:
        if isinstance(source, bytes): doc = fitz.open(stream=source, filetype="pdf")
        elif isinstance(source, str): doc = fitz.open(source)
        else: raise ValueError("Source must be file path (str) or bytes.")
        
        first_page_text = doc[0].get_text()
        
        # Improved PO search
        header_po_match = re.search(r"(?:PO|Purchase\s*order)[-\s#:]*(\d{5})\b", first_page_text, re.IGNORECASE | re.DOTALL)
        header_po = f"PO-{header_po_match.group(1)}" if header_po_match else fallback_po

        all_blocks = []
        for page in doc:
            all_blocks.extend(sorted(page.get_text("blocks"), key=lambda b: (b[1], b[0])))

        items: List[Dict] = []
        current = None
        text_acc = ""
        i = 0

        while i < len(all_blocks):
            b = all_blocks[i]
            txt = b[4].strip()

            if re.match(r"^\d+\s+\d{8}", txt) and ("Treated" in txt or "Untreated" in txt):
                if current:
                    treatment_name = re.search(r"Treatment name:\s*(.*)", text_acc)
                    current["TreatmentName"] = treatment_name.group(1).strip() if treatment_name else None
                    po_match = re.search(r"(?:PO|Purchase\s+order)[-\s:]*(\d{5})\b", text_acc, re.IGNORECASE)
                    current["PurchaseOrder"] = (f"PO-{po_match.group(1)}" if po_match else header_po)
                    try:
                        current["BCOptions"] = get_po_items(current["PurchaseOrder"], token) if current["PurchaseOrder"] else []
                    except Exception as e:
                        current["BCOptions"] = [{"No": "ERROR", "Description": str(e)}]
                    items.append(current)

                item_y0 = b[1]
                item_line_acc = txt
                j = i + 1
                while j < len(all_blocks) and abs(all_blocks[j][1] - item_y0) < 10:
                    item_line_acc += " " + all_blocks[j][4].strip()
                    j += 1

                price_nums = re.findall(r"\d{1,3}(?:,\d{3})*\.\d{2}", item_line_acc)
                total_price = float(price_nums[-1].replace(",", "")) if price_nums else None
                parts = item_line_acc.split()
                item_no = parts[1]
                ui = parts.index("EA") if "EA" in parts else len(parts)
                desc = " ".join(parts[2:ui])
                m_pkg = re.search(r"(\d+)(?=\s*[Mm]\b|\s*[Ll][Bb]\b|M$|LB$)", desc)
                pkg_qty = int(m_pkg.group(1)) if m_pkg else None
                shipped = float(parts[ui + 2].replace(',', '')) if len(parts) > ui + 2 else None
                
                usd_actual_cost = None
                if all(v is not None and v != 0 for v in [pkg_qty, shipped, total_price]):
                    usd_actual_cost = "{:.4f}".format(total_price / (shipped * pkg_qty))
                    
                # Standard calc
                original_qty = int(shipped * pkg_qty) if shipped and pkg_qty else shipped

                current = {
                    "VendorItemNumber": item_no, "VendorDescription": desc, "QtyShipped": shipped,
                    "OriginalReceivedQty": original_qty, "USD_Actual_Cost_$": usd_actual_cost, 
                    "PackageDescription": find_best_package_description(desc),
                    "TreatmentName": None, "PurchaseOrder": "", "TotalPrice": total_price, "Lots": []
                }
                text_acc = item_line_acc + "\n"
                i = j
                continue

            if current:
                text_acc += txt + "\n"
                if re.match(r"^\d{6}-\d{3}", txt):
                    current["Lots"].append(b[4])
            i += 1

        if current:
            treatment_name = re.search(r"Treatment name:\s*(.*)", text_acc)
            current["TreatmentName"] = treatment_name.group(1).strip() if treatment_name else None
            po_match = re.search(r"(?:PO|Purchase\s+order)[-\s:]*(\d{5})\b", text_acc, re.IGNORECASE)
            current["PurchaseOrder"] = (f"PO-{po_match.group(1)}" if po_match else header_po)
            try:
                current["BCOptions"] = get_po_items(current["PurchaseOrder"], token) if current["PurchaseOrder"] else []
            except Exception as e:
                current["BCOptions"] = [{"No": "ERROR", "Description": str(e)}]
            items.append(current)

        return items
    finally:
        if doc: doc.close()         

@timed_func("extract_sakata_data_from_bytes")
def extract_sakata_data_from_bytes(pdf_files: list[tuple[str, bytes]], token: str = "") -> dict[str, list[dict]]:
    if not pdf_files: return {}

    first_filename, first_bytes = pdf_files[0]
    with fitz.open(stream=first_bytes, filetype="pdf") as doc0:
        hdr = doc0[0].get_text()
        fallback_po = ""
        m = re.search(r"Purchase\s+order\s*[:\-]?(.*?)(?:Terms of payment|Ship to)", hdr, re.IGNORECASE | re.DOTALL) or \
            re.search(r"Customer\s+reference\s*[:\-]?(.*?)(?:Terms of delivery|Ship to)", hdr, re.IGNORECASE | re.DOTALL)
        if m:
            nums = re.findall(r"\b(\d{5})\b", m.group(1))
            if nums: fallback_po = " | ".join(f"PO-{n}" for n in nums)
    
    logger.info(f"Using fallback PO: {fallback_po}")
    report_map = extract_seed_analysis_reports_from_bytes(pdf_files)
    grouped_results = {}

    for filename, pdf_bytes in pdf_files:
        is_invoice = True
        extraction_method = "PyMuPDF"
        full_doc_text = ""

        try:
            with fitz.open(stream=pdf_bytes, filetype="pdf") as temp_doc:
                full_doc_text = "".join(page.get_text() for page in temp_doc)
                text_len = len(full_doc_text.strip())

                if text_len < 200:
                    logger.info(f"'{filename}': only {text_len} chars from PyMuPDF - attempting Azure OCR.")
                    try:
                        ocr_text = _extract_text_with_azure_ocr(pdf_bytes)
                        full_doc_text = ocr_text
                        extraction_method = "Azure OCR"
                        logger.info(f"Azure OCR succeeded for '{filename}': {len(ocr_text)} chars extracted.")
                    except Exception as ocr_err:
                        logger.warning(f"Azure OCR failed: {ocr_err}")
                        is_invoice = False

                _NON_INVOICE_PHRASES = ["Report of Seed Analysis", "Purity Analysis", "Purity\nAnalysis", "Packing List", "Pro forma packing slip"]
                
                if is_invoice:
                    if any(phrase in full_doc_text for phrase in _NON_INVOICE_PHRASES):
                        is_invoice = False
                        logger.info(f"Skipping supporting document (non-invoice): {filename}")
                    elif "INV-" in full_doc_text or "Invoice number" in full_doc_text:
                        is_invoice = True
                    else:
                        is_invoice = True
        except Exception as e:
            logger.warning(f"Could not read PDF '{filename}', skipping. Error: {e}")
            continue

        # --- ADDED DEBUG PRINTING ---
        print(f"\n{'='*30} START RAW TEXT: {filename} ({extraction_method}) {'='*30}")
        print(full_doc_text)
        print(f"{'='*32} END RAW TEXT: {filename} {'='*32}\n")
        # ----------------------------

        if not is_invoice: continue

        logger.info(f"Processing as invoice: {filename} (method: {extraction_method})")

        page_count = 0
        with fitz.open(stream=pdf_bytes, filetype="pdf") as d: page_count = d.page_count
        log_processing_event(
            vendor='Sakata', filename=filename,
            extraction_info={'method': extraction_method, 'page_count': page_count},
            po_number=fallback_po if fallback_po else None
        )

        m_inv = re.search(r"INV[-\s]*0*([0-9]+)", full_doc_text, re.IGNORECASE)
        invoice_id = m_inv.group(1) if m_inv else ""

        if extraction_method == "Azure OCR":
            raw_items = _extract_invoice_from_ocr_text(full_doc_text, fallback_po, token)
        else:
            raw_items = extract_invoice_from_pdf(source=pdf_bytes, fallback_po=fallback_po, token=token)

        for itm in raw_items:
            parsed_lots = []
            for raw_lot_text in itm.get("Lots", []):
                if extraction_method == "Azure OCR":
                    lot = _parse_ocr_lot_line(raw_lot_text)
                else:
                    lot = parse_lot_block(raw_lot_text)
                
                # INJECT INVOICE-LEVEL FIELDS INTO LOT
                lot["USD_Actual_Cost_$"] = itm.get("USD_Actual_Cost_$")
                lot["PackageDescription"] = itm.get("PackageDescription")
                lot["OriginalReceivedQty"] = itm.get("OriginalReceivedQty")
                
                # INJECT SEED ANALYSIS REPORT DATA
                vendor_lot_no = lot.get("VendorLotNo")
                if vendor_lot_no in report_map:
                    lot.update(report_map[vendor_lot_no])
                
                lot["PurchaseOrder"] = itm.get("PurchaseOrder") or fallback_po
                lot["InvoiceNumber"] = invoice_id
                parsed_lots.append(lot)
            
            itm["Lots"] = parsed_lots
        
        if raw_items: grouped_results[filename] = raw_items

    return grouped_results