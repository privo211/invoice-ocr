import os
import re
import fitz  # PyMuPDF
from typing import List, Dict, TypedDict, Union, Tuple
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

def _normalize_ocr_text(ocr_text: str) -> str:
    """Pre-process Azure OCR output to fix common artifacts."""
    text = ocr_text
    # Rejoin split 4-digit years: "2/26/202\n6" → "2/26/2026"
    text = re.sub(r"(\d{1,2}/\d{1,2}/\d{2})\n(\d)\b", r"\1\2", text)
    text = re.sub(r"(\d{1,2}/\d{1,2}/\d{3})\n(\d)\b", r"\1\2", text)
    # Handle date split with intervening tokens: "2/26/202\nUSA\n6" → "2/26/2026\nUSA"
    text = re.sub(r"(\d{1,2}/\d{1,2}/\d{2})\n([A-Z]{2,3})\n(\d)\b", r"\1\3\n\2", text)
    text = re.sub(r"(\d{1,2}/\d{1,2}/\d{3})\n([A-Z]{2,3})\n(\d)\b", r"\1\3\n\2", text)
    return text

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
            full_text = "\n".join(lines)
            print(f"DEBUG: Azure OCR Extracted Text:\n{full_text}")
            return full_text
        if result.get("status") == "failed":
            raise RuntimeError("Azure OCR analysis failed.")
    raise TimeoutError("Azure OCR timed out.")

def _parse_ocr_lot_line(chunk: str, vendor_lot_no: str) -> Dict:
    lot = {
        "VendorLotNo": vendor_lot_no, "CurrentGerm": None, "GermDate": None,
        "SeedCount": None, "SeedSize": None, "GrowerGerm": None,
        "GrowerGermDate": None, "Purity": None, "OriginCountry": "",
        "SproutCount": None, "Inert": None
    }

    lot_num_pattern = re.escape(vendor_lot_no)
    m = re.search(lot_num_pattern + r"\s*\n(.*)", chunk, re.DOTALL)
    if not m:
        return lot
    
    data_region = m.group(1)
    data_lines = []
    for line in data_region.split("\n"):
        line = line.strip()
        if line and not line.startswith("Other Charges") and not re.match(r"^(Ordered|Shipped|Disc|Total|All invoiced|Gross|Total Miscellaneous|Cash discount|Prepaid|Invoice total)", line, re.IGNORECASE):
            data_lines.append(line)
        elif len(data_lines) > 0:
            break
    data_text = " ".join(data_lines)
    tokens = data_text.split()

    germ_date = None
    seed_count = None
    purity = None
    germ = None
    qty = None
    origin = None
    seed_size_candidates = []

    for token in tokens:
        token_clean = token.rstrip(",")
        if re.match(r"^\d{1,2}/\d{1,2}/\d{4}$", token_clean):
            germ_date = token_clean
        elif re.match(r"^\d{1,3},\d{3}$", token_clean):
            seed_count = int(token_clean.replace(",", ""))
        elif re.match(r"^9\d\.\d{2}$", token_clean):
            purity = float(token_clean)
        elif re.match(r"^(8[0-9]|9[0-9]|100)$", token_clean):
            germ = int(token_clean)
        elif re.match(r"^\d+\.\d{2}$", token_clean) and qty is None:
            qty = float(token_clean)
        elif re.match(r"^[A-Z]{2,3}$", token_clean):
            if token_clean not in ["EA", "INV", "USD", "PO", "QTY", "KS", "M", "LB"]:
                origin = convert_to_alpha2(token_clean)
        elif token_clean not in ["%", "X"]:
            seed_size_candidates.append(token_clean)

    lot["GermDate"] = germ_date
    lot["SeedCount"] = seed_count
    lot["Purity"] = purity
    lot["CurrentGerm"] = germ
    lot["OriginCountry"] = origin or ""

    if seed_size_candidates:
        for candidate in seed_size_candidates:
            if re.match(r"^\d+/$", candidate):
                continue
            if re.match(r"^\d+\+$", candidate):
                continue
            lot["SeedSize"] = candidate
            break

    return lot

def _extract_invoice_from_ocr_text(ocr_text: str, fallback_po: str, token: str) -> List[Dict]:
    """Anchor-based extraction for OCR results where field ordering may be unstable."""
    items = []
    
    # Normalize OCR text first
    ocr_text = _normalize_ocr_text(ocr_text)
    
    # Header PO
    m_po = re.search(r"(?:Purchase\s+order)\s*(\d{5})\b", ocr_text, re.IGNORECASE)
    header_po = f"PO-{m_po.group(1)}" if m_po else fallback_po

    lines = [l.strip() for l in ocr_text.splitlines() if l.strip()]
    
    # 1. Identify anchors (8-digit item numbers)
    item_indices = []
    for i, line in enumerate(lines):
        if re.match(r"^\d{8}$", line):
            item_indices.append(i)
    
    # 2. Process each item block
    for k, start_idx in enumerate(item_indices):
        end_idx = item_indices[k+1] if k + 1 < len(item_indices) else len(lines)
        
        item_no = lines[start_idx]
        
        # Multi-line description: collect until boundary
        desc_lines = []
        desc_end_idx = start_idx + 1
        boundary_patterns = [
            r"^Alternate item:", r"^Botanical name:", r"^Treatment name:",
            r"^Comment:", r"^Line Item", r"^LotNo", r"^Other Charges"
        ]
        while desc_end_idx < end_idx:
            line = lines[desc_end_idx]
            if any(re.match(p, line) for p in boundary_patterns):
                break
            if re.match(r"^EA\b", line) or line == "EA":
                break
            desc_lines.append(line)
            desc_end_idx += 1
        
        desc = " ".join(desc_lines).strip()
        
        chunk_text = "\n".join(lines[start_idx:end_idx])
        full_context = "\n".join(lines[start_idx:])
        
        # Lots (define pattern early for financials fallback)
        lot_pattern = re.compile(r"\b(\d{6}-\d{3})\b")
        
        # Financials: two possible layouts
        shipped = None
        total_price = None
        
        # Layout B: EA followed by value lines (most common)
        ea_idx = None
        for i in range(start_idx, end_idx):
            if lines[i].strip() == "EA":
                ea_idx = i
                break
        
        if ea_idx is not None and ea_idx + 4 < end_idx:
            vals = []
            for i in range(ea_idx + 1, end_idx):
                line = lines[i]
                if re.match(r"^[\d,]+\.?\d*$", line.strip()):
                    try:
                        vals.append(float(line.strip().replace(",", "")))
                    except ValueError:
                        pass
                elif vals:
                    break
            
            if len(vals) >= 5:
                shipped = vals[1]
                total_price = vals[4]
            elif len(vals) >= 2:
                shipped = vals[0]
                total_price = vals[-1]
        
        # Layout A: financials after lot data (Ordered/Shipped/Unit price/Total price labels)
        if shipped is None or total_price is None:
            lot_end = 0
            for lm in lot_pattern.finditer(chunk_text):
                lot_end = lm.end()
            
            if lot_end > 0:
                after_lots = chunk_text[lot_end:]
                
                # Extract all financial section numbers in order
                fin_section = re.search(r"Ordered\s*\n(.*)", after_lots, re.IGNORECASE | re.DOTALL)
                if fin_section:
                    fin_text = fin_section.group(1)
                    fin_numbers = re.findall(r"([\d,]+\.\d{2})", fin_text)
                    fin_clean = []
                    for n in fin_numbers:
                        try:
                            fin_clean.append(float(n.replace(",", "")))
                        except ValueError:
                            pass
                    
                    if len(fin_clean) >= 4:
                        if shipped is None:
                            shipped = fin_clean[1]
                        if total_price is None:
                            total_price = fin_clean[-1]
                
                if shipped is None:
                    ship_m = re.search(r"Shipped\s+Unit\s+price\s*\n([\d,]+\.?\d*)", after_lots, re.IGNORECASE)
                    if ship_m:
                        shipped = float(ship_m.group(1).replace(",", ""))
                
                if total_price is None:
                    # Handle "Disc % Total price\n25\n465.00" pattern
                    disc_total_m = re.search(r"Disc\s+%.*?\n(\d+)\s+(\d+\.?\d*)", after_lots, re.IGNORECASE | re.DOTALL)
                    if disc_total_m:
                        total_price = float(disc_total_m.group(2).replace(",", ""))
                    else:
                        total_m = re.search(r"Total\s+price\s*\n(\d+\.?\d*)", after_lots, re.IGNORECASE)
                        if total_m:
                            total_price = float(total_m.group(1).replace(",", ""))

        # Package description: find package qty line (e.g., "5M", "100M", "25 lb")
        pkg_qty_line = ""
        for i in range(start_idx, end_idx):
            line = lines[i]
            if re.match(r"^\d+\s*[Mm]$", line) or re.match(r"^\d+\s*[Ll][Bb]$", line):
                pkg_qty_line = line
                break
        
        enriched_desc = desc
        if pkg_qty_line:
            enriched_desc = desc + " " + pkg_qty_line
        
        # Package Qty from enriched desc
        m_pkg = re.search(r"(\d+)\s*[Mm]\b", enriched_desc, re.IGNORECASE)
        if not m_pkg:
            m_pkg = re.search(r"(\d+)\s*[Ll][Bb]\b", enriched_desc, re.IGNORECASE)
        pkg_qty = int(m_pkg.group(1)) if m_pkg else None

        usd_actual_cost = None
        if pkg_qty and shipped and total_price and shipped != 0 and pkg_qty != 0:
            usd_actual_cost = "{:.4f}".format(total_price / (shipped * pkg_qty))
            
        original_qty = int(shipped * pkg_qty) if shipped and pkg_qty else None

        # Lots
        lots_raw = []
        lot_matches = list(lot_pattern.finditer(chunk_text))
        
        for j, lm in enumerate(lot_matches):
            vendor_lot_no = lm.group(1)
            l_start = lm.start()
            l_end = lot_matches[j+1].start() if j + 1 < len(lot_matches) else len(chunk_text)
            lot_chunk = chunk_text[l_start:min(l_end + 50, len(chunk_text))]
            lots_raw.append(_parse_ocr_lot_line(lot_chunk, vendor_lot_no))

        # Metadata
        treatment_name = None
        tn_match = re.search(r"Treatment name:\s*(.*)", chunk_text)
        if tn_match:
            treatment_name = tn_match.group(1).strip()

        item_po_match = re.search(r"(?:PO|Purchase\s+order)[#\s\-:]*(\d{5})\b", chunk_text, re.IGNORECASE)
        item_po = f"PO-{item_po_match.group(1)}" if item_po_match else header_po

        current = {
            "VendorItemNumber": item_no,
            "VendorDescription": desc,
            "QtyShipped": shipped,
            "OriginalReceivedQty": original_qty, 
            "USD_Actual_Cost_$": usd_actual_cost,
            "PackageDescription": find_best_package_description(enriched_desc),
            "TreatmentName": treatment_name,
            "PurchaseOrder": item_po,
            "TotalPrice": total_price,
            "Lots": lots_raw  
        }

        try:
            current["BCOptions"] = get_po_items(current["PurchaseOrder"], token) if current["PurchaseOrder"] else []
        except Exception:
            current["BCOptions"] = []

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
        return ""
    pkg_desc_list = _pkg_desc_list

    normalized = normalize_text(vendor_desc)
    # Case-insensitive match for M or LB
    m = re.search(r"(\d+)\s*(M|LB)\b", normalized, re.IGNORECASE)
    if m:
        qty = int(m.group(1))
        unit = m.group(2).upper()
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
    except Exception: pass

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
        except Exception: pass

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
    """Legacy parser for PyMuPDF text blocks."""
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
    """Page-by-page OCR fallback for Seed Analysis Reports."""
    report_map: Dict[str, PurityData] = {}
    for fname, bts in pdf_files:
        try:
            full_text_parts = []
            with fitz.open(stream=bts, filetype="pdf") as doc:
                for i, page in enumerate(doc):
                    pg_text = page.get_text()
                    # individual page sanity check
                    if len(pg_text.strip()) < 100:
                        logger.info(f"'{fname}' p{i+1} appears scanned. Attempting OCR.")
                        temp_doc = fitz.open()
                        temp_doc.insert_pdf(doc, from_page=i, to_page=i)
                        pg_bts = temp_doc.tobytes()
                        temp_doc.close()
                        try:
                            ocr_pg_text = _extract_text_with_azure_ocr(pg_bts)
                            full_text_parts.append(ocr_pg_text)
                        except Exception as e:
                            logger.warning(f"OCR failed for {fname} p{i+1}: {e}")
                    else:
                        full_text_parts.append(pg_text)
            
            text = "\n".join(full_text_parts)
            print(f"DEBUG: Combined Report Text ({fname}):\n{text}")
            
            # Skip if it's not a Seed Analysis Report (e.g. Health Report)
            if "Report of Seed Analysis" not in text and "Purity Analysis" not in text:
                continue

            lot_match = re.search(r"Lot Number:[\s\n]*([\d-]+)", text, re.IGNORECASE)
            if not lot_match: continue
            lot_no = lot_match.group(1).strip()

            date_match = re.search(r"Date Issued:[\s\n]*(\d{1,2}/\d{1,2}/\d{4})", text, re.IGNORECASE)
            date_issued = date_match.group(1) if date_match else None

            pure, inert, normal = None, None, None
            
            # Search for numerical values in standard report layout
            nums_match = re.search(r"\b(\d{2}\.\d{2})\s+(\d{1,2}\.\d{2})\s+(?:\d{1,2}\.\d{2})\s+(?:\d{1,2}\.\d{2})\s+(\d{1,3})\b", text)
            
            if nums_match:
                pure = float(nums_match.group(1))
                inert = float(nums_match.group(2))
                normal = int(nums_match.group(3))
            else:
                # Fallback to token-based search
                header_end_match = re.search(r"Days[\s\n]*Tested", text, re.IGNORECASE)
                if header_end_match:
                    subtext = text[header_end_match.end():]
                    tokens = re.findall(r"\b(?:\d+(?:\.\d+)?|X|TR|-TR-)\b", subtext)
                    if len(tokens) >= 5:
                        p_val, i_val, n_val = tokens[0], tokens[1], tokens[4]
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
    """Legacy PyMuPDF invoice extractor."""
    doc = None
    try:
        if isinstance(source, bytes): doc = fitz.open(stream=source, filetype="pdf")
        elif isinstance(source, str): doc = fitz.open(source)
        else: raise ValueError("Source must be file path (str) or bytes.")
        
        first_page_text = doc[0].get_text()
        
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
                    po_match = re.search(r"(?:PO|Purchase\s+order)[#\s\-:]*(\d{5})\b", text_acc, re.IGNORECASE)
                    current["PurchaseOrder"] = (f"PO-{po_match.group(1)}" if po_match else header_po)
                    try:
                        current["BCOptions"] = get_po_items(current["PurchaseOrder"], token) if current["PurchaseOrder"] else []
                    except Exception:
                        current["BCOptions"] = []
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
                    
                original_qty = int(shipped * pkg_qty) if shipped and pkg_qty else None

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
            po_match = re.search(r"(?:PO|Purchase\s+order)[#\s\-:]*(\d{5})\b", text_acc, re.IGNORECASE)
            current["PurchaseOrder"] = (f"PO-{po_match.group(1)}" if po_match else header_po)
            try:
                current["BCOptions"] = get_po_items(current["PurchaseOrder"], token) if current["PurchaseOrder"] else []
            except Exception:
                current["BCOptions"] = []
            items.append(current)

        return items
    finally:
        if doc: doc.close()         

@timed_func("extract_sakata_data_from_bytes")
def extract_sakata_data_from_bytes(pdf_files: list[tuple[str, bytes]], token: str = "") -> dict[str, list[dict]]:
    if not pdf_files: return {}

    # Extract global PO fallback
    first_filename, first_bytes = pdf_files[0]
    fallback_po = ""
    try:
        with fitz.open(stream=first_bytes, filetype="pdf") as doc0:
            hdr = doc0[0].get_text()
            m = re.search(r"Purchase\s+order\s*[:\-]?(.*?)(?:Terms of payment|Ship to)", hdr, re.IGNORECASE | re.DOTALL) or \
                re.search(r"Customer\s+reference\s*[:\-]?(.*?)(?:Terms of delivery|Ship to)", hdr, re.IGNORECASE | re.DOTALL)
            if m:
                nums = re.findall(r"\b(\d{5})\b", m.group(1))
                if nums: fallback_po = " | ".join(f"PO-{n}" for n in nums)
    except Exception: pass
    
    logger.info(f"Using fallback PO: {fallback_po}")
    
    # 1. Build report map
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
                    logger.info(f"'{filename}': low searchable text ({text_len} chars). Attempting Azure OCR.")
                    try:
                        ocr_text = _extract_text_with_azure_ocr(pdf_bytes)
                        full_doc_text = ocr_text
                        extraction_method = "Azure OCR"
                    except Exception as ocr_err:
                        logger.warning(f"Azure OCR failed for {filename}: {ocr_err}")
                        is_invoice = False

                _NON_INVOICE_PHRASES = ["Report of Seed Analysis", "Purity Analysis", "Packing List", "Pro forma packing slip"]
                if is_invoice:
                    if any(phrase in full_doc_text for phrase in _NON_INVOICE_PHRASES):
                        is_invoice = False
                        logger.info(f"Skipping supporting document: {filename}")
                    elif "INV-" not in full_doc_text and "Invoice number" not in full_doc_text:
                        # Final check for invoice signature
                        is_invoice = False
        except Exception as e:
            logger.warning(f"Error reading {filename}: {e}")
            continue

        if not is_invoice: continue

        logger.info(f"Processing as invoice: {filename} (method: {extraction_method})")

        page_count = 0
        try:
            with fitz.open(stream=pdf_bytes, filetype="pdf") as d: page_count = d.page_count
            log_processing_event(
                vendor='Sakata', filename=filename,
                extraction_info={'method': extraction_method, 'page_count': page_count},
                po_number=fallback_po if fallback_po else None
            )
        except Exception: pass

        m_inv = re.search(r"INV[-\s]*0*([0-9]+)", full_doc_text, re.IGNORECASE)
        invoice_id = m_inv.group(1) if m_inv else ""

        if extraction_method == "Azure OCR":
            raw_items = _extract_invoice_from_ocr_text(full_doc_text, fallback_po, token)
        else:
            raw_items = extract_invoice_from_pdf(source=pdf_bytes, fallback_po=fallback_po, token=token)

        for itm in raw_items:
            parsed_lots = []
            for lot_data in itm.get("Lots", []):
                
                if extraction_method != "Azure OCR":
                    lot = parse_lot_block(lot_data)
                else:
                    lot = lot_data
                
                lot["USD_Actual_Cost_$"] = itm.get("USD_Actual_Cost_$")
                lot["PackageDescription"] = itm.get("PackageDescription")
                lot["OriginalReceivedQty"] = itm.get("OriginalReceivedQty")
                
                vendor_lot_no = lot.get("VendorLotNo")
                if vendor_lot_no in report_map:
                    lot.update(report_map[vendor_lot_no])
                
                lot["PurchaseOrder"] = itm.get("PurchaseOrder") or fallback_po
                lot["InvoiceNumber"] = invoice_id
                parsed_lots.append(lot)
            
            itm["Lots"] = parsed_lots
        
        if raw_items: grouped_results[filename] = raw_items

    return grouped_results
