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

load_dotenv()
BC_TENANT  = os.getenv("AZURE_TENANT_ID")
BC_ENV      = "Production"
BC_COMPANY = os.getenv("BC_COMPANY")
CLIENT_ID     = os.getenv("AZURE_CLIENT_ID")
CLIENT_SECRET = os.getenv("AZURE_CLIENT_SECRET")

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
    Purity: Union[float, str]
    Inert: Union[float, str]
    GrowerGerm: Union[float, None]
    GrowerGermDate: Union[str, None]
    
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
    """Return cached {No, Description} rows from BC Items."""
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


#─── FETCH “Package Descriptions” ───
_pkg_desc_list = None

@timed_func("load_package_descriptions")
def load_package_descriptions(token: str) -> list[str]:
    """
    Fetch all rows from BC OData endpoint /Package_Descriptions_List_Excel, and build a list of package-description strings.
    """
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
    # remove commas, extra spaces, uppercase
    return re.sub(r"[,]", "", s).strip().upper()

@timed_func("find_best_package_description")
def find_best_package_description(vendor_desc: str) -> str:
    """
    Given a vendor description (e.g. "Beet Chioggia Guardsmark Treated 500M"),
    return the closest match from the live BC Package Descriptions.
    """
    global _pkg_desc_list
    if _pkg_desc_list is None:
        raise RuntimeError("Package descriptions not loaded. Ensure load_package_descriptions is called first.")
    pkg_desc_list = _pkg_desc_list

    normalized = normalize_text(vendor_desc)

    # 1) Try an exact numeric‐unit match, e.g. "500M" -> "500,000 SEEDS"
    m = re.search(r"(\d+)\s*(M|LB)\b", normalized)
    if m:
        qty = int(m.group(1))
        unit = m.group(2)

        if unit == "M":
            # “500M” → 500 * 1000 = 500,000
            seed_count = qty * 1000
            candidate = f"{seed_count:,} SEEDS"
        else:  # e.g. "25 LB"
            candidate = f"{qty} LB"

        candidate = candidate.strip().upper()
        if candidate in pkg_desc_list:
            return candidate

    # 2) Fallback: use fuzzy matching against all package descriptions
    matches = get_close_matches(normalized, pkg_desc_list, n=1, cutoff=0.6)
    return matches[0] if matches else ""

_po_cache = {}

@timed_func("get_po_items")
def get_po_items(po_number, token):
    if po_number in _po_cache:
        return _po_cache[po_number]

    # Split PO string if multiple are given
    po_numbers = [po.strip() for po in po_number.split("|") if po.strip()]
    if not po_numbers:
        return []

    if len(po_numbers) == 1:
        filter_clause = f"PurchaseOrderNo eq '{po_numbers[0]}'"
    else:
        filter_clause = " or ".join(f"PurchaseOrderNo eq '{po}'" for po in po_numbers)

    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json"
    }

    # Try the main PurchaseOrderQuery
    url_main = (
        f"https://api.businesscentral.dynamics.com/v2.0/{BC_TENANT}/{BC_ENV}"
        f"/ODataV4/Company('Stokes%20Seeds%20Limited')/PurchaseOrderQuery?$filter={filter_clause}"
    )
    
    response = requests.get(url_main, headers=headers)
    response.raise_for_status()
    data = []
    seen = set()

    for item in response.json().get("value", []):
        no = item.get("ItemNumber")
        if not no or no in seen:
            continue
        seen.add(no)
        data.append({
            "No": no,
            "Description": item.get("ItemDescription", "")
        })

    # If no results, try the ArchivePurchaseOrderQuery
    if not data:
        url_archive = (
            f"https://api.businesscentral.dynamics.com/v2.0/{BC_TENANT}/{BC_ENV}"
            f"/ODataV4/Company('Stokes%20Seeds%20Limited')/ArchivePurchaseOrderQuery?$filter={filter_clause}"
        )
        response = requests.get(url_archive, headers=headers)
        response.raise_for_status()
        data = []
        seen = set()

        for item in response.json().get("value", []):
            no = item.get("ItemNumber")
            if not no or no in seen:
                continue
            seen.add(no)
            data.append({
                "No": no,
                "Description": item.get("ItemDescription", "")
            })

    _po_cache[po_number] = data
    return data

def convert_to_alpha2(country_value: str) -> str:
    """
    Converts a country representation (full name, alpha-3, or alpha-2) to ISO Alpha-2 code.
    """
    if not country_value:
        return ""
    
    country_value = country_value.strip()
    
    if len(country_value) == 2:
        return country_value.upper()
    
    try:
        country = pycountry.countries.lookup(country_value)
        return country.alpha_2
    except LookupError:
        return country_value

@timed_func("parse_lot_block")
def parse_lot_block(raw_text: str) -> Dict:
    """
    Parse the raw text block of a single lot into structured fields.
    """
    parts = raw_text.replace(",", "").split("\n")
    try:
        lot_no = parts[0].strip()
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
                purity = float(text5)
            else:
                seed_size = text5

        if seed_size:
            m = re.search(r"(\d+\.\d{2})", seed_size)
            if m:
                purity = float(m.group(1))
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
    """
    Extracts purity data from all seed analysis reports in a batch of PDF files.
    """
    report_map: Dict[str, PurityData] = {}

    for fname, bts in pdf_files:
        try:
            doc = fitz.open(stream=bts, filetype="pdf")
            full_text = ""
            for page in doc:
                t = page.get_text()
                if "Purity Analysis" in t:
                    full_text += t + "\n"
            doc.close()

            if not full_text:
                continue
            
            logger.info(f"Found 'Purity Analysis' content in {fname}")
            segments = re.split(r"Lot Number:\s*([\d-]+)", full_text)
            for i in range(1, len(segments), 2):
                lot_no = segments[i].strip()
                block = segments[i + 1]
                lines = [l.strip() for l in block.splitlines() if l.strip()]

                date_completed = None
                for k, line in enumerate(lines):
                    if re.search(r"^Date\s+Test\s+Completed\s*:", line, re.IGNORECASE):
                        if k + 1 < len(lines):
                            date_str = lines[k + 1].strip()
                            if re.match(r"\d{1,2}/\d{1,2}/\d{4}", date_str):
                                date_completed = date_str
                        break

                header_idx = next(
                    (j for j, l in enumerate(lines)
                    if re.search(r"Weed\s*%", l, re.IGNORECASE) or re.search(r"Weed\s+Seed\s*%", l, re.IGNORECASE)),
                    None
                )

                if header_idx is None:
                    continue

                vals = []
                for ln in lines[header_idx + 1:]:
                    tokens = re.split(r"\s+", ln)
                    for tok in tokens:
                        if re.fullmatch(r"(TR|-TR-)", tok, re.IGNORECASE):
                            vals.append("0.0")
                        elif re.fullmatch(r"\d+(?:\.\d+)?", tok):
                            vals.append(tok)
                        if len(vals) == 5: break
                    if len(vals) == 5: break

                if len(vals) < 5: continue

                pure, inert, _, _, grower_germ = vals
                pure_val = float(pure)
                if pure_val == 100.0:
                    pure, inert = "99.99", "0.01"

                report_map[lot_no] = {
                    "Purity": pure, "Inert": inert,
                    "GrowerGerm": grower_germ, "GrowerGermDate": date_completed
                }
        except Exception as e:
            logger.warning(f"Could not process {fname} for seed analysis. Error: {e}")
            continue
            
    return report_map

@timed_func("extract_invoice_from_pdf")
def extract_invoice_from_pdf(
    source: Union[str, bytes],
    fallback_po: str = "",
    token: str = ""
) -> List[Dict]:
    """
    Extracts Sakata invoice data from a single PDF source (path or bytes).
    """
    doc = None
    try:
        if isinstance(source, bytes):
            doc = fitz.open(stream=source, filetype="pdf")
        elif isinstance(source, str):
            doc = fitz.open(source)
        else:
            raise ValueError("Source must be file path (str) or bytes.")
        
        first_page_text = doc[0].get_text()
        header_po_match = re.search(r"\bPO[-\s#:]*(\d{5})\b", first_page_text, re.IGNORECASE)
        header_po_match = header_po_match or re.search(
            r"Purchase\s*order.*?(\d{5})", first_page_text, re.IGNORECASE | re.DOTALL
        )
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
            print(txt)

            if re.match(r"^\d+\s+\d{8}", txt) and ("Treated" in txt or "Untreated" in txt):
                if current:
                    treatment_name = re.search(r"Treatment name:\s*(.*)", text_acc)
                    current["TreatmentName"] = treatment_name.group(1).strip() if treatment_name else None
                    
                    # po_match = re.search(r"\bPO[-\s:]*(\d{5})\b", text_acc, re.IGNORECASE)
                    # current["PurchaseOrder"] = f"PO-{po_match.group(1)}" if po_match else fallback_po

                    # Primary pattern: "PO-12345" etc.
                    po_match = re.search(r"\bPO[-\s:]*(\d{5})\b", text_acc, re.IGNORECASE)
                    current["PurchaseOrder"] = (f"PO-{po_match.group(1)}" if po_match else header_po)

                    # if po_match:
                        

                    #     current["PurchaseOrder"] = f"PO-{po_match.group(1)}"
                    # else:
                    #     # Secondary fallback: find after "Purchase order" even several lines later
                    #     po_match = re.search(
                    #         r"Purchase\s*order[\s\S]*?(\d{5})",
                    #         text_acc,
                    #         re.IGNORECASE
                    #     )
                    #     if po_match:
                    #         current["PurchaseOrder"] = f"PO-{po_match.group(1)}"
                    #     else:
                    #         current["PurchaseOrder"] = fallback_po

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
                shipped = int(float(parts[ui + 2])) if len(parts) > ui + 2 else None
                
                usd_actual_cost = None
                if all(v is not None and v != 0 for v in [pkg_qty, shipped, total_price]):
                    usd_actual_cost = round((total_price / (shipped * pkg_qty)), 4)
                    print(f"USD Actual Cost: {usd_actual_cost}: {total_price} / ({shipped} * {pkg_qty})")

                current = {
                    "VendorItemNumber": item_no, "VendorDescription": desc, "QtyShipped": shipped,
                    "USD_Actual_Cost_$": usd_actual_cost, "PackageDescription": find_best_package_description(desc),
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
            
            # po_match = re.search(r"\bPO[-\s:]*(\d{5})\b", text_acc, re.IGNORECASE)
            # current["PurchaseOrder"] = f"PO-{po_match.group(1)}" if po_match else fallback_po
            
            # Primary pattern: "PO-12345" etc.
            po_match = re.search(r"\bPO[-\s:]*(\d{5})\b", text_acc, re.IGNORECASE)
            current["PurchaseOrder"] = (f"PO-{po_match.group(1)}" if po_match else header_po)

            # if po_match:
            #     current["PurchaseOrder"] = f"PO-{po_match.group(1)}"
            # else:
            #     # Secondary fallback: find after "Purchase order" even several lines later
            #     po_match = re.search(
            #         r"Purchase\s*order[\s\S]*?(\d{5})",
            #         text_acc,
            #         re.IGNORECASE
            #     )
            #     if po_match:
            #         current["PurchaseOrder"] = f"PO-{po_match.group(1)}"
            #     else:
            #         current["PurchaseOrder"] = fallback_po

                    
            try:
                current["BCOptions"] = get_po_items(current["PurchaseOrder"], token) if current["PurchaseOrder"] else []
            except Exception as e:
                current["BCOptions"] = [{"No": "ERROR", "Description": str(e)}]
            items.append(current)

        return items
    finally:
        if doc:
            doc.close()

@timed_func("extract_sakata_data_from_bytes")
def extract_sakata_data_from_bytes(pdf_files: list[tuple[str, bytes]], token: str = "") -> dict[str, list[dict]]:
    """
    Memory-only Sakata extraction. Processes a batch of uploaded files, linking invoices
    to their corresponding seed analysis reports.

    Args:
        pdf_files: A list of (filename, bytes) tuples for all uploaded files in the batch.
        token: The Business Central API token.

    Returns:
        A dictionary where keys are the filenames of the invoices and values are the
        lists of extracted item data from that invoice.
    """
    if not pdf_files:
        return {}

    # --- Step 1: Get fallback PO from the first PDF in the batch ---
    first_filename, first_bytes = pdf_files[0]
    with fitz.open(stream=first_bytes, filetype="pdf") as doc0:
        hdr = doc0[0].get_text()
        fallback_po = ""
        m = re.search(
            r"Purchase\s+order\s*[:\-]?(.*?)(?:Terms of payment|Ship to)",
            hdr, re.IGNORECASE | re.DOTALL
        ) or re.search(
            r"Customer\s+reference\s*[:\-]?(.*?)(?:Terms of delivery|Ship to)",
            hdr, re.IGNORECASE | re.DOTALL
        )
        if m:
            nums = re.findall(r"\b(\d{5})\b", m.group(1))
            if nums:
                fallback_po = " | ".join(f"PO-{n}" for n in nums)
    
    logger.info(f"Using fallback PO: {fallback_po}")

    # --- Step 2: Extract all seed analysis purity data from the entire batch ---
    report_map = extract_seed_analysis_reports_from_bytes(pdf_files)
    logger.info(f"Extracted purity data for {len(report_map)} lots from analysis reports.")

    # --- Step 3: Process invoice PDFs and build grouped results ---
    grouped_results = {}
    for filename, pdf_bytes in pdf_files:
        is_invoice = True
        try:
            # CORRECTED LOGIC: Check all pages to correctly identify document type
            with fitz.open(stream=pdf_bytes, filetype="pdf") as temp_doc:
                # Concatenate text from all pages to perform the check
                full_doc_text = "".join(page.get_text() for page in temp_doc)
                
                # If the identifying phrase is anywhere in the doc, it's not a primary invoice
                if "Report of Seed Analysis" in full_doc_text or "Purity Analysis" in full_doc_text:
                    is_invoice = False
                    logger.info(f"Identified as analysis report/supporting doc; skipping invoice processing for: {filename}")

        except Exception as e:
            logger.warning(f"Could not read PDF '{filename}' to determine its type, skipping. Error: {e}")
            continue

        if not is_invoice:
            continue

        logger.info(f"Processing as invoice: {filename}")
        
        # This part remains the same
        with fitz.open(stream=pdf_bytes, filetype="pdf") as temp_doc:
            hdr = temp_doc[0].get_text()
        
        m = re.search(r"INV[-\s]*0*([0-9]+)", hdr, re.IGNORECASE)
        invoice_id = m.group(1) if m else ""

        raw_items = extract_invoice_from_pdf(
            source=pdf_bytes, fallback_po=fallback_po, token=token
        )

        for itm in raw_items:
            parsed_lots = []
            for raw_lot_text in itm.get("Lots", []):
                lot = parse_lot_block(raw_lot_text)
                lot["USD_Actual_Cost_$"] = itm.get("USD_Actual_Cost_$")
                lot["PackageDescription"] = itm.get("PackageDescription")
                
                vendor_lot_no = lot.get("VendorLotNo")
                if vendor_lot_no in report_map:
                    lot.update(report_map[vendor_lot_no])
                    logger.debug(f"Applied purity data to lot {vendor_lot_no} in invoice {filename}")
                
                lot["PurchaseOrder"] = itm.get("PurchaseOrder") or fallback_po
                lot["InvoiceNumber"] = invoice_id
                parsed_lots.append(lot)
            
            itm["Lots"] = parsed_lots
        
        if raw_items:
            grouped_results[filename] = raw_items

    return grouped_results