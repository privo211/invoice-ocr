import os
import re
import fitz  # PyMuPDF
from typing import List, Dict, TypedDict, Union
import requests
from difflib import get_close_matches
import time
import logging
from functools import wraps

# Use the root logger (or a named logger) instead of app.logger
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


# BC connection settings
BC_TENANT  = os.getenv("AZURE_TENANT_ID")
BC_ENV      = "Production"
BC_COMPANY = "Stokes%20Seeds%20Limited"

# Load from environment (.env)
CLIENT_ID     = os.getenv("AZURE_CLIENT_ID")
CLIENT_SECRET = os.getenv("AZURE_CLIENT_SECRET")

class PurityData(TypedDict):
    PureSeed: Union[float, str]
    Inert: Union[float, str]
    OtherCrop: Union[float, str]
    Weed: Union[float, str]
    
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
        f"{BC_TENANT}/Production/ODataV4/"
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
        f"{BC_TENANT}/Production/ODataV4/"
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
    environment = "SANDBOX-2025"
    tenant_id = "33b1b67a-786c-4b46-9372-c4e492d15cf1"

    # Split PO string if multiple are given
    po_numbers = [po.strip() for po in po_number.split("|") if po.strip()]
    if not po_numbers:
        return []

    if len(po_numbers) == 1:
        filter_clause = f"PurchaseOrderNo eq '{po_numbers[0]}'"
    else:
        filter_clause = " or ".join(f"PurchaseOrderNo eq '{po}'" for po in po_numbers)

    url = (
        f"https://api.businesscentral.dynamics.com/v2.0/{tenant_id}/Production"
        f"/ODataV4/Company('Stokes%20Seeds%20Limited')/PurchaseOrderQuery?$filter={filter_clause}"
    )

    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json"
    }

    response = requests.get(url, headers=headers)
    response.raise_for_status()

    data = [
        {"No": item["ItemNumber"], "Description": item["ItemDescription"]}
        for item in response.json().get("value", [])
        if item.get("ItemNumber")
    ]
    _po_cache[po_number] = data
    return data

alpha3_to_bc = {
    "ARE": "AE", "ARG": "AR", "AUT": "AT", "AUS": "AU", "AZE": "AZ",
    "BEL": "BE", "BGR": "BG", "BRN": "BN", "BOL": "BO", "BRA": "BR",
    "CAN": "CA", "CHE": "CH", "CHL": "CL", "CHN": "CN", "CRI": "CR",
    "CYP": "CY", "CZE": "CZ", "DEU": "DE", "DNK": "DK", "DZA": "DZ",
    "ECU": "EC", "EST": "EE", "GRC": "EL", "ESP": "ES", "ETH": "ET",
    "FIN": "FI", "FJI": "FJ", "FRA": "FR", "GBR": "GB", "GTM": "GT",
    "HKG": "HK", "HND": "HN", "HRV": "HR", "HUN": "HU", "IDN": "ID",
    "IRL": "IE", "ISR": "IL", "IND": "IN", "ISL": "IS", "ITA": "IT",
    "JPN": "JP", "KEN": "KE", "KGZ": "KG", "PRK": "KP", "KOR": "KR",
    "LKA": "LK", "LTU": "LT", "LUX": "LU", "LVA": "LV", "MAR": "MA",
    "MNE": "ME", "MMR": "MM", "MLT": "MT", "MEX": "MX", "MYS": "MY",
    "MOZ": "MZ", "NGA": "NG", "NLD": "NL", "NOR": "NO", "NZL": "NZ",
    "PER": "PE", "PHL": "PH", "POL": "PL", "PRT": "PT", "ROU": "RO",
    "SRB": "RS", "RUS": "RU", "SAU": "SA", "SLB": "SB", "SWE": "SE",
    "SGP": "SG", "SVN": "SI", "SVK": "SK", "SWZ": "SZ", "THA": "TH",
    "TUN": "TN", "TUR": "TR", "TWN": "TW", "TZA": "TZ", "USA": "US",
    "UGA": "UG", "VNM": "VN", "VUT": "VU", "WSM": "WS", "ZAF": "ZA",
    "ZMB": "ZM", "ZWE": "ZW"
}

def parse_lot_block(raw_text: str) -> Dict:
    """
    Parse the raw text block of a single lot into structured fields,
    including lot-level fields (germ, date, seed count, size, purity, etc.).
    """
    parts = raw_text.replace(",", "").split("\n")
    try:
        # line 1: lot number
        lot_no = parts[0].strip()

        # line 3: Current germ %
        current_germ = int(parts[2]) if len(parts) > 2 and parts[2].isdigit() else None

        # line 4: GermDate + SeedCount
        germ_date, seed_count = None, None
        if len(parts) > 3:
            combined = parts[3]
            germ_date = combined[:10]
            sc = combined[10:].strip()
            seed_count = int(sc) if sc.isdigit() else None

        # origin: last 3-letter code in the block
        origin = next(
            (p for p in reversed(parts) if re.fullmatch(r"[A-Z]{3}", p)),
            None
        )
        origin = origin.strip() if origin else None
        # convert origin to BC code, if available
        if not origin:
            bc_origin = ""
        else:
            bc_origin = alpha3_to_bc.get(origin, origin)

        # sprout count: if numeric on line 9
        sprout = (
            float(parts[8])
            if len(parts) > 8 and re.match(r"^\d+(\.\d+)?$", parts[8])
            else None
        )

        # line-5 / line-6: seed size vs purity
        seed_size = None
        purity    = None

        if len(parts) > 4:
            text5 = parts[4].strip()
            # if it *is* a two-decimal number, that’s purity
            if re.fullmatch(r"\d+\.\d{2}", text5):
                purity = float(text5)
            else:
                seed_size = text5

        # if seed_size still has an embedded two-decimal, pull it out as purity
        if seed_size:
            m = re.search(r"(\d+\.\d{2})", seed_size)
            if m:
                purity = float(m.group(1))
                seed_size = re.sub(r"\d+\.\d{2}", "", seed_size).strip() or None

        # fallback: if we have no purity yet, look at line-6
        if purity is None and len(parts) > 5:
            text6 = parts[5].strip()
            if re.fullmatch(r"\d+\.\d{2}", text6):
                purity = float(text6)

        return {
            "VendorLotNo":   lot_no,
            "CurrentGerm":   current_germ,
            "GermDate":      germ_date,
            "SeedCount":     seed_count,
            "SeedSize":      seed_size,
            "Purity":        purity,
            "OriginCountry": bc_origin,
            "SproutCount":   sprout,
            # item-level PO now handled separately
            "Inert":         None,
            "OtherCrop":     None,
            "Weed":          None
        }
    except Exception as e:
        return {"error": str(e), "raw": raw_text}

@timed_func("extract_seed_analysis_reports")
def extract_seed_analysis_reports(folder: str) -> Dict[str, PurityData]:
    """
    Pulls “Purity Analysis” sections out of any PDF in the folder
    and returns a map lot_no → { PureSeed, Inert, OtherCrop, Weed }.
    """
    report_map: Dict[str, Dict[str, float]] = {}
    for fn in os.listdir(folder):
        if not fn.lower().endswith(".pdf"):
            continue
        path = os.path.join(folder, fn)
        doc = fitz.open(path)
        full_text = ""
        for page in doc:
            t = page.get_text()
            if "Purity Analysis" in t:
                full_text += t + "\n"
        doc.close()
        if not full_text:
            continue

        segments = re.split(r"Lot Number:\s*([\d-]+)", full_text)
        for i in range(1, len(segments), 2):
            lot_no = segments[i].strip()
            block = segments[i+1]
            lines = [l.strip() for l in block.splitlines() if l.strip()]

            # find the “Weed %” header
            # header_idx = next(
            #     (j for j,l in enumerate(lines)
            #          if re.search(r"Weed\s*%", l, re.IGNORECASE)),
            #     None
            # )
            header_idx = next(
                (j for j, l in enumerate(lines)
                if re.search(r"Weed\s*%", l, re.IGNORECASE) or re.search(r"Weed\s+Seed\s*%", l, re.IGNORECASE)),
                None
            )

            if header_idx is None:
                continue

            # collect the next four floats (or “TR” → None)
            vals = []
            # for ln in lines[header_idx+1:]:
            #     for tok in re.split(r"\s+", ln):
            #         if re.fullmatch(r"TR", tok, re.IGNORECASE):
            #             vals.append(None)
            #         else:
            #             m = re.fullmatch(r"(\d+\.\d+)", tok)
            #             if m:
            #                 vals.append(float(m.group(1)))
            #         if len(vals) == 4:
            #             break
            #     if len(vals) == 4:
            #         break
            for ln in lines[header_idx+1:]:
                tokens = re.split(r"\s+", ln)
                for tok in tokens:
                    if re.fullmatch(r"(TR|-TR-)", tok, re.IGNORECASE):
                        vals.append("TR") 
                    elif re.fullmatch(r"\d+\.\d+", tok):
                        vals.append(float(tok))
                    if len(vals) == 4:
                        break
                if len(vals) == 4:
                    break
            if len(vals) < 4:
                continue

            pure, inert, other, weed = vals
            report_map[lot_no] = {
                "PureSeed":  pure,
                "Inert":     inert,
                "OtherCrop": other,
                "Weed":      weed
            }

    return report_map

@timed_func("extract_invoice_from_pdf")
def extract_invoice_from_pdf(pdf_path: str, fallback_po: str = "", token: str = "", doc: fitz.Document | None = None) -> List[Dict]:
    close_doc = False
    if doc is None:
        doc = fitz.open(pdf_path)
        close_doc = True

    all_blocks = []
    for page in doc:
        blocks = page.get_text("blocks")
        all_blocks.extend(sorted(blocks, key=lambda b: (b[1], b[0])))
    if close_doc:
        doc.close()

    items: List[Dict] = []
    current = None
    text_acc = ""
    i = 0
    while i < len(all_blocks):
        b = all_blocks[i]
        txt = b[4].strip()
        if re.match(r"^\d+\s+\d{8}", txt) and ("Treated" in txt or "Untreated" in txt):
            if current:
                # Finalize previous item
                treatment_name = None
                for line in text_acc.splitlines():
                    if "Treatment name:" in line:
                        treatment_name = line.split("Treatment name:")[1].strip()
                        break
                current["TreatmentName"] = treatment_name
                pm = re.search(r"\bPO[-\s:]*(\d{5})\b", text_acc, re.IGNORECASE)
                if pm:
                    current["PurchaseOrder"] = f"PO-{pm.group(1)}"
                elif fallback_po:
                    current["PurchaseOrder"] = fallback_po
                else:
                    current["PurchaseOrder"] = ""
                if current["PurchaseOrder"]:
                    try:
                        current["BCOptions"] = get_po_items(current["PurchaseOrder"], token)
                    except Exception as e:
                        current["BCOptions"] = [{"No": "ERROR", "Description": str(e)}]
                else:
                    current["BCOptions"] = []
                items.append(current)
            # Start new item
            item_y0 = b[1]  # Record the y-coordinate of the item start
            item_line_acc = txt  # Accumulate item line text
            j = i + 1
            # Accumulate blocks on the same horizontal level (within 10 units tolerance)
            while j < len(all_blocks) and abs(all_blocks[j][1] - item_y0) < 10:
                item_line_acc += " " + all_blocks[j][4].strip()
                j += 1
            # Extract total price from accumulated item line
            price_nums = re.findall(r"\d{1,3}(?:,\d{3})*\.\d{2}", item_line_acc)
            total_price = float(price_nums[-1].replace(",", "")) if price_nums else None
            # Extract other fields from accumulated text
            parts = item_line_acc.split()
            item_no = parts[1]
            ui = parts.index("EA") if "EA" in parts else len(parts)
            desc = " ".join(parts[2:ui])
            
            # (1) Pull package‐quantity from the very end of `desc`, e.g. "500M" or "25 lb"
            pkg_qty = None
            m_pkg = re.search(r"(\d+)(?=\s*[Mm]\b|\s*[Ll][Bb]\b|M$|LB$)", desc)
            if m_pkg:
                pkg_qty = int(m_pkg.group(1))
            else:
                # if you expect descriptions to always end in digits+M or digits+lb,
                # this fallback probably won't trigger. You could raise or log instead.
                pkg_qty = None

            # (2) Parse QtyShipped out of parts: after "EA" comes [Ordered, Shipped, UnitPrice, Disc%, TotalPrice]
            shipped = None
            try:
                shipped = float(parts[ui + 2])   # parts[ui+1] is Ordered, parts[ui+2] is Shipped
            except Exception:
                shipped = None

            # (3) TotalPrice was already computed via regex → stored in `total_price`
            usd_actual_cost = None
            if (pkg_qty not in (None, 0)) and (shipped not in (None, 0)) and (total_price not in (None, 0)):
                usd_actual_cost = round(total_price / (shipped * pkg_qty), 2)
                
            best_pkg_desc = find_best_package_description(desc)
       
            current = {
                "VendorItemNumber": item_no,
                "VendorDescription": desc,
                "Pkg_Qty": pkg_qty,
                "QtyShipped": shipped,
                "USD_Actual_Cost_$": usd_actual_cost,
                "PackageDescription": best_pkg_desc,
                "TreatmentName": None,
                "PurchaseOrder": "",
                "TotalPrice": total_price,
                "Lots": []
            }
            text_acc = item_line_acc + "\n"
            i = j  # Skip to the next block after the item line
            continue
        if current:
            txt = b[4].strip()
            text_acc += txt + "\n"
            if re.match(r"^\d{6}-\d{3}", txt):
                current["Lots"].append(b[4])
        i += 1

    # Finalize the last item
    if current:
        treatment_name = None
        for line in text_acc.splitlines():
            if "Treatment name:" in line:
                treatment_name = line.split("Treatment name:")[1].strip()
                break
        current["TreatmentName"] = treatment_name
        pm = re.search(r"\bPO[-\s:]*(\d{5})\b", text_acc, re.IGNORECASE)
        if pm:
            current["PurchaseOrder"] = f"PO-{pm.group(1)}"
        elif fallback_po:
            current["PurchaseOrder"] = fallback_po
        else:
            current["PurchaseOrder"] = ""
        if current["PurchaseOrder"]:
            try:
                current["BCOptions"] = get_po_items(current["PurchaseOrder"], token)
            except Exception as e:
                current["BCOptions"] = [{"No": "ERROR", "Description": str(e)}]
        else:
            current["BCOptions"] = []
        items.append(current)

    return items

@timed_func("extract_sakata_data")
def extract_sakata_data(pdf_paths: List[str]) -> List[Dict]:
    """
    For each invoice PDF:
      1) Extract invoice # (INV-000181037 → “181037”)
      2) Build fallback POs from top-of-page if needed
      3) Skip any “Report of Seed Analysis” PDFs
      4) Pull items & their lot blocks
      5) Parse each lot and enrich with purity-report data
      6) Fill in invoice number on each lot
    """
    if not pdf_paths:
        return []
    
    # pull the raw header text from the first PDF
    first = pdf_paths[0]
    doc0 = fitz.open(first)
    hdr  = doc0[0].get_text()
    doc0.close()

    # 2) Fallback PO logic: capture all 5-digit IDs under "Purchase order" (or, if empty, "Customer reference")
    fallback_po = ""
    m = re.search(
        r"Purchase\s+order\s*[:\-]?(.*?)(?:Terms of payment|Ship to)",
        hdr,
        re.IGNORECASE | re.DOTALL
    )
    if not m:
        m = re.search(
            r"Customer\s+reference\s*[:\-]?(.*?)(?:Terms of delivery|Ship to)",
            hdr,
            re.IGNORECASE | re.DOTALL
        )
    if m:
        nums = re.findall(r"\b(\d{5})\b", m.group(1))
        if nums:
            fallback_po = " | ".join(f"PO-{n}" for n in nums)

    # 3) purity‐report map
    report_map = extract_seed_analysis_reports(os.path.dirname(first))

    # fallback_po = ",".join(fallback) if fallback else ""
    all_items: List[Dict] = []
    for path in pdf_paths:
        doc = fitz.open(path)
        page_texts = [p.get_text() for p in doc]
        if any("Report of Seed Analysis" in t for t in page_texts):
            doc.close()
            continue

        hdr = page_texts[0]
        m = re.search(r"INV[-\s]*0*([0-9]+)", hdr, re.IGNORECASE)
        invoice_id = m.group(1) if m else ""

        raw_items = extract_invoice_from_pdf(path, fallback_po=fallback_po, token=token, doc=doc)
        doc.close()
        for itm in raw_items:
            parsed = []
            for raw in itm["Lots"]:
                lot = parse_lot_block(raw)
                
                # ─── COPY parent’s Pkg + USD Cost + PackageDescription fields into each lot ───
                lot["Pkg_Qty"]            = itm.get("Pkg_Qty")
                lot["USD_Actual_Cost_$"]  = itm.get("USD_Actual_Cost_$")
                lot["PackageDescription"]  = itm.get("PackageDescription")
                
                # enrich purity‐analysis
                key = lot.get("VendorLotNo")
                if key in report_map:
                    lot.update(report_map[key])
                    
                # fallback PO at lot‐level if none on the item
                if not itm["PurchaseOrder"] and fallback_po:
                    lot["PurchaseOrder"] = fallback_po
                    
                # invoice #
                lot["InvoiceNumber"] = invoice_id
                parsed.append(lot)
            itm["Lots"] = parsed
            all_items.append(itm)

    return all_items