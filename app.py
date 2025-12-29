#app.py

from flask import Flask, render_template, request, jsonify, session, redirect, url_for
from werkzeug.utils import secure_filename
from werkzeug.middleware.proxy_fix import ProxyFix
import os
import re
import fitz
import requests
from datetime import datetime, timedelta
import difflib
from difflib import get_close_matches
import msal
from dotenv import load_dotenv
from functools import wraps
from multiprocessing import Pool, cpu_count
from vendor_extractors.sakata import load_package_descriptions, get_po_items
from vendor_extractors.hm_clause import extract_hm_clause_data_from_bytes, find_best_hm_clause_package_description
from vendor_extractors.seminis import extract_seminis_data_from_bytes, find_best_seminis_package_description
from vendor_extractors.syngenta import extract_syngenta_data_from_bytes
from vendor_extractors.nunhems import extract_nunhems_data_from_bytes, find_best_nunhems_package_description
import time
import logging
import psycopg2
import db_logger

app = Flask(__name__)
# Application setup
app.wsgi_app = ProxyFix(app.wsgi_app, x_proto=1, x_host=1)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
UPLOAD_FOLDER = os.path.join(BASE_DIR, "uploads")
app.config["UPLOAD_FOLDER"] = UPLOAD_FOLDER
app.secret_key = os.environ["SECRET_KEY"]
app.permanent_session_lifetime = timedelta(hours=8)
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

# Configure logging
logging.basicConfig(level=logging.INFO)
app.logger.setLevel(logging.INFO)
db_logger.init_app(app)
# ---- POSTGRES MSAL CACHE CONFIG ----
DB_CONFIG = {
    'host': 'localhost',
    'database': 'invoice_ocr',
    'user': 'priyanshu',
    'password': 'reorg0211',
}

# Load environment variables
load_dotenv()
BC_TENANT = os.environ["AZURE_TENANT_ID"]
BC_COMPANY = os.environ["BC_COMPANY"]
#BC_ENV = os.environ["BC_ENV"]
BC_ENV_DEFAULT = os.environ.get("BC_ENV", "SANDBOX-25C")
CLIENT_ID = os.environ["AZURE_CLIENT_ID"]
CLIENT_SECRET = os.environ["AZURE_CLIENT_SECRET"]
AUTHORITY = f"https://login.microsoftonline.com/{BC_TENANT}"
REDIRECT_PATH = "/auth/callback"
SCOPE_BC = ["https://api.businesscentral.dynamics.com/.default"]

def get_bc_env(vendor: str | None = None) -> str:
    """Return 'Production' if vendor in ["seminis", "hm_clause"], else use default BC_ENV."""
    if vendor and vendor.strip().lower() in ["seminis", "hm_clause", "sakata"]:
        return "Production"
    return BC_ENV_DEFAULT

def load_cache():
    """Load MSAL cache for this user from PostgreSQL."""
    cache = msal.SerializableTokenCache()
    user_name = session.get("user_name")
    if user_name:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        cur.execute("""
            SELECT token_cache_data 
            FROM msal_token_cache 
            WHERE user_name = %s
        """, (user_name,))
        row = cur.fetchone()
        if row:
            cache.deserialize(row[0])
        cur.close()
        conn.close()
    return cache

def save_cache(cache):
    """Save MSAL cache for this user to PostgreSQL."""
    if cache.has_state_changed:
        user_name = session.get("user_name")
        if user_name:
            conn = psycopg2.connect(**DB_CONFIG)
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO msal_token_cache (user_name, token_cache_data, updated_at) 
                VALUES (%s, %s, %s) 
                ON CONFLICT (user_name) 
                DO UPDATE SET token_cache_data = EXCLUDED.token_cache_data,
                              updated_at = EXCLUDED.updated_at
            """, (user_name, cache.serialize(), datetime.now()))
            conn.commit()
            cur.close()
            conn.close()

def clear_cache():
    """Clear MSAL Token Cache for this user."""
    user_name = session.get("user_name")
    if user_name:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        cur.execute("DELETE FROM msal_token_cache WHERE user_name = %s", (user_name,))
        conn.commit()
        cur.close()
        conn.close()

def build_msal_app(cache=None) -> msal.ConfidentialClientApplication:
    return msal.ConfidentialClientApplication(
        client_id=CLIENT_ID,
        client_credential=CLIENT_SECRET,
        authority=AUTHORITY,
        token_cache=cache,
    )

# Timing decorators
def timed_func(label: str):
    def decorator(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            start = time.perf_counter()
            result = fn(*args, **kwargs)
            elapsed = time.perf_counter() - start
            app.logger.info(f"[TIMING] {label} took {elapsed:.2f}s")
            return result
        return wrapper
    return decorator

def timed_get(url, **kwargs):
    start = time.perf_counter()
    resp = requests.get(url, **kwargs)
    elapsed = time.perf_counter() - start
    app.logger.info(f"[TIMING] GET {url} took {elapsed:.2f}s")
    resp.raise_for_status()
    return resp

def timed_post(url, **kwargs):
    start = time.perf_counter()
    resp = requests.post(url, **kwargs)
    elapsed = time.perf_counter() - start
    app.logger.info(f"[TIMING] POST {url} took {elapsed:.2f}s")
    resp.raise_for_status()
    return resp

# Token validation
@timed_func("token_is_valid")
def token_is_valid(access_token: str) -> bool:
    if not access_token:
        return False
    test_url = (
        f"https://api.businesscentral.dynamics.com/v2.0/"
        f"{BC_TENANT}/Production/ODataV4/"
        f"Company('{BC_COMPANY}')/Items?$top=1"
    )
    try:
        resp = timed_get(
            test_url,
            headers={"Authorization": f"Bearer {access_token}"}
        )
        return resp.status_code == 200
    except requests.exceptions.RequestException:
        return False

def find_best_bc_item_match(vendor_desc: str, bc_options: list[dict], vendor: str = None) -> str | None:
    """
    Finds the best BC Item Number using a hybrid approach with strict validation:
    1. Fuzzy String Similarity (Base Score 0-100)
    2. ID Substring Matching (Bonus +50) -> STRICTLY for Numeric/Alphanumeric codes
    3. Tie-Breaking: If multiple items have the same top score, return None (Manual).
    4. Strict Threshold: Only accepts matches with very high confidence, 
       UNLESS vendor is HM Clause, where we lower the threshold.
    """
    if not vendor_desc or not bc_options:
        return None

    # Normalization helper: Lowercase and space-separated punctuation
    def normalize(text):
        return re.sub(r'[^\w\s]', ' ', text.lower())

    norm_vendor = normalize(vendor_desc)
    # Create a set of tokens for ID checking
    vendor_tokens = set(norm_vendor.split())
    
    best_match_no = None
    best_score = 0
    is_tie = False

    for option in bc_options:
        bc_desc = option.get("Description", "")
        if not bc_desc:
            continue
        
        bc_no = option.get("No", "")
        norm_bc = normalize(bc_desc)
        bc_tokens = set(norm_bc.split())

        # --- Scoring Logic ---
        score = 0

        # 1. Fuzzy Similarity (Base Score 0-100)
        # Sort words to ensure "Corn Sweet" matches "Sweet Corn"
        sorted_vendor = " ".join(sorted(norm_vendor.split()))
        sorted_bc = " ".join(sorted(norm_bc.split()))
        
        # difflib calculates how many characters match
        fuzzy_ratio = difflib.SequenceMatcher(None, sorted_vendor, sorted_bc).ratio()
        score += fuzzy_ratio * 100 

        # 2. Critical ID Match (Bonus +50)
        # Only matches if the token is NOT purely alphabetic (must contain digits)
        id_match_found = False
        for v_tok in vendor_tokens:
            # Skip short tokens (< 4 chars) OR purely alphabetic words (e.g. "UNTREATED", "HYBRID")
            if len(v_tok) < 4 or v_tok.isalpha(): 
                continue 
            
            # Check against all BC tokens
            for b_tok in bc_tokens:
                # BC tokens must also be long enough and not just words
                if len(b_tok) < 4 or b_tok.isalpha(): 
                    continue
                
                # Check for substring match (e.g. Vendor "SV9010SA" vs BC "SV9010SA")
                if v_tok == b_tok or v_tok in b_tok or b_tok in v_tok:
                    score += 61 # Reduced bonus
                    id_match_found = True
                    break
            if id_match_found: break

        # 3. Selection & Tie Detection
        if score > best_score:
            best_score = score
            best_match_no = bc_no
            is_tie = False
        elif score == best_score and score > 0:
            is_tie = True

    # --- Strict Acceptance Criteria ---
    
    # 1. If there's a tie (ambiguity), force manual selection
    if is_tie:
        return None

    # 2. Threshold check:
    # Default: Score >= 60 (High confidence)
    # HM Clause exception: Score >= 30 (Lower confidence due to single keyword matches)
    threshold = 60
    if vendor == "hm_clause":
        threshold = 30
    if vendor == "seminis":
        threshold = 50
    
    if best_score >= threshold:
        return best_match_no
    
    return None

def aggregate_duplicate_lots(grouped_results: dict, vendor: str) -> dict:
    """
    Aggregates quantities and prices for duplicate lots based on the vendor.
    """
    if vendor == "sakata":
        flattened_results = {}
        for filename, items_list in grouped_results.items():
            flat_lots_list = []
            for item in items_list:
                for lot in item.get("Lots", []):
                    combined_lot = {k: v for k, v in item.items() if k != "Lots"}
                    combined_lot.update(lot)
                    if 'TotalQuantity' not in combined_lot:
                         combined_lot['TotalQuantity'] = item.get('QtyShipped')
                    flat_lots_list.append(combined_lot)
            
            if flat_lots_list:
                flattened_results[filename] = flat_lots_list
        grouped_results = flattened_results
        
    unique_items_map = {}
    processed_grouped_results = {}

    lot_keys = ["VendorLot", "VendorLotNo", "VendorProductLot"]
    batch_keys = ["VendorBatchLot", "VendorBatchNo"]
    desc_keys = ["VendorItemDescription", "VendorDescription"]

    for filename, items_list in grouped_results.items():
        processed_items_for_file = []
        for item in items_list:
            agg_key = None
            
            lot_no_raw = next((item.get(key) for key in lot_keys if item.get(key)), None)
            lot_no = lot_no_raw.strip() if lot_no_raw else None

            if vendor in ["hm_clause", "seminis"]:
                batch_no_raw = next((item.get(key) for key in batch_keys if item.get(key)), None)
                batch_no = batch_no_raw.strip() if batch_no_raw else None
                if lot_no and batch_no:
                    agg_key = (lot_no, batch_no)
            
            elif vendor in ["sakata", "syngenta"]:
                item_no = item.get("VendorItemNumber")
                if lot_no and item_no:
                    agg_key = (lot_no, item_no)
                elif lot_no:
                    agg_key = (lot_no,)

            if not agg_key:
                processed_items_for_file.append(item)
                continue

            try:
                current_qty = float(item.get("TotalQuantity", 0) or 0)
                current_price = float(item.get("TotalPrice", 0) or 0)
                current_upcharge = float(item.get("TotalUpcharge", 0) or 0)
                current_discount = float(item.get("TotalDiscount", 0) or 0)
            except (ValueError, TypeError):
                processed_items_for_file.append(item)
                continue

            if agg_key in unique_items_map:
                existing_item = unique_items_map[agg_key]
                desc_key = next((key for key in desc_keys if key in existing_item), None)

                # --- FIX: Smarter Description Merging ---
                if desc_key:
                    existing_desc = existing_item.get(desc_key, "")
                    new_desc = item.get(desc_key, "")
                    
                    # If we haven't already marked it as combined, prepare the base description
                    if "[COMBINED]" not in existing_desc:
                        # For HM Clause: Pick the LONGEST description. 
                        # This avoids cases where the first item has a truncated desc ("Pump...") 
                        # but the second item has the full one ("Pump... Pail 30 Ks").
                        best_desc = existing_desc
                        if len(new_desc) > len(existing_desc):
                            best_desc = new_desc
                        
                        if vendor == "hm_clause":
                            existing_item[desc_key] = f"{best_desc} [COMBINED]"
                        else:
                            # Standard logic for others
                            modified_desc = re.sub(r"\s+[\d,]+\s+\w+$", "", best_desc).strip()
                            existing_item[desc_key] = f"{modified_desc} [COMBINED]"
                
                existing_qty = float(existing_item.get("TotalQuantity", 0) or 0)
                existing_price = float(existing_item.get("TotalPrice", 0) or 0)
                
                existing_upcharge = float(existing_item.get("TotalUpcharge", 0) or 0)
                existing_discount = float(existing_item.get("TotalDiscount", 0) or 0)
                
                existing_item["TotalQuantity"] = existing_qty + current_qty
                existing_item["TotalPrice"] = existing_price + current_price
                
                existing_item["TotalUpcharge"] = existing_upcharge + current_upcharge
                existing_item["TotalDiscount"] = existing_discount + current_discount
                
                new_total_qty = existing_item["TotalQuantity"]
                new_total_price = existing_item["TotalPrice"]
                new_total_upcharge = existing_item["TotalUpcharge"]
                new_total_discount = existing_item["TotalDiscount"]
                
                cost_key = "USD_Actual_Cost_$"
                if cost_key not in existing_item:
                    cost_key = next((k for k in existing_item if "Cost" in k), "USD_Actual_Cost_$")

                if new_total_qty > 0:
                    if vendor == "sakata":
                        pkg_qty = None
                        desc = existing_item.get("VendorDescription", "")
                        if m_pkg := re.search(r"(\d+)(?=\s*[Mm]\b|\s*[Ll][Bb]\b|M$|LB$)", desc):
                            pkg_qty = int(m_pkg.group(1))

                        if pkg_qty and pkg_qty > 0:
                            total_seed_units = new_total_qty * pkg_qty
                            if total_seed_units > 0:
                                existing_item[cost_key] = round(new_total_price / total_seed_units, 4)
                    else:
                        if new_total_price:
                            #existing_item[cost_key] = round(new_total_price / new_total_qty, 4)
                            existing_item[cost_key] = round((new_total_price + new_total_upcharge - new_total_discount) / new_total_qty, 4)
            else:
                item["TotalQuantity"] = current_qty
                item["TotalPrice"] = current_price
                item["TotalUpcharge"] = current_upcharge
                item["TotalDiscount"] = current_discount
                unique_items_map[agg_key] = item
                processed_items_for_file.append(item)

        if processed_items_for_file:
            processed_grouped_results[filename] = processed_items_for_file

    return processed_grouped_results

@app.route("/api/items")
def api_items():
    from vendor_extractors.sakata import load_all_items
    return jsonify(load_all_items())

@app.route("/bc-options")
def bc_options():
    po_raw = request.args.get("po", "").strip()
    app.logger.debug(f"BC lookup called with raw po = {po_raw!r}")

    if not po_raw:
        return jsonify([])

    m = re.search(r"\bPO[-\s]*(\d{5})\b", po_raw, re.IGNORECASE)
    if not m:
        app.logger.error("bc-options: could not parse a valid PO-##### from %r", po_raw)
        return jsonify([])

    po = f"PO-{m.group(1)}"
    app.logger.debug(f"bc-options: using normalized po = {po}")

    try:
        from vendor_extractors.sakata import get_po_items
        opts = get_po_items(po, session.get("user_token"))
    except Exception as e:
        app.logger.error("bc-options lookup failed: %s", str(e))
        return jsonify([{"No": "ERROR", "Description": str(e)}])

    return jsonify(opts)

# Treatments cache
_treatments_cache = {}

@timed_func("load_treatments")
def load_treatments(endpoint: str, token: str) -> list[str]:
    if endpoint in _treatments_cache:
        return _treatments_cache[endpoint]
    
    # Validate token
    if not token_is_valid(token):
        cache = load_cache()
        msal_app = build_msal_app(cache)
        app.logger.warning(f"Invalid token for endpoint {endpoint}, attempting refresh")
        accounts = msal_app.get_accounts()
        if accounts:
            result = msal_app.acquire_token_silent(scopes=SCOPE_BC, account=accounts[0])
            if "access_token" in result:
                token = result["access_token"]
            else:
                app.logger.error(f"Silent token refresh failed: {result.get('error_description')}")
                return []
        else:
            app.logger.error("No accounts available for token refresh")
            return []

    url = (
        f"https://api.businesscentral.dynamics.com/v2.0/"
        f"{BC_TENANT}/Production/ODataV4/"
        f"Company('{BC_COMPANY}')/{endpoint}"
    )
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json"
    }
    try:
        resp = timed_get(url, headers=headers)
        rows = resp.json().get("value", [])
        treatments = [r["Treatment_Name"].strip() for r in rows if r.get("Treatment_Name")]
        _treatments_cache[endpoint] = treatments
        return treatments
    except requests.exceptions.RequestException as e:
        app.logger.error(f"Failed to load treatments from {endpoint}: {e}")
        return []

# Multiprocessing initializer
def init_worker(pkg_descs: list[str]):
    import vendor_extractors.sakata
    vendor_extractors.sakata._pkg_desc_list = pkg_descs

def _extract_hm_clause_file(path):
    try:
        return os.path.basename(path), extract_hm_clause_data(path)
    except Exception as e:
        app.logger.error(f"Error processing {path}: {e}")
        return os.path.basename(path), []

def _extract_seminis_file(path):
    try:
        # The Seminis extractor manages its own sub-files, so it just needs the main path
        return os.path.basename(path), extract_seminis_invoice_data(path)
    except Exception as e:
        app.logger.error(f"Error processing {path}: {e}")
        return os.path.basename(path), []
    
def _extract_nunhems_file(path):
    try:
        # The Nunhems extractor is designed to find its own related files in the same folder
        return os.path.basename(path), extract_nunhems_invoice_data(path)
    except Exception as e:
        app.logger.error(f"Error processing {path} for Nunhems: {e}")
        return os.path.basename(path), []
    
# Login required decorator
def login_required(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        if not session.get("user_token"):
            return redirect(url_for("sign_in"))
        return f(*args, **kwargs)
    return wrapper

@app.route("/sign-in")
def sign_in():
    if session.get("user_token") and token_is_valid(session.get("user_token")):
        return redirect(url_for("index"))

    cache = load_cache()
    msal_app = build_msal_app(cache)
    auth_url = msal_app.get_authorization_request_url(
        scopes=SCOPE_BC,
        redirect_uri=url_for("auth_callback", _external=True)
    )
    save_cache(cache)
    return redirect(auth_url)

@app.route(REDIRECT_PATH)
def auth_callback():
    
    if session.get("user_token"):
        app.logger.debug("User already signed in, skipping callback.")
        return redirect(url_for("index"))
    
    code = request.args.get("code")
    if not code:
        return "Authentication failed: No code received", 400
    
    cache = load_cache()
    msal_app = build_msal_app(cache)
    
    result = msal_app.acquire_token_by_authorization_code(
        code=code,
        scopes=SCOPE_BC,
        redirect_uri=url_for("auth_callback", _external=True)
    )
    if "access_token" not in result:
        app.logger.error(f"Auth error: {result.get('error_description')}")
        return "Authentication failed", 400
    
    session.permanent = True
    session["user_token"] = result["access_token"]
    session["user_name"] = result.get("id_token_claims", {}).get("name", "User")
    save_cache(cache)
    
    return redirect(url_for("index"))

@app.route("/sign-out")
def sign_out():
    user_name = session.get("user_name")
    clear_cache()
    session.clear()
    return redirect(url_for("sign_in", _external=True))

@app.route("/logout")
def logout():
    user_name = session.get("user_name")
    clear_cache()
    session.clear()
    return render_template("logout.html")

# --- NEW ROUTE for viewing detailed logs ---
@app.route("/logs")
@login_required
def logs():
    page = request.args.get('page', 1, type=int)
    logs, total_logs = db_logger.get_paginated_logs(page=page, per_page=50)
    stats = db_logger.get_log_stats()
    
    # Calculate total pages for pagination
    total_pages = (total_logs + 49) // 50
    
    return render_template("logs.html", 
                           logs=logs, 
                           stats=stats,
                           current_page=page,
                           total_pages=total_pages,
                           user_name=session.get("user_name"))

# Admin route to fix stats manually on production ---
@app.route("/fix-stats")
@login_required
def fix_stats():
    """Manual trigger to fix statistics synchronization issues."""
    message = db_logger.recalculate_stats()
    return f"<h1>Stats Maintenance</h1><p>{message}</p><p><a href='/logs'>Back to Logs</a></p>"

# Main route
@app.route("/", methods=["GET", "POST"])
@login_required
@timed_func("index handler")
def index():
    user_token = session.get("user_token")
    if not token_is_valid(user_token):
        session.pop("user_token", None)
        session.pop("user_name", None)
        return redirect(url_for("sign_in"))

    if request.method == "POST":
        vendor = request.form.get("vendor")
        files = request.files.getlist("pdfs")
      
        pdf_files = []
        for f in files:
            if f and f.filename.lower().endswith(".pdf"):
                pdf_bytes = f.read()
                pdf_files.append((f.filename, pdf_bytes))

        if not pdf_files:
            return "No valid PDF files uploaded", 400

        if vendor == "sakata":
            # Load shared data from Business Central
            pkg_descs = load_package_descriptions(user_token)
            treatments1 = load_treatments("Lot_Treatments_Card_Excel", user_token)
            treatments2 = load_treatments("Lot_Treatments_Card_2_Excel", user_token)

            from vendor_extractors.sakata import extract_sakata_data_from_bytes
            grouped_results = extract_sakata_data_from_bytes(pdf_files, token=user_token)
            
            # Aggregate duplicate lots
            final_grouped_results = aggregate_duplicate_lots(grouped_results, vendor = "sakata")

            # Flatten the results to find all unique PO numbers
            all_items = [item for items_list in final_grouped_results.values() for item in items_list]
            all_pos = set(item.get("PurchaseOrder") for item in all_items if item.get("PurchaseOrder"))

            if all_pos:
                try:
                    # Fetch BC options for all found POs at once
                    po_items = get_po_items("|".join(all_pos), user_token)
                    for item in all_items:
                        # The BCOptions are already populated by the extractor, but this ensures consistency
                        if item.get("PurchaseOrder"):
                            item["BCOptions"] = po_items
                        else:
                            item["BCOptions"] = []
                        
                        # Find the best matching BC item
                        vendor_desc = item.get("VendorDescription", "") # Corrected key from VendorItemDescription
                        item["SuggestedBCItemNo"] = find_best_bc_item_match(vendor_desc, item["BCOptions"], vendor=vendor)
                        
                except Exception as e:
                    app.logger.error(f"Failed to fetch PO items: {e}")
                    for item in all_items:
                        item["BCOptions"] = [{"No": "ERROR", "Description": str(e)}]

            return render_template(
                "results_sakata.html",
                items=final_grouped_results,
                treatments1=treatments1,
                treatments2=treatments2,
                pkg_descs=pkg_descs
            )
        
        elif vendor == "hm_clause":
            # 1. Load shared data from BC first
            pkg_descs = load_package_descriptions(user_token)
            treatments1 = load_treatments("Lot_Treatments_Card_Excel", user_token)
            treatments2 = load_treatments("Lot_Treatments_Card_2_Excel", user_token)

            grouped_results = extract_hm_clause_data_from_bytes(pdf_files)
            
            # Aggregate duplicate lots
            final_grouped_results = aggregate_duplicate_lots(grouped_results, vendor = "hm_clause")

            # 3. Create a flat list for post-processing
            all_items_flat = [item for items_list in final_grouped_results.values() for item in items_list]
            
            # 4. Fetch all PO data at once
            all_pos = set(item.get("PurchaseOrder") for item in all_items_flat if item.get("PurchaseOrder"))
            po_items_for_all = []
            if all_pos:
                try:
                    po_items_for_all = get_po_items("|".join(all_pos), user_token)
                except Exception as e:
                    app.logger.error(f"Failed to fetch PO items for HM Clause: {e}")
                    po_items_for_all = [{"No": "ERROR", "Description": str(e)}]
            
            # 5. Enrich each item with PO options and package description
            for item in all_items_flat:
                if item.get("PurchaseOrder"):
                    item["BCOptions"] = po_items_for_all
                else:
                    item["BCOptions"] = []
                    
                vendor_desc = item.get("VendorItemDescription", "")
                item["SuggestedBCItemNo"] = find_best_bc_item_match(vendor_desc, item["BCOptions"], vendor=vendor)
                
                # Find and add the best package description
                item["PackageDescription"] = find_best_hm_clause_package_description(vendor_desc, pkg_descs)
            
            # 6. Render the template
            return render_template(
                "results_hm_clause.html",
                items=final_grouped_results,
                treatments1=treatments1,
                treatments2=treatments2,
                pkg_descs=pkg_descs
            )

        elif vendor == "seminis":
            # 1. Load shared data from BC
            pkg_descs = load_package_descriptions(user_token)
            treatments1 = load_treatments("Lot_Treatments_Card_Excel", user_token)
            treatments2 = load_treatments("Lot_Treatments_Card_2_Excel", user_token)

            # 2. Call the new in-memory extractor directly
            grouped_results = extract_seminis_data_from_bytes(pdf_files, pkg_descs)
            
            # Aggregate duplicate lots
            final_grouped_results = aggregate_duplicate_lots(grouped_results, vendor = "seminis")
            
            # 3. Flatten results for post-processing
            all_items_flat = [item for items_list in final_grouped_results.values() for item in items_list]

            # 4. Fetch all PO data at once
            all_pos = set(item.get("PurchaseOrder") for item in all_items_flat if item.get("PurchaseOrder"))
            po_items_for_all = []
            if all_pos:
                try:
                    po_items_for_all = get_po_items("|".join(all_pos), user_token)
                except Exception as e:
                    app.logger.error(f"Failed to fetch PO items for Seminis: {e}")
                    po_items_for_all = [{"No": "ERROR", "Description": str(e)}]

            # 5. Enrich each item with BC options and package descriptions
            for item in all_items_flat:
                item["BCOptions"] = po_items_for_all if item.get("PurchaseOrder") else []
                vendor_desc = item.get("VendorItemDescription", "")
                item["SuggestedBCItemNo"] = find_best_bc_item_match(vendor_desc, item["BCOptions"], vendor=vendor)
                #item["PackageDescription"] = find_best_seminis_package_description(vendor_desc, pkg_descs)

            # 6. Render the template
            return render_template(
                "results_seminis.html",
                items=final_grouped_results,
                treatments1=treatments1,
                treatments2=treatments2,
                pkg_descs=pkg_descs
            )
        
        elif vendor == "nunhems":
            # 1. Load shared data from BC first
            pkg_descs = load_package_descriptions(user_token)
            treatments1 = load_treatments("Lot_Treatments_Card_Excel", user_token)
            treatments2 = load_treatments("Lot_Treatments_Card_2_Excel", user_token)

            grouped_results = extract_nunhems_data_from_bytes(pdf_files, pkg_descs)
            
            # Aggregate duplicate lots
            final_grouped_results = aggregate_duplicate_lots(grouped_results, vendor = "nunhems")

            # 3. Create a flat list for post-processing
            all_items_flat = [item for items_list in final_grouped_results.values() for item in items_list]

            # 4. Fetch all PO data at once
            all_pos = set(item.get("PurchaseOrder") for item in all_items_flat if item.get("PurchaseOrder"))
            po_items_for_all = []
            if all_pos:
                try:
                    po_items_for_all = get_po_items("|".join(all_pos), user_token)
                except Exception as e:
                    app.logger.error(f"Failed to fetch PO items for Nunhems: {e}")
                    po_items_for_all = [{"No": "ERROR", "Description": str(e)}]

            # 5. Enrich each item with PO options and package description
            for item in all_items_flat:
                item["BCOptions"] = po_items_for_all if item.get("PurchaseOrder") else []
                vendor_desc = item.get("VendorItemDescription", "")
                item["SuggestedBCItemNo"] = find_best_bc_item_match(vendor_desc, item["BCOptions"], vendor=vendor)
            
            # 6. Render the template
            return render_template(
                "results_nunhems.html",
                items=final_grouped_results,
                treatments1=treatments1,
                treatments2=treatments2,
                pkg_descs=pkg_descs
            )

        elif vendor == "syngenta":
            # 1. Load shared data from Business Central
            pkg_descs = load_package_descriptions(user_token)
            treatments1 = load_treatments("Lot_Treatments_Card_Excel", user_token)
            treatments2 = load_treatments("Lot_Treatments_Card_2_Excel", user_token)

            # 2. Extract Data using the new Syngenta module
            grouped_results = extract_syngenta_data_from_bytes(pdf_files, pkg_descs)
            
            # 3. Aggregate duplicates (combines split lots if any)
            final_grouped_results = aggregate_duplicate_lots(grouped_results, vendor="syngenta")

            # 4. Flatten results to process POs efficiently
            all_items_flat = [item for items_list in final_grouped_results.values() for item in items_list]
            
            # 5. Fetch PO Items for ALL unique POs found in one request (Bulk Fetch)
            all_pos = set(item.get("PurchaseOrder") for item in all_items_flat if item.get("PurchaseOrder"))
            po_items_for_all = []
            
            if all_pos:
                try:
                    # Joins all POs (e.g., "PO-1001|PO-1002") for a single API call
                    po_items_for_all = get_po_items("|".join(all_pos), user_token)
                except Exception as e:
                    app.logger.error(f"Failed to fetch PO items for Syngenta: {e}")
                    po_items_for_all = [{"No": "ERROR", "Description": str(e)}]

            # 6. Enrich items with BC Options and Suggestions
            for item in all_items_flat:
                # Assign the bulk-fetched options if the item has a PO
                item["BCOptions"] = po_items_for_all if item.get("PurchaseOrder") else []

                # Find best match based on description
                vendor_desc = item.get("VendorItemDescription", "")
                item["SuggestedBCItemNo"] = find_best_bc_item_match(vendor_desc, item["BCOptions"], vendor=vendor)

            # 7. Render Template
            return render_template(
                "results_syngenta.html",
                items=final_grouped_results,
                treatments1=treatments1,
                treatments2=treatments2,
                pkg_descs=pkg_descs
            )

        else:
            for path in pdf_paths:
                try:
                    os.remove(path)
                except Exception as e:
                    app.logger.error(f"Could not delete {path}: {e}")
            return "Unsupported vendor selected", 400

    stats = db_logger.get_log_stats()
    return render_template("index.html", user_name=session.get("user_name"), stats=stats)

# Lot creation endpoint
@app.route("/create-lot", methods=["POST"])
@login_required
@timed_func("create_lot")
def create_lot():

    # Silent MSAL refresh on every create_lot call
    cache = load_cache()
    msal_app = build_msal_app(cache)
    accounts = msal_app.get_accounts()
    if accounts:
        result = msal_app.acquire_token_silent(
            scopes=SCOPE_BC,
            account=accounts[0]
        )
        if "access_token" in result:
            session["user_token"] = result["access_token"]
            app.logger.info("Token refreshed successfully")
            save_cache(cache)
        else:
            app.logger.error("Silent token refresh failed: %s", result.get("error_description", "No details"))
            session.pop("user_token", None)
            session.pop("user_name", None)
            return redirect(url_for("sign_in"))
    else:
        app.logger.warning("No accounts found for silent token refresh")
        session.pop("user_token", None)
        session.pop("user_name", None)
        return redirect(url_for("sign_in"))
    
    def normalize_text(val):
        if val is None:
            return ""
        val_str = str(val).strip()
        return "" if val_str.lower() == "none" else val_str

    data = request.get_json()
    vendor = normalize_text(data.get("vendor"))
    print(f"Received data for lot creation: {data}")
    def parse_decimal(val):
        s = str(val or "").strip()
        if not s or s.lower() == "none":
            return None
        try:
            return float(s)
        except ValueError:
            return None
    
    def parse_integer(val):
        s = str(val or "").strip()
        if not s or s.lower() == "none":
            return None
        try:
            # Convert to float first to handle decimals (e.g., "123.0")
            return int(float(s))
        except (ValueError, TypeError):
            return None
    
    # Extract and normalize all fields
    item_no        = normalize_text(data.get("BCItemNo"))
    vendor_lot     = normalize_text(data.get("VendorLotNo"))
    vendor_batch   = normalize_text(data.get("VendorBatchLot"))
    country        = normalize_text(data.get("OriginCountry"))
    td1            = normalize_text(data.get("TreatmentsDescription"))
    td2_text       = normalize_text(data.get("TreatmentsDescription2"))
    seed_size      = normalize_text(data.get("SeedSize"))
    raw_sprout     = normalize_text(data.get("SproutCount"))
    ktt            = normalize_text(data.get("KTT"))
    seed_count     = parse_decimal(data.get("SeedCount"))
    germ_pct       = normalize_text(data.get("CurrentGerm"))
    pure           = parse_decimal(data.get("Purity"))
    inert          = parse_decimal(data.get("Inert"))
    grower_germ    = parse_decimal(data.get("GrowerGerm"))
    usd_cost_val   = parse_decimal(data.get("USD_Actual_Cost_$"))
    original_received_qty = parse_integer(data.get("TotalQuantity"))

    raw_date            = normalize_text(data.get("CurrentGermDate"))
    raw_grower_date     = normalize_text(data.get("GrowerGermDate"))

    pkg_desc_val   = normalize_text(data.get("PackageDescription"))
    
    def normalize_date(raw):
        try:
            if re.match(r"\d{2}/\d{2}/\d{2}$", raw):  # e.g., 04/22/25
                raw = re.sub(r"/(\d{2})$", lambda m: f"/20{m.group(1)}", raw)
            return datetime.strptime(raw, "%m/%d/%Y").date().isoformat()
        except Exception:
            return None

    germ_date_iso = normalize_date(raw_date)
    grower_germ_date_iso = normalize_date(raw_grower_date)

    treated = "Yes" if td1 and td1.lower() != "untreated" else "No"
    if raw_sprout:
        td2_text = f"SPROUT COUNT-{raw_sprout}" + (", " + td2_text if td2_text else "")
    td2 = td2_text
    
    pkg_desc_val = data.get("PackageDescription")
    
    lot_no = 'AUTO'

    raw_payload = {
        "Item_No":                     item_no,
        "Lot_No":                      lot_no,
        "TMG_Vendor_Lot_No":           vendor_lot,
        "TMG_Vendor_Batch_No":         vendor_batch,
        "TMG_Country_Of_Origin":       country,
        "TMG_Treatment_Description":   td1,
        "TMG_Treatment_Description_2": td2,
        "TMG_Seed_Size":               seed_size,
        "TMG_Seed_Count":              seed_count,
        "TMG_Grower_Germ_Percent":     grower_germ,
        "TMG_GrowerGermDate":          grower_germ_date_iso,
        "TMG_Current_Germ":            germ_pct,
        "TMG_Germ_Date":               germ_date_iso,
        "TMG_Purity":                  pure,
        "TMG_Inert":                   inert,
        "TMG_Treated":                 treated,
        "KTT":                         ktt,
        "OriginalReceivedQty":         original_received_qty,
        "TMG_USD_Actual_Cost":         usd_cost_val,
        "TMG_PackageDesc":             pkg_desc_val
    }

    payload = {k: v for k, v in raw_payload.items() if v != None}

    app.logger.info("Extracted data: %s", data)
    app.logger.info("Raw payload: %s", raw_payload)
    app.logger.info("Prepared payload for BC: %s", payload)
    
    bc_url = (
        f"https://api.businesscentral.dynamics.com/v2.0/"
        f"{BC_TENANT}/{get_bc_env(vendor)}/ODataV4/"
        f"Company('{BC_COMPANY}')/Lot_Info_Card"
    )
    headers = {
        "Authorization": f"Bearer {session.get('user_token')}",
        "Content-Type": "application/json",
        "Prefer": "odata.maxversion=4.0;IEEE754Compatible=true"
    }

    try:
        resp = timed_post(bc_url, json=payload, headers=headers)
        resp.raise_for_status()
        lot_data = resp.json()
        
        # --- NEW DEBUGGING LOGS ---
        app.logger.info("============== BC RESPONSE DEBUG ==============")
        app.logger.info(f"Status Code: {resp.status_code}")
        app.logger.info(f"Created Lot No: {lot_data.get('Lot_No')}")
        app.logger.info(f"Returned Treatment 1: {lot_data.get('TMG_Treatment_Description')}")
        app.logger.info(f"Returned Treatment 2: {lot_data.get('TMG_Treatment_Description_2')}")
        app.logger.info("Full Response Body:")
        app.logger.info(lot_data)
        app.logger.info("===============================================")
        # --------------------------
        
        generated_lot_no = lot_data.get("Lot_No")
        return jsonify({"status": "success", "Lot_No": generated_lot_no})
        #return jsonify({"status": "success"})
    except requests.exceptions.HTTPError as e:
        app.logger.error("HTTP error creating lot: %d - %s", e.response.status_code, e.response.text)
        if e.response.status_code == 401:
            session.pop("user_token", None)
            session.pop("user_name", None)
            return redirect(url_for("sign_in"))
        return jsonify({"status": "error", "message": e.response.text}), e.response.status_code
    except Exception as e:
        app.logger.error("Unexpected error creating lot: %s", str(e))
        return jsonify({"status": "error", "message": str(e)}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5001, debug=False)