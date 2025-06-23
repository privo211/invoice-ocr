from flask import Flask, render_template, request, jsonify, session, redirect, url_for
from werkzeug.utils import secure_filename
from werkzeug.middleware.proxy_fix import ProxyFix
import os
import re
import requests
from datetime import datetime
import msal
from dotenv import load_dotenv
from functools import wraps
from multiprocessing import Pool, cpu_count
from vendor_extractors.sakata import extract_sakata_data, load_package_descriptions, get_po_items
from vendor_extractors.hm_clause import extract_hm_clause_data
import time
import logging

app = Flask(__name__)

# Configure logging
lot_counter = None
logging.basicConfig(level=logging.INFO)
app.logger.setLevel(logging.INFO)

# Business Central connection settings
BC_TENANT = "33b1b67a-786c-4b46-9372-c4e492d15cf1"
BC_ENV = "SANDBOX-2025"
BC_COMPANY = "Stokes%20Seeds%20Limited"

# Load environment variables
load_dotenv()
TENANT_ID = os.environ["AZURE_TENANT_ID"]
CLIENT_ID = os.environ["AZURE_CLIENT_ID"]
CLIENT_SECRET = os.environ["AZURE_CLIENT_SECRET"]
AUTHORITY = f"https://login.microsoftonline.com/{TENANT_ID}"
REDIRECT_PATH = "/auth/callback"
SCOPE_BC = ["https://api.businesscentral.dynamics.com/.default"]

# MSAL configuration
msal_app = msal.ConfidentialClientApplication(
    client_id=CLIENT_ID,
    client_credential=CLIENT_SECRET,
    authority=AUTHORITY
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
        f"{BC_TENANT}/{BC_ENV}/ODataV4/"
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

# Fetch start counter with fallback
@timed_func("fetch_start_counter")
def fetch_start_counter(token: str = None):
    try:
        url = (
            f"https://api.businesscentral.dynamics.com/v2.0/"
            f"{BC_TENANT}/{BC_ENV}/ODataV4/"
            f"Company('{BC_COMPANY}')/Lot_No_Information_Card_Excel"
            "?$filter=startswith(Lot_No,'V-INV-AUTO-TEST-')"
            "&$orderby=Lot_No"
            "&$top=1"
        )
        from vendor_extractors.sakata import token as bc_token
        auth_token = token if token else bc_token
        resp = timed_get(url, headers={"Authorization": f"Bearer {auth_token}"})
        resp.raise_for_status()
        vals = resp.json().get('value', [])
        if not vals:
            return 1
        match = re.match(r"V-INV-AUTO-TEST-(\d+)", vals[0]["Lot_No"])
        return int(match.group(1)) + 1 if match else 1
    except Exception as e:
        app.logger.error(f"Failed to fetch start counter: {e}")
        return 1

# Application setup
app.wsgi_app = ProxyFix(app.wsgi_app, x_proto=1, x_host=1)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
UPLOAD_FOLDER = os.path.join(BASE_DIR, "uploads")
app.config["UPLOAD_FOLDER"] = UPLOAD_FOLDER
app.secret_key = os.environ["SECRET_KEY"]
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

# Treatments cache
_treatments_cache = {}

@timed_func("load_treatments")
def load_treatments(endpoint: str, token: str) -> list[str]:
    if endpoint in _treatments_cache:
        return _treatments_cache[endpoint]
    
    # Validate token
    if not token_is_valid(token):
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

def _extract_sakata_file(path):
    try:
        return os.path.basename(path), extract_sakata_data([path])
    except Exception as e:
        app.logger.error(f"Error processing {path}: {e}")
        return os.path.basename(path), []

def _extract_hm_clause_file(path):
    try:
        return os.path.basename(path), extract_hm_clause_data(path)
    except Exception as e:
        app.logger.error(f"Error processing {path}: {e}")
        return os.path.basename(path), []

# Authentication routes
@app.route("/sign-in")
def sign_in():
    auth_url = msal_app.get_authorization_request_url(
        scopes=SCOPE_BC,
        redirect_uri=url_for("auth_callback", _external=True)
    )
    return redirect(auth_url)

@app.route(REDIRECT_PATH)
def auth_callback():
    code = request.args.get("code")
    if not code:
        return "Authentication failed: No code received", 400
    result = msal_app.acquire_token_by_authorization_code(
        code=code,
        scopes=SCOPE_BC,
        redirect_uri=url_for("auth_callback", _external=True)
    )
    if "access_token" not in result:
        app.logger.error(f"Auth error: {result.get('error_description')}")
        return "Authentication failed", 400
    session["user_token"] = result["access_token"]
    session["user_name"] = result.get("id_token_claims", {}).get("name", "User")
    return redirect(url_for("index"))

@app.route("/sign-out")
def sign_out():
    session.pop("user_token", None)
    session.pop("user_name", None)
    return redirect(url_for("sign_in"))

# Login required decorator
def login_required(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        if not session.get("user_token"):
            return redirect(url_for("sign_in"))
        return f(*args, **kwargs)
    return wrapper

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
        pdf_paths = []
        for f in files:
            if f and f.filename.lower().endswith(".pdf"):
                path = os.path.join(UPLOAD_FOLDER, secure_filename(f.filename))
                f.save(path)
                pdf_paths.append(path)

        if not pdf_paths:
            return "No valid PDF files uploaded", 400

        if vendor == "sakata":
            grouped = {}
            all_items = []
            pkg_descs = load_package_descriptions(user_token)
            treatments1 = load_treatments("Lot_Treatments_Card_Excel", user_token)
            treatments2 = load_treatments("Lot_Treatments_Card_2_Excel", user_token)

            with Pool(processes=cpu_count(), initializer=init_worker, initargs=(pkg_descs,)) as pool:
                results = pool.map(_extract_sakata_file, pdf_paths)

            for filename, items in results:
                if items:
                    grouped[filename] = items
                    all_items.extend(items)
                try:
                    os.remove(os.path.join(app.config["UPLOAD_FOLDER"], filename))
                except Exception as e:
                    app.logger.error(f"Could not delete {filename}: {e}")

            all_pos = set(item.get("PurchaseOrder") for item in all_items if item.get("PurchaseOrder"))
            if all_pos:
                try:
                    po_items = get_po_items("|".join(all_pos), user_token)
                    for item in all_items:
                        po = item.get("PurchaseOrder")
                        if po:
                            item["BCOptions"] = po_items  # Assign all PO items to each item with a matching PO
                        else:
                            item["BCOptions"] = []
                except Exception as e:
                    app.logger.error(f"Failed to fetch PO items: {e}")
                    for item in all_items:
                        item["BCOptions"] = [{"No": "ERROR", "Description": str(e)}]

            return render_template(
                "results_sakata.html",
                items=grouped,
                treatments1=treatments1,
                treatments2=treatments2,
                pkg_descs=pkg_descs
            )

        elif vendor == "hm_clause":
            grouped = {}
            with Pool(processes=cpu_count()) as pool:
                results = pool.map(_extract_hm_clause_file, pdf_paths)

            for filename, items in results:
                if items:
                    grouped[filename] = items
                try:
                    os.remove(os.path.join(app.config["UPLOAD_FOLDER"], filename))
                except Exception as e:
                    app.logger.error(f"Could not delete {filename}: {e}")

            return render_template("results_hm_clause.html", items=grouped)

        else:
            for path in pdf_paths:
                try:
                    os.remove(path)
                except Exception as e:
                    app.logger.error(f"Could not delete {path}: {e}")
            return "Unsupported vendor selected", 400

    return render_template("index.html", user_name=session.get("user_name"))

# Lot creation endpoint
@app.route("/create-lot", methods=["POST"])
@login_required
@timed_func("create_lot")
def create_lot():
    global lot_counter
    
    # Silent MSAL refresh on every create_lot call
    accounts = msal_app.get_accounts()
    if accounts:
        result = msal_app.acquire_token_silent(
            scopes=SCOPE_BC,
            account=accounts[0]
        )
        if "access_token" in result:
            session["user_token"] = result["access_token"]
            app.logger.info("Token refreshed successfully")
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
    
    data = request.get_json()
    
    def parse_decimal(val):
        s = str(val or "").strip()
        if not s or s.lower() == "none":
            return None
        try:
            return float(s)
        except ValueError:
            return None
    
    for key, val in data.items():
        if isinstance(val, str) and val.lower() == "none":
            data[key] = ""
            
    raw_sprout = data.get("SproutCount", "").strip()

    # Initialize lot_counter if not set
    if lot_counter is None:
        lot_counter = fetch_start_counter(session.get("user_token"))

    # Generate new Lot No. with zero-padding
    lot_no = f"V-INV-AUTO-TEST-{lot_counter:03d}"
    lot_counter += 1

    # Null-guard all inputs
    item_no     = str(data.get("BCItemNo", "")).strip() or None
    vendor_lot  = str(data.get("VendorLotNo", "")).strip() or None
    country     = str(data.get("OriginCountry", "")).strip() or None
    td1         = str(data.get("TreatmentsDescription", "")).strip() or None
    td2_text    = str(data.get("TreatmentsDescription2", "")).strip() or None
    seed_size   = str(data.get("SeedSize", "")).strip() or None

    seed_count = parse_decimal(data.get("SeedCount"))
    germ_pct   = parse_decimal(data.get("GrowerGerm"))
    purity     = parse_decimal(data.get("Purity"))
    inert      = parse_decimal(data.get("Inert"))
    usd_cost_val = parse_decimal(data.get("USD_Actual_Cost_$"))
    pkg_qty_dec_val = parse_decimal(data.get("Pkg_Qty"))
    pkg_qty_val = int(pkg_qty_dec_val) if pkg_qty_dec_val is not None else None

    raw_date = data.get("GrowerGermDate", "").strip()
    germ_date_iso = (
        datetime.strptime(raw_date, "%m/%d/%Y").date().isoformat()
        if raw_date else None
    )

    treated = "Yes" if td1 and td1.lower() != "untreated" else "No"
    
    if raw_sprout:
        td2_text = f"SPROUT COUNT-{raw_sprout}" + (", " + td2_text if td2_text else "")
    td2 = td2_text or None
    
    pkg_desc_val = data.get("PackageDescription")

    raw_payload = {
        "Item_No":                     item_no,
        "Lot_No":                      lot_no,
        "TMG_Vendor_Lot_No":           vendor_lot,
        "TMG_Country_Of_Origin":       country,
        "TMG_Treatment_Description":   td1,
        "TMG_Treatment_Description_2": td2,
        "TMG_Seed_Size":               seed_size,
        "TMG_Seed_Count":              seed_count,
        "TMG_Grower_Germ_Percent":     germ_pct,
        "TMG_GrowerGermDate":          germ_date_iso,
        "TMG_Purity":                  purity,
        "TMG_Inert":                   inert,
        "TMG_Treated":                 treated,
        "TMG_PackageQty":              pkg_qty_val,
        "TMG_USD_Actual_Cost":         usd_cost_val,
        "TMG_PackageDesc":             pkg_desc_val
    }

    payload = {k: v for k, v in raw_payload.items() if v is not None}

    app.logger.info("Extracted data: %s", data)
    app.logger.info("Raw payload: %s", raw_payload)
    app.logger.info("Prepared payload for BC: %s", payload)
    
    bc_url = (
        f"https://api.businesscentral.dynamics.com/v2.0/"
        f"{BC_TENANT}/{BC_ENV}/ODataV4/"
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
        return jsonify({"status": "success", "lotNo": lot_no})
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