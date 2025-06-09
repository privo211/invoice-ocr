from flask import Flask, render_template, request, jsonify
from flask import session, redirect, url_for, render_template_string
from werkzeug.utils import secure_filename
from werkzeug.middleware.proxy_fix import ProxyFix
import os
import re
import traceback
import pandas as pd
import requests
from datetime import datetime
import msal
from dotenv import load_dotenv
from functools import wraps
from msal import ConfidentialClientApplication
from multiprocessing import Pool, cpu_count
from vendor_extractors.sakata import extract_sakata_data, load_all_items, load_package_descriptions, get_po_items, token as bc_token
from vendor_extractors.hm_clause import extract_hm_clause_data
import time
import logging
from functools import wraps

app = Flask(__name__)

logging.basicConfig(level=logging.INFO)

def _extract_sakata_file(path):
    """Helper for Pool: returns (filename, items)"""
    from vendor_extractors.sakata import extract_sakata_data
    return os.path.basename(path), extract_sakata_data([path])

# ─── A decorator to time any function and log its elapsed time ───
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

# ─── Timing wrappers ───
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

# BC connection settings
BC_TENANT  = "33b1b67a-786c-4b46-9372-c4e492d15cf1"
BC_ENV      = "SANDBOX-2025"
BC_COMPANY = "Stokes%20Seeds%20Limited"

# ─── Load environment variables ────
load_dotenv()

TENANT_ID     = os.environ["AZURE_TENANT_ID"]
CLIENT_ID     = os.environ["AZURE_CLIENT_ID"]
CLIENT_SECRET = os.environ["AZURE_CLIENT_SECRET"]
AUTHORITY     = f"https://login.microsoftonline.com/{TENANT_ID}"
REDIRECT_PATH = "/auth/callback"
SCOPE_BC      = ["https://api.businesscentral.dynamics.com/.default"]

msal_app = msal.ConfidentialClientApplication(
    client_id=CLIENT_ID,
    client_credential=CLIENT_SECRET,
    authority=AUTHORITY
)

@timed_func("token_is_valid")
def token_is_valid(access_token: str) -> bool:
    """
    Do a tiny GET against a read‐only BC endpoint to confirm the token still works.
    Returns True if BC returns 200, False otherwise.
    """
    if not access_token:
        return False

    test_url = (
        f"https://api.businesscentral.dynamics.com/v2.0/"
        f"{BC_TENANT}/{BC_ENV}/ODataV4/"
        f"Company('{BC_COMPANY}')/Items?$top=1"
    )
    resp = timed_get(
        test_url,
        headers={"Authorization": f"Bearer {access_token}"}
    )
    return resp.status_code == 200

@timed_func("fetch_start_counter")
def fetch_start_counter():
    # 1) Pull the top 1 V-INV-AUTO-TEST-xxxx record, ordered by Lot No descending
    url = (
      f"https://api.businesscentral.dynamics.com/v2.0/"
      f"{BC_TENANT}/{BC_ENV}/ODataV4/"
      f"Company('{BC_COMPANY}')/Lot_No_Information_Card_Excel"
      "?$filter=startswith(Lot_No,'V-INV-AUTO-TEST-')"
      "&$orderby=Lot_No desc"
      "&$top=1"
    )
    resp = timed_get(url, headers={"Authorization":f"Bearer {bc_token}"})
    resp.raise_for_status()
    vals = resp.json().get('value', [])
    if not vals:
        return 1
    # assume Lot No. is like V-INV-AUTO-TEST-012
    match = re.match(r"V-INV-AUTO-TEST-(\d+)", vals[0]["Lot_No"])
    return int(match.group(1)) + 1 if match else 1

# at module scope, instead of lot_counter=1:
lot_counter = fetch_start_counter()

# Tell Flask to honor X-Forwarded-Proto / X-Forwarded-For headers from NGINX
app.wsgi_app = ProxyFix(app.wsgi_app, x_proto=1, x_host=1)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
UPLOAD_FOLDER = os.path.join(BASE_DIR, "uploads")
app.config["UPLOAD_FOLDER"] = UPLOAD_FOLDER

app.secret_key = os.environ["SECRET_KEY"]

DATA_DIR = os.path.join(BASE_DIR, "data")

# ─── pull live “Lot_Treatments_Card_Excel” tables from BC ───
@timed_func("load_treatments")
def load_treatments(endpoint: str) -> list[str]:
    """
    Fetch all rows from BC OData endpoint
    /<endpoint>, and return a list of Treatment_Name values.
    """
    url = (
        f"https://api.businesscentral.dynamics.com/v2.0/"
        f"{BC_TENANT}/Production/ODataV4/"
        f"Company('{BC_COMPANY}')/{endpoint}"
    )
    headers = {
        "Authorization": f"Bearer {bc_token}",
        "Accept":         "application/json"
    }
    resp = timed_get(url, headers=headers)
    resp.raise_for_status()
    rows = resp.json().get("value", [])
    # assume each row has a field called "Treatment_Name"
    return [r["Treatment_Name"].strip() for r in rows if r.get("Treatment_Name")]

# ─── preload caches at module-import (works with gunicorn --preload) ───
load_package_descriptions()
load_treatments("Lot_Treatments_Card_Excel")
load_treatments("Lot_Treatments_Card_2_Excel")

def login_required(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        if not session.get("user_token"):
            return redirect(url_for("sign_in"))
        return f(*args, **kwargs)
    return wrapper

@app.route("/", methods=["GET", "POST"])
@login_required
@timed_func("index handler")
def index():
    
    user_token = session.get("user_token")

    # 1) If token is missing or invalid, drop session and force sign-in
    if not token_is_valid(user_token):
        session.pop("user_token", None)
        session.pop("user_name", None)
        return redirect(url_for("sign_in"))

    if request.method == "POST":
        vendor = request.form.get("vendor")
        files = request.files.getlist("pdfs")
        os.makedirs(UPLOAD_FOLDER, exist_ok=True)

        # save uploaded PDFs
        pdf_paths = []
        for f in files:
            if f.filename.lower().endswith(".pdf"):
                path = os.path.join(UPLOAD_FOLDER, secure_filename(f.filename))
                f.save(path)
                pdf_paths.append(path)

        # group results by filename
        grouped = {}
        # if vendor == "sakata":
        #     for path in pdf_paths:
        #         filename = os.path.basename(path)
        #         items = extract_sakata_data([path])   # returns List[Dict]
        #         if items:
        #             grouped[filename] = items
                    
        #         # ── DELETE the file immediately once it’s been OCR’d ──
        #         try:
        #             os.remove(path)
        #         except Exception as e:
        #             app.logger.error(f"Could not delete {path}: {e}")
                
        #     for fname, items in grouped.items():
        #             for i, itm in enumerate(items):
        #                 for j, lot in enumerate(itm['Lots']):
        #                     td1 = request.form.getlist(f"td1-{fname}-{j}")
        #                     td2 = request.form.getlist(f"td2-{fname}-{j}")
        #                     lot['TreatmentsDescription']  = ",".join(td1)
        #                     lot['TreatmentsDescription2'] = ",".join(td2)
                            
        #     # ── fetch live list of package descriptions from BC ──
        #     pkg_descs = load_package_descriptions()
            
        #     # now fetch both lists:
        #     treatments1 = load_treatments("Lot_Treatments_Card_Excel")
        #     treatments2 = load_treatments("Lot_Treatments_Card_2_Excel")
                            
        #     return render_template(
        #         "results_sakata.html", 
        #         items=grouped, 
        #         treatments1=treatments1, 
        #         treatments2=treatments2,
        #         pkg_descs=pkg_descs
        #     )
        if vendor == "sakata":
            grouped = {}

            # spawn one process per CPU, each preloading the caches
            with Pool(
                processes=cpu_count(),
                initializer=lambda: (
                    load_package_descriptions(),
                    load_treatments("Lot_Treatments_Card_Excel"),
                    load_treatments("Lot_Treatments_Card_2_Excel")
                )
            ) as pool:
                results = pool.map(_extract_sakata_file, pdf_paths)

            for filename, items in results:
                if items:
                    grouped[filename] = items
                # delete each file after processing
                try:
                    os.remove(os.path.join(app.config["UPLOAD_FOLDER"], filename))
                except Exception as e:
                    app.logger.error(f"Could not delete {filename}: {e}")

            pkg_descs   = load_package_descriptions()
            treatments1 = load_treatments("Lot_Treatments_Card_Excel")
            treatments2 = load_treatments("Lot_Treatments_Card_2_Excel")
            return render_template(
                "results_sakata.html",
                items=grouped,
                treatments1=treatments1,
                treatments2=treatments2,
                pkg_descs=pkg_descs
            )


        elif vendor == "hm_clause":
            for path in pdf_paths:
                filename = os.path.basename(path)
                items = extract_hm_clause_data([path])  # returns List[Dict]
                if items:
                    grouped[filename] = items
                    
                # ── DELETE the file immediately once it’s been OCR’d ──
                try:
                    os.remove(path)
                except Exception as e:
                    app.logger.error(f"Could not delete {path}: {e}")
                    
            return render_template("results_hm_clause.html", items=grouped)

        else:
            return "Unsupported vendor selected", 400

    return render_template("index.html", user_name=session.get("user_name"))

# ─── Sign-In Route ────
@app.route("/sign_in")
def sign_in():
    auth_url = msal_app.get_authorization_request_url(
        scopes=SCOPE_BC,
        redirect_uri=url_for("auth_callback", _external=True),
        prompt="select_account"
    )
    return redirect(auth_url)

# ─── OAuth Callback (authorization code) ─────
@app.route(REDIRECT_PATH)
def auth_callback():
    if "error" in request.args:
        return f"Error: {request.args['error']}, {request.args.get('error_description')}", 400

    code = request.args.get("code")
    if not code:
        return "No authorization code provided", 400

    result = msal_app.acquire_token_by_authorization_code(
        code=code,
        scopes=SCOPE_BC,
        redirect_uri=url_for("auth_callback", _external=True)
    )

    if "access_token" in result:
        session["user_token"] = result["access_token"]
        session["user_name"]  = result.get("id_token_claims", {}).get("preferred_username", "")
        return redirect(url_for("index"))
    else:
        return f"Token acquisition failed: {result.get('error')} - {result.get('error_description')}", 400

# ─── Sign-Out Route ────
@app.route("/sign_out")
def sign_out():
    session.clear()
    return redirect(
        f"{AUTHORITY}/oauth2/v2.0/logout?post_logout_redirect_uri={url_for('index', _external=True)}"
    )

@app.route("/bc-options")
def bc_options():
    po_raw = request.args.get("po", "").strip()
    app.logger.debug(f"BC lookup called with raw po = {po_raw!r}")

    if not po_raw:
        return jsonify([])

    # extract the *first* PO-##### in that string
    m = re.search(r"\bPO[-\s]*(\d{5})\b", po_raw, re.IGNORECASE)
    if not m:
        app.logger.error("bc-options: could not parse a valid PO-##### from %r", po_raw)
        return jsonify([])

    po = f"PO-{m.group(1)}"
    app.logger.debug(f"bc-options: using normalized po = {po}")

    try:
        opts = get_po_items(po, bc_token)
    except Exception as e:
        app.logger.error("bc-options lookup failed:\n%s", traceback.format_exc())
        # return a 200 so the frontend can read the error object
        return jsonify([{"No": "ERROR", "Description": str(e)}])

    return jsonify(opts)

@app.route("/create-lot", methods=["POST"])
@login_required
@timed_func("create_lot")
def create_lot():
    global lot_counter
    
    # silent MSAL refresh on every create_lot call
    accounts = msal_app.get_accounts()
    if accounts:
        result = msal_app.acquire_token_silent(
            scopes=SCOPE_BC,
            account=accounts[0]
        )
        if "access_token" in result:
            session["user_token"] = result["access_token"]
        else:
            session.pop("user_token", None)
            session.pop("user_name", None)
            return redirect(url_for("sign_in"))
    else:
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

    # 1) generate new Lot No.
    lot_no = f"V-INV-AUTO-TEST-{lot_counter:03d}"
    lot_counter += 1

    # 2) null‐guard all inputs

    # Strings (always safe to str())
    item_no     = str(data.get("BCItemNo", "")).strip() or None
    vendor_lot  = str(data.get("VendorLotNo", "")).strip() or None
    country     = str(data.get("OriginCountry", "")).strip() or None
    td1         = str(data.get("TreatmentsDescription", "")).strip() or None
    td2_text    = str(data.get("TreatmentsDescription2", "")).strip() or None
    seed_size   = str(data.get("SeedSize", "")).strip() or None

    # Decimals → only convert if non-empty   
    seed_count = parse_decimal(data.get("SeedCount"))
    germ_pct   = parse_decimal(data.get("GrowerGerm"))
    purity     = parse_decimal(data.get("Purity"))
    inert      = parse_decimal(data.get("Inert"))
    
    usd_cost_val = parse_decimal(data.get("USD_Actual_Cost_$"))
    
    #Integer → only convert if non-empty
    pkg_qty_dec_val = parse_decimal(data.get("Pkg_Qty"))
    pkg_qty_val = int(pkg_qty_dec_val) if pkg_qty_dec_val is not None else None

    # Date → parse only if non-empty
    raw_date = data.get("GrowerGermDate", "").strip()
    if raw_date:
        germ_date_iso = (
            datetime.strptime(raw_date, "%m/%d/%Y")
            .date()
            .isoformat()
        )
    else:
        germ_date_iso = None

    # Treated flag
    treated = "Yes" if td1 and td1.lower() != "untreated" else "No"
    
    # build the td2 string unconditionally from raw_sprout + TreatmentsDescription2
    if raw_sprout:
        td2_text = f"SPROUT COUNT-{raw_sprout}" + (", " + td2_text if td2_text else "")
    # if data.get("TreatmentsDescription2", "").strip():
    #     td2_text += (", " if td2_text else "") + data["TreatmentsDescription2"].strip()

    td2 = td2_text or None
    
    pkg_desc_val = data.get("PackageDescription")

    # 3) build payload
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

    # filter out any None values so those keys are left off entirely
    payload = { k: v for k, v in raw_payload.items() if v is not None }

    print("Extracted data:", data)
    print("Raw payload:", raw_payload)
    print("Prepared payload for BC:", payload)
    
    # 4) POST to BC
    bc_url = (
        f"https://api.businesscentral.dynamics.com/v2.0/"
        f"{BC_TENANT}/{BC_ENV}/ODataV4/"
        f"Company('{BC_COMPANY}')/Lot_Info_Card"
    )
    headers = {
        "Authorization": f"Bearer {session.get('user_token')}",
        "Content-Type":  "application/json",
        "Prefer":        "odata.maxversion=4.0;IEEE754Compatible=true"
    }

    resp = timed_post(bc_url, json=payload, headers=headers)
    try:
        resp.raise_for_status()
        return jsonify({"status": "success", "lotNo": lot_no})

    except requests.exceptions.HTTPError:
        # If BC still returns 401 despite the silent-refresh, force sign-in
        if resp.status_code == 401:
            session.pop("user_token", None)
            session.pop("user_name", None)
            return redirect(url_for("sign_in"))

        print("BC API error:", resp.status_code, resp.text)
        return jsonify({
            "status": "error",
            "message": resp.text
        }), resp.status_code

    except Exception as e:
        print("Unexpected error:", traceback.format_exc())
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route("/api/items")
def api_items():
    """
    Returns a JSON array of all { No, Description } from BC.
    """
    return jsonify(load_all_items())

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5001)
