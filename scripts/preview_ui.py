"""Local UI preview with synthetic data; never imports the production application.

Run: python3 scripts/preview_ui.py
Open: http://127.0.0.1:8766/
"""
from datetime import datetime
from pathlib import Path
import re
from urllib.parse import parse_qs, urlparse

from flask import Flask, jsonify, redirect, render_template, request, url_for

ROOT = Path(__file__).resolve().parents[1]
app = Flask(__name__, template_folder=str(ROOT / 'templates'), static_folder=str(ROOT / 'static'))
app.config['TEMPLATES_AUTO_RELOAD'] = True
VENDORS = ('sakata', 'hm_clause', 'seminis', 'nunhems', 'syngenta', 'kamterter', 'kamterter_us', 'kamterter_shipping')


def customer_po(value):
    matches = re.findall(r'(?<!\d)(\d{5})(?!\d)', str(value or '').strip())
    return matches[0] if len(matches) == 1 else ''


app.jinja_env.filters['customer_po'] = customer_po
BC_OPTIONS = [{'No': '1750286-MS', 'Description': 'Tomato — Sample Variety', 'UnitOfMeasure': 'MS', 'Quantity': 50}]


def sample_items(vendor, count=1):
    filename = f'{vendor}_sample_invoice_10482.pdf'
    if vendor == 'kamterter_shipping':
        return {filename: {'customer_po': '91256', 'date_shipped': '10/03/2026', 'est_date_from_treater': '10/08/2026', 'errors': []}}
    if vendor in ('kamterter', 'kamterter_us'):
        return {filename: [{
            'VendorInvoiceNo': '10482', 'DocumentDate': '2026-10-03',
            'BuyFromVendorName': 'Kamterter', 'IsUSWarning': False,
            'Type': 'Item', 'No': '1750286-MS', 'Description': 'Tomato — Sample Variety',
            'Quantity': 50, 'DirectUnitCost': 12.5,
        }]}
    record = {
        'InvoiceNumber': '10482', 'VendorInvoiceNo': '10482', 'PurchaseOrder': '91256',
        'BCItemNo': '1750286-MS', 'SuggestedBCItemNo': '1750286-MS', 'BCOptions': BC_OPTIONS,
        'VendorItemNumber': 'T-104', 'VendorDescription': 'Tomato — Sample Variety',
        'VendorItemDescription': 'Tomato — Sample Variety', 'VendorLotNo': '4513632829',
        'VendorLot': '4513632829', 'VendorProductLot': '4513632829',
        'VendorBatchLot': '0262198116', 'VendorBatch': '0262198116', 'VendorBatchNo': '0262198116',
        'Germ': 94, 'CurrentGerm': 94, 'PackingGerm': 94, 'GrowerGerm': 96,
        'GermDate': '10/03/2026', 'CurrentGermDate': '10/03/2026',
        'PackingGermDate': '10/03/2026', 'GrowerGermDate': '10/03/2026',
        'Purity': 99.9, 'PureSeed': 99.9, 'Inert': 0.1, 'InertMatter': 0.1,
        'SeedCount': 1000, 'SeedCountPerLB': 12000, 'SproutCount': 0,
        'SeedSize': 'Standard', 'SeedForm': 'Raw', 'ProductForm': 'Raw',
        'OriginCountry': 'United States', 'PackageDescription': '50 MS',
        'Treatment': 'Untreated', 'TreatmentName': 'Untreated',
        'VendorTreatment': 'Untreated', 'TreatmentsDescription': 'Untreated',
        'TotalPrice': 625, 'TotalQuantity': 50, 'QtyShipped': 50,
        'TotalUpcharge': 0, 'TotalDiscount': 0, 'OriginalReceivedQty': 50, 'USD_Actual_Cost_$': 12.5,
    }
    records = []
    for index in range(count):
        lot = str(4513632829 + index)
        records.append({**record, 'VendorLotNo': lot, 'VendorLot': lot, 'VendorProductLot': lot})
    return {filename: records}


def context():
    return {
        'session': {} if request.args.get('signed_out') else {'user_token': 'synthetic-preview-token', 'user_name': 'Alex Morgan'},
        'kamterter_us_label': 'US SANDBOX', 'pkg_descs': ['50 MS', '100 MS'],
        'treatments1': ['Untreated', 'Primed'], 'treatments2': ['Uncoated', 'Film coated'],
        'bc_items': BC_OPTIONS, 'items': {}, 'bc_target': 'kamterter', 'bc_destination_label': 'Canada LIVE',
    }


@app.route('/', methods=['GET', 'POST'])
def index():
    if request.method == 'POST':
        # UI preview only: discard uploaded fixtures and show sample results.
        vendor = request.form.get('vendor', 'sakata')
        return redirect(url_for('results', vendor=vendor))
    return render_template('index.html', **context())


@app.route('/_preview/results/<vendor>')
def results(vendor):
    if vendor not in VENDORS:
        return 'Unknown preview vendor', 404
    values = context()
    count = 3 if request.args.get('lot_feedback') == 'mixed' else 1
    values['items'] = {} if request.args.get('empty') else sample_items(vendor, count=count)
    values['bc_target'] = vendor
    values['bc_destination_label'] = 'US SANDBOX' if vendor == 'kamterter_us' else 'Canada LIVE'
    template_vendor = 'kamterter' if vendor == 'kamterter_us' else vendor
    return render_template(f'results_{template_vendor}.html', **values)


@app.route('/logs')
def logs():
    values = context()
    values.update(stats={'total': 126, 'text_pages': 174, 'ocr_pages': 42, 'ocr_percent': 19.4},
                  logs=[] if request.args.get('empty') else [
                      (datetime(2026, 10, 3, 10, 30), 'Sakata', '91256', 'sakata_sample_invoice_10482.pdf', 'Text Extraction', 2),
                      (datetime(2026, 10, 3, 9, 15), 'Seminis', '91257', 'seminis_sample_invoice_10483.pdf', 'Azure OCR', 3),
                  ], current_page=request.args.get('page', 1, type=int), total_pages=3)
    return render_template('logs.html', **values)


@app.route('/logout')
def logout():
    values = context(); values['session'] = {}
    return render_template('logout.html', **values)


@app.route('/sign-in')
def sign_in():
    return redirect(url_for('index'))


@app.route('/_preview/message')
def message():
    return render_template('message.html', title='Unable to process PDFs', message='No valid PDF files uploaded',
                           action_endpoint='index', action_label='Back to Invoice Processor', **context()), 400


@app.route('/api/items')
@app.route('/bc-options')
def preview_items():
    return jsonify(BC_OPTIONS)


@app.route('/create-purchase-invoice', methods=['POST'])
@app.route('/update-kamterter-shipping-report', methods=['POST'])
def no_business_writes():
    return jsonify(status='error', message='Business writes are disabled in the UI preview.'), 409


@app.route('/create-lot', methods=['POST'])
def simulate_lot_feedback():
    # Explicit local preview scenarios only. These responses never create real lots.
    source = urlparse(request.referrer or '')
    mode = parse_qs(source.query).get('lot_feedback', [''])[0]
    if source.hostname != '127.0.0.1' or not source.path.startswith('/_preview/results/'):
        return no_business_writes()
    lot = str((request.get_json(silent=True) or {}).get('VendorLotNo', ''))
    if mode == 'mixed':
        mode = {'9': 'success', '0': 'partial', '1': 'error'}.get(lot[-1:], 'error')
    if mode == 'success':
        return jsonify(status='success', Lot_No=f'PREVIEW-{lot[-4:]}')
    if mode == 'partial':
        return jsonify(status='partial', Lot_No=f'PREVIEW-{lot[-4:]}',
                       message='The lot was created, but its germ dates and lot setup fields could not be updated. Review the lot in Business Central before retrying.'), 502
    if mode == 'error':
        return jsonify(status='error', message='The selected item cannot be used for this lot. Check the Business Central item number and vendor lot number.'), 400
    if mode == 'session':
        return jsonify(status='error', message='Synthetic expired session'), 401
    if mode == 'unconfirmed':
        return 'Synthetic unreadable response', 502
    return no_business_writes()


@app.route('/_preview')
def preview_directory():
    # Test-only appearance controls allow browser verification of historic preferences.
    links = ''.join(f'<li><a href="{url_for("results", vendor=vendor)}">{vendor}</a></li>' for vendor in VENDORS)
    return f'''<!doctype html><html lang="en"><title>UI preview directory</title><body>
    <h1>UI preview directory — synthetic data</h1><ul>{links}</ul>
    <p><a href="/">Home</a> · <a href="/logs">Logs</a> · <a href="/logout">Logout</a></p>
    <p>Lot feedback samples:
      <a href="/_preview/results/sakata?lot_feedback=success">Success</a> ·
      <a href="/_preview/results/sakata?lot_feedback=error">Error</a> ·
      <a href="/_preview/results/sakata?lot_feedback=mixed">Mixed outcomes</a>
    </p>
    <button onclick="localStorage.setItem('theme','dark-mode');location.href='/'">Historic results dark theme</button>
    <button onclick="localStorage.setItem('theme','dark');location.href='/'">Historic logs dark theme</button>
    </body></html>'''


if __name__ == '__main__':
    app.run(host='127.0.0.1', port=8766, debug=False)
