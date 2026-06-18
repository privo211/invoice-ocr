import os
from flask import Flask, render_template, session


def make_test_app():
    # Minimal Flask app for rendering templates without importing app.py
    base_dir = os.path.dirname(os.path.dirname(__file__))
    tpl_dir = os.path.join(base_dir, 'templates')
    app = Flask(__name__, template_folder=tpl_dir, static_folder=os.path.join(base_dir, 'static'))
    app.secret_key = 'test-secret'  # required for session machinery
    
    # provide a dummy sign_in endpoint used by the template
    @app.route('/sign_in')
    def sign_in():
        return 'sign-in'

    return app


def test_kamterter_shipping_template_structure():
    app = make_test_app()
    items = {
        'file_ok.pdf': {
            'date_shipped': '2026-06-16',
            'customer_po': '174-CAR-RA',
            'est_date_from_treater': '2026-06-22',
            'extraction_method': 'text',
            'errors': []
        },
        'file_err.pdf': {
            'date_shipped': '2026-06-17',
            'customer_po': 'PO-ERR',
            'est_date_from_treater': '2026-06-25',
            'extraction_method': 'ocr',
            'errors': ['bad date']
        }
    }

    with app.test_request_context('/'):
        session['user_token'] = 'test-token'
        html = render_template('results_kamterter_shipping.html', items=items)

    # Verification per task expectations
    assert '<table' not in html  # no tables allowed now
    assert 'Estimated Date From Treater' in html  # renamed label present
    assert 'Extraction Method' not in html  # field removed from UI
    assert '/create-purchase-invoice' not in html  # unrelated route must not appear

    # Modal + global action
    assert 'Update Selected in BC' in html
    assert 'id="shipping-selection-modal"' in html
    assert 'id="confirm-shipping-update-btn"' in html
