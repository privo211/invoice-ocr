"""Offline regression checks for UI responses and the existing upload contract."""
import io
from html.parser import HTMLParser
import re

import fitz
from openpyxl import Workbook
import pytest
import requests

from test_lot_creation_fields import app_module, _authenticated_client, _Response


@pytest.fixture
def offline_ui_app(app_module, monkeypatch):
    def block_network(*args, **kwargs):
        raise AssertionError("UI response tests must not make network requests")

    monkeypatch.setattr(requests.sessions.Session, "request", block_network)
    monkeypatch.setattr(app_module, "token_is_valid", lambda token: True)
    return app_module


def test_no_valid_pdf_keeps_bad_request_status_and_message(offline_ui_app, monkeypatch):
    client = _authenticated_client(offline_ui_app, monkeypatch)
    response = client.post("/", data={
        "vendor": "sakata",
        "pdfs": (io.BytesIO(b"plain text"), "notes.txt"),
    })
    assert response.status_code == 400
    html = response.get_data(as_text=True)
    assert "No valid PDF files uploaded" in html
    assert "Back to Invoice Processor" in html
    assert 'href="/"' in html


def test_missing_auth_code_keeps_failure_status_and_message(offline_ui_app):
    response = offline_ui_app.app.test_client().get(offline_ui_app.REDIRECT_PATH)
    assert response.status_code == 400
    html = response.get_data(as_text=True)
    assert "Authentication failed: No code received" in html
    assert 'href="/sign-in"' in html


def test_failed_auth_result_keeps_failure_status_and_message(offline_ui_app, monkeypatch):
    class FailedMsalApp:
        def acquire_token_by_authorization_code(self, **kwargs):
            return {"error": "synthetic_failure", "error_description": "Synthetic sign-in failure"}

    monkeypatch.setattr(offline_ui_app, "load_cache", lambda: object())
    monkeypatch.setattr(offline_ui_app, "build_msal_app", lambda cache: FailedMsalApp())
    client = offline_ui_app.app.test_client()
    response = client.get(offline_ui_app.REDIRECT_PATH, query_string={"code": "synthetic-code"})
    assert response.status_code == 400
    assert "Authentication failed" in response.get_data(as_text=True)
    with client.session_transaction() as session:
        assert "user_token" not in session


def test_stats_maintenance_keeps_operation_and_result(offline_ui_app, monkeypatch):
    calls = []
    monkeypatch.setattr(offline_ui_app.db_logger, "recalculate_stats", lambda: calls.append("recalculate") or "Stats synchronized")
    client = _authenticated_client(offline_ui_app, monkeypatch)
    response = client.get("/fix-stats")
    assert response.status_code == 200
    assert calls == ["recalculate"]
    html = response.get_data(as_text=True)
    assert "Stats synchronized" in html
    assert 'href="/logs"' in html


def test_home_keeps_multipart_upload_fields_and_vendor_destinations(offline_ui_app, monkeypatch):
    class UploadParser(HTMLParser):
        def __init__(self):
            super().__init__()
            self.form = None
            self.file_input = None
            self.vendor_select = None
            self.vendors = []
            self.in_vendor = False

        def handle_starttag(self, tag, attrs):
            attrs = dict(attrs)
            if tag == "form" and attrs.get("id") == "extract-form":
                self.form = attrs
            if tag == "input" and attrs.get("name") == "pdfs":
                self.file_input = attrs
            if tag == "select" and attrs.get("name") == "vendor":
                self.vendor_select = attrs
                self.in_vendor = True
            if tag == "option" and self.in_vendor:
                self.vendors.append(attrs["value"])

        def handle_endtag(self, tag):
            if tag == "select":
                self.in_vendor = False

    client = _authenticated_client(offline_ui_app, monkeypatch)
    response = client.get("/")
    assert response.status_code == 200
    parser = UploadParser()
    parser.feed(response.get_data(as_text=True))
    assert parser.form["action"] == "/"
    assert parser.form["method"].lower() == "post"
    assert parser.form["enctype"] == "multipart/form-data"
    assert parser.file_input["type"] == "file"
    assert parser.file_input["accept"] == "application/pdf"
    assert "multiple" in parser.file_input
    assert "disabled" not in parser.file_input
    assert parser.vendor_select is not None
    assert parser.vendors == ["sakata", "hm_clause", "seminis", "nunhems", "syngenta", "kamterter", "kamterter_us", "kamterter_shipping"]


def _kamterter_upload(include_export=True):
    with fitz.open() as pdf:
        pdf.new_page().insert_text((40, 40), "\n".join([
            "Invoice #: 12345", "Invoiced Date: 10/03/2026",
            "Lot #: SAMPLE", "KTT #: 100", "PO #: TR-00009", "Seed Type: Onion",
            "Shipped Weight: 8181.31 M", "Shipping Mode: Freight", "Subtotal: $120.00",
            "Lot #: SAMPLE", "KTT #: 100", "PO #: TR-00010", "Seed Type: Onion",
            "Shipped Weight: 50.42 M", "Shipping Mode: Freight", "Subtotal: $25.50",
            "Total: $145.50",
        ]))
        data = {"vendor": "kamterter_us", "pdfs": (io.BytesIO(pdf.tobytes()), "invoice.pdf")}
    if include_export:
        workbook = Workbook()
        headers = workbook.active
        headers.title = "910 Posted Assembly Header"
        headers.append(["No.", "Order No.", "CustomerPO", "Reversed"])
        headers.append(["PA-00001", "TR-00009", "TR-00009", False])
        headers.append(["PA-00002", "TR-00010", "TR-00010", False])
        headers.append(["PA-00003", "TR-00009", "TR-00009", True])
        lines = workbook.create_sheet("911 Posted Assembly Line")
        lines.append(["Document No.", "Type", "No.", "Quantity", "Unit of Measure Code"])
        lines.append(["PA-00001", "Item", "SEED", 8181, "MS"])
        lines.append(["PA-00001", "Resource", "ON-THI", 8181, "MS"])
        lines.append(["PA-00002", "Resource", "ON-MK", 50, "MS"])
        export = io.BytesIO()
        workbook.save(export)
        export.seek(0)
        data["assembly_export"] = (export, "assembly.xlsx")
    return data


@pytest.mark.parametrize("line_status", [201, 400])
def test_kamterter_us_review_creates_sandbox_draft_once(offline_ui_app, monkeypatch, tmp_path, line_status):
    app = offline_ui_app
    monkeypatch.setitem(app.app.config, "UPLOAD_FOLDER", str(tmp_path))
    monkeypatch.setitem(app.KAMTERTER_BC_TARGETS["kamterter_us"], "environment", "Sandbox-Holland-260216")
    monkeypatch.setitem(app.KAMTERTER_BC_TARGETS["kamterter_us"], "company", "Stokes Seeds US")
    client = _authenticated_client(app, monkeypatch)
    upload = _kamterter_upload()
    pdf_bytes = upload["pdfs"][0].getvalue()
    response = client.post("/", data=upload)
    assert response.status_code == 200
    html = response.get_data(as_text=True)
    assert "INV12345_Onion_TR-00009" in html and "INV12345_Onion_TR-00010" in html
    assert 'value="8181.0"' in html and 'value="50.0"' in html and 'value="0.01467"' in html
    assert "PDF shipped weight 8181.31 M" in html
    assert 'data-invoice-total="145.50"' in html
    token = re.search(r'data-upload-token="([^"]+)"', html)[1]
    writes = []

    def get(url, **kwargs):
        assert "/Sandbox-Holland-260216/" in url
        assert kwargs["params"]["$filter"] == "name eq 'Stokes Seeds US'"
        return _Response(200, {"value": [{"id": "us-company"}]})

    def post(url, **kwargs):
        assert "/Sandbox-Holland-260216/" in url and "companies(us-company)" in url
        writes.append(kwargs["json"])
        if url.endswith("/PurchaseHeaders"):
            return _Response(201, {"No": "DRAFT-1", "SystemId": "draft-id"})
        if url.endswith("/PurchaseLines"):
            return _Response(line_status)
        assert url.endswith("/documentAttachments")
        return _Response(201, {"id": "attachment-id"})

    def patch(url, **kwargs):
        assert "/Sandbox-Holland-260216/" in url
        if url.endswith("/attachmentContent"):
            assert kwargs["data"].read() == pdf_bytes
        else:
            assert kwargs["json"] == {"Document_Date": "2026-10-03"}
        return _Response(204)

    monkeypatch.setattr(app.requests, "get", get)
    monkeypatch.setattr(app.requests, "post", post)
    monkeypatch.setattr(app.requests, "patch", patch)
    lines = [
        {"Type": "Resource", "No": "ON-THI", "Description": "INV12345_Onion_TR-00009",
         "Quantity": 8181, "Direct_Unit_Cost": 0.01467},
        {"Type": "Resource", "No": "ON-MK", "Description": "INV12345_Onion_TR-00010",
         "Quantity": 50, "Direct_Unit_Cost": 0.51},
    ]
    payload = {"bc_target": "kamterter_us", "UploadToken": token, "PurchaseLines": lines}
    assert client.post("/create-purchase-invoice", json={**payload, "bc_target": "kamterter"}).status_code == 400
    assert client.post("/create-purchase-invoice", json={**payload, "UploadToken": token + "x"}).status_code == 400
    response = client.post("/create-purchase-invoice", json=payload)
    assert response.status_code == line_status
    assert response.json["No"] == "DRAFT-1"
    assert writes[0] == {
        "Document_Type": "Invoice", "Vendor_Name": "KAMTERTER II, LLC (M&T EDI)",
        "Vendor_Invoice_No": "12345", "Document_Date": "2026-10-03",
    }
    assert writes[1]["No"] == "ON-THI"
    assert writes[1]["Description"] == lines[0]["Description"]
    assert writes[1]["Quantity"] == 8181
    if line_status == 201:
        assert response.json["invoice_discount_amount"] == "0.02"
        assert writes[2]["No"] == "ON-MK"
        assert writes[3]["fileName"] == "invoice.pdf"
        assert not list(tmp_path.iterdir())
    else:
        assert len(writes) == 2

    write_count = len(writes)
    assert client.post("/create-purchase-invoice", json=payload).status_code == 409
    assert len(writes) == write_count


def test_kamterter_us_requires_tr_match_and_rejects_live_target(offline_ui_app, monkeypatch, tmp_path):
    app = offline_ui_app
    monkeypatch.setitem(app.app.config, "UPLOAD_FOLDER", str(tmp_path))
    target = app.KAMTERTER_BC_TARGETS["kamterter_us"]
    monkeypatch.setitem(target, "environment", "Sandbox-Holland-260216")
    client = _authenticated_client(app, monkeypatch)
    response = client.post("/", data=_kamterter_upload(include_export=False))
    assert response.status_code == 200
    assert "TR-00009 needs one matching posted assembly" in response.get_data(as_text=True)
    assert not list(tmp_path.iterdir())

    monkeypatch.setitem(target, "environment", "Production-Holland")
    assert client.post("/", data=_kamterter_upload()).status_code == 400
    assert client.post("/create-purchase-invoice", json={"bc_target": "kamterter_us"}).status_code == 400


def test_kamterter_us_separates_unprocessed_seed_and_billed_freight(offline_ui_app, monkeypatch, tmp_path):
    monkeypatch.setitem(offline_ui_app.app.config, "UPLOAD_FOLDER", str(tmp_path))
    monkeypatch.setitem(offline_ui_app.KAMTERTER_BC_TARGETS["kamterter_us"], "environment", "Sandbox-Test")
    with fitz.open() as pdf:
        pdf.new_page().insert_text((40, 40), "\n".join([
            "Invoice #: 12345", "Invoiced Date: 10/03/2026",
            "Lot #: SAMPLE", "KTT #: 100", "PO #: TR-00009",
            "Comments: Seeds were left unprocessed.",
            "$12.45", "Freight: UPS", "$30.45", "Subtotal:", "$30.45", "Total:",
        ]))
        upload = {"vendor": "kamterter_us", "pdfs": (io.BytesIO(pdf.tobytes()), "invoice.pdf")}
    response = _authenticated_client(offline_ui_app, monkeypatch).post("/", data=upload)
    assert response.status_code == 200
    html = response.get_data(as_text=True)
    assert html.count('value="609100"') == 2
    assert "INV12345_TR-00009_Unprocessed Seed" in html and 'value="18.0"' in html
    assert "INV12345_TR-00009_Freight" in html and 'value="12.45"' in html
