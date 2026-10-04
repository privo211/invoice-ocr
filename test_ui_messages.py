"""Offline regression checks for UI responses and the existing upload contract."""
import io
from html.parser import HTMLParser

import pytest
import requests

from test_lot_creation_fields import app_module, _authenticated_client


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
