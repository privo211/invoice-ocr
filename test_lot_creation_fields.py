import importlib
from pathlib import Path
import sys
import types

import pytest
import requests
from jinja2 import Environment, FileSystemLoader


class _Response:
    def __init__(self, status_code, payload=None, text=""):
        self.status_code = status_code
        self._payload = payload or {}
        self.text = text

    def json(self):
        return self._payload

    def raise_for_status(self):
        if self.status_code >= 400:
            raise AssertionError(f"Unexpected HTTP error in test response: {self.status_code}")


def _module(name, **attributes):
    module = types.ModuleType(name)
    for key, value in attributes.items():
        setattr(module, key, value)
    return module


@pytest.fixture
def app_module(monkeypatch):
    no_op = lambda *args, **kwargs: None
    no_items = lambda *args, **kwargs: []
    fake_modules = {
        "db_logger": _module(
            "db_logger",
            init_app=no_op,
            log_processing_event=no_op,
            get_log_stats=lambda: {},
            get_paginated_logs=lambda **kwargs: ([], 0),
            recalculate_stats=lambda: "ok",
        ),
        "vendor_extractors.sakata": _module(
            "vendor_extractors.sakata",
            load_package_descriptions=no_items,
            get_po_items=no_items,
        ),
        "vendor_extractors.hm_clause": _module(
            "vendor_extractors.hm_clause",
            extract_hm_clause_data_from_bytes=no_items,
            find_best_hm_clause_package_description=no_op,
        ),
        "vendor_extractors.kamterter": _module(
            "vendor_extractors.kamterter",
            extract_kamterter_data_from_bytes=no_items,
        ),
        "vendor_extractors.kamterter_us": _module(
            "vendor_extractors.kamterter_us",
            extract_kamterter_us_data_from_bytes=no_items,
        ),
        "vendor_extractors.kamterter_shipping": _module(
            "vendor_extractors.kamterter_shipping",
            extract_kamterter_shipping_data_from_bytes=no_items,
        ),
        "vendor_extractors.seminis": _module(
            "vendor_extractors.seminis",
            extract_seminis_data_from_bytes=no_items,
            find_best_seminis_package_description=no_op,
        ),
        "vendor_extractors.syngenta": _module(
            "vendor_extractors.syngenta",
            extract_syngenta_data_from_bytes=no_items,
        ),
        "vendor_extractors.nunhems": _module(
            "vendor_extractors.nunhems",
            extract_nunhems_data_from_bytes=no_items,
            find_best_nunhems_package_description=no_op,
        ),
    }
    for name, module in fake_modules.items():
        monkeypatch.setitem(sys.modules, name, module)

    monkeypatch.setenv("SECRET_KEY", "test-secret")
    monkeypatch.setenv("AZURE_TENANT_ID", "test-tenant")
    monkeypatch.setenv("AZURE_CLIENT_ID", "test-client")
    monkeypatch.setenv("AZURE_CLIENT_SECRET", "test-client-secret")
    monkeypatch.setenv("BC_COMPANY", "Stokes%20Seeds%20Limited")

    sys.modules.pop("app", None)
    module = importlib.import_module("app")
    module.app.config.update(TESTING=True)
    yield module
    sys.modules.pop("app", None)


def _authenticated_client(app_module, monkeypatch):
    class _MsalApp:
        def get_accounts(self):
            return [object()]

        def acquire_token_silent(self, **kwargs):
            return {"access_token": "refreshed-token"}

    monkeypatch.setattr(app_module, "load_cache", lambda: object())
    monkeypatch.setattr(app_module, "build_msal_app", lambda cache: _MsalApp())
    monkeypatch.setattr(app_module, "save_cache", lambda cache: None)

    client = app_module.app.test_client()
    with client.session_transaction() as flask_session:
        flask_session["user_token"] = "test-token"
    return client


def test_lot_field_normalizers(app_module):
    assert app_module.normalize_customer_po("PO-91256") == "91256"
    assert app_module.normalize_customer_po("91256") == "91256"
    assert app_module.normalize_customer_po("PO-1234") == ""
    assert app_module.normalize_customer_po("91256 / 91257") == ""

    assert app_module.normalize_lot_date("07/09/2026") == "2026-07-09"
    assert app_module.normalize_lot_date("7/9/26") == "2026-07-09"
    assert app_module.normalize_lot_date("2026-07-09") == "2026-07-09"
    assert app_module.normalize_lot_date("") is None


def test_create_lot_updates_dates_and_lot_setup_fields(app_module, monkeypatch):
    calls = {"post": [], "patch": []}

    def fake_post(url, **kwargs):
        calls["post"].append((url, kwargs))
        return _Response(201, {
            "Item_No": "1750286-MS",
            "Lot_No": "37-0030-50MS",
        })

    def fake_patch(url, **kwargs):
        calls["patch"].append((url, kwargs))
        return _Response(204)

    monkeypatch.setattr(app_module.requests, "post", fake_post)
    monkeypatch.setattr(app_module.requests, "patch", fake_patch)

    client = _authenticated_client(app_module, monkeypatch)

    response = client.post("/create-lot", json={
        "vendor": "seminis",
        "BCItemNo": "1750286-MS",
        "VendorLotNo": "4513632829/0310",
        "VendorBatchLot": "0262198116",
        "CurrentGerm": "94",
        "CurrentGermDate": "07/09/2026",
        "GrowerGerm": "96",
        "GrowerGermDate": "07/16/2026",
        "CustomerPO": "91256",
        "GPCertificationOutstanding": True,
        "UnderWeightExemption": False,
        "GermSampleRequired": True,
    })

    assert response.status_code == 200
    assert response.get_json() == {"status": "success", "Lot_No": "37-0030-50MS"}

    create_url, create_request = calls["post"][0]
    assert create_url.endswith("/Lot_Info_Card")
    assert create_request["json"]["TMG_Germ_Date"] == "2026-07-09"
    assert create_request["json"]["TMG_GrowerGermDate"] == "2026-07-16"

    patch_url, patch_request = calls["patch"][0]
    assert "/Lot_No_Information_Card_Excel(" in patch_url
    assert "Item_No='1750286-MS'" in patch_url
    assert "Variant_Code=''" in patch_url
    assert "Lot_No='37-0030-50MS'" in patch_url
    assert patch_request["headers"]["If-Match"] == "*"
    assert patch_request["json"] == {
        "Germ_Date": "2026-07-09",
        "TMG_GrowerGermDate": "2026-07-16",
        "Customer_PO": "91256",
        "G_x0026_P_Certification_Outstanding": True,
        "Under_Weight_Exemption": False,
        "Germ_Sample_Required": True,
    }


def test_followup_http_failure_reports_created_lot_for_safe_recovery(app_module, monkeypatch):
    monkeypatch.setattr(
        app_module.requests,
        "post",
        lambda *args, **kwargs: _Response(201, {
            "Item_No": "1750286-MS",
            "Lot_No": "37-0030-50MS",
        }),
    )
    monkeypatch.setattr(
        app_module.requests,
        "patch",
        lambda *args, **kwargs: _Response(
            500,
            {"error": {"message": "Lot card update failed"}},
        ),
    )

    client = _authenticated_client(app_module, monkeypatch)
    response = client.post("/create-lot", json={
        "vendor": "seminis",
        "BCItemNo": "1750286-MS",
        "VendorLotNo": "4513632829/0310",
        "CustomerPO": "91256",
        "GPCertificationOutstanding": False,
        "UnderWeightExemption": False,
        "GermSampleRequired": False,
    })

    assert response.status_code == 502
    assert response.get_json() == {
        "status": "partial",
        "Lot_No": "37-0030-50MS",
        "message": (
            "Lot 37-0030-50MS was created, but the germ dates and lot setup fields "
            "could not be updated: Lot card update failed"
        ),
    }


def test_followup_network_failure_preserves_created_lot_number(app_module, monkeypatch):
    monkeypatch.setattr(
        app_module.requests,
        "post",
        lambda *args, **kwargs: _Response(201, {
            "Item_No": "1750286-MS",
            "Lot_No": "37-0030-50MS",
        }),
    )
    monkeypatch.setattr(
        app_module.requests,
        "patch",
        lambda *args, **kwargs: (_ for _ in ()).throw(requests.ConnectionError("offline")),
    )

    client = _authenticated_client(app_module, monkeypatch)
    response = client.post("/create-lot", json={
        "vendor": "seminis",
        "BCItemNo": "1750286-MS",
        "VendorLotNo": "4513632829/0310",
        "CustomerPO": "91256",
        "GPCertificationOutstanding": False,
        "UnderWeightExemption": False,
        "GermSampleRequired": False,
    })

    body = response.get_json()
    assert response.status_code == 502
    assert body["status"] == "partial"
    assert body["Lot_No"] == "37-0030-50MS"
    assert "Check this lot in Business Central before retrying" in body["message"]


def test_missing_created_lot_number_stops_before_followup_patch(app_module, monkeypatch):
    patch_calls = []
    monkeypatch.setattr(
        app_module.requests,
        "post",
        lambda *args, **kwargs: _Response(201, {"Item_No": "1750286-MS"}),
    )
    monkeypatch.setattr(
        app_module.requests,
        "patch",
        lambda *args, **kwargs: patch_calls.append((args, kwargs)),
    )

    client = _authenticated_client(app_module, monkeypatch)
    response = client.post("/create-lot", json={
        "vendor": "seminis",
        "BCItemNo": "1750286-MS",
        "VendorLotNo": "4513632829/0310",
        "CustomerPO": "91256",
        "GPCertificationOutstanding": False,
        "UnderWeightExemption": False,
        "GermSampleRequired": False,
    })

    assert response.status_code == 502
    assert response.get_json()["status"] == "error"
    assert "avoid creating a duplicate lot" in response.get_json()["message"]
    assert patch_calls == []


def test_lot_setup_partial_prefills_po_and_renders_three_boolean_switches(app_module):
    environment = Environment(loader=FileSystemLoader(Path(__file__).parent / "templates"))
    environment.filters["customer_po"] = app_module.normalize_customer_po

    html = environment.get_template("_lot_setup_fields.html").render(
        lot_record={"PurchaseOrder": "PO-91256"}
    )

    assert 'data-field="CustomerPO"' in html
    assert 'value="91256"' in html
    assert html.count('type="checkbox"') == 3
    assert "Lot Setup:" not in html


@pytest.mark.parametrize(
    ("invalid_field", "invalid_value", "message_fragment"),
    [
        ("CustomerPO", "PO-1234", "exactly one five-digit PO number"),
        ("CustomerPO", "91256 / 91257", "exactly one five-digit PO number"),
        ("CurrentGermDate", "2026/07/09", "Current Germ Date must be"),
        ("GrowerGermDate", "July 16, 2026", "Certificate Germ Date must be"),
        ("GermSampleRequired", "sometimes", "Invalid boolean value"),
    ],
)
def test_invalid_setup_fields_are_rejected_before_lot_creation(
    app_module,
    monkeypatch,
    invalid_field,
    invalid_value,
    message_fragment,
):
    post_calls = []
    monkeypatch.setattr(
        app_module.requests,
        "post",
        lambda *args, **kwargs: post_calls.append((args, kwargs)),
    )
    client = _authenticated_client(app_module, monkeypatch)
    payload = {
        "vendor": "seminis",
        "BCItemNo": "1750286-MS",
        "VendorLotNo": "4513632829/0310",
        "CustomerPO": "91256",
        "CurrentGermDate": "07/09/2026",
        "GrowerGermDate": "07/16/2026",
        "GPCertificationOutstanding": False,
        "UnderWeightExemption": False,
        "GermSampleRequired": False,
    }
    payload[invalid_field] = invalid_value

    response = client.post("/create-lot", json=payload)

    assert response.status_code == 400
    assert message_fragment in response.get_json()["message"]
    assert post_calls == []


@pytest.mark.parametrize(
    "template_name",
    [
        "results_sakata.html",
        "results_hm_clause.html",
        "results_seminis.html",
        "results_nunhems.html",
        "results_syngenta.html",
    ],
)
def test_every_lot_creation_page_submits_the_setup_fields(template_name):
    source = (Path(__file__).parent / "templates" / template_name).read_text()

    assert 'include "_lot_setup_fields.html"' in source
    assert "CustomerPO: " in source
    assert "GPCertificationOutstanding: " in source
    assert "UnderWeightExemption: " in source
    assert "GermSampleRequired: " in source
    assert "el.matches('input[type=\"checkbox\"]')" in source
    assert "querySelectorAll('.lookup-ok[data-modal-id]')" in source
    assert "querySelectorAll('.lookup-ok')" not in source
    assert "modalEl.addEventListener('hidden.bs.modal'" in source
