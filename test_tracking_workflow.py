"""Offline coverage for uploaded tracking PDFs through the lot review pages."""

from copy import deepcopy
from html.parser import HTMLParser
from io import BytesIO
import sys

import fitz
import pytest
import requests

from test_lot_creation_fields import app_module, _authenticated_client


class _TrackingInputParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.inputs = []

    def handle_starttag(self, tag, attrs):
        attributes = dict(attrs)
        if tag == "input" and attributes.get("data-field") == "InboundTrackingNo":
            self.inputs.append(attributes)


def _tracking_inputs(html):
    parser = _TrackingInputParser()
    parser.feed(html)
    return parser.inputs


def _pdf_bytes(lines):
    with fitz.open() as doc:
        page = doc.new_page()
        page.insert_text((72, 72), "\n".join(lines), fontsize=11)
        return doc.tobytes()


def _vendor_item(vendor):
    item = {
        "VendorInvoiceNo": "SAMPLE-INVOICE",
        "PurchaseOrder": "PO-91276",
        "VendorItemDescription": "SAMPLE SEEDS 1,000 SDS",
        "VendorItemNumber": "1400",
        "TotalQuantity": 2,
        "TotalPrice": 20,
        "USD_Actual_Cost_$": "10.0000",
    }
    if vendor == "seminis":
        item.update(VendorLot="104782310", VendorBatch="0262445982")
    elif vendor == "nunhems":
        item.update(VendorLotNo="33507901004")
    elif vendor == "syngenta":
        item.update(PurchaseOrder="PO-90677", VendorLotNo="150559634", VendorBatchNo="18784821")
    return item


@pytest.fixture
def offline_upload_app(app_module, monkeypatch):
    def block_network(*args, **kwargs):
        raise AssertionError("Tracking workflow tests must not make network requests")

    monkeypatch.setattr(requests.sessions.Session, "request", block_network)
    monkeypatch.setattr(app_module, "token_is_valid", lambda token: True)
    monkeypatch.setattr(app_module, "load_package_descriptions", lambda token: ["1,000 SEEDS"])
    monkeypatch.setattr(app_module, "load_treatments", lambda endpoint, token: [])
    monkeypatch.setattr(app_module, "get_po_items", lambda pos, token: [
        {"No": "1750286-MS", "Description": "SAMPLE SEEDS"},
    ])
    monkeypatch.setattr(app_module, "find_best_bc_item_match", lambda *args, **kwargs: "1750286-MS")
    return app_module


@pytest.mark.parametrize(
    ("vendor", "tracking_number", "shipment_refs", "carrier"),
    [
        (
            "seminis", "1Z9282590357491960",
            ["Purchase Order: 91276", "Lot Number: 104782310", "Batch Number: 0262445982"], "UPS",
        ),
        (
            "nunhems", "396918627059",
            ["Lot Number: 33507901004_001", "Batch Number: I000004805"], "FedEx Freight",
        ),
        (
            "syngenta", "1Z812W720396903368",
            ["Purchase Order: APR90677", "Lot Number: 150559634", "Batch Number: 18784821"], "UPS",
        ),
    ],
)
def test_uploaded_shipment_pdf_prefills_supported_vendor_review_page(
    offline_upload_app, monkeypatch, vendor, tracking_number, shipment_refs, carrier,
):
    item = _vendor_item(vendor)
    extractor_name = f"extract_{vendor}_data_from_bytes"
    monkeypatch.setattr(
        offline_upload_app, extractor_name,
        lambda pdf_files, pkg_descs: {"invoice.pdf": [deepcopy(item)]},
    )
    shipment_filename = f"{vendor}-shipment.pdf"
    shipment = _pdf_bytes([
        f"{vendor.upper()} PACKING LIST", f"Carrier: {carrier}",
        f"Tracking Number: {tracking_number}", *shipment_refs,
    ])
    client = _authenticated_client(offline_upload_app, monkeypatch)
    response = client.post("/", data={
        "vendor": vendor,
        "pdfs": [
            (BytesIO(_pdf_bytes(["INVOICE", "No tracking information on this page"])), "invoice.pdf"),
            (BytesIO(shipment), shipment_filename),
        ],
    })

    assert response.status_code == 200
    html = response.get_data(as_text=True)
    tracking_inputs = _tracking_inputs(html)
    assert len(tracking_inputs) == 1
    assert tracking_inputs[0]["value"] == tracking_number
    assert tracking_inputs[0]["maxlength"] == "100"
    assert "readonly" not in tracking_inputs[0]
    assert "disabled" not in tracking_inputs[0]
    assert f"Source: {shipment_filename}, page 1" in html
    assert "Please verify before creating the lot." in html


def test_sakata_upload_keeps_tracking_manual(offline_upload_app, monkeypatch):
    item = {
        "InvoiceNumber": "SAMPLE-INVOICE",
        "PurchaseOrder": "PO-91276",
        "VendorItemNumber": "1400",
        "VendorDescription": "SAMPLE SEEDS 1M",
        "TotalPrice": 20,
        "Lots": [{
            "VendorProductLot": "104782310", "SeedCount": None,
            "TotalQuantity": 2, "InboundTrackingNo": "DO-NOT-PREFILL",
        }],
    }
    monkeypatch.setattr(
        sys.modules["vendor_extractors.sakata"], "extract_sakata_data_from_bytes",
        lambda pdf_files, token: {"sakata.pdf": [deepcopy(item)]}, raising=False,
    )
    client = _authenticated_client(offline_upload_app, monkeypatch)
    response = client.post("/", data={
        "vendor": "sakata",
        "pdfs": (BytesIO(_pdf_bytes([
            "SAKATA", "Tracking Number: 1Z9282590357491960",
            "Lot Number: 104782310", "Purchase Order: 91276",
        ])), "sakata.pdf"),
    })

    assert response.status_code == 200
    html = response.get_data(as_text=True)
    assert [field["value"] for field in _tracking_inputs(html)] == [""]
    assert "DO-NOT-PREFILL" not in html
    assert "Please verify before creating the lot." not in html


@pytest.mark.parametrize("vendor", ["syngenta", "seminis"])
@pytest.mark.parametrize("other_tracking", ["1Z812W720396903368", ""])
def test_existing_duplicate_aggregation_clears_conflicting_or_missing_tracking(
    offline_upload_app, vendor, other_tracking,
):
    first = _vendor_item(vendor)
    if vendor == "seminis":
        # Existing aggregation supports this legacy key; active VendorBatch stays unchanged.
        first["VendorBatchNo"] = first["VendorBatch"]
    first.update(
        TrackingPrefillEnabled=True,
        InboundTrackingNo="1Z9282590357491960",
        InboundTrackingSources=[{"filename": "first.pdf", "page": 1, "carrier": "UPS"}],
    )
    second = deepcopy(first)
    second.update(
        InboundTrackingNo=other_tracking,
        InboundTrackingSources=[{"filename": "second.pdf", "page": 1, "carrier": "UPS"}] if other_tracking else [],
    )
    grouped = offline_upload_app.aggregate_duplicate_lots({
        "first-invoice.pdf": [first], "second-invoice.pdf": [second],
    }, vendor)

    rows = [row for items in grouped.values() for row in items]
    assert len(rows) == 1
    merged = rows[0]
    assert merged["TotalQuantity"] == 4
    assert merged["TotalPrice"] == 40
    assert merged["InboundTrackingNo"] == ""
    assert merged["InboundTrackingWarning"]
    with offline_upload_app.app.test_request_context():
        html = offline_upload_app.render_template("_lot_setup_fields.html", lot_record=merged)
    assert [field["value"] for field in _tracking_inputs(html)] == [""]
    assert merged["InboundTrackingWarning"] in html
    assert "Please verify before creating the lot." not in html
