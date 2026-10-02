"""Tracking tests use generated documents; no supplier files or services are needed."""

from copy import deepcopy
import importlib.util
from pathlib import Path

import fitz
import pytest

from vendor_extractors import tracking


UPS_TRACKING = "1Z9282590357491960"
OTHER_UPS_TRACKING = "1Z9282590357491961"


def _pdf(*pages):
    with fitz.open() as document:
        for lines in pages:
            page = document.new_page()
            for index, line in enumerate(lines):
                page.insert_text((40, 45 + index * 18), line, fontsize=10)
        return document.tobytes()


def _mixed_pdf(native_lines, scanned_lines):
    """Use a real image-only page to exercise OCR without retaining supplier data."""
    with fitz.open(stream=_pdf(native_lines), filetype="pdf") as document:
        with fitz.open(stream=_pdf(scanned_lines), filetype="pdf") as scan:
            page = document.new_page()
            page.insert_image(page.rect, stream=scan[0].get_pixmap().tobytes("png"))
        return document.tobytes()


def _ocr_page(page_number, lines):
    return tracking.PageText(
        page_number,
        "\n".join(lines),
        [tracking.TextLine(line, 40, 45 + i * 18, 500, 55 + i * 18)
         for i, line in enumerate(lines)],
    )


def _item(**changes):
    item = {
        "PurchaseOrder": "91276",
        "VendorInvoiceNo": "916000123",
        "VendorLot": "104782310/0020",
    }
    item.update(changes)
    return item


def _prefill(lines, *, vendor="seminis", item=None, filename="shipment.pdf"):
    row = _item() if item is None else item
    grouped = {"invoice.pdf": [row]}
    result = tracking.prefill_tracking_numbers(
        grouped, [(filename, _pdf(lines))], vendor
    )
    assert result is grouped
    assert result["invoice.pdf"][0] is row
    return row


def _assert_manual(row):
    assert row["TrackingPrefillEnabled"] is True
    assert row["InboundTrackingNo"] == ""
    assert isinstance(row["InboundTrackingSources"], list)
    assert isinstance(row["InboundTrackingWarning"], str)


@pytest.mark.parametrize("label", ["Tracking Number", "Tracking#", "TRACKING"])
def test_explicit_tracking_labels_prefill_the_matching_lot(label):
    row = _prefill([
        "BOL/CMR Number: 0080282179",
        "Delivery Number: 7100025671",
        "PO #: 91276",
        "Lot: 104782310",
        f"{label}: {UPS_TRACKING}",
    ])

    assert row["InboundTrackingNo"] == UPS_TRACKING
    assert row["InboundTrackingWarning"] == ""
    assert row["InboundTrackingSources"] == [{
        "filename": "shipment.pdf", "page": 1, "carrier": "UPS",
    }]


def test_tracking_column_uses_the_value_below_the_label():
    with fitz.open() as document:
        page = document.new_page()
        page.insert_text((40, 40), "PO #: 91276")
        page.insert_text((40, 65), "Lot: 104782310")
        for x, label, value in [
            (40, "BOL/CMR Number", "0080282179"),
            (200, "Tracking Number", UPS_TRACKING),
            (390, "Delivery Number", "7100025671"),
        ]:
            page.insert_text((x, 100), label, fontsize=10)
            page.insert_text((x, 125), value, fontsize=10)
        pdf_bytes = document.tobytes()
    grouped = {"invoice.pdf": [_item()]}

    tracking.prefill_tracking_numbers(grouped, [("columns.pdf", pdf_bytes)], "seminis")

    assert grouped["invoice.pdf"][0]["InboundTrackingNo"] == UPS_TRACKING


def test_blank_tracking_heading_cannot_pick_up_an_adjacent_delivery_field():
    with fitz.open() as document:
        page = document.new_page()
        page.insert_text((40, 40), "PO #: 91276")
        page.insert_text((40, 65), "Lot: 104782310")
        page.insert_text((40, 100), "Tracking Number", fontsize=10)
        page.insert_text((200, 100), "Delivery Number: 7100025671", fontsize=10)
        pdf_bytes = document.tobytes()
    grouped = {"invoice.pdf": [_item()]}

    tracking.prefill_tracking_numbers(grouped, [("blank-tracking.pdf", pdf_bytes)], "seminis")

    _assert_manual(grouped["invoice.pdf"][0])


def test_unavailable_inline_tracking_cannot_pick_up_the_bol_number():
    row = _prefill([
        "PO #: 91276",
        "Lot: 104782310",
        "Tracking Number: N/A BOL/CMR Number: 0080282179",
    ])

    _assert_manual(row)


def test_document_evidence_records_references_and_the_tracking_page():
    documents = tracking.extract_tracking_documents([("shipment.pdf", _pdf(
        ["PO #: 91276", "Invoice Number: 916000123", "Lot: 104782310"],
        ["BOL/CMR Number: 0080282179", f"Tracking Number: {UPS_TRACKING}"],
    ))])

    assert len(documents) == 1
    assert documents[0]["filename"] == "shipment.pdf"
    assert documents[0]["purchase_orders"] == {"91276"}
    assert documents[0]["invoice_numbers"] == {"916000123"}
    assert documents[0]["numbers"] == [UPS_TRACKING]
    assert documents[0]["sources"] == [{
        "filename": "shipment.pdf", "page": 2, "carrier": "UPS",
    }]
    assert documents[0]["incomplete"] is False


@pytest.mark.parametrize("vendor", ["seminis", "syngenta", "nunhems"])
def test_numeric_tracking_keeps_leading_zeroes_for_enabled_vendors(vendor):
    row = _prefill([
        "PO #: 91276",
        "Lot: 104782310",
        "Tracking Number: 000123456789",
    ], vendor=vendor, item=_item(VendorLot="104782310"))

    assert row["TrackingPrefillEnabled"] is True
    assert row["InboundTrackingNo"] == "000123456789"


def test_lowercase_ups_tracking_is_normalized_to_uppercase():
    row = _prefill([
        "PO #: 91276",
        "Lot: 104782310",
        f"Tracking Number: {UPS_TRACKING.lower()}",
    ])

    assert row["InboundTrackingNo"] == UPS_TRACKING


@pytest.mark.parametrize("tracking_line", [
    UPS_TRACKING,
    f"BOL/CMR Number: {UPS_TRACKING}",
    "Tracking Number: 1Z928259",
    "Tracking Number: 1Z928259035749196",
    "Tracking Number: 1Z92825903574919600",
    "Tracking Number: 1Z92825903574919!0",
])
def test_unlabelled_or_malformed_numbers_are_left_for_manual_entry(tracking_line):
    row = _prefill(["PO #: 91276", "Lot: 104782310", tracking_line])

    _assert_manual(row)


def test_tracking_without_a_po_lot_or_invoice_does_not_attach_to_only_row():
    row = _prefill([f"Tracking Number: {UPS_TRACKING}"])

    _assert_manual(row)


@pytest.mark.parametrize("document_lot", ["1047823101", "0104782310", "10478231"])
def test_lot_match_requires_complete_identifier_boundaries(document_lot):
    row = _prefill([
        f"Lot: {document_lot}",
        f"Tracking Number: {UPS_TRACKING}",
    ])

    _assert_manual(row)


@pytest.mark.parametrize("invoice_lot,document_lot", [
    ("104782310/0020", "104782310"),
    ("4513632829/0310", "4513632829"),
])
def test_seminis_invoice_suffix_matches_shipping_base_lot(invoice_lot, document_lot):
    row = _prefill([
        f"Lot: {document_lot}",
        f"Tracking Number: {UPS_TRACKING}",
    ], item=_item(VendorLot=invoice_lot))

    assert row["InboundTrackingNo"] == UPS_TRACKING


def test_nunhems_shipping_suffix_matches_the_invoice_base_lot():
    row = _prefill([
        "Lot: 12345678901_001",
        "Tracking Number: 000123456789",
    ], vendor="nunhems", item={
        "PurchaseOrder": "91276",
        "VendorInvoiceNo": "916000123",
        "VendorLotNo": "12345678901",
    })

    assert row["InboundTrackingNo"] == "000123456789"


@pytest.mark.parametrize("contradiction", [
    "PO #: 99999",
    "Invoice Number: 916999999",
])
def test_lot_match_cannot_override_a_contradicting_reference(contradiction):
    row = _prefill([
        contradiction,
        "Lot: 104782310",
        f"Tracking Number: {UPS_TRACKING}",
    ])

    _assert_manual(row)


def test_two_tracking_numbers_matching_a_row_require_manual_review():
    row = _prefill([
        "PO #: 91276",
        "Lot: 104782310",
        f"Tracking Number: {UPS_TRACKING}",
        f"Tracking Number: {OTHER_UPS_TRACKING}",
    ])

    _assert_manual(row)
    assert row["InboundTrackingWarning"]


def test_same_tracking_on_repeated_documents_retains_both_sources():
    grouped = {"invoice.pdf": [_item()]}
    lines = ["PO #: 91276", "Lot: 104782310", f"Tracking Number: {UPS_TRACKING}"]
    files = [("shipment.pdf", _pdf(lines)), ("packing.pdf", _pdf(lines))]

    tracking.prefill_tracking_numbers(grouped, files, "seminis")

    row = grouped["invoice.pdf"][0]
    assert row["InboundTrackingNo"] == UPS_TRACKING
    assert row["InboundTrackingWarning"] == ""
    assert {source["filename"] for source in row["InboundTrackingSources"]} == {
        "shipment.pdf", "packing.pdf",
    }


def test_separate_shipments_route_by_matching_po_and_lot():
    first = _item()
    second = _item(PurchaseOrder="91277", VendorLot="4513632829/0310")
    unmatched = _item(PurchaseOrder="91278", VendorLot="123456789/0110")
    grouped = {"invoice-one.pdf": [first], "invoice-two.pdf": [second, unmatched]}
    files = [
        ("shipment-one.pdf", _pdf([
            "PO #: 91276", "Lot: 104782310", f"Tracking Number: {UPS_TRACKING}",
        ])),
        ("shipment-two.pdf", _pdf([
            "PO #: 91277", "Lot: 4513632829", f"Tracking Number: {OTHER_UPS_TRACKING}",
        ])),
    ]

    tracking.prefill_tracking_numbers(grouped, files, "seminis")

    assert first["InboundTrackingNo"] == UPS_TRACKING
    assert second["InboundTrackingNo"] == OTHER_UPS_TRACKING
    _assert_manual(unmatched)


def test_split_shipments_on_the_same_po_route_to_their_own_lots():
    first = _item()
    second = _item(VendorLot="4513632829/0310")
    grouped = {"invoice.pdf": [first, second]}
    files = [
        ("shipment-one.pdf", _pdf([
            "PO #: 91276", "Lot: 104782310", f"Tracking Number: {UPS_TRACKING}",
        ])),
        ("shipment-two.pdf", _pdf([
            "PO #: 91276", "Lot: 4513632829", f"Tracking Number: {OTHER_UPS_TRACKING}",
        ])),
    ]

    tracking.prefill_tracking_numbers(grouped, files, "seminis")

    assert first["InboundTrackingNo"] == UPS_TRACKING
    assert second["InboundTrackingNo"] == OTHER_UPS_TRACKING


def test_multi_order_document_does_not_apply_one_pages_tracking_to_other_lots():
    first = _item()
    second = _item(PurchaseOrder="91277", VendorLot="4513632829/0310")
    grouped = {"invoice.pdf": [first, second]}
    pdf_bytes = _pdf(
        ["PO #: 91276", "Lot: 104782310", f"Tracking Number: {UPS_TRACKING}"],
        ["PO #: 91277", "Lot: 4513632829", "No tracking provided on this page"],
    )

    tracking.prefill_tracking_numbers(grouped, [("combined-orders.pdf", pdf_bytes)], "seminis")

    for row in (first, second):
        _assert_manual(row)
        assert row["InboundTrackingWarning"]


def test_ocr_reads_image_only_tracking_page_and_preserves_its_page_number(monkeypatch):
    native_lines = ["PO #: 91276", "Invoice Number: 916000123", "Lot: 104782310"]
    scanned_lines = ["Lot: 104782310", f"Tracking Number: {UPS_TRACKING}"]
    pdf_bytes = _mixed_pdf(native_lines, scanned_lines)
    calls = []

    def fake_ocr(received):
        calls.append(received)
        return [_ocr_page(2, scanned_lines)]

    monkeypatch.setattr(tracking, "_extract_azure_pages", fake_ocr)
    grouped = {"invoice.pdf": [_item()]}

    tracking.prefill_tracking_numbers(grouped, [("mixed.pdf", pdf_bytes)], "seminis")

    row = grouped["invoice.pdf"][0]
    assert calls == [pdf_bytes]
    assert row["InboundTrackingNo"] == UPS_TRACKING
    assert row["InboundTrackingSources"] == [{
        "filename": "mixed.pdf", "page": 2, "carrier": "UPS",
    }]


def test_ocr_keeps_conflicting_tracking_on_a_scanned_page_from_being_ignored(monkeypatch):
    native_lines = ["PO #: 91276", "Lot: 104782310", f"Tracking Number: {UPS_TRACKING}"]
    scanned_lines = ["Lot: 104782310", f"Tracking Number: {OTHER_UPS_TRACKING}"]
    monkeypatch.setattr(tracking, "_extract_azure_pages", lambda content: [_ocr_page(2, scanned_lines)])
    grouped = {"invoice.pdf": [_item()]}

    tracking.prefill_tracking_numbers(
        grouped, [("mixed.pdf", _mixed_pdf(native_lines, scanned_lines))], "seminis"
    )

    row = grouped["invoice.pdf"][0]
    _assert_manual(row)
    assert row["InboundTrackingWarning"]
    assert {source["page"] for source in row["InboundTrackingSources"]} == {1, 2}


def test_native_and_raster_tracking_on_one_page_are_both_considered(monkeypatch):
    native_lines = ["PO #: 91276", "Lot: 104782310", f"Tracking Number: {UPS_TRACKING}"]
    scanned_lines = ["Lot: 104782310", f"Tracking Number: {OTHER_UPS_TRACKING}"]
    with fitz.open(stream=_pdf(native_lines), filetype="pdf") as document:
        with fitz.open(stream=_pdf(scanned_lines), filetype="pdf") as scan:
            document[0].insert_image(
                fitz.Rect(40, 200, 550, 650),
                stream=scan[0].get_pixmap().tobytes("png"),
            )
        pdf_bytes = document.tobytes()
    calls = []

    def fake_ocr(received):
        calls.append(received)
        return [_ocr_page(1, scanned_lines)]

    monkeypatch.setattr(tracking, "_extract_azure_pages", fake_ocr)
    grouped = {"invoice.pdf": [_item()]}

    tracking.prefill_tracking_numbers(grouped, [("mixed-page.pdf", pdf_bytes)], "seminis")

    assert calls == [pdf_bytes]
    row = grouped["invoice.pdf"][0]
    _assert_manual(row)
    assert row["InboundTrackingWarning"]


@pytest.mark.parametrize("ocr_result", ["exception", "empty"])
def test_unreadable_scanned_page_requires_manual_entry_despite_native_tracking(monkeypatch, ocr_result):
    native_lines = ["PO #: 91276", "Lot: 104782310", f"Tracking Number: {UPS_TRACKING}"]
    scanned_lines = ["Lot: 104782310", f"Tracking Number: {OTHER_UPS_TRACKING}"]

    def fake_ocr(content):
        if ocr_result == "exception":
            raise RuntimeError("Synthetic OCR service failure")
        return []

    monkeypatch.setattr(tracking, "_extract_azure_pages", fake_ocr)
    grouped = {"invoice.pdf": [_item()]}

    tracking.prefill_tracking_numbers(
        grouped, [("mixed.pdf", _mixed_pdf(native_lines, scanned_lines))], "seminis"
    )

    row = grouped["invoice.pdf"][0]
    _assert_manual(row)
    assert row["InboundTrackingWarning"]


@pytest.mark.parametrize("vendor", ["sakata", "hm_clause", "kamterter", "kamterter_us"])
def test_unsupported_vendors_keep_the_existing_manual_workflow(vendor):
    grouped = {"invoice.pdf": [_item(InboundTrackingNo="manually entered")]}
    before = deepcopy(grouped)

    result = tracking.prefill_tracking_numbers(grouped, [("shipment.pdf", _pdf([
        "PO #: 91276", "Lot: 104782310", f"Tracking Number: {UPS_TRACKING}",
    ]))], vendor)

    assert result is grouped
    assert grouped == before


def test_searchable_syngenta_packing_list_avoids_invoice_extractor_ocr(monkeypatch):
    # Load the real extractor in isolation from the app fixtures' vendor stubs.
    path = Path(__file__).parent / "vendor_extractors" / "syngenta.py"
    spec = importlib.util.spec_from_file_location("tracking_test_syngenta", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    ocr_calls = []
    monkeypatch.setattr(module, "extract_text_with_azure_ocr", lambda content: ocr_calls.append(content) or [])
    monkeypatch.setattr(module, "log_processing_event", lambda *args, **kwargs: None)

    def block_network(*args, **kwargs):
        raise AssertionError("Searchable packing-list extraction must stay offline")

    monkeypatch.setattr(module.requests.sessions.Session, "request", block_network)
    pdf_bytes = _pdf([
        "SYNGENTA PACKING LIST",
        "PO #: 91276",
        "Lot: 104782310",
        f"Tracking Number: {UPS_TRACKING}",
    ])

    results = module.extract_syngenta_data_from_bytes([("packing.pdf", pdf_bytes)], [])

    assert results == {}
    assert ocr_calls == []


def _metadata(number, filename="shipment.pdf", warning=""):
    return {
        "TrackingPrefillEnabled": True,
        "InboundTrackingNo": number,
        "InboundTrackingSources": [{"filename": filename, "page": 1, "carrier": "UPS"}],
        "InboundTrackingWarning": warning,
    }


def test_duplicate_lot_merge_preserves_one_tracking_number_and_all_sources():
    existing = _metadata(UPS_TRACKING)
    item = _metadata(UPS_TRACKING, filename="packing.pdf")

    tracking.merge_tracking_metadata(existing, item)

    assert existing["InboundTrackingNo"] == UPS_TRACKING
    assert existing["InboundTrackingWarning"] == ""
    assert {source["filename"] for source in existing["InboundTrackingSources"]} == {
        "shipment.pdf", "packing.pdf",
    }


def test_duplicate_lot_merge_clears_conflicting_tracking_numbers():
    existing = _metadata(UPS_TRACKING)

    tracking.merge_tracking_metadata(existing, _metadata(OTHER_UPS_TRACKING, "other.pdf"))

    _assert_manual(existing)
    assert existing["InboundTrackingWarning"]


def test_duplicate_lot_merge_cannot_restore_tracking_after_ambiguity():
    existing = _metadata(UPS_TRACKING)
    tracking.merge_tracking_metadata(existing, _metadata(OTHER_UPS_TRACKING, "other.pdf"))

    tracking.merge_tracking_metadata(existing, _metadata(UPS_TRACKING))

    _assert_manual(existing)
    assert existing["InboundTrackingWarning"]
