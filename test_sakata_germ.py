"""Offline regressions for missing Sakata germination fields."""

from importlib import import_module

import fitz
import pytest
import requests

from test_lot_creation_fields import _Response


@pytest.fixture
def sakata(monkeypatch):
    def block_network(*args, **kwargs):
        raise AssertionError("Sakata tests must not make network requests")

    monkeypatch.setattr(requests.sessions.Session, "request", block_network)
    with monkeypatch.context() as auth:
        auth.setattr(requests, "post", lambda *args, **kwargs: _Response(200, {"access_token": "test-token"}))
        extractor = import_module("vendor_extractors.sakata")
    monkeypatch.setattr(extractor, "get_po_items", lambda *args: [])
    monkeypatch.setattr(extractor, "log_processing_event", lambda **kwargs: None)
    return extractor


def _pdf(*blocks):
    with fitz.open() as doc:
        page = doc.new_page()
        for index, text in enumerate(blocks):
            page.insert_text((40, 40 + 175 * index), text, fontsize=10)
        return doc.tobytes()


def _native_invoice(germ_date, germ="0"):
    return _pdf(
        "Invoice number INV-00012345\nPurchase order 91469\n" + "Invoice header text\n" * 8,
        "1 10084333 Broccoli Archer Treated 100M EA 9.00 9.00 700.00 0 6,300.00",
        f"548096-013\n9.00\n{germ}\n{germ_date}92,040\n5\n99.99\n95+\nUSA",
    )


def _report(germ, completion_date, inert="0.01"):
    return (
        "Report of Seed Analysis\nLot Number: 548096-013\nDate Issued: 11/03/2026\n"
        f"Date Test Completed: {completion_date}\nPurity Analysis\nDays Tested\n"
        f"99.99 {inert} 0.00 0.00 {germ} X 0 0 1 400 10"
    )


_OCR_INVOICE = (
    "Invoice number INV-00012345\nPurchase order 91469\n"
    "10084333\nBroccoli Archer Treated 100M\nEA\n9.00\n9.00\n700.00\n0\n6,300.00\n"
    "548096-013\n9.00\n99\n10/29/2026\n92,040\n5\n99.99\n95+\nUSA\nOther Charges"
)


def _extract_lot(sakata, invoice, certificate=None):
    files = [("invoice.pdf", invoice)]
    if certificate is not None:
        files.append(("certificate.pdf", certificate))
    return sakata.extract_sakata_data_from_bytes(files)["invoice.pdf"][0]["Lots"][0]


@pytest.mark.parametrize(("inert", "expected_inert"), [("0.01", 0.01), ("X", None)])
def test_searchable_stamp_does_not_hide_scanned_report(sakata, monkeypatch, inert, expected_inert):
    invoice = _pdf()
    stamp = _pdf("Seed Shipment Release From Customs\n" + "Customs approval stamp text\n" * 8)
    calls = []

    def read_ocr(pdf):
        calls.append(pdf)
        return _OCR_INVOICE if pdf == invoice else _report("99", "10/29/2026", inert=inert)

    monkeypatch.setattr(sakata, "_extract_text_with_azure_ocr", read_ocr)
    lot = _extract_lot(sakata, invoice, stamp)

    assert calls == [invoice, stamp]
    assert (lot["CurrentGerm"], lot["GermDate"]) == (99, "10/29/2026")
    assert (lot["GrowerGerm"], lot["GrowerGermDate"]) == (99, "10/29/2026")
    assert (lot["Purity"], lot["Inert"]) == (99.99, expected_inert)


def test_invoice_purity_does_not_populate_inert(sakata, monkeypatch):
    monkeypatch.setattr(sakata, "_extract_text_with_azure_ocr", lambda pdf: _OCR_INVOICE)
    lot = _extract_lot(sakata, _pdf())

    assert (lot["Purity"], lot["Inert"]) == (99.99, None)


@pytest.mark.parametrize(("ocr_germ", "expected_germ"), [("98", 98), ("X", None)])
def test_missing_invoice_germ_does_not_trigger_certificate_retry(sakata, monkeypatch, ocr_germ, expected_germ):
    invoice = _native_invoice("10/27/2026 ", germ="X")
    certificate = _pdf(_report("X", "10/28/2026", inert="0.00"))
    calls = []

    def read_ocr(pdf):
        calls.append(pdf)
        return _report(ocr_germ, "10/29/2026")

    monkeypatch.setattr(sakata, "_extract_text_with_azure_ocr", read_ocr)
    lot = _extract_lot(sakata, invoice, certificate)

    assert calls == [certificate]
    assert (lot["CurrentGerm"], lot["GermDate"]) == (None, "10/27/2026")
    assert (lot["GrowerGerm"], lot["GrowerGermDate"]) == (expected_germ, "10/28/2026")
    assert (lot["Purity"], lot["Inert"]) == (99.99, 0.0)


def test_certificate_issue_date_does_not_prevent_retry_for_missing_test_date(sakata, monkeypatch):
    invoice = _native_invoice("10/29/2026 ")
    certificate = _pdf(_report("99", ""))
    calls = []

    def read_ocr(pdf):
        calls.append(pdf)
        return _report("99", "")

    monkeypatch.setattr(sakata, "_extract_text_with_azure_ocr", read_ocr)
    lot = _extract_lot(sakata, invoice, certificate)

    assert calls == [certificate]
    assert lot["GrowerGerm"] == 99
    assert lot["GrowerGermDate"] is None


@pytest.mark.parametrize(("inert", "calls_expected"), [("X", 1), ("0.00", 0)])
def test_certificate_ocr_checks_inert_even_when_germ_is_present(sakata, monkeypatch, inert, calls_expected):
    invoice = _native_invoice("10/29/2026 ")
    certificate = _pdf(_report("99", "10/28/2026", inert=inert))
    calls = []

    def read_ocr(pdf):
        calls.append(pdf)
        return _report("98", "10/29/2026", inert="0.10")

    monkeypatch.setattr(sakata, "_extract_text_with_azure_ocr", read_ocr)
    lot = _extract_lot(sakata, invoice, certificate)

    assert calls == [certificate] * calls_expected
    assert (lot["GrowerGerm"], lot["GrowerGermDate"], lot["Inert"]) == (
        99, "10/28/2026", 0.1 if calls_expected else 0.0
    )
