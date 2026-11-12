"""Certificate-only OCR regressions for HM Clause and Syngenta."""

import fitz
import pytest
import requests

from vendor_extractors import hm_clause, syngenta


_STAMP = "Seed Shipment Release From Customs\nApproved 11/05/2026"


def _pdf(text):
    with fitz.open() as doc:
        doc.new_page().insert_text((40, 40), text, fontsize=9)
        return doc.tobytes()


def _certificate(vendor, lot, purity, germ, date, inert):
    if vendor == "hm_clause":
        return (
            f"Report of Seed Analysis\nLot Number: {lot}\nPure Seed: {purity}%\n"
            f"Inert Matter: {inert}\nGermination: {germ}%\nTest Date: {date}\n{_STAMP}"
        )
    return (
        f"Report of Analysis\nLot Number: {lot}\nViability\n"
        f"Analyzed: 510.0 {purity} 0.00 {inert} 0.00 {germ}\nDate Tested: {date}\n{_STAMP}"
    )


@pytest.mark.parametrize("vendor", ["hm_clause", "syngenta"])
@pytest.mark.parametrize("scenario", ["partial", "still_missing", "complete", "stamp_only", "ocr_failure", "wrong_lot"])
def test_live_certificate_fallback(monkeypatch, vendor, scenario):
    def block_network(*args, **kwargs):
        raise AssertionError("Certificate tests must use offline OCR fixtures")

    monkeypatch.setattr(requests.sessions.Session, "request", block_network)
    lot, other_lot = ("K123456", "K987654") if vendor == "hm_clause" else ("ABC123456", "XYZ987654")
    invoice = _pdf(
        "STOKES SYNGENTA\nInvoice Number: 123456\nInvoice: 123456\n"
        + "Searchable invoice description for vendor seed order.\n" * 4
    )
    native_date = "" if scenario == "still_missing" else "10/28/2026"
    native_inert = "0.00%" if scenario == "complete" and vendor == "hm_clause" else (
        "0.00" if scenario == "complete" else "X"
    )
    native = _certificate(vendor, lot, "99.7", "97", native_date, native_inert)
    certificate = _pdf(_STAMP if scenario == "stamp_only" else native)
    files = [("invoice.pdf", invoice), ("certificate.pdf", certificate)]
    calls = []

    def read_ocr(pdf_bytes):
        with fitz.open(stream=pdf_bytes, filetype="pdf") as doc:
            calls.append(doc[0].get_text())
        if scenario == "ocr_failure":
            raise RuntimeError("Synthetic OCR failure")
        return _certificate(
            vendor, other_lot if scenario == "wrong_lot" else lot, "99.8", "98",
            "" if scenario == "still_missing" else "10/29/2026", "0.2",
        ).splitlines()

    if vendor == "hm_clause":
        monkeypatch.setattr(hm_clause, "extract_text_with_azure_ocr", read_ocr)
        monkeypatch.setattr(hm_clause, "log_processing_event", lambda **kwargs: None)
        monkeypatch.setattr(
            hm_clause, "extract_hm_clause_invoice_data_from_bytes",
            lambda pdf_bytes, read_ocr: ([{"VendorBatchLot": lot}] if pdf_bytes == invoice else [], {}),
        )
        item = hm_clause.extract_hm_clause_data_from_bytes(files)["invoice.pdf"][0]
        germ_field, date_field, purity_field, inert_field = "Germ", "GermDate", "PureSeed", "InertMatter"
    else:
        monkeypatch.setattr(syngenta, "extract_text_with_azure_ocr", read_ocr)
        monkeypatch.setattr(syngenta, "log_processing_event", lambda *args: None)
        monkeypatch.setattr(syngenta, "parse_invoice_text", lambda *args: [{"VendorLotNo": lot}])
        item = syngenta.extract_syngenta_data_from_bytes(files, [])["invoice.pdf"][0]
        germ_field, date_field, purity_field, inert_field = "CurrentGerm", "CurrentGermDate", "Purity", "Inert"

    assert len(calls) == (0 if scenario == "complete" else 1)
    assert all("Seed Shipment Release From Customs" in text for text in calls)
    expected_germ = 98 if scenario == "stamp_only" else 97
    expected_date = {"still_missing": None, "stamp_only": "10/29/2026"}.get(scenario, "10/28/2026")
    expected_inert = {"complete": 0.0, "ocr_failure": None, "wrong_lot": None}.get(scenario, 0.2)
    assert item["GrowerGerm"] == item[germ_field] == expected_germ
    assert item.get("GrowerGermDate") == item.get(date_field) == expected_date
    assert item[purity_field] == (99.8 if scenario == "stamp_only" else 99.7)
    assert item.get(inert_field) == expected_inert
