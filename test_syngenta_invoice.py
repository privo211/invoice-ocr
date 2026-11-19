import fitz
import pytest

from vendor_extractors import syngenta


def test_syngenta_invoice_includes_only_matching_continuation_pages(monkeypatch):
    def unexpected_ocr(_):
        pytest.fail("Searchable invoice and packing list pages should not require OCR")

    def invoice_pdf(*pages):
        with fitz.open() as doc:
            for heading, invoice_no, item_no, lot_no in pages:
                doc.new_page().insert_text((40, 40), (
                    f"{heading}\nInvoice: {invoice_no}\nItem Material Description Batch/Lot\n"
                    f"{item_no}\n11237325\nSample 100 KS Foil\n21628198\n/{lot_no}\n"
                    "2.00\nEA\n50.00\n100.00\nPO# : NOV 91234\n"
                ))
            return doc.tobytes()

    monkeypatch.setattr(syngenta, "extract_text_with_azure_ocr", unexpected_ocr)
    monkeypatch.setattr(syngenta, "log_processing_event", lambda *args: None)
    continuation = ("Syngenta Invoice", "12345678", "10101", "LOT123457")
    invoice = invoice_pdf(
        ("Stokes Syngenta Invoice", "12345678", "10100", "LOT123456"),
        continuation,
        ("Syngenta Invoice", "87654321", "10102", "LOT123458"),
        ("Syngenta Packing List", "12345678", "10103", "LOT123459"),
    )

    results = syngenta.extract_syngenta_data_from_bytes([
        ("invoice.pdf", invoice), ("unconfirmed.pdf", invoice_pdf(continuation)),
    ], [])

    assert list(results) == ["invoice.pdf"]
    assert [
        (item["VendorLotNo"], item["VendorInvoiceNo"], item["PurchaseOrder"], item["TotalPrice"])
        for item in results["invoice.pdf"]
    ] == [
        ("LOT123456", "12345678", "PO-91234", 100.0),
        ("LOT123457", "12345678", "PO-91234", 100.0),
    ]


@pytest.mark.parametrize("date,full_date,month", [
    ("10/26", None, "10/2026"),
    ("10/15/2026", "10/15/2026", None),
    ("02/30/2026", None, None),
])
def test_syngenta_invoice_preserves_germ_date_precision_and_vendor_treatment(date, full_date, month):
    text = (
        "10100\n11237325\nSample 100 KS Foil\nMetal-M/Azoxy/Fludi FarMore«\nTRT CODE:484\n"
        f"21628198\n/LOT123456\n2.00\nEA\n50.00\n100.00\nGerm: 98 %\nDate: {date}\n"
        "PO# : NOV 91234\nInvoice Date: 11/16/2026"
    )
    item, = syngenta.parse_invoice_text(text, "invoice.pdf", "PO-91234", "12345678")

    assert item["CurrentGerm"] == 98
    assert item["CurrentGermDate"] == full_date
    assert item.get("InvoiceGermMonth") == month
    assert item["VendorTreatment"] == "Metal-M/Azoxy/Fludi FarMore"
    assert item["VendorTreatmentCode"] == "484"
    assert item["PackageDescription"] == "100,000 SEEDS"


def _invoice_pdf(*pages):
    with fitz.open() as doc:
        for text in pages:
            doc.new_page().insert_text((40, 40), text)
        return doc.tobytes()


_TABLE = "Item Material Description Batch/Lot QTY UoM Unit Price Total USD\n"
_ROW = (
    "10100 11237325 Sample variety 21628198 2.00 EA 50.00 100.00\n"
    "100 KS Foil /LOT123456\nGerm: 98% Date: 10/26\nPO# : NOV 91234\n"
)


def test_syngenta_restores_rows_from_ocr_and_reconciles_goods_subtotal(monkeypatch, caplog):
    header = "Stokes Syngenta Invoice\nInvoice No.: 12345678\n" + _TABLE
    footer = "Sub Total 100.00\nINLAND FREIGHT 20.00\nTOTAL 120.00\nPage 1 of 1"
    native = header + "Sample variety 2.00 EA 50.00 100.00\n" + footer
    monkeypatch.setattr(syngenta, "log_processing_event", lambda *args: None)
    monkeypatch.setattr(syngenta, "extract_text_with_azure_ocr", lambda _: (header + _ROW + footer).splitlines())
    caplog.set_level("INFO", logger=syngenta.__name__)

    result = syngenta.extract_syngenta_data_from_bytes([("invoice.pdf", _invoice_pdf(native))], [])

    item, = result["invoice.pdf"]
    assert (item["VendorLotNo"], item["VendorBatchNo"], item["VendorItemDescription"], item["TotalPrice"]) == (
        "LOT123456", "21628198", "Sample variety 100 KS Foil", 100.0,
    )
    assert len(caplog.records) == 1
    assert caplog.records[0].levelname == "INFO" and "came from OCR" in caplog.messages[0]


def test_syngenta_includes_numbered_continuation_and_separates_invoice_numbers(monkeypatch, caplog):
    monkeypatch.setattr(syngenta, "log_processing_event", lambda *args: None)
    monkeypatch.setattr(syngenta, "extract_text_with_azure_ocr", lambda _: pytest.fail("Unexpected OCR request"))
    page1 = "Stokes Syngenta Invoice\nInvoice: 12345678\n" + _TABLE + _ROW + "Page 1 of 2"
    page2 = _TABLE + _ROW.replace("LOT123456", "LOT123457") + "Sub Total 200.00\nPage 2 of 2"
    page3 = "Stokes Syngenta Invoice\nInvoice: 87654321\n" + _TABLE + _ROW + "Sub Total 100.00\nPage 1 of 1"
    caplog.set_level("INFO", logger=syngenta.__name__)

    result = syngenta.extract_syngenta_data_from_bytes([("two.pdf", _invoice_pdf(page1, page2, page3))], [])

    assert [[(item["VendorInvoiceNo"], item["VendorLotNo"]) for item in rows] for rows in result.values()] == [
        [("12345678", "LOT123456"), ("12345678", "LOT123457")], [("87654321", "LOT123456")],
    ]
    assert not caplog.records


def test_syngenta_logs_missing_pages_and_unreconciled_prices(monkeypatch, caplog):
    monkeypatch.setattr(syngenta, "log_processing_event", lambda *args: None)
    monkeypatch.setattr(syngenta, "extract_text_with_azure_ocr", lambda _: [])
    incomplete_row = _ROW.replace("/LOT123456", "1LOT123456")
    invoice = "Stokes Syngenta Invoice\nInvoice: 12345678\n" + _TABLE + incomplete_row + "Sub Total 300.00\nPage 3 of 3"
    results = syngenta.extract_syngenta_data_from_bytes([
        ("invoice.pdf", _invoice_pdf(invoice, "")), ("broken.pdf", b"not a PDF"),
    ], [])

    assert len(results["invoice.pdf"]) == 1
    assert {record.args[0] for record in caplog.records if record.name == syngenta.__name__} == {
        "invoice.pdf", "invoice.pdf, page 2", "broken.pdf",
    }
    assert "reports 3" in caplog.text
    assert "printed goods subtotal is 300.00" in caplog.text
    assert "incomplete on 1 of 1" in caplog.text


def test_syngenta_does_not_use_an_invoice_number_as_a_missing_batch():
    text = _ROW.replace("21628198 ", "") + "Page 1 of 2\nInvoice: 12345678\n"
    item, = syngenta.parse_invoice_text(text, "invoice.pdf", "PO-91234", "12345678")
    assert item["VendorBatchNo"] is None


def test_syngenta_preserves_a_row_with_an_ocr_damaged_item_number():
    damaged = _ROW.replace("10100", "1/100")
    item, = syngenta.parse_invoice_text(damaged, "invoice.pdf", "PO-91234", "12345678")
    assert not item["VendorItemNumber"]
    assert (item["VendorLotNo"], item["VendorBatchNo"], item["TotalPrice"]) == ("LOT123456", "21628198", 100.0)
