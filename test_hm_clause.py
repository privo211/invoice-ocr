import fitz
import pytest

from vendor_extractors import hm_clause


def test_two_invoice_items_keep_their_own_lot_fields(monkeypatch):
    def no_ocr(_pdf):
        pytest.fail("The searchable invoice and certificates should parse without OCR")

    monkeypatch.setattr(hm_clause, "extract_text_with_azure_ocr", no_ocr)
    monkeypatch.setattr(hm_clause, "log_processing_event", lambda **kwargs: None)

    with fitz.open() as doc:
        page = doc.new_page()
        page.insert_text((30, 40), "Invoice No. 12345678\nCustomer PO No. 12345", fontsize=10)
        page.insert_text((30, 150), "123456 Tomatillo First F1 Pro", fontsize=10)
        page.insert_text((75, 167), "Flc. Foil 25 Ks", fontsize=9)
        page.insert_text((245, 148), "K12345", fontsize=9)
        page.insert_text((245, 167), "PL123456", fontsize=9)
        page.insert_text((420, 148), "25 KS   1331.25 N", fontsize=9)
        page.insert_text(
            (30, 195), "Product Form: Filmcoat    Treatment: Maxim\nSeed Count: 269998    Seed Size: 5",
            fontsize=9,
        )
        page.insert_text((30, 245), "654321 Tomato Second F1 Pro", fontsize=10)
        page.insert_text((75, 262), "Plt.GCDI Foil 20 Ks", fontsize=9)
        page.insert_text((245, 243), "L98765", fontsize=9)
        page.insert_text((245, 262), "PL987654", fontsize=9)
        page.insert_text((420, 243), "40 KS   3298.94 N", fontsize=9)
        page.insert_text(
            (30, 290), "Product Form: Gro-Coat Midi    Treatment: Thiram\nSeed Count: 33476    Seed Size:",
            fontsize=9,
        )
        files = [("invoice.pdf", doc.tobytes())]

    for lot, germ, date in (("PL987654", 92, "8/3/2026"), ("PL123456", 96, "8/27/2026")):
        with fitz.open() as doc:
            doc.new_page().insert_text(
                (30, 40),
                f"Report Of Seed Analysis\nLot #: {lot}\nTest Date:\n{date}\n"
                f"Germination (%):\n{germ}\nPure Seed (%):\n99.9\nInert Matter (%):\n0.1",
                fontsize=9,
            )
            files.append((f"{lot}.pdf", doc.tobytes()))

    first, second = hm_clause.extract_hm_clause_data_from_bytes(files)["invoice.pdf"]
    packages = ["20,000 SEEDS", "25,000 SEEDS"]
    for item, expected in (
        (first, (
            "123456", "K12345", "PL123456", "25,000 SEEDS", 25, 1331.25, 53.25,
            "5", 269998, "Filmcoat", "Maxim", 96, "08/27/2026",
        )),
        (second, (
            "654321", "L98765", "PL987654", "20,000 SEEDS", 40, 3298.94, 82.4735,
            None, 33476, "Gro-Coat Midi", "Thiram", 92, "08/03/2026",
        )),
    ):
        assert item["PurchaseOrder"] == "PO-12345"
        assert item["VendorInvoiceNo"] == "12345678"
        package = hm_clause.find_best_hm_clause_package_description(item["VendorItemDescription"], packages)
        assert (
            item["VendorItemNumber"], item["VendorBatchLot"], item["VendorProductLot"],
            package, item["TotalQuantity"], item["TotalPrice"], item["USD_Actual_Cost_$"],
            item["SeedSize"], item["SeedCount"], item["ProductForm"], item["Treatment"],
            item["Germ"], item["GermDate"],
        ) == expected
        assert item["GrowerGerm"] == item["Germ"]
        assert item["GrowerGermDate"] == item["GermDate"]
        assert item["PureSeed"] == 99.9
        assert item["InertMatter"] == 0.1
