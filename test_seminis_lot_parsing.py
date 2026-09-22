from vendor_extractors import seminis


def test_wrapped_ten_digit_vendor_lot_keeps_batch_and_germ_data():
    lines = [
        "Invoice Number: 916219845",
        "PO #: 91256",
        "Amount",
        "HYB PEPPER, HOT - DON MATIAS",
        "TRT: TH 2 FILMCOAT TURQUOISE",
        "2 x 50 MK POUCH",
        "4513632829/03",
        "10",
        "TH",
        "0262198116",
        "100 MK",
        "63.40",
        "Total Item",
        "46.38",
        "4,637.71",
    ]
    analysis = {
        "4513632829": {
            "PureSeed": 99.99,
            "InertMatter": 0.01,
            "Germ": 96,
            "GermDate": "07/16/2026",
        }
    }
    packing = {
        "0262198116": {
            "SeedCountPerLB": 42647,
            "PackingGerm": 94,
            "PackingGermDate": "07/09/2026",
        }
    }

    items = seminis._process_single_seminis_invoice(lines, analysis, packing, [])

    assert len(items) == 1
    assert items[0]["VendorLot"] == "4513632829/0310"
    assert items[0]["VendorBatch"] == "0262198116"
    assert items[0]["TotalQuantity"] == 100
    assert items[0]["PackingGermDate"] == "07/09/2026"
    assert items[0]["GermDate"] == "07/16/2026"


def test_ten_digit_vendor_lot_and_batch_can_share_one_line():
    lines = [
        "Invoice Number: 916219845",
        "PO #: 91256",
        "Amount",
        "HYB PEPPER, HOT - DON MATIAS",
        "TRT: TH 2 FILMCOAT TURQUOISE",
        "2 x 50 MK POUCH",
        "4513632829/0310 TH 0262198116",
        "100 MK",
        "63.40",
        "Total Item",
        "46.38",
        "4,637.71",
    ]
    packing = {
        "0262198116": {
            "SeedCountPerLB": 42647,
            "PackingGerm": 94,
            "PackingGermDate": "07/09/2026",
        }
    }

    items = seminis._process_single_seminis_invoice(lines, {}, packing, [])

    assert len(items) == 1
    assert items[0]["VendorLot"] == "4513632829/0310"
    assert items[0]["VendorBatch"] == "0262198116"
    assert items[0]["PackingGermDate"] == "07/09/2026"
