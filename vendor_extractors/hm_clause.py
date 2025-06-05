import os
import json
import fitz  # PyMuPDF
import re
from typing import List, Dict
import requests

AZURE_ENDPOINT = "https://vendorinvoiceautomation.cognitiveservices.azure.com/"
AZURE_KEY = "797f478c12334975b0ca4e4339b261fe"

def extract_text_with_azure_ocr(pdf_path: str) -> List[str]:
    import time
    import os

    with open(pdf_path, "rb") as f:
        headers = {
            "Ocp-Apim-Subscription-Key": AZURE_KEY,
            "Content-Type": "application/pdf"
        }
        response = requests.post(
            f"{AZURE_ENDPOINT}formrecognizer/documentModels/prebuilt-layout:analyze?api-version=2023-07-31",
            headers=headers,
            data=f
        )
    if response.status_code != 202:
        raise RuntimeError(f"OCR request failed: {response.text}")

    result_url = response.headers["Operation-Location"]

    for _ in range(30):
        time.sleep(1.5)
        result = requests.get(result_url, headers={"Ocp-Apim-Subscription-Key": AZURE_KEY}).json()
        if result.get("status") == "succeeded":
            lines = []
            for page in result.get("analyzeResult", {}).get("pages", []):
                for line in page.get("lines", []):
                    content = line.get("content", "").strip()
                    if content:
                        lines.append(content)
                lines.append("--- PAGE BREAK ---")

            # # Save exact copy to .azure.txt
            # txt_path = os.path.splitext(pdf_path)[0] + "_azureocr.txt"
            # with open(txt_path, "w", encoding="utf-8") as f:
            #     f.write("\n".join(lines))

            return lines

        elif result.get("status") == "failed":
            raise RuntimeError("OCR analysis failed")
    raise TimeoutError("OCR timed out")

def extract_items_from_ocr_lines(lines: List[str]) -> List[Dict]:
    """
    Parses Azure-OCR lines into the same per-item dicts that the searchable-PDF logic produces.
    Flushes on both new VendorItemNumber (6-digits) *and* standalone VendorBatchLot.
    """
    line_items = []
    current = {}
    desc_part1 = ""

    def flush_item():
        nonlocal current
        if "VendorBatchLot" in current:
            line_items.append({
                "VendorItemNumber":      current.get("VendorItemNumber"),
                "VendorItemDescription": current.get("VendorItemDescription", "").strip(),
                "VendorBatchLot":        current.get("VendorBatchLot"),
                "VendorProductLot":      current.get("VendorProductLot"),
                "OriginCountry":         current.get("OriginCountry"),
                "UnitPrice":             current.get("UnitPrice"),
                "ProductForm":           current.get("ProductForm"),
                "Treatment":             current.get("Treatment"),
                "Germ":                  current.get("Germ"),
                "GermDate":              current.get("GermDate"),
                "SeedCount":             current.get("SeedCount"),
                "Purity":                current.get("Purity"),
                "SeedSize":              current.get("SeedSize"),
                # purity‐analysis fields will get filled later
                "PureSeed":     None,
                "OtherCropSeed":None,
                "InertMatter":  None,
                "WeedSeed":     None
            })
        current.clear()
    
    last_vendor_item_number = None

    for i, raw in enumerate(lines):
        line = raw.strip()
        
        # — new item trigger: 6 digits + inline description — must go first
        m2 = re.match(r"^(\d{6})\s+(.+)$", line)
        if m2:
            flush_item()
            current["VendorItemNumber"] = m2.group(1)
            desc_part1                 = m2.group(2).strip()
            continue

        # — new item trigger: bare 6-digit SKU —
        m1 = re.fullmatch(r"\d{6}", line)
        if m1:
            flush_item()
            current["VendorItemNumber"] = m1.group()
            desc_part1                 = ""
            continue

        # # — new batch trigger —
        if re.fullmatch(r"[A-Z]\d{5}", line):
            if "VendorBatchLot" in current and "VendorItemNumber" in current:
                flush_item()
            current["VendorBatchLot"] = line
            continue

        # accumulate lines for parsing
        # first non-trigger after item# is desc_part1
        if "VendorItemNumber" in current and not desc_part1:
            desc_part1 = line

        # second part of description on the "Flc." line
        if desc_part1 and line.startswith("Flc."):
            # strip off trailing "HM …"
            part2 = re.sub(r"HM.*$", "",
                    line.replace("Flc.", "")).strip()
            current["VendorItemDescription"] = f"{desc_part1} {part2}"

        # VendorProductLot
        if "VendorProductLot" not in current:
            m_pl = re.search(r"\bPL\d{6}\b", line)
            if m_pl:
                current["VendorProductLot"] = m_pl.group()

        # OriginCountry
        if "OriginCountry" not in current and "Country of origin:" in line:
            for o in (1,2):
                if i+o < len(lines) and re.fullmatch(r"[A-Z]{2}", lines[i+o].strip()):
                    current["OriginCountry"] = lines[i+o].strip()
                    break

        # UnitPrice
        if "UnitPrice" not in current:
            m_up = re.search(r"\d+\.\d{4}/KS", line)
            if m_up:
                current["UnitPrice"] = m_up.group()

        # ProductForm
        if "ProductForm" not in current:
            m_pf = re.search(r"Product Form:\s*(\w+)", line)
            if m_pf:
                current["ProductForm"] = m_pf.group(1)

        # Treatment
        if "Treatment" not in current:
            m_tr = re.search(r"Treatment:\s*(.+)", line)
            if m_tr:
                current["Treatment"] = m_tr.group(1).strip()

        # Germ
        if "Germ" not in current:
            m_g  = re.search(r"Germ:\s*(\d+\.\d+)", line)
            if m_g:
                current["Germ"] = float(m_g.group(1))

        # GermDate
        if "GermDate" not in current:
            m_gd = re.search(r"Germ Date:\s*(\d{2}/\d{2}/\d{2})", line)
            if m_gd:
                current["GermDate"] = m_gd.group(1)

        # SeedCount
        if "SeedCount" not in current:
            m_sc = re.search(r"Seed Count:\s*(\d+)", line)
            if m_sc:
                current["SeedCount"] = int(m_sc.group(1))

        # Purity
        if "Purity" not in current:
            m_pr = re.search(r"Purity:\s*(\d+\.\d+)", line)
            if m_pr:
                current["Purity"] = float(m_pr.group(1))

        # SeedSize
        if "SeedSize" not in current:
            m_sz = re.search(r"Seed Size:\s*(\w+)", line)
            if m_sz:
                current["SeedSize"] = m_sz.group(1)

    # flush the final item
    flush_item()
    return line_items

def extract_purity_analysis_reports(input_folder: str) -> Dict[str, Dict]:
    """
    Extracts purity analysis data from all seed analysis report PDFs in the folder.
    Returns a dictionary mapping batch lot prefixes (first 6 characters of filename) to purity data.
    """
    purity_data = {}
    for file in os.listdir(input_folder):
        if file.lower().endswith(".pdf") and len(file) >= 6:
            batch_key = os.path.splitext(file)[0][:6]
            pdf_path = os.path.join(input_folder, file)
            doc = fitz.open(pdf_path)
            text = ""
            for page in doc:
                text += page.get_text()

            text = text.replace('\n', ' ')
            text = re.sub(r'\s{2,}', ' ', text)

            match_pure = re.search(r"Pure Seed:\s*(\d+\.\d+)\s*%", text)
            match_other = re.search(r"Other Crop Seed\s*:\s*(\d+\.\d+)\s*%", text)
            match_inert = re.search(r"Inert Matter:\s*(\d+\.\d+)\s*%", text)
            match_weed = re.search(r"Weed Seed\s*:\s*(\d+\.\d+)\s*%", text)

            if match_pure or match_other or match_inert or match_weed:
                purity_data[batch_key] = {
                    "PureSeed": float(match_pure.group(1)) if match_pure else None,
                    "OtherCropSeed": float(match_other.group(1)) if match_other else None,
                    "InertMatter": float(match_inert.group(1)) if match_inert else None,
                    "WeedSeed": float(match_weed.group(1)) if match_weed else None
                }
    return purity_data

def enrich_invoice_items_with_purity(items: List[Dict], purity_data: Dict[str, Dict]) -> List[Dict]:
    """
    Adds purity analysis fields to each invoice item if a match is found by batch lot prefix.
    """
    for item in items:
        batch_lot = item.get("VendorBatchLot", "")
        key = batch_lot[:6]
        match = purity_data.get(key, {})
        item["PureSeed"] = match.get("PureSeed")
        item["OtherCropSeed"] = match.get("OtherCropSeed")
        item["InertMatter"] = match.get("InertMatter")
        item["WeedSeed"] = match.get("WeedSeed")
    return items

def extract_hm_clause_invoice_data(pdf_path: str) -> List[Dict]:
    """
    Extracts structured item-level invoice data from an HM Clause invoice PDF.
    """
    doc = fitz.open(pdf_path)
    all_blocks = []
    line_items = []

    # collect & sort all text blocks
    for page in doc:
        blocks = page.get_text("blocks")
        if not blocks or not any(b[4].strip() for b in blocks):
            ocr_lines = extract_text_with_azure_ocr(pdf_path)
            return extract_items_from_ocr_lines(ocr_lines)
        all_blocks.extend(sorted(blocks, key=lambda b: (b[1], b[0])))

    for i, b in enumerate(all_blocks):
        text = b[4].strip()
        if not re.fullmatch(r"[A-Z]\d{5}", text):
            continue

        # ─ init every per-item var ───────────────────────────────────────────
        vendor_batch_lot   = text
        vendor_item_number = None
        desc_part1         = ""
        desc_part2         = None   # <-- new
        vendor_product_lot = None
        origin_country     = None
        unit_price         = None
        product_form       = None
        treatment          = None
        germ               = None
        germ_date          = None
        seed_count         = None
        purity             = None
        seed_size          = None

        # wider window around the batch-lot
        start = max(0, i - 15)
        end   = min(len(all_blocks), i + 15)
        window = all_blocks[start:end]

        for block in window:
            block_text = block[4].strip()

            # always overwrite, drop any "if not X" guards
            m = re.match(r"^(\d{6})\s+(.+)", block_text)
            if m:
                vendor_item_number = m.group(1)
                desc_part1         = m.group(2).strip()

            m = re.search(r"\bPL\d{6}\b", block_text)
            if m:
                vendor_product_lot = m.group()

            m = re.search(r"Country of origin:\s*([A-Z]{2})", block_text)
            if m:
                origin_country = m.group(1)

            m = re.search(r"\d{1,3}(?:,\d{3})*(?:\.\d{4})?/KS", block_text)
            if m:
                unit_price = m.group()

            m = re.search(r"Product Form:\s*(\w+)", block_text)
            if m:
                product_form = m.group(1)

            m = re.search(r"Treatment:\s*(.+)", block_text)
            if m:
                treatment = m.group(1).strip()

            m = re.search(r"Germ:\s*(\d+\.\d+)", block_text)
            if m:
                germ = float(m.group(1))

            m = re.search(r"Germ Date:\s*(\d{2}/\d{2}/\d{2})", block_text)
            if m:
                germ_date = m.group(1)

            if "Purity:" in block_text:
                m = re.search(r"Seed Count:\s*([\d,]+)", block_text)
                if m:
                    seed_count = int(m.group(1).replace(",", ""))

            m = re.search(r"Purity:\s*(\d+\.\d+)", block_text)
            if m:
                purity = float(m.group(1))

            m = re.search(r"Seed Size:\s*([\d.]+)", block_text)
            if m:
                seed_size = m.group(1)

            # ─ pull out the second half of the description ────────────────
            if desc_part1 and block_text.startswith("Flc."):
                part2 = re.sub(r"HM.*$", "", block_text.replace("Flc.", "")).strip()
                desc_part2 = part2

        # only append if we actually found a number
        if vendor_item_number:
            full_desc = f"{desc_part1} {desc_part2}" if desc_part2 else desc_part1
            line_items.append({
                "VendorItemNumber":      vendor_item_number,
                "VendorItemDescription": full_desc.strip(),
                "VendorBatchLot":        vendor_batch_lot,
                "VendorProductLot":      vendor_product_lot,
                "OriginCountry":         origin_country,
                "UnitPrice":             unit_price,
                "ProductForm":           product_form,
                "Treatment":             treatment,
                "Germ":                  germ,
                "GermDate":              germ_date,
                "SeedCount":             seed_count,
                "Purity":                purity,
                "SeedSize":              seed_size
            })

    return line_items

def extract_hm_clause_data(pdf_paths: List[str]) -> List[Dict]:
    all_items = []
    folder = os.path.dirname(pdf_paths[0])
    purity_data = extract_purity_analysis_reports(folder)

    for pdf_path in pdf_paths:
        filename = os.path.basename(pdf_path)
        if re.match(r"^[A-Z]\d{5}", filename):  # skip seed analysis report
            continue

        items = extract_hm_clause_invoice_data(pdf_path)
        enriched = enrich_invoice_items_with_purity(items, purity_data)
        all_items.extend(enriched)

    return all_items
