import os
import json
import fitz  # PyMuPDF
import re
from typing import List, Dict, Tuple
import requests
from difflib import get_close_matches
import time
from collections import defaultdict
item_usage_counter = defaultdict(int)

AZURE_ENDPOINT = os.getenv("AZURE_ENDPOINT")
AZURE_KEY = os.getenv("AZURE_KEY")

def extract_text_with_azure_ocr(pdf_path: str) -> List[str]:

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
                # Concatenate all lines on the page to check for the target text
                page_text = " ".join(line.get("content", "").strip() for line in page.get("lines", []) if line.get("content"))
                # Skip pages that start with or contain the specified text (case-insensitive)
                if page_text.lower().startswith("limitation of warranty and liability") or \
                   "limitation of warranty and liability" in page_text.lower():
                    continue
                for line in page.get("lines", []):
                    content = line.get("content", "").strip()
                    if content:
                        lines.append(content)
                lines.append("--- PAGE BREAK ---")

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
    vendor_invoice_no = None
    po_number = None
    
    # Extract Invoice No. (8-digit number after "Invoice No.")
    for line in lines:
        m_invoice = re.search(r"Invoice No\.\s*(\d{8})", line, re.IGNORECASE)
        if m_invoice:
            vendor_invoice_no = m_invoice.group(1)
            break
        
    # Extract Purchase Order No. from blocks
    for line in lines:
        m_po = re.search(r"Customer PO No\.\s*.*?(\d{5})", line, re.IGNORECASE)
        if m_po:
            po_number = f"PO-{m_po.group(1)}"
            break

    def flush_item():
        nonlocal current
        if "VendorBatchLot" in current:
            line_items.append({
                "VendorInvoiceNo":       vendor_invoice_no,
                "PurchaseOrder":         po_number,
                "VendorItemNumber":      current.get("VendorItemNumber"),
                "VendorItemDescription": current.get("VendorItemDescription", "").strip(),
                "VendorBatchLot":        current.get("VendorBatchLot"),
                "VendorProductLot":      current.get("VendorProductLot"),
                "OriginCountry":         current.get("OriginCountry"),
                "Discount":              current.get("Discount"),
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
                current["Germ"] = int(float(m_g.group(1)))

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
            m_sz = re.search(r"Seed Size:\s*([\w\.]+)", line)
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
    if not input_folder or not os.path.isdir(input_folder):
        return {}
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
                
                # Grower Germ Date: first date before "REPORT OF SEED ANALYSIS"
                date_matches = re.findall(r"(\d{1,2}/\d{1,2}/\d{4})(?=.*?REPORT OF SEED ANALYSIS)", text, re.IGNORECASE)
                if len(date_matches) >= 2:
                    purity_data[batch_key]["GrowerGermDate"] = date_matches[-1]

                # Grower Germ: extract the number immediately after "% Comments:"
                match = re.search(r"%\s*Comments:\s*(?:[A-Za-z]+\s+)*(\d{2,3})\b", text)
                if match:
                    purity_data[batch_key]["GrowerGerm"] = int(float(match.group(1)))

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
        item["GrowerGerm"] = match.get("GrowerGerm")
        item["GrowerGermDate"] = match.get("GrowerGermDate")

    return items

def extract_discounts(blocks: List) -> List[Tuple[str, float]]:
    discounts = []
    prev_discount = None  # Track last discount to avoid duplicates
    
    for i, b in enumerate(blocks):
        block_text = b[4].strip()
        print(block_text)
        if "discount" in block_text.lower():

            discount_amount = None
            item_number = None

            # Find discount amount (backward)
            for j in range(i - 1, max(i - 6, -1), -1):
                prev_text = blocks[j][4].strip()
                for match in re.finditer(r"-[\d,]+\.\d{2}", prev_text):
                    if "/KS" not in prev_text[match.start():match.end()+5]:
                        discount_amount = abs(float(match.group().replace(",", "")))
                        break
                if discount_amount:
                    break

            # Find associated item number (backward first)
            for j in range(i - 1, max(i - 6, -1), -1):
                prev_text = blocks[j][4].strip()
                m = re.match(r"^(\d{6})\b", prev_text)
                if m:
                    item_number = m.group(1)
                    break

            # Add discount if valid and not duplicate
            if item_number and discount_amount:
                current_discount = (item_number, discount_amount)
                if current_discount != prev_discount:  # Avoid consecutive duplicates
                    discounts.append(current_discount)
                    prev_discount = current_discount

    return discounts

def extract_hm_clause_invoice_data(pdf_path: str) -> List[Dict]:
    item_usage_counter.clear()
    doc = fitz.open(pdf_path)
    all_blocks = []
    vendor_invoice_no = None
    po_number = None

    # Collect & sort blocks
    for page in doc:
        page_text = page.get_text("text").strip()
        if "limitation of warranty and liability" in page_text.lower():
            continue
        blocks = page.get_text("blocks")
        if not blocks or not any(b[4].strip() for b in blocks):
            ocr_lines = extract_text_with_azure_ocr(pdf_path)
            return extract_items_from_ocr_lines(ocr_lines)
        all_blocks.extend(sorted(blocks, key=lambda b: (b[1], b[0])))
        
    # Extract Invoice No. from blocks
    for block in all_blocks:
        text = block[4].strip()
        m_invoice = re.search(r"Invoice No\.\s*(\d{8})", text, re.IGNORECASE)
        if m_invoice:
            vendor_invoice_no = m_invoice.group(1)
            break
        
    # Extract Purchase Order No. from blocks
    text = ""
    for block in all_blocks:
        
        text += block[4].strip() + "\n"
        
        # Try primary pattern first
        m_po = re.search(r"Customer PO No\.\s*(\d+)", text, re.IGNORECASE)
        if m_po:
            po_number = f"PO-{m_po.group(1)}"
        else:
            # Fallback to a secondary pattern if primary fails
            m_po = re.search(r"Customer PO No\.\s*.*?(\d{5})", text, re.IGNORECASE)
            if m_po:
                po_number = f"PO-{m_po.group(1)}"
        if po_number:
            break

    # --- Extract discounts FIRST ---
    discount_amounts = extract_discounts(all_blocks)
    discounts = []  # This will be the [(item_num, discount)] list we'll build

    print("\nDISCOUNTS BEFORE MATCHING:", discounts)

    line_items = []
    current_item_data = {}
    desc_part1 = ""

    def flush_item():
        nonlocal current_item_data, desc_part1
        if "VendorBatchLot" in current_item_data and "VendorItemNumber" in current_item_data:
            line_items.append({
                "VendorInvoiceNo":       vendor_invoice_no,
                "PurchaseOrder":         po_number,
                "VendorItemNumber":      current_item_data.get("VendorItemNumber"),
                "VendorItemDescription": current_item_data.get("VendorItemDescription", "").strip() or desc_part1,
                "VendorBatchLot":        current_item_data.get("VendorBatchLot"),
                "VendorProductLot":      current_item_data.get("VendorProductLot"),
                "OriginCountry":         current_item_data.get("OriginCountry"),
                "TotalPrice":            current_item_data.get("TotalPrice"),
                "TotalUpcharge":         current_item_data.get("TotalUpcharge"),
                "TotalDiscount":         None,  # matched later
                "TotalQuantity":         current_item_data.get("TotalQuantity"),
                "USD_Actual_Cost_$":     None,  # computed later
                "ProductForm":           current_item_data.get("ProductForm"),
                "Treatment":             current_item_data.get("Treatment"),
                "Germ":                  current_item_data.get("Germ"),
                "GermDate":              current_item_data.get("GermDate"),
                "SeedCount":             current_item_data.get("SeedCount"),
                "Purity":                current_item_data.get("Purity"),
                "SeedSize":              current_item_data.get("SeedSize"),
                "PureSeed":              None,
                "OtherCropSeed":         None,
                "InertMatter":           None,
                "WeedSeed":              None
            })
            print(f"FLUSHED ITEM → VendorItemNumber: {current_item_data.get('VendorItemNumber')}, Batch: {current_item_data.get('VendorBatchLot')}, Seed Count: {current_item_data.get('SeedCount')}")
        current_item_data = {}
        desc_part1 = ""
        
    def is_disqualified(block_idx: int, all_blocks: List) -> bool:
        block_text = all_blocks[block_idx][4].strip().lower()

        disqualifiers = ["cust no.", "cust no", "freight charges", "freight"]

        # Check current block text itself
        if any(term in block_text for term in disqualifiers):
            return True

        # Check nearby lines (before and after)
        nearby_texts = [
            all_blocks[j][4].strip().lower()
            for j in range(max(0, block_idx - 2), min(len(all_blocks), block_idx + 3))
            if j != block_idx
        ]
        return any(any(term in text for term in disqualifiers) for text in nearby_texts)



    # Parse all blocks
    for b in all_blocks:
        block_text = b[4].strip()

        if re.fullmatch(r"[A-Z]\d{5}", block_text):  # Batch Lot
            if current_item_data:
                flush_item()
            current_item_data["VendorBatchLot"] = block_text
            continue

        if not current_item_data:
            continue
        
        m = re.match(r"^(\d{6})\s+(.+)", block_text)
        if m:
            if is_disqualified(all_blocks.index(b), all_blocks):
                print(f"[SKIP] {block_text} near disqualifying context — not a VendorItemNumber")
                continue
            current_item_data["VendorItemNumber"] = m.group(1)
            desc_part1 = m.group(2).strip()
            continue

        if re.fullmatch(r"\d{6}", block_text):
            if is_disqualified(all_blocks.index(b), all_blocks):
                print(f"[SKIP] {block_text} near disqualifying context — not a VendorItemNumber")
                continue
            current_item_data["VendorItemNumber"] = block_text
            desc_part1 = ""
            continue

        if desc_part1 and block_text.startswith("Flc."):
            part2 = re.sub(r"HM.*$", "", block_text.replace("Flc.", "")).strip()
            current_item_data["VendorItemDescription"] = f"{desc_part1} {part2}"
            continue

        if "VendorItemDescription" not in current_item_data and desc_part1:
            current_item_data["VendorItemDescription"] = desc_part1
            
        # Financials
        if "TotalPrice" not in current_item_data:
            m_price = re.search(r"(?<!-)(\d[\d,]*\.\d{2})\s+N", block_text)
            if m_price:
                current_item_data["TotalPrice"] = float(m_price.group(1).replace(",", ""))
        
        if "TotalUpcharge" not in current_item_data:
            m_upcharge = re.search(r"(\d[\d,]*\.\d{2})\s+Y", block_text)
            if m_upcharge:
                current_item_data["TotalUpcharge"] = float(m_upcharge.group(1).replace(",", ""))

        if "TotalQuantity" not in current_item_data:
            m_qty = re.search(r"(\d+)\s*KS\b", block_text)
            if m_qty:
                qty = int(m_qty.group(1))
                if qty > 0:
                    current_item_data["TotalQuantity"] = qty

        # Other attributes
        if not current_item_data.get("VendorProductLot"):
            m_pl = re.search(r"\bPL\d{6}\b", block_text)
            if m_pl:
                current_item_data["VendorProductLot"] = m_pl.group()
                
        m_oc = re.search(r"Country of origin:\s*([A-Z]{2})", block_text)
        if m_oc:
            current_item_data["OriginCountry"] = m_oc.group(1)
            
        m_pf = re.search(r"Product Form:\s*(\w+)", block_text)
        if m_pf:
            current_item_data["ProductForm"] = m_pf.group(1)
            
        m_tr = re.search(r"Treatment:\s*(.+)", block_text)
        if m_tr:
            current_item_data["Treatment"] = m_tr.group(1).strip()
            
        m_g = re.search(r"Germ:\s*(\d+\.\d+)", block_text)
        if m_g:
            current_item_data["Germ"] = int(float((m_g.group(1))))
            
        m_gd = re.search(r"Germ Date:\s*(\d{2}/\d{2}/\d{2})", block_text)
        if m_gd:
            current_item_data["GermDate"] = m_gd.group(1)
        
        m_sc = re.search(r"(?<!Approx\.\s)Seed Count:\s*(\d+)", block_text)
        if m_sc:
            current_item_data["SeedCount"] = int(m_sc.group(1))


        m_pr = re.search(r"Purity:\s*(\d+\.\d+)", block_text)
        if m_pr:
            current_item_data["Purity"] = float(m_pr.group(1))
            
        m_sz = re.search(r"Seed Size:\s*([\w\.]+)", block_text)
        if m_sz:
            current_item_data["SeedSize"] = m_sz.group(1)

    flush_item()
    
    # Maintain a per-item occurrence counter
    item_counter = defaultdict(int)
    discounts_by_item = defaultdict(list)

    for item_num, amount in discount_amounts:
        discounts_by_item[item_num].append(amount)

    for item in line_items:
        item_num = item.get("VendorItemNumber")
        
        # Get current occurrence index for this item
        occurrence_idx = item_counter[item_num]
        item_counter[item_num] += 1
        
        # Assign discount if available for this occurrence
        if occurrence_idx < len(discounts_by_item[item_num]):
            item["TotalDiscount"] = discounts_by_item[item_num][occurrence_idx]
        else:
            item["TotalDiscount"] = None  # fallback

        # Calculate actual cost
        tp = item.get("TotalPrice") or 0.0
        tu = item.get("TotalUpcharge") or 0.0
        td = item["TotalDiscount"] or 0.0
        qty = item.get("TotalQuantity") or 0
        item["USD_Actual_Cost_$"] = round(((tp + tu - td) / qty), 4) if qty > 0 else None

    return line_items


def extract_hm_clause_data(pdf_path: str) -> List[Dict]:
    folder = os.path.dirname(pdf_path)
    purity_data = extract_purity_analysis_reports(folder)

    # for pdf_path in pdf_path:
    filename = os.path.basename(pdf_path)
    if re.match(r"^[A-Z]\d{5}", filename):  # skip seed analysis report
        return []

    items = extract_hm_clause_invoice_data(pdf_path)
    enriched = enrich_invoice_items_with_purity(items, purity_data)

    return enriched

def find_best_hm_clause_package_description(vendor_desc: str, pkg_desc_list: list[str]) -> str:
    """
    Given an HM Clause vendor description, find the best match from the BC Package Descriptions.
    Logic: 50 Ks -> 50,000 SEEDS, 30 Ms -> 30,000,000 SEEDS.
    """
    if not vendor_desc or not pkg_desc_list:
        return ""

    normalized_desc = vendor_desc.upper()
    candidate = ""

    # Search for patterns like "50 KS", "50KS", "30 MS", "30MS"
    m = re.search(r"(\d+)\s*(KS|MS)\b", normalized_desc)
    if m:
        qty = int(m.group(1))
        unit = m.group(2)

        if unit == "KS":
            seed_count = qty * 1000
            candidate = f"{seed_count:,} SEEDS"
        elif unit == "MS":
            seed_count = qty * 1000000
            candidate = f"{seed_count:,} SEEDS"

        # Check if the generated candidate exists in the BC list
        if candidate in pkg_desc_list:
            return candidate

    # Fallback to fuzzy matching against all package descriptions
    matches = get_close_matches(normalized_desc, pkg_desc_list, n=1, cutoff=0.6)
    return matches[0] if matches else ""