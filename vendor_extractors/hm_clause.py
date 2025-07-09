import os
import json
import fitz  # PyMuPDF
import re
from typing import List, Dict
import requests
from difflib import get_close_matches
import time

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
            #print(text)

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
                    purity_data[batch_key]["GrowerGerm"] = float(match.group(1))

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

# def extract_hm_clause_invoice_data(pdf_path: str) -> List[Dict]:
#     """
#     Extracts structured item-level invoice data from an HM Clause invoice PDF.
#     """
#     doc = fitz.open(pdf_path)
#     all_blocks = []
#     line_items = []
#     vendor_invoice_no = None
    
#     # collect & sort all text blocks
#     for page in doc:
#         blocks = page.get_text("blocks")
#         if not blocks or not any(b[4].strip() for b in blocks):
#             ocr_lines = extract_text_with_azure_ocr(pdf_path)
#             return extract_items_from_ocr_lines(ocr_lines)
#         all_blocks.extend(sorted(blocks, key=lambda b: (b[1], b[0])))
        
#     # Extract Invoice No. from blocks
#     for block in all_blocks:
#         text = block[4].strip()
#         m_invoice = re.search(r"Invoice No\.\s*(\d{8})", text, re.IGNORECASE)
#         if m_invoice:
#             vendor_invoice_no = m_invoice.group(1)
#             break
        
#     # Extract Purchase Order No. from blocks
#     text = ""
#     for block in all_blocks:
#         text += block[4].strip() + "\n"
        
#     po_number = None

#     # Try primary pattern first
#     m_po = re.search(r"Customer PO No\.\s*(\d+)", text, re.IGNORECASE)
#     if m_po:
#         po_number = f"PO-{m_po.group(1)}"
#     else:
#         # Fallback to a secondary pattern if primary fails
#         m_po = re.search(r"Customer PO No\.\s*.*?(\d{5})", text, re.IGNORECASE)
#         if m_po:
#             po_number = f"PO-{m_po.group(1)}"

            

#     for i, b in enumerate(all_blocks):
#         text = b[4].strip()
#         if not re.fullmatch(r"[A-Z]\d{5}", text):
#             continue

#         # ─ init every per-item var ───────────────────────────────────────────
#         vendor_batch_lot   = text
#         vendor_item_number = None
#         desc_part1         = ""
#         desc_part2         = None   # <-- new
#         vendor_product_lot = None
#         origin_country     = None
#         unit_price         = None
#         product_form       = None
#         treatment          = None
#         germ               = None
#         germ_date          = None
#         seed_count         = None
#         purity             = None
#         seed_size          = None

#         # wider window around the batch-lot
#         start = max(0, i - 15)
#         end   = min(len(all_blocks), i + 15)
#         window = all_blocks[start:end]
        
        

#         for block in window:
#             block_text = block[4].strip()

#             # always overwrite, drop any "if not X" guards
#             m = re.match(r"^(\d{6})\s+(.+)", block_text)
#             if m:
#                 vendor_item_number = m.group(1)
#                 desc_part1         = m.group(2).strip()

#             m = re.search(r"\bPL\d{6}\b", block_text)
#             if m:
#                 vendor_product_lot = m.group()

#             m = re.search(r"Country of origin:\s*([A-Z]{2})", block_text)
#             if m:
#                 origin_country = m.group(1)

#             # m = re.search(r"\d{1,3}(?:,\d{3})*(?:\.\d{4})?/KS", block_text)
#             # if m:
#             #     unit_price = m.group()
               
#             m = re.search(r"\d{1,3}(?:,\d{3})*(?:\.\d+)?\s*/\s*KS", block_text, re.IGNORECASE)
#             if m:
#                 unit_price = m.group().replace(" ", "")
#                 # print(f"Processing lot {vendor_product_lot}")
#                 # print(f"Unit Price found: {unit_price}")


#             m = re.search(r"Product Form:\s*(\w+)", block_text)
#             if m:
#                 product_form = m.group(1)

#             m = re.search(r"Treatment:\s*(.+)", block_text)
#             if m:
#                 treatment = m.group(1).strip()

#             m = re.search(r"Germ:\s*(\d+\.\d+)", block_text)
#             if m:
#                 germ = float(m.group(1))

#             m = re.search(r"Germ Date:\s*(\d{2}/\d{2}/\d{2})", block_text)
#             if m:
#                 germ_date = m.group(1)

#             if "Purity:" in block_text:
#                 m = re.search(r"Seed Count:\s*([\d,]+)", block_text)
#                 if m:
#                     seed_count = int(m.group(1).replace(",", ""))

#             m = re.search(r"Purity:\s*(\d+\.\d+)", block_text)
#             if m:
#                 purity = float(m.group(1))

#             m = re.search(r"Seed Size:\s*([\w\.]+)", block_text)
#             if m:
#                 seed_size = m.group(1)

#             # ─ pull out the second half of the description ────────────────
#             if desc_part1 and block_text.startswith("Flc."):
#                 part2 = re.sub(r"HM.*$", "", block_text.replace("Flc.", "")).strip()
#                 desc_part2 = part2

#         # only append if we actually found a number
#         if vendor_item_number:
#             full_desc = f"{desc_part1} {desc_part2}" if desc_part2 else desc_part1
#             vendor_description = full_desc.strip()
            
#             line_items.append({
#                 "VendorInvoiceNo":       vendor_invoice_no,
#                 "PurchaseOrder":         po_number,
#                 "VendorItemNumber":      vendor_item_number,
#                 "VendorItemDescription": vendor_description,
#                 "VendorBatchLot":        vendor_batch_lot,
#                 "VendorProductLot":      vendor_product_lot,
#                 "OriginCountry":         origin_country,
#                 "UnitPrice":             unit_price,
#                 "ProductForm":           product_form,
#                 "Treatment":             treatment,
#                 "Germ":                  germ,
#                 "GermDate":              germ_date,
#                 "SeedCount":             seed_count,
#                 "Purity":                purity,
#                 "SeedSize":              seed_size
#             })
#             print(f"Item: {line_items}")

#     return line_items

def extract_hm_clause_invoice_data(pdf_path: str) -> List[Dict]:
    """
    Extracts structured item-level invoice data from an HM Clause invoice PDF.
    """
    doc = fitz.open(pdf_path)
    all_blocks = []
    line_items = []
    vendor_invoice_no = None
    po_number = None
    
    # collect & sort all text blocks
    for page in doc:
        page_text = page.get_text("text").strip()
        # Skip pages that start with or contain the specified text (case-insensitive)
        if page_text.lower().startswith("limitation of warranty and liability") or \
           "limitation of warranty and liability" in page_text.lower():
            continue
        blocks = page.get_text("blocks")
        if not blocks or not any(b[4].strip() for b in blocks):
            ocr_lines = extract_text_with_azure_ocr(pdf_path)
            return extract_items_from_ocr_lines(ocr_lines)
        all_blocks.extend(sorted(blocks, key=lambda b: (b[1], b[0])))
        
    # Extract Invoice No. from blocks
    for block in all_blocks[:30]:
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
    
    # --- New variables to hold the *current* item's data ---
    current_item_data = {}
    desc_part1 = ""
    price_index = 0
    upcharge_index = 0
    discount_index = 0
    quantity_index = 0

    def flush_item():
        """Helper to append the current item to line_items and reset."""
        nonlocal current_item_data, desc_part1
        if "VendorBatchLot" in current_item_data and "VendorItemNumber" in current_item_data:
            if "VendorItemDescription" not in current_item_data and desc_part1:
                current_item_data["VendorItemDescription"] = desc_part1
                
            #if "TotalPrice" in current_item_data and "TotalQuantity" in current_item_data:
            total_price = current_item_data.get("TotalPrice", 0.0)
            total_upcharge = current_item_data.get("TotalUpcharge", 0.0)
            total_discount = current_item_data.get("TotalDiscount", 0.0)
            total_quantity = current_item_data.get("TotalQuantity", 0)
            
            # Ensure TotalUpcharge and TotalDiscount are treated as floats
            total_upcharge = float(total_upcharge) if total_upcharge is not None else 0.0
            total_discount = float(total_discount) if total_discount is not None else 0.0
            
            if total_quantity > 0:
                current_item_data["USD_Actual_Cost_$"] = round(
                    (total_price + total_upcharge - total_discount) / total_quantity, 4
                )
            else:
                current_item_data["USD_Actual_Cost_$"] = None # Cannot calculate if quantity is zero    
            
            # # Compute USDActualCost if we got UnitPrice
            # if "UnitPrice" in current_item_data:
            #     up = current_item_data.get("UnitPrice", 0.0)
            #     uc = float(current_item_data["Upcharge"]) if "Upcharge" in current_item_data else 0.0
            #     dc = float(current_item_data["Discount"]) if "Discount" in current_item_data else 0.0

            #     current_item_data["USD_Actual_Cost_$"] = round(up + uc - dc, 4)
                    
            line_items.append({
                "VendorInvoiceNo":       vendor_invoice_no,
                "PurchaseOrder":         po_number,
                "VendorItemNumber":      current_item_data.get("VendorItemNumber"),
                "VendorItemDescription": current_item_data.get("VendorItemDescription", "").strip(),
                "VendorBatchLot":        current_item_data.get("VendorBatchLot"),
                "VendorProductLot":      current_item_data.get("VendorProductLot"),
                "OriginCountry":         current_item_data.get("OriginCountry"),
                "TotalPrice":            current_item_data.get("TotalPrice"),
                "TotalUpcharge":         current_item_data.get("TotalUpcharge"),
                "TotalDiscount":         current_item_data.get("TotalDiscount"),
                "TotalQuantity":         current_item_data.get("TotalQuantity"),
                "USD_Actual_Cost_$":     current_item_data.get("USD_Actual_Cost_$"),
                "ProductForm":           current_item_data.get("ProductForm"),
                "Treatment":             current_item_data.get("Treatment"),
                "Germ":                  current_item_data.get("Germ"),
                "GermDate":              current_item_data.get("GermDate"),
                "SeedCount":             current_item_data.get("SeedCount"),
                "Purity":                current_item_data.get("Purity"),
                "SeedSize":              current_item_data.get("SeedSize"),
                # purity‐analysis fields will get filled later
                "PureSeed":     None,
                "OtherCropSeed":None,
                "InertMatter":  None,
                "WeedSeed":     None
            })
        current_item_data = {} # Reset for the next item
        desc_part1 = "" # Reset description part 1 as it's per-item
        
    
    prev_block = ""
    next_block_text = ""
    for i, b in enumerate(all_blocks):
        if i + 1 < len(all_blocks):
            next_block_text = all_blocks[i + 1][4].strip()
        block_text = b[4].strip()
        print(block_text)

        # ─ New Item Trigger: VendorBatchLot ──────────────────────────────
        m_batch_lot = re.fullmatch(r"[A-Z]\d{5}", block_text)
        if m_batch_lot:
            # If we've started collecting data for an item, flush it first
            if current_item_data:
                flush_item()

            current_item_data["VendorBatchLot"] = m_batch_lot.group()
            # Reset flags for new item
            looking_for_total_price = False
            looking_for_total_upcharge = False
            looking_for_total_discount = False
            continue # Move to next block to find details for this new item

        # Only process blocks if we are currently building an item
        if not current_item_data:
            continue

        # --- Extract other details for the current item ---

        # VendorItemNumber and first part of description
        m = re.match(r"^(\d{6})\s+(.+)", block_text)
        if m:
            current_item_data["VendorItemNumber"] = m.group(1)
            desc_part1 = m.group(2).strip()
            continue

        # Bare 6-digit SKU (if description is on a separate line)
        if re.fullmatch(r"\d{6}", block_text):
            current_item_data["VendorItemNumber"] = block_text
            desc_part1 = ""  # wait for desc on next line
            continue

        # Second part of description ("Flc." line)
        if desc_part1 and block_text.startswith("Flc."):
            part2 = re.sub(r"HM.*$", "", block_text.replace("Flc.", "")).strip()
            full_desc = f"{desc_part1} {part2}".strip()
            current_item_data["VendorItemDescription"] = full_desc
            continue

        # Fallback: if we have desc_part1 but no Flc. or second half
        if desc_part1 and "VendorItemDescription" not in current_item_data:
            current_item_data["VendorItemDescription"] = desc_part1

        # VendorProductLot
        m_pl = re.search(r"\bPL\d{6}\b", block_text)
        if m_pl and "VendorProductLot" not in current_item_data:
            current_item_data["VendorProductLot"] = m_pl.group()

        # OriginCountry
        m_oc = re.search(r"Country of origin:\s*([A-Z]{2})", block_text)
        if m_oc and "OriginCountry" not in current_item_data:
            current_item_data["OriginCountry"] = m_oc.group(1).strip()
            
        # # Upcharge: Only capture if we're inside an item block
        # if current_item_data and "Upcharge" not in current_item_data and "upcharge" in block_text.lower():
        #     # Try previous, current, and next lines for the value
        #     for candidate in [prev_block, block_text, next_block_text]:
        #         m_upc = re.search(r"(\d+\.\d{4})", candidate)
        #         if m_upc:
        #             current_item_data["Upcharge"] = float(m_upc.group(1))
        #             break

        # # Discount: Same idea
        # if current_item_data and "Discount" not in current_item_data and "discount" in block_text.lower():
        #     m_disc = re.search(r"(-?\d+\.\d{4})(?:\s*/?\s*KS)?", block_text)
        #     if m_disc:
        #         current_item_data["Discount"] = abs(float(m_disc.group(1)))  # make it positive for calculation


        # # UnitPrice (Updated regex and logic)
        # m_up = re.search(r"(\d{1,3}(?:,\d{3})*(?:\.\d+)?)\s*/\s*KS", block_text, re.IGNORECASE)
        # if m_up and "UnitPrice" not in current_item_data:
        #     current_item_data["UnitPrice"] = float(m_up.group(1))

        # ProductForm
        m_pf = re.search(r"Product Form:\s*(\w+)", block_text)
        if m_pf and "ProductForm" not in current_item_data:
            current_item_data["ProductForm"] = m_pf.group(1)

        # Treatment
        m_tr = re.search(r"Treatment:\s*(.+)", block_text)
        if m_tr and "Treatment" not in current_item_data:
            current_item_data["Treatment"] = m_tr.group(1).strip()

        # Germ
        m_g  = re.search(r"Germ:\s*(\d+\.\d+)", block_text)
        if m_g and "Germ" not in current_item_data:
            current_item_data["Germ"] = float(m_g.group(1))

        # GermDate
        m_gd = re.search(r"Germ Date:\s*(\d{2}/\d{2}/\d{2})", block_text)
        if m_gd and "GermDate" not in current_item_data:
            current_item_data["GermDate"] = m_gd.group(1)

        # SeedCount
        if "Purity:" in block_text and "SeedCount" not in current_item_data: # Often on the same line as purity
            m_sc = re.search(r"Seed Count:\s*([\d,]+)", block_text)
            if m_sc:
                current_item_data["SeedCount"] = int(m_sc.group(1).replace(",", ""))

        # Purity
        m_pr = re.search(r"Purity:\s*(\d+\.\d+)", block_text)
        if m_pr and "Purity" not in current_item_data:
            current_item_data["Purity"] = float(m_pr.group(1))

        # SeedSize
        m_sz = re.search(r"Seed Size:\s*([\w\.]+)", block_text)
        if m_sz and "SeedSize" not in current_item_data:
            current_item_data["SeedSize"] = m_sz.group(1)
            
        # --- Extraction of Total Price, Upcharge, Discount, Quantity ---
        # We look for a 2-decimal float followed by an "N" (for Total Price/Discount) or "Y" (for Upcharge)
        # and then the quantity.

        # Total Quantity: Look for non-zero integer with "KS" or " KS" at the end (not /KS)
        # It's usually near the Amount and N/Y indicator.
        m_qty = re.search(r"(\d+)\s*(?:KS)", block_text)
        if m_qty and "TotalQuantity" not in current_item_data:
            qty_str = m_qty.group(1)
            # Ensure it's not part of a unit price, e.g., "56.8710/KS"
            if "/KS" not in block_text:
                current_item_data["TotalQuantity"] = int(qty_str)
                # print(f"Found TotalQuantity: {current_item_data['TotalQuantity']} in block: {block_text}")


        # Look for Total Price / Upcharge / Discount based on the line immediately following the numerical value
        # This assumes the amount and its N/Y indicator are on consecutive blocks or lines within a block
        m_value = re.search(r"(-?\d+\.\d{2})", block_text) # Matches 2 decimal places exactly
        if m_value:
            value = float(m_value.group(1))
            
            # Check the next block's text for 'N' or 'Y'
            next_block_text = ""
            if i + 1 < len(all_blocks):
                next_block_text = all_blocks[i+1][4].strip().upper()
            
            if "N" in next_block_text:
                if value >= 0 and "TotalPrice" not in current_item_data:
                    current_item_data["TotalPrice"] = value
                elif value < 0 and "TotalDiscount" not in current_item_data:
                    current_item_data["TotalDiscount"] = abs(value) # Store as positive
            elif "Y" in next_block_text and "TotalUpcharge" not in current_item_data:
                current_item_data["TotalUpcharge"] = value
            
        #prev_block = block_text

    # --- After the loop, flush the last collected item ---
    flush_item()
    
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