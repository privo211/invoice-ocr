# import os
# import json
# import fitz  # PyMuPDF
# import re
# from typing import List, Dict, Tuple
# import requests
# from difflib import get_close_matches
# import time
# from collections import defaultdict
# item_usage_counter = defaultdict(int)

# AZURE_ENDPOINT = os.getenv("AZURE_ENDPOINT")
# AZURE_KEY = os.getenv("AZURE_KEY")

# # --- This function remains from the new code, as it correctly handles in-memory OCR ---
# def extract_text_with_azure_ocr(pdf_bytes: bytes) -> List[str]:
#     """
#     Performs OCR on in-memory PDF bytes using Azure Form Recognizer.
#     """
#     headers = {
#         "Ocp-Apim-Subscription-Key": AZURE_KEY,
#         "Content-Type": "application/pdf"
#     }
#     # Post the byte stream directly
#     response = requests.post(
#         f"{AZURE_ENDPOINT}formrecognizer/documentModels/prebuilt-layout:analyze?api-version=2023-07-31",
#         headers=headers,
#         data=pdf_bytes
#     )

#     if response.status_code != 202:
#         raise RuntimeError(f"OCR request failed: {response.text}")

#     result_url = response.headers["Operation-Location"]

#     for _ in range(30):
#         time.sleep(1.5)
#         result = requests.get(result_url, headers={"Ocp-Apim-Subscription-Key": AZURE_KEY}).json()
#         if result.get("status") == "succeeded":
#             lines = []
#             for page in result.get("analyzeResult", {}).get("pages", []):
#                 page_text = " ".join(line.get("content", "").strip() for line in page.get("lines", []) if line.get("content"))
#                 if page_text.lower().startswith("limitation of warranty and liability") or \
#                    "limitation of warranty and liability" in page_text.lower():
#                     continue
#                 for line in page.get("lines", []):
#                     content = line.get("content", "").strip()
#                     if content:
#                         lines.append(content)
#                 lines.append("--- PAGE BREAK ---")

#             return lines

#         elif result.get("status") == "failed":
#             raise RuntimeError("OCR analysis failed")
#     raise TimeoutError("OCR timed out")

# # --- This OCR-specific parsing logic is unchanged, as it was consistent ---
# def extract_items_from_ocr_lines(lines: List[str]) -> List[Dict]:
#     line_items = []
#     current = {}
#     desc_part1 = ""
#     vendor_invoice_no = None
#     po_number = None
    
#     # This function is being brought back from the original code to parse discounts from OCR text
#     def extract_discounts_from_ocr_lines(lines: List[str]) -> Dict[str, List[float]]:
#         discounts_by_item = defaultdict(list)
#         prev_discount = None

#         for i, line in enumerate(lines):
#             if "discount" in line.lower():
#                 discount_amount = None
#                 item_number = None
#                 for j in range(i - 1, max(i - 6, -1), -1):
#                     m = re.search(r"-[\d,]+\.\d{2}", lines[j])
#                     if m:
#                         discount_amount = abs(float(m.group().replace(",", "")))
#                         break
#                 for j in range(i - 1, max(i - 6, -1), -1):
#                     m = re.match(r"^(\d{6})\b", lines[j])
#                     if m:
#                         item_number = m.group(1)
#                         break
#                 if item_number and discount_amount:
#                     current = (item_number, discount_amount)
#                     if current != prev_discount:
#                         discounts_by_item[item_number].append(discount_amount)
#                         prev_discount = current
#             if "discount-pack size" in line.lower():
#                 item_number = None
#                 for j in range(i - 1, max(i - 6, -1), -1):
#                     m = re.match(r"^(\d{6})\b", lines[j])
#                     if m:
#                         item_number = m.group(1)
#                         break
#                 for j in range(i + 1, min(i + 6, len(lines))):
#                     discount_line = lines[j].strip()
#                     m_disc = re.search(r"-([\d,]+\.\d{2})\s+N", discount_line)
#                     if m_disc and item_number:
#                         discount_value = float(m_disc.group(1).replace(",", ""))
#                         discounts_by_item[item_number].append(discount_value)
#                         break
#         return discounts_by_item

#     discounts_by_item = extract_discounts_from_ocr_lines(lines)

#     for line in lines:
#         m_invoice = re.search(r"Invoice No\.\s*(\d{8})", line, re.IGNORECASE)
#         if m_invoice:
#             vendor_invoice_no = m_invoice.group(1)
#             break
        
#     for line in lines:
#         m_po = re.search(r"Customer PO No\.\s*.*?(\d{5})", line, re.IGNORECASE)
#         if m_po:
#             po_number = f"PO-{m_po.group(1)}"
#             break

#     def flush_item():
#         nonlocal current
#         if "VendorBatchLot" in current:
#             line_items.append({
#                 "VendorInvoiceNo":       vendor_invoice_no,
#                 "PurchaseOrder":         po_number,
#                 "VendorItemNumber":      current.get("VendorItemNumber"),
#                 "VendorItemDescription": current.get("VendorItemDescription", "").strip(),
#                 "VendorBatchLot":        current.get("VendorBatchLot"),
#                 "VendorProductLot":      current.get("VendorProductLot"),
#                 "OriginCountry":         current.get("OriginCountry"),
#                 "TotalPrice":            current.get("TotalPrice"),
#                 "TotalUpcharge":         current.get("TotalUpcharge"),
#                 "TotalDiscount":         None,
#                 "TotalQuantity":         current.get("TotalQuantity"),
#                 "USD_Actual_Cost_$":     None,
#                 "ProductForm":           current.get("ProductForm"),
#                 "Treatment":             current.get("Treatment"),
#                 "SeedCount":             current.get("SeedCount"),
#                 "Purity":                current.get("Purity"),
#                 "SeedSize":              current.get("SeedSize"),
#                 "PureSeed":              None,
#                 "InertMatter":           None,
#                 "Germ":                  None,
#                 "GermDate":              None
#             })
#         current.clear()

#     for i, raw in enumerate(lines):
#         line = raw.strip()
        
#         m2 = re.match(r"^(\d{6})\s+(.+)$", line)
#         if m2:
#             flush_item()
#             current["VendorItemNumber"] = m2.group(1)
#             desc_part1 = m2.group(2).strip()
#             continue

#         m1 = re.fullmatch(r"\d{6}", line)
#         if m1:
#             flush_item()
#             current["VendorItemNumber"] = m1.group()
#             desc_part1 = ""
#             continue

#         if re.fullmatch(r"[A-Z]\d{5}", line):
#             if "VendorBatchLot" in current and "VendorItemNumber" in current:
#                 flush_item()
#             current["VendorBatchLot"] = line
#             continue

#         if "VendorItemNumber" in current and not desc_part1:
#             desc_part1 = line

#         if desc_part1 and line.startswith("Flc."):
#             part2 = re.sub(r"HM.*$", "", line.replace("Flc.", "")).strip()
#             current["VendorItemDescription"] = f"{desc_part1} {part2}"

#         if "VendorProductLot" not in current and (m_pl := re.search(r"\bPL\d{6}\b", line)):
#             current["VendorProductLot"] = m_pl.group()

#         if "OriginCountry" not in current and "Country of origin:" in line:
#             for o in (1,2):
#                 if i+o < len(lines) and re.fullmatch(r"[A-Z]{2}", lines[i+o].strip()):
#                     current["OriginCountry"] = lines[i+o].strip()
#                     break

#         if "ProductForm" not in current and (m_pf := re.search(r"Product Form:\s*(\w+)", line)):
#             current["ProductForm"] = m_pf.group(1)

#         if "Treatment" not in current and (m_tr := re.search(r"Treatment:\s*(.+)", line)):
#             current["Treatment"] = m_tr.group(1).strip()

#         if "SeedCount" not in current and (m_sc := re.search(r"Seed Count:\s*(\d+)", line)):
#             current["SeedCount"] = int(m_sc.group(1))

#         if "Purity" not in current and (m_pr := re.search(r"Purity:\s*(\d+\.\d+)", line)):
#             current["Purity"] = float(m_pr.group(1))

#         if "SeedSize" not in current and (m_sz := re.search(r"Seed Size:\s*([\w\.]+)", line)):
#             current["SeedSize"] = m_sz.group(1)
                
#         if "TotalPrice" not in current and (m_price := re.search(r"(?<!-)(\d[\d,]*\.\d{2})\s+N", line)):
#             current["TotalPrice"] = float(m_price.group(1).replace(",", ""))
        
#         if "TotalUpcharge" not in current:
#             m_upcharge = re.search(r"(\d[\d,]*\.\d{2})\s+Y", line)
#             if m_upcharge:
#                 current["TotalUpcharge"] = float(m_upcharge.group(1).replace(",", ""))
#             else:
#                 if i + 1 < len(lines) and lines[i + 1].strip() == "Y":
#                     if m_val := re.search(r"(\d[\d,]*\.\d{2})", line):
#                         current["TotalUpcharge"] = float(m_val.group(1).replace(",", ""))
#                 elif i > 0 and lines[i - 1].strip() == ">" and re.search(r"\d[\d,]*\.\d{2}", line):
#                     if m_val := re.search(r"\d[\d,]*\.\d{2}", line):
#                         current["TotalUpcharge"] = float(m_val.group(0).replace(",", ""))

#         if "TotalQuantity" not in current and (m_qty := re.search(r"(\d+)\s*KS\b", line)):
#             if (qty := int(m_qty.group(1))) > 0:
#                 current["TotalQuantity"] = qty

#     flush_item()
    
#     item_counter = defaultdict(int)
#     for item in line_items:
#         if not (item_num := item.get("VendorItemNumber")):
#             continue
#         occurrence_idx = item_counter[item_num]
#         item_counter[item_num] += 1
#         if occurrence_idx < len(discounts_by_item.get(item_num, [])):
#             item["TotalDiscount"] = discounts_by_item[item_num][occurrence_idx]
#         else:
#             item["TotalDiscount"] = None
    
#     for item in line_items:
#         tp = item.get("TotalPrice") or 0.0
#         tu = item.get("TotalUpcharge") or 0.0
#         td = item.get("TotalDiscount") or 0.0
#         qty = item.get("TotalQuantity")
#         item["USD_Actual_Cost_$"] = round(((tp + tu - td) / qty), 4) if qty and qty > 0 else None

#     return line_items

# # --- This function remains from the new code ---
# def extract_purity_analysis_reports_from_bytes(pdf_files: list[tuple[str, bytes]]) -> Dict[str, Dict]:
#     purity_data = defaultdict(dict)
#     for filename, pdf_bytes in pdf_files:
#         try:
#             doc = fitz.open(stream=pdf_bytes, filetype="pdf")
#             text = ""
#             for page in doc:
#                 if "REPORT OF SEED ANALYSIS" in page.get_text("text", flags=fitz.TEXT_INHIBIT_SPACES).upper():
#                     text += page.get_text() + " "
#             doc.close()

#             if not text:
#                 continue
            
#             text = text.replace('\n', ' ').replace('\r', ' ')
#             text = re.sub(r'\s{2,}', ' ', text)
            
#             if not (batch_match := re.search(r"\b([A-Z]\d{5,})\b", text)):
#                 continue
#             batch_key = batch_match.group(1)

#             if (match_pure := re.search(r"Pure Seed:\s*(\d+\.\d+)\s*%", text)) and \
#                (match_inert := re.search(r"Inert Matter:\s*(\d+\.\d+)\s*%", text)):
                
#                 pure_seed = float(match_pure.group(1))
#                 purity_data[batch_key]["PureSeed"] = 99.99 if pure_seed == 100 else pure_seed
#                 purity_data[batch_key]["InertMatter"] = 0.01 if pure_seed == 100 else float(match_inert.group(1))
                
#                 if date_matches := re.findall(r"(\d{1,2}/\d{1,2}/\d{4})(?=.*?REPORT OF SEED ANALYSIS)", text, re.IGNORECASE):
#                     if len(date_matches) >= 2:
#                         purity_data[batch_key]["GrowerGermDate"] = date_matches[-1]
#                         purity_data[batch_key]["GermDate"] = date_matches[-1]

#                 if match := re.search(r"%\s*Comments:\s*(?:[A-Za-z]+\s+)*(\d{2,3})\b", text):
#                     germ = int(float(match.group(1)))
#                     purity_data[batch_key]["GrowerGerm"] = germ
#                     purity_data[batch_key]["Germ"] = 98 if germ == 100 else germ
#         except Exception as e:
#             print(f"Could not process {filename} for purity analysis: {e}")
#             continue
#     return purity_data

# # --- This function remains from the new code ---
# def enrich_invoice_items_with_purity(items: List[Dict], purity_data: Dict[str, Dict]) -> List[Dict]:
#     for item in items:
#         if batch_lot := item.get("VendorBatchLot", ""):
#             key = batch_lot[:6]
#             if match := purity_data.get(key, {}):
#                 item.update(match)
#     return items

# # --- This is the original `extract_discounts` function, which is simpler and now restored ---
# def extract_discounts(blocks: List) -> List[Tuple[str, float]]:
#     discounts = []
#     prev_discount = None
    
#     for i, b in enumerate(blocks):
#         block_text = b[4].strip()
        
#         if "discount" in block_text.lower():
#             discount_amount = None
#             item_number = None

#             for j in range(i - 1, max(i - 6, -1), -1):
#                 prev_text = blocks[j][4].strip()
#                 for match in re.finditer(r"-[\d,]+\.\d{2}", prev_text):
#                     if "/KS" not in prev_text[match.start():match.end()+5]:
#                         discount_amount = abs(float(match.group().replace(",", "")))
#                         break
#                 if discount_amount:
#                     break

#             for j in range(i - 1, max(i - 6, -1), -1):
#                 prev_text = blocks[j][4].strip()
#                 if m := re.match(r"^(\d{6})\b", prev_text):
#                     item_number = m.group(1)
#                     break

#             if item_number and discount_amount:
#                 current_discount = (item_number, discount_amount)
#                 if current_discount != prev_discount:
#                     discounts.append(current_discount)
#                     prev_discount = current_discount
#     return discounts

# # ##################################################################################
# # ## --- REVERTED LOGIC STARTS HERE ---
# # ##################################################################################

# def extract_hm_clause_invoice_data_from_bytes(pdf_bytes: bytes) -> List[Dict]:
#     """
#     Extracts invoice data from in-memory PDF bytes using the ORIGINAL parsing logic.
#     """
#     item_usage_counter.clear()
#     doc = fitz.open(stream=pdf_bytes, filetype="pdf")
#     all_blocks = []
#     vendor_invoice_no = None
#     po_number = None

#     # Collect & sort blocks from pages that are not the warranty page
#     for page in doc:
#         if "limitation of warranty and liability" in page.get_text("text").lower():
#             continue
        
#         blocks = page.get_text("blocks")
#         # Fallback to OCR if page is blank or image-based
#         if not blocks or not any(b[4].strip() for b in blocks):
#             ocr_lines = extract_text_with_azure_ocr(pdf_bytes)
#             return extract_items_from_ocr_lines(ocr_lines)
            
#         all_blocks.extend(sorted(blocks, key=lambda b: (b[1], b[0])))
#     doc.close()
        
#     # Extract Invoice No.
#     for block in all_blocks:
#         text = block[4].strip()
#         if m_invoice := re.search(r"Invoice No\.\s*(\d{8})", text, re.IGNORECASE):
#             vendor_invoice_no = m_invoice.group(1)
#             break
        
#     # Extract Purchase Order No. using the original cumulative text method
#     text = ""
#     for block in all_blocks:
#         text += block[4].strip() + "\n"
#         m_po = re.search(r"Customer PO No\.\s*(\d+)", text, re.IGNORECASE)
#         if not m_po:
#             m_po = re.search(r"Customer PO No\.\s*.*?(\d{5})", text, re.IGNORECASE)
#         if m_po:
#             po_number = f"PO-{m_po.group(1)}"
#             break

#     # --- This section now uses the original logic from old_hm_clause.py.txt ---
#     discount_amounts = extract_discounts(all_blocks)
#     line_items = []
#     current_item_data = {}
#     desc_part1 = ""

#     def flush_item():
#         nonlocal current_item_data, desc_part1
#         if "VendorBatchLot" in current_item_data and "VendorItemNumber" in current_item_data:
#             line_items.append({
#                 "VendorInvoiceNo":       vendor_invoice_no,
#                 "PurchaseOrder":         po_number,
#                 "VendorItemNumber":      current_item_data.get("VendorItemNumber"),
#                 "VendorItemDescription": current_item_data.get("VendorItemDescription", "").strip() or desc_part1,
#                 "VendorBatchLot":        current_item_data.get("VendorBatchLot"),
#                 "VendorProductLot":      current_item_data.get("VendorProductLot"),
#                 "OriginCountry":         current_item_data.get("OriginCountry"),
#                 "TotalPrice":            current_item_data.get("TotalPrice"),
#                 "TotalUpcharge":         current_item_data.get("TotalUpcharge"),
#                 "TotalDiscount":         None,
#                 "TotalQuantity":         current_item_data.get("TotalQuantity"),
#                 "USD_Actual_Cost_$":     None,
#                 "ProductForm":           current_item_data.get("ProductForm"),
#                 "Treatment":             current_item_data.get("Treatment"),
#                 "SeedCount":             current_item_data.get("SeedCount"),
#                 "Purity":                current_item_data.get("Purity"),
#                 "SeedSize":              current_item_data.get("SeedSize"),
#                 "PureSeed":              None, "InertMatter": None,
#                 "Germ":                  None, "GermDate":    None
#             })
#         current_item_data = {}
#         desc_part1 = ""
        
#     def is_disqualified(block_idx: int, all_blocks: List) -> bool:
#         block_text = all_blocks[block_idx][4].strip().lower()
#         disqualifiers = ["cust no.", "cust no", "freight charges", "freight"]
#         if any(term in block_text for term in disqualifiers):
#             return True
#         nearby_texts = [
#             all_blocks[j][4].strip().lower()
#             for j in range(max(0, block_idx - 2), min(len(all_blocks), block_idx + 3))
#             if j != block_idx
#         ]
#         return any(any(term in text for term in disqualifiers) for text in nearby_texts)

#     # --- ORIGINAL PARSING LOOP ---
#     for idx, b in enumerate(all_blocks):
#         block_text = b[4].strip()

#         # Original logic: Batch lot is a primary trigger that flushes the previous item
#         if re.fullmatch(r"[A-Z]\d{5}", block_text):
#             if current_item_data:
#                 flush_item()
#             current_item_data["VendorBatchLot"] = block_text
#             continue

#         if not current_item_data:
#             continue
        
#         if m := re.match(r"^(\d{6})\s+(.+)", block_text):
#             if is_disqualified(idx, all_blocks): continue
#             current_item_data["VendorItemNumber"] = m.group(1)
#             desc_part1 = m.group(2).strip()
#             continue

#         if re.fullmatch(r"\d{6}", block_text):
#             if is_disqualified(idx, all_blocks): continue
#             current_item_data["VendorItemNumber"] = block_text
#             desc_part1 = ""
#             continue

#         if desc_part1 and block_text.startswith("Flc."):
#             part2 = re.sub(r"HM.*$", "", block_text.replace("Flc.", "")).strip()
#             current_item_data["VendorItemDescription"] = f"{desc_part1} {part2}"
#             continue

#         if "VendorItemDescription" not in current_item_data and desc_part1:
#             current_item_data["VendorItemDescription"] = desc_part1
            
#         if "TotalPrice" not in current_item_data and (m_price := re.search(r"(?<!-)(\d[\d,]*\.\d{2})\s+N", block_text)):
#             current_item_data["TotalPrice"] = float(m_price.group(1).replace(",", ""))
        
#         if "TotalUpcharge" not in current_item_data and (m_upcharge := re.search(r"(\d[\d,]*\.\d{2})\s+Y", block_text)):
#             current_item_data["TotalUpcharge"] = float(m_upcharge.group(1).replace(",", ""))

#         if "TotalQuantity" not in current_item_data and (m_qty := re.search(r"(\d+)\s*KS\b", block_text)):
#             if (qty := int(m_qty.group(1))) > 0: current_item_data["TotalQuantity"] = qty

#         if not current_item_data.get("VendorProductLot") and (m_pl := re.search(r"\bPL\d{6}\b", block_text)):
#             current_item_data["VendorProductLot"] = m_pl.group()
         
#         if m_oc := re.search(r"Country of origin:\s*([A-Z]{2})", block_text):
#             current_item_data["OriginCountry"] = m_oc.group(1)
            
#         if m_pf := re.search(r"Product Form:\s*(\w+)", block_text):
#             current_item_data["ProductForm"] = m_pf.group(1)
            
#         if m_tr := re.search(r"Treatment:\s*(.+)", block_text):
#             current_item_data["Treatment"] = m_tr.group(1).strip()
        
#         if m_sc := re.search(r"(?<!Approx\.\s)Seed Count:\s*(\d+)", block_text):
#             current_item_data["SeedCount"] = int(m_sc.group(1))

#         if m_pr := re.search(r"Purity:\s*(\d+\.\d+)", block_text):
#             current_item_data["Purity"] = float(m_pr.group(1))
            
#         if m_sz := re.search(r"Seed Size:\s*([\w\.]+)", block_text):
#             current_item_data["SeedSize"] = m_sz.group(1)

#     flush_item()
    
#     # Assign discounts and calculate final cost using original logic
#     item_counter = defaultdict(int)
#     discounts_by_item = defaultdict(list)
#     for item_num, amount in discount_amounts:
#         discounts_by_item[item_num].append(amount)

#     for item in line_items:
#         if item_num := item.get("VendorItemNumber"):
#             occurrence_idx = item_counter[item_num]
#             item_counter[item_num] += 1
            
#             if occurrence_idx < len(discounts_by_item.get(item_num, [])):
#                 item["TotalDiscount"] = discounts_by_item[item_num][occurrence_idx]
#             else:
#                 item["TotalDiscount"] = None

#             tp = item.get("TotalPrice", 0.0) or 0.0
#             tu = item.get("TotalUpcharge", 0.0) or 0.0
#             td = item.get("TotalDiscount", 0.0) or 0.0
#             qty = item.get("TotalQuantity") or 0
#             item["USD_Actual_Cost_$"] = round(((tp + tu - td) / qty), 4) if qty > 0 else None

#     return line_items

# # ##################################################################################
# # ## --- REVERTED LOGIC ENDS HERE ---
# # ##################################################################################


# # --- This is the NEW top-level function that processes batches in memory ---
# def extract_hm_clause_data_from_bytes(pdf_files: list[tuple[str, bytes]]) -> dict[str, list[dict]]:
#     """
#     Main entry point for extracting HM Clause data from a batch of in-memory PDF files.
#     """
#     if not pdf_files:
#         return {}

#     # 1. Extract purity data from ALL provided files first.
#     purity_data = extract_purity_analysis_reports_from_bytes(pdf_files)

#     # 2. Process each file, identifying main invoices and extracting their data.
#     grouped_results = {}
#     for filename, pdf_bytes in pdf_files:
#         # Skip files that are identified as seed analysis reports.
#         if re.match(r"^[A-Z]\d{5}", os.path.basename(filename), re.IGNORECASE):
#             continue

#         try:
#             # 3. Call the invoice extractor which now contains the ORIGINAL logic.
#             items = extract_hm_clause_invoice_data_from_bytes(pdf_bytes)
#             if items:
#                 # 4. Enrich the extracted items with the pre-compiled purity data.
#                 enriched_items = enrich_invoice_items_with_purity(items, purity_data)
#                 grouped_results[filename] = enriched_items
#         except Exception as e:
#             print(f"Error processing invoice {filename}: {e}")
#             continue

#     return grouped_results


# def find_best_hm_clause_package_description(vendor_desc: str, pkg_desc_list: list[str]) -> str:
#     """
#     (No changes needed here)
#     """
#     if not vendor_desc or not pkg_desc_list:
#         return ""

#     normalized_desc = vendor_desc.upper()
#     candidate = ""

#     m = re.search(r"(\d+)\s*(KS|MS)\b", normalized_desc)
#     if m:
#         qty = int(m.group(1))
#         unit = m.group(2)

#         if unit == "KS":
#             seed_count = qty * 1000
#             candidate = f"{seed_count:,} SEEDS"
#         elif unit == "MS":
#             seed_count = qty * 1000000
#             candidate = f"{seed_count:,} SEEDS"

#         if candidate in pkg_desc_list:
#             return candidate

#     matches = get_close_matches(normalized_desc, pkg_desc_list, n=1, cutoff=0.6)
#     return matches[0] if matches else ""
#____________________________________________________________________________________________________________


# import os
# import json
# import fitz  # PyMuPDF
# import re
# from typing import List, Dict, Tuple
# import requests
# from difflib import get_close_matches
# import time
# from collections import defaultdict
# item_usage_counter = defaultdict(int)

# AZURE_ENDPOINT = os.getenv("AZURE_ENDPOINT")
# AZURE_KEY = os.getenv("AZURE_KEY")

# # --- This function remains from the new code, as it correctly handles in-memory OCR ---
# def extract_text_with_azure_ocr(pdf_bytes: bytes) -> List[str]:
#     """
#     Performs OCR on in-memory PDF bytes using Azure Form Recognizer.
#     """
#     headers = {
#         "Ocp-Apim-Subscription-Key": AZURE_KEY,
#         "Content-Type": "application/pdf"
#     }
#     # Post the byte stream directly
#     response = requests.post(
#         f"{AZURE_ENDPOINT}formrecognizer/documentModels/prebuilt-layout:analyze?api-version=2023-07-31",
#         headers=headers,
#         data=pdf_bytes
#     )

#     if response.status_code != 202:
#         raise RuntimeError(f"OCR request failed: {response.text}")

#     result_url = response.headers["Operation-Location"]

#     for _ in range(30):
#         time.sleep(1.5)
#         result = requests.get(result_url, headers={"Ocp-Apim-Subscription-Key": AZURE_KEY}).json()
#         if result.get("status") == "succeeded":
#             lines = []
#             for page in result.get("analyzeResult", {}).get("pages", []):
#                 page_text = " ".join(line.get("content", "").strip() for line in page.get("lines", []) if line.get("content"))
#                 if page_text.lower().startswith("limitation of warranty and liability") or \
#                    "limitation of warranty and liability" in page_text.lower():
#                     continue
#                 for line in page.get("lines", []):
#                     content = line.get("content", "").strip()
#                     if content:
#                         lines.append(content)
#                 lines.append("--- PAGE BREAK ---")

#             return lines

#         elif result.get("status") == "failed":
#             raise RuntimeError("OCR analysis failed")
#     raise TimeoutError("OCR timed out")

# # --- This OCR-specific parsing logic is unchanged, as it was consistent ---
# def extract_items_from_ocr_lines(lines: List[str]) -> List[Dict]:
#     line_items = []
#     current = {}
#     desc_part1 = ""
#     vendor_invoice_no = None
#     po_number = None
    
#     # This function is being brought back from the original code to parse discounts from OCR text
#     def extract_discounts_from_ocr_lines(lines: List[str]) -> Dict[str, List[float]]:
#         discounts_by_item = defaultdict(list)
#         prev_discount = None

#         for i, line in enumerate(lines):
#             if "discount" in line.lower():
#                 discount_amount = None
#                 item_number = None
#                 for j in range(i - 1, max(i - 6, -1), -1):
#                     m = re.search(r"-[\d,]+\.\d{2}", lines[j])
#                     if m:
#                         discount_amount = abs(float(m.group().replace(",", "")))
#                         break
#                 for j in range(i - 1, max(i - 6, -1), -1):
#                     m = re.match(r"^(\d{6})\b", lines[j])
#                     if m:
#                         item_number = m.group(1)
#                         break
#                 if item_number and discount_amount:
#                     current = (item_number, discount_amount)
#                     if current != prev_discount:
#                         discounts_by_item[item_number].append(discount_amount)
#                         prev_discount = current
#             if "discount-pack size" in line.lower():
#                 item_number = None
#                 for j in range(i - 1, max(i - 6, -1), -1):
#                     m = re.match(r"^(\d{6})\b", lines[j])
#                     if m:
#                         item_number = m.group(1)
#                         break
#                 for j in range(i + 1, min(i + 6, len(lines))):
#                     discount_line = lines[j].strip()
#                     m_disc = re.search(r"-([\d,]+\.\d{2})\s+N", discount_line)
#                     if m_disc and item_number:
#                         discount_value = float(m_disc.group(1).replace(",", ""))
#                         discounts_by_item[item_number].append(discount_value)
#                         break
#         return discounts_by_item

#     discounts_by_item = extract_discounts_from_ocr_lines(lines)

#     for line in lines:
#         m_invoice = re.search(r"Invoice No\.\s*(\d{8})", line, re.IGNORECASE)
#         if m_invoice:
#             vendor_invoice_no = m_invoice.group(1)
#             break
        
#     for line in lines:
#         m_po = re.search(r"Customer PO No\.\s*.*?(\d{5})", line, re.IGNORECASE)
#         if m_po:
#             po_number = f"PO-{m_po.group(1)}"
#             break

#     def flush_item():
#         nonlocal current
#         if "VendorBatchLot" in current:
#             line_items.append({
#                 "VendorInvoiceNo":       vendor_invoice_no,
#                 "PurchaseOrder":         po_number,
#                 "VendorItemNumber":      current.get("VendorItemNumber"),
#                 "VendorItemDescription": current.get("VendorItemDescription", "").strip(),
#                 "VendorBatchLot":        current.get("VendorBatchLot"),
#                 "VendorProductLot":      current.get("VendorProductLot"),
#                 "OriginCountry":         current.get("OriginCountry"),
#                 "TotalPrice":            current.get("TotalPrice"),
#                 "TotalUpcharge":         current.get("TotalUpcharge"),
#                 "TotalDiscount":         None,
#                 "TotalQuantity":         current.get("TotalQuantity"),
#                 "USD_Actual_Cost_$":     None,
#                 "ProductForm":           current.get("ProductForm"),
#                 "Treatment":             current.get("Treatment"),
#                 "SeedCount":             current.get("SeedCount"),
#                 "Purity":                current.get("Purity"),
#                 "SeedSize":              current.get("SeedSize"),
#                 "PureSeed":              None,
#                 "InertMatter":           None,
#                 "Germ":                  None,
#                 "GermDate":              None
#             })
#         current.clear()

#     for i, raw in enumerate(lines):
#         line = raw.strip()
        
#         m2 = re.match(r"^(\d{6})\s+(.+)$", line)
#         if m2:
#             flush_item()
#             current["VendorItemNumber"] = m2.group(1)
#             desc_part1 = m2.group(2).strip()
#             continue

#         m1 = re.fullmatch(r"\d{6}", line)
#         if m1:
#             flush_item()
#             current["VendorItemNumber"] = m1.group()
#             desc_part1 = ""
#             continue

#         if re.fullmatch(r"[A-Z]\d{5}", line):
#             if "VendorBatchLot" in current and "VendorItemNumber" in current:
#                 flush_item()
#             current["VendorBatchLot"] = line
#             continue

#         if "VendorItemNumber" in current and not desc_part1:
#             desc_part1 = line

#         if desc_part1 and line.startswith("Flc."):
#             part2 = re.sub(r"HM.*$", "", line.replace("Flc.", "")).strip()
#             current["VendorItemDescription"] = f"{desc_part1} {part2}"

#         if "VendorProductLot" not in current and (m_pl := re.search(r"\bPL\d{6}\b", line)):
#             current["VendorProductLot"] = m_pl.group()

#         if "OriginCountry" not in current and "Country of origin:" in line:
#             for o in (1,2):
#                 if i+o < len(lines) and re.fullmatch(r"[A-Z]{2}", lines[i+o].strip()):
#                     current["OriginCountry"] = lines[i+o].strip()
#                     break

#         if "ProductForm" not in current and (m_pf := re.search(r"Product Form:\s*(\w+)", line)):
#             current["ProductForm"] = m_pf.group(1)

#         if "Treatment" not in current and (m_tr := re.search(r"Treatment:\s*(.+)", line)):
#             current["Treatment"] = m_tr.group(1).strip()

#         if "SeedCount" not in current and (m_sc := re.search(r"Seed Count:\s*(\d+)", line)):
#             current["SeedCount"] = int(m_sc.group(1))

#         if "Purity" not in current and (m_pr := re.search(r"Purity:\s*(\d+\.\d+)", line)):
#             current["Purity"] = float(m_pr.group(1))

#         if "SeedSize" not in current and (m_sz := re.search(r"Seed Size:\s*([\w\.]+)", line)):
#             current["SeedSize"] = m_sz.group(1)
                
#         if "TotalPrice" not in current and (m_price := re.search(r"(?<!-)(\d[\d,]*\.\d{2})\s+N", line)):
#             current["TotalPrice"] = float(m_price.group(1).replace(",", ""))
        
#         if "TotalUpcharge" not in current:
#             m_upcharge = re.search(r"(\d[\d,]*\.\d{2})\s+Y", line)
#             if m_upcharge:
#                 current["TotalUpcharge"] = float(m_upcharge.group(1).replace(",", ""))
#             else:
#                 if i + 1 < len(lines) and lines[i + 1].strip() == "Y":
#                     if m_val := re.search(r"(\d[\d,]*\.\d{2})", line):
#                         current["TotalUpcharge"] = float(m_val.group(1).replace(",", ""))
#                 elif i > 0 and lines[i - 1].strip() == ">" and re.search(r"\d[\d,]*\.\d{2}", line):
#                     if m_val := re.search(r"\d[\d,]*\.\d{2}", line):
#                         current["TotalUpcharge"] = float(m_val.group(0).replace(",", ""))

#         if "TotalQuantity" not in current and (m_qty := re.search(r"(\d+)\s*KS\b", line)):
#             if (qty := int(m_qty.group(1))) > 0:
#                 current["TotalQuantity"] = qty

#     flush_item()
    
#     item_counter = defaultdict(int)
#     for item in line_items:
#         if not (item_num := item.get("VendorItemNumber")):
#             continue
#         occurrence_idx = item_counter[item_num]
#         item_counter[item_num] += 1
#         if occurrence_idx < len(discounts_by_item.get(item_num, [])):
#             item["TotalDiscount"] = discounts_by_item[item_num][occurrence_idx]
#         else:
#             item["TotalDiscount"] = None
    
#     for item in line_items:
#         tp = item.get("TotalPrice") or 0.0
#         tu = item.get("TotalUpcharge") or 0.0
#         td = item.get("TotalDiscount") or 0.0
#         qty = item.get("TotalQuantity")
#         item["USD_Actual_Cost_$"] = round(((tp + tu - td) / qty), 4) if qty and qty > 0 else None

#     return line_items

# # --- This function remains from the new code ---
# def extract_purity_analysis_reports_from_bytes(pdf_files: list[tuple[str, bytes]]) -> Dict[str, Dict]:
#     purity_data = defaultdict(dict)
#     for filename, pdf_bytes in pdf_files:
#         try:
#             doc = fitz.open(stream=pdf_bytes, filetype="pdf")
#             text = ""
#             for page in doc:
#                 if "REPORT OF SEED ANALYSIS" in page.get_text("text", flags=fitz.TEXT_INHIBIT_SPACES).upper():
#                     text += page.get_text() + " "
#             doc.close()

#             if not text:
#                 continue
            
#             text = text.replace('\n', ' ').replace('\r', ' ')
#             text = re.sub(r'\s{2,}', ' ', text)
            
#             if not (batch_match := re.search(r"\b([A-Z]\d{5,})\b", text)):
#                 continue
#             batch_key = batch_match.group(1)

#             if (match_pure := re.search(r"Pure Seed:\s*(\d+\.\d+)\s*%", text)) and \
#                (match_inert := re.search(r"Inert Matter:\s*(\d+\.\d+)\s*%", text)):
                
#                 pure_seed = float(match_pure.group(1))
#                 purity_data[batch_key]["PureSeed"] = 99.99 if pure_seed == 100 else pure_seed
#                 purity_data[batch_key]["InertMatter"] = 0.01 if pure_seed == 100 else float(match_inert.group(1))
                
#                 if date_matches := re.findall(r"(\d{1,2}/\d{1,2}/\d{4})(?=.*?REPORT OF SEED ANALYSIS)", text, re.IGNORECASE):
#                     if len(date_matches) >= 2:
#                         purity_data[batch_key]["GrowerGermDate"] = date_matches[-1]
#                         purity_data[batch_key]["GermDate"] = date_matches[-1]

#                 if match := re.search(r"%\s*Comments:\s*(?:[A-Za-z]+\s+)*(\d{2,3})\b", text):
#                     germ = int(float(match.group(1)))
#                     purity_data[batch_key]["GrowerGerm"] = germ
#                     purity_data[batch_key]["Germ"] = 98 if germ == 100 else germ
#         except Exception as e:
#             print(f"Could not process {filename} for purity analysis: {e}")
#             continue
#     return purity_data

# # --- This function remains from the new code ---
# def enrich_invoice_items_with_purity(items: List[Dict], purity_data: Dict[str, Dict]) -> List[Dict]:
#     for item in items:
#         if batch_lot := item.get("VendorBatchLot", ""):
#             key = batch_lot[:6]
#             if match := purity_data.get(key, {}):
#                 item.update(match)
#     return items

# # --- This is the original `extract_discounts` function, which is simpler and now restored ---
# def extract_discounts(blocks: List) -> List[Tuple[str, float]]:
#     discounts = []
#     prev_discount = None
    
#     for i, b in enumerate(blocks):
#         block_text = b[4].strip()
        
#         if "discount" in block_text.lower():
#             discount_amount = None
#             item_number = None

#             for j in range(i - 1, max(i - 6, -1), -1):
#                 prev_text = blocks[j][4].strip()
#                 for match in re.finditer(r"-[\d,]+\.\d{2}", prev_text):
#                     if "/KS" not in prev_text[match.start():match.end()+5]:
#                         discount_amount = abs(float(match.group().replace(",", "")))
#                         break
#                 if discount_amount:
#                     break

#             for j in range(i - 1, max(i - 6, -1), -1):
#                 prev_text = blocks[j][4].strip()
#                 if m := re.match(r"^(\d{6})\b", prev_text):
#                     item_number = m.group(1)
#                     break

#             if item_number and discount_amount:
#                 current_discount = (item_number, discount_amount)
#                 if current_discount != prev_discount:
#                     discounts.append(current_discount)
#                     prev_discount = current_discount
#     return discounts

# # ##################################################################################
# # ## --- REVERTED LOGIC STARTS HERE ---
# # ##################################################################################

# def extract_hm_clause_invoice_data_from_bytes(pdf_bytes: bytes) -> List[Dict]:
#     """
#     Extracts invoice data from in-memory PDF bytes using the ORIGINAL parsing logic.
#     """
#     item_usage_counter.clear()
#     doc = fitz.open(stream=pdf_bytes, filetype="pdf")
#     all_blocks = []
#     vendor_invoice_no = None
#     po_number = None

#     # Collect & sort blocks from pages that are not the warranty page
#     for page in doc:
#         if "limitation of warranty and liability" in page.get_text("text").lower():
#             continue
        
#         blocks = page.get_text("blocks")
#         # Fallback to OCR if page is blank or image-based
#         if not blocks or not any(b[4].strip() for b in blocks):
#             ocr_lines = extract_text_with_azure_ocr(pdf_bytes)
#             return extract_items_from_ocr_lines(ocr_lines)
            
#         all_blocks.extend(sorted(blocks, key=lambda b: (b[1], b[0])))
#     doc.close()
        
#     # Extract Invoice No.
#     for block in all_blocks:
#         text = block[4].strip()
#         if m_invoice := re.search(r"Invoice No\.\s*(\d{8})", text, re.IGNORECASE):
#             vendor_invoice_no = m_invoice.group(1)
#             break
        
#     # Extract Purchase Order No. using the original cumulative text method
#     text = ""
#     for block in all_blocks:
#         text += block[4].strip() + "\n"
#         m_po = re.search(r"Customer PO No\.\s*(\d+)", text, re.IGNORECASE)
#         if not m_po:
#             m_po = re.search(r"Customer PO No\.\s*.*?(\d{5})", text, re.IGNORECASE)
#         if m_po:
#             po_number = f"PO-{m_po.group(1)}"
#             break

#     # --- This section now uses the original logic from old_hm_clause.py.txt ---
#     discount_amounts = extract_discounts(all_blocks)
#     line_items = []
#     current_item_data = {}
#     desc_part1 = ""

#     def flush_item():
#         nonlocal current_item_data, desc_part1
#         if "VendorBatchLot" in current_item_data and "VendorItemNumber" in current_item_data:
#             line_items.append({
#                 "VendorInvoiceNo":       vendor_invoice_no,
#                 "PurchaseOrder":         po_number,
#                 "VendorItemNumber":      current_item_data.get("VendorItemNumber"),
#                 "VendorItemDescription": current_item_data.get("VendorItemDescription", "").strip() or desc_part1,
#                 "VendorBatchLot":        current_item_data.get("VendorBatchLot"),
#                 "VendorProductLot":      current_item_data.get("VendorProductLot"),
#                 "OriginCountry":         current_item_data.get("OriginCountry"),
#                 "TotalPrice":            current_item_data.get("TotalPrice"),
#                 "TotalUpcharge":         current_item_data.get("TotalUpcharge"),
#                 "TotalDiscount":         None,
#                 "TotalQuantity":         current_item_data.get("TotalQuantity"),
#                 "USD_Actual_Cost_$":     None,
#                 "ProductForm":           current_item_data.get("ProductForm"),
#                 "Treatment":             current_item_data.get("Treatment"),
#                 "SeedCount":             current_item_data.get("SeedCount"),
#                 "Purity":                current_item_data.get("Purity"),
#                 "SeedSize":              current_item_data.get("SeedSize"),
#                 "PureSeed":              None, "InertMatter": None,
#                 "Germ":                  None, "GermDate":    None
#             })
#         current_item_data = {}
#         desc_part1 = ""
        
#     def is_disqualified(block_idx: int, all_blocks: List) -> bool:
#         block_text = all_blocks[block_idx][4].strip().lower()
#         disqualifiers = ["cust no.", "cust no", "freight charges", "freight"]
#         if any(term in block_text for term in disqualifiers):
#             return True
#         nearby_texts = [
#             all_blocks[j][4].strip().lower()
#             for j in range(max(0, block_idx - 2), min(len(all_blocks), block_idx + 3))
#             if j != block_idx
#         ]
#         return any(any(term in text for term in disqualifiers) for text in nearby_texts)

#     # --- ORIGINAL PARSING LOOP ---
#     for idx, b in enumerate(all_blocks):
#         block_text = b[4].strip()

#         # Original logic: Batch lot is a primary trigger that flushes the previous item
#         if re.fullmatch(r"[A-Z]\d{5}", block_text):
#             if current_item_data:
#                 flush_item()
#             current_item_data["VendorBatchLot"] = block_text
#             continue

#         if not current_item_data:
#             continue
        
#         if m := re.match(r"^(\d{6})\s+(.+)", block_text):
#             if is_disqualified(idx, all_blocks): continue
#             current_item_data["VendorItemNumber"] = m.group(1)
#             desc_part1 = m.group(2).strip()
#             continue

#         if re.fullmatch(r"\d{6}", block_text):
#             if is_disqualified(idx, all_blocks): continue
#             current_item_data["VendorItemNumber"] = block_text
#             desc_part1 = ""
#             continue

#         if desc_part1 and block_text.startswith("Flc."):
#             part2 = re.sub(r"HM.*$", "", block_text.replace("Flc.", "")).strip()
#             current_item_data["VendorItemDescription"] = f"{desc_part1} {part2}"
#             continue

#         if "VendorItemDescription" not in current_item_data and desc_part1:
#             current_item_data["VendorItemDescription"] = desc_part1
            
#         if "TotalPrice" not in current_item_data and (m_price := re.search(r"(?<!-)(\d[\d,]*\.\d{2})\s+N", block_text)):
#             current_item_data["TotalPrice"] = float(m_price.group(1).replace(",", ""))
        
#         if "TotalUpcharge" not in current_item_data and (m_upcharge := re.search(r"(\d[\d,]*\.\d{2})\s+Y", block_text)):
#             current_item_data["TotalUpcharge"] = float(m_upcharge.group(1).replace(",", ""))

#         if "TotalQuantity" not in current_item_data and (m_qty := re.search(r"(\d+)\s*KS\b", block_text)):
#             if (qty := int(m_qty.group(1))) > 0: current_item_data["TotalQuantity"] = qty

#         if not current_item_data.get("VendorProductLot") and (m_pl := re.search(r"\bPL\d{6}\b", block_text)):
#             current_item_data["VendorProductLot"] = m_pl.group()
         
#         if m_oc := re.search(r"Country of origin:\s*([A-Z]{2})", block_text):
#             current_item_data["OriginCountry"] = m_oc.group(1)
            
#         if m_pf := re.search(r"Product Form:\s*(\w+)", block_text):
#             current_item_data["ProductForm"] = m_pf.group(1)
            
#         if m_tr := re.search(r"Treatment:\s*(.+)", block_text):
#             current_item_data["Treatment"] = m_tr.group(1).strip()
        
#         if m_sc := re.search(r"(?<!Approx\.\s)Seed Count:\s*(\d+)", block_text):
#             current_item_data["SeedCount"] = int(m_sc.group(1))

#         if m_pr := re.search(r"Purity:\s*(\d+\.\d+)", block_text):
#             current_item_data["Purity"] = float(m_pr.group(1))
            
#         if m_sz := re.search(r"Seed Size:\s*([\w\.]+)", block_text):
#             current_item_data["SeedSize"] = m_sz.group(1)

#     flush_item()
    
#     # Assign discounts and calculate final cost using original logic
#     item_counter = defaultdict(int)
#     discounts_by_item = defaultdict(list)
#     for item_num, amount in discount_amounts:
#         discounts_by_item[item_num].append(amount)

#     for item in line_items:
#         if item_num := item.get("VendorItemNumber"):
#             occurrence_idx = item_counter[item_num]
#             item_counter[item_num] += 1
            
#             if occurrence_idx < len(discounts_by_item.get(item_num, [])):
#                 item["TotalDiscount"] = discounts_by_item[item_num][occurrence_idx]
#             else:
#                 item["TotalDiscount"] = None

#             tp = item.get("TotalPrice", 0.0) or 0.0
#             tu = item.get("TotalUpcharge", 0.0) or 0.0
#             td = item.get("TotalDiscount", 0.0) or 0.0
#             qty = item.get("TotalQuantity") or 0
#             item["USD_Actual_Cost_$"] = round(((tp + tu - td) / qty), 4) if qty > 0 else None

#     return line_items

# # ##################################################################################
# # ## --- REVERTED LOGIC ENDS HERE ---
# # ##################################################################################


# # --- This is the NEW top-level function that processes batches in memory ---
# def extract_hm_clause_data_from_bytes(pdf_files: list[tuple[str, bytes]]) -> dict[str, list[dict]]:
#     """
#     Main entry point for extracting HM Clause data from a batch of in-memory PDF files.
#     """
#     if not pdf_files:
#         return {}

#     # 1. Extract purity data from ALL provided files first.
#     purity_data = extract_purity_analysis_reports_from_bytes(pdf_files)

#     # 2. Process each file, identifying main invoices and extracting their data.
#     grouped_results = {}
#     for filename, pdf_bytes in pdf_files:
#         # Skip files that are identified as seed analysis reports.
#         if re.match(r"^[A-Z]\d{5}", os.path.basename(filename), re.IGNORECASE):
#             continue

#         try:
#             # 3. Call the invoice extractor which now contains the ORIGINAL logic.
#             items = extract_hm_clause_invoice_data_from_bytes(pdf_bytes)
#             if items:
#                 # 4. Enrich the extracted items with the pre-compiled purity data.
#                 enriched_items = enrich_invoice_items_with_purity(items, purity_data)
#                 grouped_results[filename] = enriched_items
#         except Exception as e:
#             print(f"Error processing invoice {filename}: {e}")
#             continue

#     return grouped_results


# def find_best_hm_clause_package_description(vendor_desc: str, pkg_desc_list: list[str]) -> str:
#     """
#     (No changes needed here)
#     """
#     if not vendor_desc or not pkg_desc_list:
#         return ""

#     normalized_desc = vendor_desc.upper()
#     candidate = ""

#     m = re.search(r"(\d+)\s*(KS|MS)\b", normalized_desc)
#     if m:
#         qty = int(m.group(1))
#         unit = m.group(2)

#         if unit == "KS":
#             seed_count = qty * 1000
#             candidate = f"{seed_count:,} SEEDS"
#         elif unit == "MS":
#             seed_count = qty * 1000000
#             candidate = f"{seed_count:,} SEEDS"

#         if candidate in pkg_desc_list:
#             return candidate

#     matches = get_close_matches(normalized_desc, pkg_desc_list, n=1, cutoff=0.6)
#     return matches[0] if matches else ""


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

def extract_text_with_azure_ocr(pdf_bytes: bytes) -> List[str]:
    """
    Performs OCR on in-memory PDF bytes using Azure Form Recognizer.
    """
    headers = {
        "Ocp-Apim-Subscription-Key": AZURE_KEY,
        "Content-Type": "application/pdf"
    }
    # Post the byte stream directly
    response = requests.post(
        f"{AZURE_ENDPOINT}formrecognizer/documentModels/prebuilt-layout:analyze?api-version=2023-07-31",
        headers=headers,
        data=pdf_bytes
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
                page_text = " ".join(line.get("content", "").strip() for line in page.get("lines", []) if line.get("content"))
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
    line_items = []
    current = {}
    desc_part1 = ""
    vendor_invoice_no = None
    po_number = None
    
    def extract_discounts_from_ocr_lines(lines: List[str]) -> Dict[str, List[float]]:
        discounts_by_item = defaultdict(list)
        prev_discount = None

        for i, line in enumerate(lines):
            if "discount" in line.lower():
                discount_amount = None
                item_number = None
                for j in range(i - 1, max(i - 6, -1), -1):
                    m = re.search(r"-[\d,]+\.\d{2}", lines[j])
                    if m:
                        discount_amount = abs(float(m.group().replace(",", "")))
                        break
                for j in range(i - 1, max(i - 6, -1), -1):
                    m = re.match(r"^(\d{6})\b", lines[j])
                    if m:
                        item_number = m.group(1)
                        break
                if item_number and discount_amount:
                    current = (item_number, discount_amount)
                    if current != prev_discount:
                        discounts_by_item[item_number].append(discount_amount)
                        prev_discount = current
            if "discount-pack size" in line.lower():
                item_number = None
                for j in range(i - 1, max(i - 6, -1), -1):
                    m = re.match(r"^(\d{6})\b", lines[j])
                    if m:
                        item_number = m.group(1)
                        break
                for j in range(i + 1, min(i + 6, len(lines))):
                    discount_line = lines[j].strip()
                    m_disc = re.search(r"-([\d,]+\.\d{2})\s+N", discount_line)
                    if m_disc and item_number:
                        discount_value = float(m_disc.group(1).replace(",", ""))
                        discounts_by_item[item_number].append(discount_value)
                        break
        return discounts_by_item

    discounts_by_item = extract_discounts_from_ocr_lines(lines)

    for line in lines:
        m_invoice = re.search(r"Invoice No\.\s*(\d{8})", line, re.IGNORECASE)
        if m_invoice:
            vendor_invoice_no = m_invoice.group(1)
            break
        
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
                "TotalPrice":            current.get("TotalPrice"),
                "TotalUpcharge":         current.get("TotalUpcharge"),
                "TotalDiscount":         None,
                "TotalQuantity":         current.get("TotalQuantity"),
                "USD_Actual_Cost_$":     None,
                "ProductForm":           current.get("ProductForm"),
                "Treatment":             current.get("Treatment"),
                "SeedCount":             current.get("SeedCount"),
                "Purity":                current.get("Purity"),
                "SeedSize":              current.get("SeedSize"),
                "PureSeed":              None,
                "InertMatter":           None,
                "Germ":                  None,
                "GermDate":              None
            })
        current.clear()

    for i, raw in enumerate(lines):
        line = raw.strip()
        
        m2 = re.match(r"^(\d{6})\s+(.+)$", line)
        if m2:
            flush_item()
            current["VendorItemNumber"] = m2.group(1)
            desc_part1 = m2.group(2).strip()
            continue

        m1 = re.fullmatch(r"\d{6}", line)
        if m1:
            flush_item()
            current["VendorItemNumber"] = m1.group()
            desc_part1 = ""
            continue

        if re.fullmatch(r"[A-Z]\d{5}", line):
            if "VendorBatchLot" in current and "VendorItemNumber" in current:
                flush_item()
            current["VendorBatchLot"] = line
            continue

        if "VendorItemNumber" in current and not desc_part1:
            desc_part1 = line

        # if desc_part1 and line.startswith("Flc."):
        #     part2 = re.sub(r"HM.*$", "", line.replace("Flc.", "")).strip()
        #     current["VendorItemDescription"] = f"{desc_part1} {part2}"
            
        # if desc_part1 and re.search(r"\bFoil\s+\d+\s*(Ks|MS)\b", line, re.IGNORECASE):
        #     # Remove trailing HM codes or irrelevant text
        #     part2 = re.sub(r"\bHM.*$", "", line, flags=re.IGNORECASE).strip()
        #     # Remove any leading prefixes like "Flc." or "Plt.GCDI"
        #     part2 = re.sub(r"^(Flc\.|Plt\.\w+)\s*", "", part2, flags=re.IGNORECASE)
        #     current["VendorItemDescription"] = f"{desc_part1} {part2}"
        
        if desc_part1 and re.search(r"\b\d+\s*(Ks|MS)\b", line, re.IGNORECASE):
            # Remove trailing HM codes or irrelevant text
            part2 = re.sub(r"\bHM.*$", "", line, flags=re.IGNORECASE).strip()
            # Remove any leading prefixes like "Flc." or "Plt.GCDI"
            part2 = re.sub(r"^(Flc\.|Plt\.\w+)\s*", "", part2, flags=re.IGNORECASE)
            current["VendorItemDescription"] = f"{desc_part1} {part2}"

        if "VendorProductLot" not in current and (m_pl := re.search(r"\bPL\d{6}\b", line)):
            current["VendorProductLot"] = m_pl.group()

        if "OriginCountry" not in current and "Country of origin:" in line:
            for o in (1,2):
                if i+o < len(lines) and re.fullmatch(r"[A-Z]{2}", lines[i+o].strip()):
                    current["OriginCountry"] = lines[i+o].strip()
                    break

        if "ProductForm" not in current and (m_pf := re.search(r"Product Form:\s*(\w+)", line)):
            current["ProductForm"] = m_pf.group(1)

        if "Treatment" not in current and (m_tr := re.search(r"Treatment:\s*(.+)", line)):
            current["Treatment"] = m_tr.group(1).strip()

        if "SeedCount" not in current and (m_sc := re.search(r"Seed Count:\s*(\d+)", line)):
            current["SeedCount"] = int(m_sc.group(1))

        if "Purity" not in current and (m_pr := re.search(r"Purity:\s*(\d+\.\d+)", line)):
            current["Purity"] = float(m_pr.group(1))

        if "SeedSize" not in current and (m_sz := re.search(r"Seed Size:\s*([\w\.]+)", line)):
            current["SeedSize"] = m_sz.group(1)
                
        if "TotalPrice" not in current and (m_price := re.search(r"(?<!-)(\d[\d,]*\.\d{2})\s+N", line)):
            current["TotalPrice"] = float(m_price.group(1).replace(",", ""))
        
        if "TotalUpcharge" not in current:
            m_upcharge = re.search(r"(\d[\d,]*\.\d{2})\s+Y", line)
            if m_upcharge:
                current["TotalUpcharge"] = float(m_upcharge.group(1).replace(",", ""))
            else:
                if i + 1 < len(lines) and lines[i + 1].strip() == "Y":
                    if m_val := re.search(r"(\d[\d,]*\.\d{2})", line):
                        current["TotalUpcharge"] = float(m_val.group(1).replace(",", ""))
                elif i > 0 and lines[i - 1].strip() == ">" and re.search(r"\d[\d,]*\.\d{2}", line):
                    if m_val := re.search(r"\d[\d,]*\.\d{2}", line):
                        current["TotalUpcharge"] = float(m_val.group(0).replace(",", ""))

        if "TotalQuantity" not in current and (m_qty := re.search(r"(\d+)\s*KS\b", line)):
            if (qty := int(m_qty.group(1))) > 0:
                current["TotalQuantity"] = qty

    flush_item()
    
    item_counter = defaultdict(int)
    for item in line_items:
        if not (item_num := item.get("VendorItemNumber")):
            continue
        occurrence_idx = item_counter[item_num]
        item_counter[item_num] += 1
        if occurrence_idx < len(discounts_by_item.get(item_num, [])):
            item["TotalDiscount"] = discounts_by_item[item_num][occurrence_idx]
        else:
            item["TotalDiscount"] = None
    
    for item in line_items:
        tp = item.get("TotalPrice") or 0.0
        tu = item.get("TotalUpcharge") or 0.0
        td = item.get("TotalDiscount") or 0.0
        qty = item.get("TotalQuantity")
        item["USD_Actual_Cost_$"] = round(((tp + tu - td) / qty), 4) if qty and qty > 0 else None

    return line_items

def _choose_batch_key(report_text_upper: str, filename: str) -> str | None:
    # 1) Prefer explicit labels in the report
    m = re.search(r"(?:LOT\s*#|BATCH\s*#)\s*[:\-]?\s*([A-Z]\d{5,})", report_text_upper, re.IGNORECASE)
    if m:
        return m.group(1)[:6].upper()  # normalize to 6-char key

    # 2) Try filename starting token
    m = re.match(r"([A-Z]\d{5,})", os.path.basename(filename).upper())
    if m:
        return m.group(1)[:6].upper()

    # 3) As a last resort, use the LAST code-looking token (true lot is often last)
    codes = re.findall(r"\b([A-Z]\d{5,})\b", report_text_upper)
    if codes:
        return codes[-1][:6].upper()

    return None

def extract_purity_analysis_reports_from_bytes(pdf_files: list[tuple[str, bytes]]) -> Dict[str, Dict]:
    """
    Extracts purity data. It now correctly uses OCR as a fallback for scanned reports.
    """
    purity_data = defaultdict(dict)
    for filename, pdf_bytes in pdf_files:
        try:
            doc = fitz.open(stream=pdf_bytes, filetype="pdf")
            text = ""
            is_report_document = False
            
            # Pass 1: Get text with Fitz and check if it's a report
            for page in doc:
                page_text = page.get_text()
                if "REPORT" in page_text.upper():
                    is_report_document = True
                    print(f"{filename} as a seed analysis report:")
                    print(page_text)
                text += page_text + " " # Aggregate all text
            doc.close()

            # If the document is not a seed analysis report, skip it
            if not is_report_document:
                continue

            # If it is a report but has no text, it must be scanned. Use OCR.
            if not text.strip():
                print(f"INFO: Identified {filename} as a scanned seed analysis report. Running OCR.")
                ocr_lines = extract_text_with_azure_ocr(pdf_bytes)
                text = " ".join(ocr_lines) # Replace empty text with OCR result

            # If there's still no text, we can't process it.
            if not text.strip():
                continue
            
            # # Proceed with the proven regex parsing
            # text = text.replace('\n', ' ').replace('\r', ' ')
            # text = re.sub(r'\s{2,}', ' ', text)
            
            # if not (batch_match := re.search(r"\b([A-Z]\d{5,})\b", text)):
            #     continue
            # batch_key = batch_match.group(1)
            
            # # Truncate the key to 6 chars to match the invoice batch lot key
            # batch_key = batch_match.group(1)[:6]
            
            # new
            U = text.upper()
            batch_key = _choose_batch_key(U, filename)
            if not batch_key:
                continue

            if (match_pure := re.search(r"Pure Seed:\s*(\d+\.\d+)\s*%", text)) and \
               (match_inert := re.search(r"Inert Matter:\s*(\d+\.\d+)\s*%", text)):
                
                pure_seed = float(match_pure.group(1))
                purity_data[batch_key]["PureSeed"] = 99.99 if pure_seed == 100 else pure_seed
                purity_data[batch_key]["InertMatter"] = 0.01 if pure_seed == 100 else float(match_inert.group(1))
                
                # if date_matches := re.findall(r"(\d{1,2}/\d{1,2}/\d{4})(?=.*?REPORT OF SEED ANALYSIS)", text, re.IGNORECASE):
                #     if len(date_matches) >= 2:
                #         purity_data[batch_key]["GrowerGermDate"] = date_matches[-1]
                #         purity_data[batch_key]["GermDate"] = date_matches[-1]

                if match := re.search(r"%\s*Comments:\s*(?:[A-Za-z]+\s+)*(\d{2,3})\b", text):
                    germ = int(float(match.group(1)))
                    purity_data[batch_key]["GrowerGerm"] = germ
                    purity_data[batch_key]["Germ"] = 98 if germ == 100 else germ
                    
            germ_date = _extract_germ_date_from_report(text)
            if germ_date:
                purity_data[batch_key]["GrowerGermDate"] = germ_date
                purity_data[batch_key]["GermDate"] = germ_date
                    
        except Exception as e:
            print(f"Could not process {filename} for purity analysis: {e}")
            continue
    return purity_data

# def enrich_invoice_items_with_purity(items: List[Dict], purity_data: Dict[str, Dict]) -> List[Dict]:
#     for item in items:
#         if batch_lot := item.get("VendorBatchLot", ""):
#             key = batch_lot[:6]
#             if match := purity_data.get(key, {}):
#                 item.update(match)
#     return items

def _normalize_mdy(s: str) -> str:
    m, d, y = re.match(r"(\d{1,2})[/-](\d{1,2})[/-](\d{2,4})", s).groups()
    y = int(y)
    if y < 100:  # expand 2-digit year
        y += 2000 if y < 50 else 1900
    return f"{int(m)}/{int(d)}/{y}"

def _extract_germ_date_from_report(txt: str) -> str | None:
    """
    Prefer 'Test Date' value even if it's on the next line/column.
    Falls back to 'Germination Date', then proximity to the 'GERMINATION ANALYSIS' block.
    Returns normalized M/D/YYYY or None.
    """
    # Normalize whitespace for easy regex matching
    flat = re.sub(r"\s+", " ", txt)

    def _norm(mdy: str) -> str:
        return _normalize_mdy(mdy)

    # 1) Try direct 'Test Date' with anything (non-digits) between label and the date
    m = re.search(r"Test\s*Date[^0-9]{0,80}([0-9]{1,2}[/-][0-9]{1,2}[/-][0-9]{2,4})", flat, re.IGNORECASE)
    if m:
        print("1- Test date extracted")
        return _norm(m.group(1))

    # 2) Try 'Germination Date'
    m = re.search(r"Germ(?:ination)?\s*Date[^0-9]{0,80}([0-9]{1,2}[/-][0-9]{1,2}[/-][0-9]{2,4})", flat, re.IGNORECASE)
    if m:
        print("2 - Germination date extracted")
        return _norm(m.group(1))

    # 3) Proximity heuristic in the ORIGINAL (non-flattened) text:
    #    Find the first date that appears AFTER the 'Test Date' label, within the next ~400 characters
    lbl = re.search(r"Test\s*Date", txt, re.IGNORECASE)
    if lbl:
        window = txt[lbl.end(): lbl.end() + 400]
        m = re.search(r"([0-9]{1,2}[/-][0-9]{1,2}[/-][0-9]{2,4})", window)
        if m:
            print("3 - Test date extracted")
            return _norm(m.group(1))

    # 4) Fallback: pick a date near 'GERMINATION ANALYSIS'
    gpos = re.search(r"GERMINATION\s+ANALYSIS", txt, re.IGNORECASE)
    if gpos:
        window = txt[max(0, gpos.start()-250): gpos.end()+250]
        cands = re.findall(r"([0-9]{1,2}[/-][0-9]{1,2}[/-][0-9]{2,4})", window)
        if cands:
            print("4 - Test date extracted")
            return _norm(cands[0])

    return None

def enrich_invoice_items_with_purity(items: List[Dict], purity_data: Dict[str, Dict]) -> List[Dict]:
    for item in items:
        lot = (item.get("VendorBatchLot") or "").upper()
        if not lot:
            continue

        # 1) exact match (full lot, e.g., "K35088A1" if your reports ever carry a suffix)
        data = purity_data.get(lot)

        # 2) fallback to 6-char canonical key (e.g., "K35088")
        if data is None and len(lot) >= 6:
            data = purity_data.get(lot[:6])

        if data:
            item.update(data)
    return items

# --- This is the original `extract_discounts` function, which is simpler and now restored ---
def extract_discounts(blocks: List) -> List[Tuple[str, float]]:
    discounts = []
    prev_discount = None
    
    for i, b in enumerate(blocks):
        block_text = b[4].strip()
        
        if "discount" in block_text.lower():
            discount_amount = None
            item_number = None

            for j in range(i - 1, max(i - 6, -1), -1):
                prev_text = blocks[j][4].strip()
                for match in re.finditer(r"-[\d,]+\.\d{2}", prev_text):
                    if "/KS" not in prev_text[match.start():match.end()+5]:
                        discount_amount = abs(float(match.group().replace(",", "")))
                        break
                if discount_amount:
                    break

            for j in range(i - 1, max(i - 6, -1), -1):
                prev_text = blocks[j][4].strip()
                if m := re.match(r"^(\d{6})\b", prev_text):
                    item_number = m.group(1)
                    break

            if item_number and discount_amount:
                current_discount = (item_number, discount_amount)
                if current_discount != prev_discount:
                    discounts.append(current_discount)
                    prev_discount = current_discount
    return discounts

def extract_hm_clause_invoice_data_from_bytes(pdf_bytes: bytes) -> List[Dict]:
    """
    Extracts invoice data from in-memory PDF bytes using the ORIGINAL parsing logic.
    """
    item_usage_counter.clear()
    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    all_blocks = []
    vendor_invoice_no = None
    po_number = None

    # Collect & sort blocks from pages that are not the warranty page
    for page in doc:
        if "limitation of warranty and liability" in page.get_text("text").lower():
            continue
        
        blocks = page.get_text("blocks")
        # Fallback to OCR if page is blank or image-based
        if not blocks or not any(b[4].strip() for b in blocks):
            ocr_lines = extract_text_with_azure_ocr(pdf_bytes)
            return extract_items_from_ocr_lines(ocr_lines)
            
        all_blocks.extend(sorted(blocks, key=lambda b: (b[1], b[0])))
    doc.close()
        
    # Extract Invoice No.
    for block in all_blocks:
        text = block[4].strip()
        if m_invoice := re.search(r"Invoice No\.\s*(\d{8})", text, re.IGNORECASE):
            vendor_invoice_no = m_invoice.group(1)
            break
        
    # Extract Purchase Order No. using the original cumulative text method
    text = ""
    for block in all_blocks:
        text += block[4].strip() + "\n"
        m_po = re.search(r"Customer PO No\.\s*(\d+)", text, re.IGNORECASE)
        if not m_po:
            m_po = re.search(r"Customer PO No\.\s*.*?(\d{5})", text, re.IGNORECASE)
        if m_po:
            po_number = f"PO-{m_po.group(1)}"
            break

    discount_amounts = extract_discounts(all_blocks)
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
                "TotalDiscount":         None,
                "TotalQuantity":         current_item_data.get("TotalQuantity"),
                "USD_Actual_Cost_$":     None,
                "ProductForm":           current_item_data.get("ProductForm"),
                "Treatment":             current_item_data.get("Treatment"),
                "SeedCount":             current_item_data.get("SeedCount"),
                "Purity":                current_item_data.get("Purity"),
                "SeedSize":              current_item_data.get("SeedSize"),
                "PureSeed":              None, "InertMatter": None,
                "Germ":                  None, "GermDate":    None
            })
        current_item_data = {}
        desc_part1 = ""
        
    def is_disqualified(block_idx: int, all_blocks: List) -> bool:
        block_text = all_blocks[block_idx][4].strip().lower()
        disqualifiers = ["cust no.", "cust no", "freight charges", "freight"]
        if any(term in block_text for term in disqualifiers):
            return True
        nearby_texts = [
            all_blocks[j][4].strip().lower()
            for j in range(max(0, block_idx - 2), min(len(all_blocks), block_idx + 3))
            if j != block_idx
        ]
        return any(any(term in text for term in disqualifiers) for text in nearby_texts)

    # --- ORIGINAL PARSING LOOP ---
    for idx, b in enumerate(all_blocks):
        block_text = b[4].strip()
        
        print(block_text)

        # Original logic: Batch lot is a primary trigger that flushes the previous item
        if re.fullmatch(r"[A-Z]\d{5}", block_text):
            if current_item_data:
                flush_item()
            current_item_data["VendorBatchLot"] = block_text
            continue

        if not current_item_data:
            continue
        
        if m := re.match(r"^(\d{6})\s+(.+)", block_text):
            if is_disqualified(idx, all_blocks): continue
            current_item_data["VendorItemNumber"] = m.group(1)
            desc_part1 = m.group(2).strip()
            continue

        if re.fullmatch(r"\d{6}", block_text):
            if is_disqualified(idx, all_blocks): continue
            current_item_data["VendorItemNumber"] = block_text
            desc_part1 = ""
            continue

        # if desc_part1 and block_text.startswith("Flc."):
        #     part2 = re.sub(r"HM.*$", "", block_text.replace("Flc.", "")).strip()
        #     current_item_data["VendorItemDescription"] = f"{desc_part1} {part2}"
        #     continue
        
        # if desc_part1 and re.search(r"\bFoil\s+\d+\s*(Ks|MS)\b", block_text, re.IGNORECASE):
        #     # Remove trailing HM codes or irrelevant text
        #     part2 = re.sub(r"\bHM.*$", "", block_text, flags=re.IGNORECASE).strip()
        #     # Remove any leading prefixes like "Flc." or "Plt.GCDI"
        #     part2 = re.sub(r"^(Flc\.|Plt\.\w+)\s*", "", part2, flags=re.IGNORECASE)
        #     current_item_data["VendorItemDescription"] = f"{desc_part1} {part2}"
        #     continue
        
        if desc_part1 and re.search(r"\b\d+\s*(Ks|MS)\b", block_text, re.IGNORECASE):
            # Remove trailing HM codes or irrelevant text
            part2 = re.sub(r"\bHM.*$", "", block_text, flags=re.IGNORECASE).strip()
            # Remove any leading prefixes like "Flc." or "Plt.GCDI"
            part2 = re.sub(r"^(Flc\.|Plt\.\w+)\s*", "", part2, flags=re.IGNORECASE)
            current_item_data["VendorItemDescription"] = f"{desc_part1} {part2}"
            continue

        if "VendorItemDescription" not in current_item_data and desc_part1:
            current_item_data["VendorItemDescription"] = desc_part1
            
        # if "TotalPrice" not in current_item_data and (m_price := re.search(r"(?<!-)(\d[\d,]*\.\d{2})\s+N", block_text)):
        #     current_item_data["TotalPrice"] = float(m_price.group(1).replace(",", ""))
        
        # if "TotalUpcharge" not in current_item_data and (m_upcharge := re.search(r"(\d[\d,]*\.\d{2})\s+Y", block_text)):
        #     current_item_data["TotalUpcharge"] = float(m_upcharge.group(1).replace(",", ""))
        
        if "TotalPrice" not in current_item_data and "TotalUpcharge" not in current_item_data:
            # Case 1: Price and "N" are in the same block
            if m_price := re.search(r"(?<!-)(\d[\d,]*\.\d{2})\s+N", block_text):
                current_item_data["TotalPrice"] = float(m_price.group(1).replace(",", ""))
            
            # Case 2: Price and "Y" are in the same block
            elif m_upcharge := re.search(r"(\d[\d,]*\.\d{2})\s+Y", block_text):
                current_item_data["TotalUpcharge"] = float(m_upcharge.group(1).replace(",", ""))
            
            # Case 3: Price is in this block, "Y" is in the *next* block
            elif (idx + 1 < len(all_blocks) and all_blocks[idx + 1][4].strip() == "Y"):
                if m_val := re.search(r"(\d[\d,]*\.\d{2})", block_text):
                    current_item_data["TotalUpcharge"] = float(m_val.group(1).replace(",", ""))
            
            # Case 4: Price is in this block, "N" is in the *next* block
            elif (idx + 1 < len(all_blocks) and all_blocks[idx + 1][4].strip() == "N"):
                if m_val := re.search(r"(\d[\d,]*\.\d{2})", block_text):
                    current_item_data["TotalPrice"] = float(m_val.group(1).replace(",", ""))

        if "TotalQuantity" not in current_item_data and (m_qty := re.search(r"(\d+)\s*KS\b", block_text)):
            if (qty := int(m_qty.group(1))) > 0: current_item_data["TotalQuantity"] = qty

        if not current_item_data.get("VendorProductLot") and (m_pl := re.search(r"\bPL\d{6}\b", block_text)):
            current_item_data["VendorProductLot"] = m_pl.group()
         
        if m_oc := re.search(r"Country of origin:\s*([A-Z]{2})", block_text):
            current_item_data["OriginCountry"] = m_oc.group(1)
            
        if m_pf := re.search(r"Product Form:\s*(\w+)", block_text):
            current_item_data["ProductForm"] = m_pf.group(1)
            
        if m_tr := re.search(r"Treatment:\s*(.+)", block_text):
            current_item_data["Treatment"] = m_tr.group(1).strip()
        
        if m_sc := re.search(r"(?<!Approx\.\s)Seed Count:\s*(\d+)", block_text):
            current_item_data["SeedCount"] = int(m_sc.group(1))

        if m_pr := re.search(r"Purity:\s*(\d+\.\d+)", block_text):
            current_item_data["Purity"] = float(m_pr.group(1))
            
        if m_sz := re.search(r"Seed Size:\s*([\w\.]+)", block_text):
            current_item_data["SeedSize"] = m_sz.group(1)

    flush_item()
    
    # Assign discounts and calculate final cost using original logic
    item_counter = defaultdict(int)
    discounts_by_item = defaultdict(list)
    for item_num, amount in discount_amounts:
        discounts_by_item[item_num].append(amount)

    for item in line_items:
        if item_num := item.get("VendorItemNumber"):
            occurrence_idx = item_counter[item_num]
            item_counter[item_num] += 1
            
            if occurrence_idx < len(discounts_by_item.get(item_num, [])):
                item["TotalDiscount"] = discounts_by_item[item_num][occurrence_idx]
            else:
                item["TotalDiscount"] = None

            tp = item.get("TotalPrice", 0.0) or 0.0
            tu = item.get("TotalUpcharge", 0.0) or 0.0
            td = item.get("TotalDiscount", 0.0) or 0.0
            qty = item.get("TotalQuantity") or 0
            item["USD_Actual_Cost_$"] = round(((tp + tu - td) / qty), 4) if qty > 0 else None

    return line_items

# --- This is the NEW top-level function that processes batches in memory ---
def extract_hm_clause_data_from_bytes(pdf_files: list[tuple[str, bytes]]) -> dict[str, list[dict]]:
    """
    Main entry point for extracting HM Clause data from a batch of in-memory PDF files.
    """
    if not pdf_files:
        return {}

    # 1. Extract purity data from ALL provided files first.
    purity_data = extract_purity_analysis_reports_from_bytes(pdf_files)

    # 2. Process each file, identifying main invoices and extracting their data.
    grouped_results = {}
    for filename, pdf_bytes in pdf_files:
        # Skip files that are identified as seed analysis reports.
        if re.match(r"^[A-Z]\d{5}", os.path.basename(filename), re.IGNORECASE):
            continue

        try:
            # 3. Call the invoice extractor which now contains the ORIGINAL logic.
            items = extract_hm_clause_invoice_data_from_bytes(pdf_bytes)
            if items:
                # 4. Enrich the extracted items with the pre-compiled purity data.
                enriched_items = enrich_invoice_items_with_purity(items, purity_data)
                grouped_results[filename] = enriched_items
        except Exception as e:
            print(f"Error processing invoice {filename}: {e}")
            continue

    return grouped_results


def find_best_hm_clause_package_description(vendor_desc: str, pkg_desc_list: list[str]) -> str:
    """
    (No changes needed here)
    """
    if not vendor_desc or not pkg_desc_list:
        return ""

    normalized_desc = vendor_desc.upper()
    candidate = ""

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

        if candidate in pkg_desc_list:
            return candidate

    matches = get_close_matches(normalized_desc, pkg_desc_list, n=1, cutoff=0.6)
    return matches[0] if matches else ""