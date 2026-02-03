# import fitz  # PyMuPDF
# import re
# from db_logger import log_processing_event

# def parse_currency(value_str):
#     """Cleans '$1,234.56' -> 1234.56"""
#     if not value_str:
#         return 0.0
#     clean = re.sub(r"[^\d\.-]", "", value_str)
#     try:
#         return float(clean)
#     except ValueError:
#         return 0.0

# def extract_kamterter_data_from_bytes(pdf_files: list[tuple[str, bytes]]) -> dict[str, list[dict]]:
#     grouped_results = {}

#     for filename, pdf_bytes in pdf_files:
#         doc = fitz.open(stream=pdf_bytes, filetype="pdf")
        
#         # Standard extraction to inspect raw stream order
#         full_text = "".join([page.get_text() for page in doc])
        
#         # # 'sort=True' forces reading order by vertical position, fixing the "shift"
#         # full_text = "".join([page.get_text("text", sort=True) for page in doc])
        
#         page_count = doc.page_count
#         doc.close()

#         # --- DEBUG LOGGING START ---
#         print(f"\n{'='*50}")
#         print(f"📄 DEBUG DUMP: {filename}")
#         print(f"{'='*50}")
#         print(full_text)
#         print(f"{'='*50}\n")
#         # --- DEBUG LOGGING END ---

#         # --- 1. Global Metadata ---
#         invoice_no = None
#         if m := re.search(r"Invoice\s*(?:#|No\.?)[:\s]*(\d+)", full_text, re.IGNORECASE):
#             invoice_no = m.group(1)

#         doc_date = None
#         if m := re.search(r"Invoiced\s*Date[:\s]*(\d{1,2}/\d{1,2}/\d{4})", full_text, re.IGNORECASE):
#             doc_date = m.group(1)

#         # --- 2. Extract Grand Total ---
#         grand_total = 0.0
#         if m_total := re.search(r"(\$[\d,]+\.\d{2})\s*\n\s*Total:", full_text, re.IGNORECASE):
#             grand_total = parse_currency(m_total.group(1))
#         elif m_total_std := re.search(r"Total\s*:.*?(\$[\d,]+\.\d{2})", full_text, re.IGNORECASE | re.DOTALL):
#              all_totals = re.findall(r"Total\s*:.*?(\$[\d,]+\.\d{2})", full_text, re.IGNORECASE | re.DOTALL)
#              if all_totals:
#                  grand_total = parse_currency(all_totals[-1])

#         # --- 3. Block Processing ---
#         # IMPROVED SPLIT: Look for "KTT" followed by optional punctuation, space, and a DIGIT.
#         # This ensures we catch "KTT #: 123" and "KTT 123" but avoid "KTT Products".
#         ktt_blocks = re.split(r"(?=KTT\s*[:#]*\s*\d)", full_text)
        
#         resource_lines = []
#         processed_line_total_sum = 0.0
#         numeric_po_found = False
        
#         for i, block in enumerate(ktt_blocks):
#             if "KTT" not in block:
#                 continue

#             # --- DEBUG BLOCK CONTENT ---
#             print(f"--- [DEBUG] Processing Block {i} ---")
#             print(f"Preview: {block[:100].replace(chr(10), ' ')}...") # Print first 100 chars
            
#             # Extract PO
#             po_match = re.search(r"PO\s*(?:#)?[:\s]*([^\n]+)", block, re.IGNORECASE)
#             po_raw = po_match.group(1).strip() if po_match else "UNKNOWN"
            
#             print(f"   -> Found PO Raw: '{po_raw}'")

#             # Numeric PO Check
#             if re.match(r"^\d+$", po_raw):
#                 print(f"   -> SKIPPING: Detected Numeric PO")
#                 numeric_po_found = True
#                 continue 

#             # G/L Logic Check
#             if re.match(r"\d{1,2}/\d{1,2}/\d{4}", po_raw) or "left unprocessed" in block.lower():
#                 print(f"   -> SKIPPING: Detected Date/G/L PO")
#                 continue

#             # --- Item Logic (FIXED REGEX) ---
#             seed_type = "Unknown"
#             # Look for Seed Type label, consume colon/newlines ([\s:]*), then capture the first non-empty line
#             if m_seed := re.search(r"Seed\s*Type[\s:]*(\S[^\n]*)", block, re.IGNORECASE):
#                 seed_type = m_seed.group(1).strip()
            
#             print(f"   -> Found Seed Type: '{seed_type}'")

#             # Quantity
#             quantity = 0.0
#             if m_qty := re.search(r"Shipped\s*Weight[:\s]*([\d,]+\.\d{2})", block, re.IGNORECASE):
#                 quantity = parse_currency(m_qty.group(1))

#             # --- FINANCIALS ---
#             subtotal = 0.0
#             if m_sub_rev := re.search(r"(\$[\d,]+\.\d{2})\s*\n\s*Subtotal:", block, re.IGNORECASE):
#                 subtotal = parse_currency(m_sub_rev.group(1))
#             elif m_sub_fwd := re.search(r"Subtotal[:\s]*.*?(\$[\d,]+\.\d{2})", block, re.IGNORECASE | re.DOTALL):
#                 subtotal = parse_currency(m_sub_fwd.group(1))
#             elif not subtotal:
#                 prices = re.findall(r"\$([\d,]+\.\d{2})", block)
#                 if prices:
#                     subtotal = parse_currency(prices[-1])

#             freight = 0.0
#             if m_frt_rev := re.search(r"(\$[\d,]+\.\d{2})\s*\n\s*Freight:", block, re.IGNORECASE):
#                 freight = parse_currency(m_frt_rev.group(1))
#             elif m_frt_fwd := re.search(r"Freight:\s*.*?(\$[\d,]+\.\d{2})", block, re.IGNORECASE | re.DOTALL):
#                  freight = parse_currency(m_frt_fwd.group(1))

#             adjusted_subtotal = subtotal
#             if freight > 0 and adjusted_subtotal > freight:
#                 adjusted_subtotal = adjusted_subtotal - freight

#             unit_cost = 0.0
#             if quantity > 0:
#                 unit_cost = adjusted_subtotal / quantity

#             # Item No
#             item_no = po_raw
#             if "-" in po_raw:
#                 sub_m = re.search(r"^\d+-(.+)", po_raw)
#                 if sub_m:
#                     item_no = sub_m.group(1).strip()
#                 else:
#                     item_no = po_raw

#             description = f"INV{invoice_no}_{seed_type}_{po_raw}"
#             line_amount = round(quantity * round(unit_cost, 5), 2)
#             processed_line_total_sum += line_amount

#             resource_lines.append({
#                 "Type": "Resource",
#                 "No": item_no,
#                 "Description": description,
#                 "Quantity": quantity,
#                 "DirectUnitCost": round(unit_cost, 5),
#                 "LineAmount": line_amount
#             })

#         # --- 4. Balancing G/L Line ---
#         gl_amount = grand_total - processed_line_total_sum
        
#         if resource_lines and abs(gl_amount) >= 0.01:
#             resource_lines.append({
#                 "Type": "G/L Account",
#                 "No": "609100",
#                 "Description": f"INV{invoice_no}_Unprocessed_WOSplit_Shipping",
#                 "Quantity": 1,
#                 "DirectUnitCost": round(gl_amount, 2),
#                 "LineAmount": round(gl_amount, 2)
#             })
        
#         # --- 5. US PO Warning ---
#         if not resource_lines and numeric_po_found:
#              resource_lines.append({
#                 "Type": "NOTE",
#                 "No": "",
#                 "Description": "The detected PO# Lines for this invoice belong to the US.",
#                 "Quantity": 0,
#                 "DirectUnitCost": 0,
#                 "LineAmount": 0,
#                 "IsUSWarning": True
#             })

#         # --- 6. Logging & Final Return ---
#         log_processing_event(
#             vendor='Kamterter',
#             filename=filename,
#             extraction_info={'method': 'PyMuPDF', 'page_count': page_count},
#             po_number=None
#         )

#         if resource_lines:
#             for line in resource_lines:
#                 line["VendorInvoiceNo"] = invoice_no
#                 line["DocumentDate"] = doc_date
#                 line["BuyFromVendorName"] = "KAMTERTER II, LLC"
            
#             grouped_results[filename] = resource_lines

#     return grouped_results

import fitz  # PyMuPDF
import re
from db_logger import log_processing_event

def parse_currency(value_str):
    """Cleans '$1,234.56' -> 1234.56"""
    if not value_str:
        return 0.0
    clean = re.sub(r"[^\d\.-]", "", value_str)
    try:
        return float(clean)
    except ValueError:
        return 0.0

def extract_kamterter_data_from_bytes(pdf_files: list[tuple[str, bytes]]) -> dict[str, list[dict]]:
    grouped_results = {}

    for filename, pdf_bytes in pdf_files:
        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
        
        # Standard extraction (per your request, no sort=True)
        full_text = "".join([page.get_text() for page in doc])
        
        page_count = doc.page_count
        doc.close()

        # --- 1. Global Metadata ---
        invoice_no = None
        if m := re.search(r"Invoice\s*(?:#|No\.?)[:\s]*(\d+)", full_text, re.IGNORECASE):
            invoice_no = m.group(1)

        doc_date = None
        if m := re.search(r"Invoiced\s*Date[:\s]*(\d{1,2}/\d{1,2}/\d{4})", full_text, re.IGNORECASE):
            doc_date = m.group(1)

        # --- 2. Extract Grand Total ---
        grand_total = 0.0
        # Strict: Look for 'Total' followed by price (ignoring 'Work Order Split' above it)
        all_totals = re.findall(r"\bTotal\s*[:\n]+\s*(\$[\d,]+\.\d{2})", full_text, re.IGNORECASE)
        if all_totals:
            grand_total = parse_currency(all_totals[-1])
        elif m_total_std := re.search(r"Total\s*:.*?(\$[\d,]+\.\d{2})", full_text, re.IGNORECASE | re.DOTALL):
             grand_total = parse_currency(m_total_std.group(1))

        # --- 3. Global Seed Type Extraction (Fix for Shifted Data) ---
        # Instead of finding Seed Type inside the split block (which might be shifted),
        # we find ALL seed types in the document and map them sequentially to the blocks.
        raw_seed_matches = re.findall(r"Seed\s*Type[\s:]*([^\n]*)", full_text, re.IGNORECASE)
        all_seed_types = []
        for raw in raw_seed_matches:
            # Cleanup: Remove "Lot #" if it appears on the same line
            clean_seed = re.split(r"Lot\s*#", raw, flags=re.IGNORECASE)[0].strip()
            all_seed_types.append(clean_seed)

        # --- 4. Block Processing ---
        ktt_blocks = re.split(r"(?=KTT\s*[:#]*\s*\d)", full_text)
        
        resource_lines = []
        processed_line_total_sum = 0.0
        skipped_numeric_amount = 0.0 
        numeric_po_found = False
        
        # We need a separate counter for KTT blocks to map the Seed Types correctly
        # The first split element is often the preamble (header), so we skip it if it has no KTT
        ktt_block_index = 0 

        for block in ktt_blocks:
            if "KTT" not in block:
                continue

            # Map the seed type by index (Fixes the shift issue)
            current_seed_type = "Unknown"
            if ktt_block_index < len(all_seed_types):
                current_seed_type = all_seed_types[ktt_block_index]
            
            ktt_block_index += 1 # Increment for next valid block

            # Extract PO
            po_match = re.search(r"PO\s*(?:#)?[:\s]*([^\n]+)", block, re.IGNORECASE)
            po_raw = po_match.group(1).strip() if po_match else "UNKNOWN"
            clean_po = re.sub(r"[\s\u00A0]+", "", po_raw)

            # --- FINANCIALS ---
            subtotal = 0.0
            
            # 1. Look for 'Subtotal:'
            m_sub = re.search(r"(\$[\d,]+\.\d{2})\s*\n\s*Subtotal|Subtotal\s*[:\n]*\s*(\$[\d,]+\.\d{2})", block, re.IGNORECASE)
            if m_sub:
                val = m_sub.group(1) if m_sub.group(1) else m_sub.group(2)
                subtotal = parse_currency(val)
            
            # 2. Fallback
            if subtotal == 0.0 or abs(subtotal - 20.00) < 0.01:
                all_prices_raw = re.findall(r"\$([\d,]+\.\d{2})", block)
                valid_prices = [parse_currency(p) for p in all_prices_raw if parse_currency(p) > 20.00]
                if valid_prices:
                    subtotal = max(valid_prices)

            # --- FREIGHT (FIX: Check Forward First) ---
            freight = 0.0
            # Priority 1: "Freight: ... $1063.00" (Forward look finds the real cost)
            if m_frt_fwd := re.search(r"Freight:\s*.*?(\$[\d,]+\.\d{2})", block, re.IGNORECASE | re.DOTALL):
                 freight = parse_currency(m_frt_fwd.group(1))
            # Priority 2: "$1063.00 ... Freight" (Reverse look)
            elif m_frt_rev := re.search(r"(\$[\d,]+\.\d{2})\s*\n\s*Freight:", block, re.IGNORECASE):
                freight = parse_currency(m_frt_rev.group(1))

            adjusted_subtotal = subtotal
            if freight > 0 and adjusted_subtotal > freight:
                adjusted_subtotal = adjusted_subtotal - freight

            # --- PO CHECKS ---
            
            # 1. Numeric POs (US Lines) -> SKIP but track cost
            if re.match(r"^\d+$", clean_po):
                numeric_po_found = True
                # We subtract freight here because freight DOES belong in the G/L bucket (expense),
                # while the seed cost (US Inventory) does NOT.
                amount_to_skip = subtotal - freight
                skipped_numeric_amount += amount_to_skip 
                continue 

            # 2. G/L Logic (Dates/Unprocessed) -> SKIP completely
            if re.match(r"\d{1,2}/\d{1,2}/\d{4}", po_raw) or "left unprocessed" in block.lower():
                continue

            # Quantity
            quantity = 0.0
            if m_qty := re.search(r"Shipped\s*Weight[:\s]*([\d,]+\.\d{2})", block, re.IGNORECASE):
                quantity = parse_currency(m_qty.group(1))

            unit_cost = 0.0
            if quantity > 0:
                unit_cost = adjusted_subtotal / quantity

            item_no = po_raw
            if "-" in po_raw:
                sub_m = re.search(r"^\d+-(.+)", po_raw)
                if sub_m:
                    item_no = sub_m.group(1).strip()
                else:
                    item_no = po_raw

            description = f"INV{invoice_no}_{current_seed_type}_{po_raw}"
            line_amount = round(quantity * round(unit_cost, 5), 2)
            
            processed_line_total_sum += line_amount

            resource_lines.append({
                "Type": "Resource",
                "No": item_no,
                "Description": description,
                "Quantity": quantity,
                "DirectUnitCost": round(unit_cost, 5),
                "LineAmount": line_amount
            })

        # --- 4. Balancing G/L Line ---
        # GrandTotal - (Resource Lines) - (US Seeds) = Freight + Unprocessed Fees + Rounding
        gl_amount = grand_total - processed_line_total_sum - skipped_numeric_amount
        
        if resource_lines and abs(gl_amount) >= 0.01:
            resource_lines.append({
                "Type": "G/L Account",
                "No": "609100",
                "Description": f"INV{invoice_no}_Unprocessed_WOSplit_Shipping",
                "Quantity": 1,
                "DirectUnitCost": round(gl_amount, 2),
                "LineAmount": round(gl_amount, 2)
            })
        
        # --- 5. US PO Warning ---
        if not resource_lines and numeric_po_found:
             resource_lines.append({
                "Type": "NOTE",
                "No": "",
                "Description": "The detected PO# Lines for this invoice belong to the US.",
                "Quantity": 0,
                "DirectUnitCost": 0,
                "LineAmount": 0,
                "IsUSWarning": True
            })

        # --- 6. Logging & Final Return ---
        log_processing_event(
            vendor='Kamterter',
            filename=filename,
            extraction_info={'method': 'PyMuPDF', 'page_count': page_count},
            po_number=None
        )

        if resource_lines:
            for line in resource_lines:
                line["VendorInvoiceNo"] = invoice_no
                line["DocumentDate"] = doc_date
                line["BuyFromVendorName"] = "KAMTERTER II, LLC"
            
            grouped_results[filename] = resource_lines

    return grouped_results