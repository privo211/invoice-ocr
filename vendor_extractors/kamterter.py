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
        
        # [DEBUG] Standard extraction (No sort=True) to inspect raw stream order
        full_text = "".join([page.get_text() for page in doc])
        
        page_count = doc.page_count
        doc.close()

        # --- DEBUG LOGGING START ---
        print(f"\n{'='*50}")
        print(f"📄 DEBUG DUMP: {filename}")
        print(f"{'='*50}")
        print(full_text)
        print(f"{'='*50}\n")
        # --- DEBUG LOGGING END ---

        # --- 1. Global Metadata ---
        invoice_no = None
        if m := re.search(r"Invoice\s*(?:#|No\.?)[:\s]*(\d+)", full_text, re.IGNORECASE):
            invoice_no = m.group(1)

        doc_date = None
        if m := re.search(r"Invoiced\s*Date[:\s]*(\d{1,2}/\d{1,2}/\d{4})", full_text, re.IGNORECASE):
            doc_date = m.group(1)

        # --- 2. Extract Grand Total ---
        grand_total = 0.0
        if m_total := re.search(r"(\$[\d,]+\.\d{2})\s*\n\s*Total:", full_text, re.IGNORECASE):
            grand_total = parse_currency(m_total.group(1))
        elif m_total_std := re.search(r"Total\s*:.*?(\$[\d,]+\.\d{2})", full_text, re.IGNORECASE | re.DOTALL):
             all_totals = re.findall(r"Total\s*:.*?(\$[\d,]+\.\d{2})", full_text, re.IGNORECASE | re.DOTALL)
             if all_totals:
                 grand_total = parse_currency(all_totals[-1])

        # --- 3. Block Processing ---
        # IMPROVED SPLIT: Look for "KTT" followed by optional punctuation, space, and a DIGIT.
        # This ensures we catch "KTT #: 123" and "KTT 123" but avoid "KTT Products".
        ktt_blocks = re.split(r"(?=KTT\s*[:#]*\s*\d)", full_text)
        
        resource_lines = []
        processed_line_total_sum = 0.0
        numeric_po_found = False
        
        for i, block in enumerate(ktt_blocks):
            if "KTT" not in block:
                continue

            # --- DEBUG BLOCK CONTENT ---
            print(f"--- [DEBUG] Processing Block {i} ---")
            print(f"Preview: {block[:100].replace(chr(10), ' ')}...") # Print first 100 chars
            
            # Extract PO
            po_match = re.search(r"PO\s*(?:#)?[:\s]*([^\n]+)", block, re.IGNORECASE)
            po_raw = po_match.group(1).strip() if po_match else "UNKNOWN"
            
            print(f"   -> Found PO Raw: '{po_raw}'")

            # Numeric PO Check
            if re.match(r"^\d+$", po_raw):
                print(f"   -> SKIPPING: Detected Numeric PO")
                numeric_po_found = True
                continue 

            # G/L Logic Check
            if re.match(r"\d{1,2}/\d{1,2}/\d{4}", po_raw) or "left unprocessed" in block.lower():
                print(f"   -> SKIPPING: Detected Date/G/L PO")
                continue

            # --- Item Logic (FIXED REGEX) ---
            seed_type = "Unknown"
            # Look for Seed Type label, consume colon/newlines ([\s:]*), then capture the first non-empty line
            if m_seed := re.search(r"Seed\s*Type[\s:]*(\S[^\n]*)", block, re.IGNORECASE):
                seed_type = m_seed.group(1).strip()
            
            print(f"   -> Found Seed Type: '{seed_type}'")

            # Quantity
            quantity = 0.0
            if m_qty := re.search(r"Shipped\s*Weight[:\s]*([\d,]+\.\d{2})", block, re.IGNORECASE):
                quantity = parse_currency(m_qty.group(1))

            # --- FINANCIALS ---
            subtotal = 0.0
            if m_sub_rev := re.search(r"(\$[\d,]+\.\d{2})\s*\n\s*Subtotal:", block, re.IGNORECASE):
                subtotal = parse_currency(m_sub_rev.group(1))
            elif m_sub_fwd := re.search(r"Subtotal[:\s]*.*?(\$[\d,]+\.\d{2})", block, re.IGNORECASE | re.DOTALL):
                subtotal = parse_currency(m_sub_fwd.group(1))
            elif not subtotal:
                prices = re.findall(r"\$([\d,]+\.\d{2})", block)
                if prices:
                    subtotal = parse_currency(prices[-1])

            freight = 0.0
            if m_frt_rev := re.search(r"(\$[\d,]+\.\d{2})\s*\n\s*Freight:", block, re.IGNORECASE):
                freight = parse_currency(m_frt_rev.group(1))
            elif m_frt_fwd := re.search(r"Freight:\s*.*?(\$[\d,]+\.\d{2})", block, re.IGNORECASE | re.DOTALL):
                 freight = parse_currency(m_frt_fwd.group(1))

            adjusted_subtotal = subtotal
            if freight > 0 and adjusted_subtotal > freight:
                adjusted_subtotal = adjusted_subtotal - freight

            unit_cost = 0.0
            if quantity > 0:
                unit_cost = adjusted_subtotal / quantity

            # Item No
            item_no = po_raw
            if "-" in po_raw:
                sub_m = re.search(r"^\d+-(.+)", po_raw)
                if sub_m:
                    item_no = sub_m.group(1).strip()
                else:
                    item_no = po_raw

            description = f"INV{invoice_no}_{seed_type}_{po_raw}"
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
        gl_amount = grand_total - processed_line_total_sum
        
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