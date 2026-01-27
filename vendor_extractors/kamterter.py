import fitz  # PyMuPDF
import re
from db_logger import log_processing_event

def extract_kamterter_data_from_bytes(pdf_files: list[tuple[str, bytes]]) -> dict[str, list[dict]]:
    grouped_results = {}

    for filename, pdf_bytes in pdf_files:
        print(f"\n--- DEBUG: Processing File {filename} ---")
        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
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

        # Extract Grand Total
        grand_total = 0.0
        # Check for Total at the very end of the document
        if m := re.search(r"Total\s*:.*?\$?([\d,]+\.\d{2})", full_text, re.IGNORECASE | re.DOTALL):
            all_totals = re.findall(r"Total\s*:.*?\$?([\d,]+\.\d{2})", full_text, re.IGNORECASE | re.DOTALL)
            if all_totals:
                grand_total = float(all_totals[-1].replace(",", ""))
        
        print(f"DEBUG: Invoice: {invoice_no}, Date: {doc_date}, Grand Total: {grand_total}")

        # --- 2. Block Processing ---
        ktt_blocks = re.split(r"(?=KTT\s*#:)", full_text)
        
        resource_lines = []
        processed_line_total_sum = 0.0
        
        for i, block in enumerate(ktt_blocks):
            if "KTT #:" not in block:
                continue

            print(f"\n--- DEBUG: Analyzing Block {i} ---")
            # print(block) # Uncomment to see full raw text of the block
            
            # 1. Extract PO
            po_match = re.search(r"PO\s*(?:#)?[:\s]*([^\n]+)", block, re.IGNORECASE)
            po_raw = po_match.group(1).strip() if po_match else "UNKNOWN"
            
            # G/L Logic Check
            if re.match(r"\d{1,2}/\d{1,2}/\d{4}", po_raw) or "left unprocessed" in block.lower():
                print(f"Skipping Item Logic (Unprocessed/Date PO): {po_raw}")
                continue

            # 2. Extract Data
            seed_type = "Unknown"
            if m_seed := re.search(r"Seed\s*Type[:\s]*([^\n]+)", block, re.IGNORECASE):
                seed_type = m_seed.group(1).strip()

            quantity = 0.0
            if m_qty := re.search(r"Shipped\s*Weight[:\s]*([\d,]+\.\d{2})", block, re.IGNORECASE):
                quantity = float(m_qty.group(1).replace(",", ""))

            # 3. CRITICAL: Financial Extraction Debugging
            subtotal = 0.0
            freight = 0.0
            
            # Regex Strategy 1: Look for "Subtotal:" label
            if m_sub := re.search(r"Subtotal[:\s]*.*?\$?([\d,]+\.\d{2})", block, re.IGNORECASE | re.DOTALL):
                subtotal = float(m_sub.group(1).replace(",", ""))
                print(f"DEBUG: Found Subtotal via Regex: {subtotal}")
            else:
                # Regex Strategy 2: Look for the last dollar amount in the block (Fall back)
                # This handles cases where "Subtotal:" label is messy or missing
                prices = re.findall(r"\$([\d,]+\.\d{2})", block)
                if prices:
                    subtotal = float(prices[-1].replace(",", ""))
                    print(f"DEBUG: Found Subtotal via Fallback (Last Price): {subtotal}")
                else:
                    print("DEBUG: FAILED to find Subtotal")

            # Look for Freight explicitly to subtract it
            # Note: We must avoid subtracting "Small Box" if it's listed separately
            if m_freight := re.search(r"Freight[:\s]*.*?\$?([\d,]+\.\d{2})", block, re.IGNORECASE | re.DOTALL):
                freight = float(m_freight.group(1).replace(",", ""))
                print(f"DEBUG: Found Freight: {freight}")

            # 4. Calculation
            # Logic: If the Subtotal regex grabbed the final block total, it likely INCLUDES freight.
            # We subtract freight to get the goods cost.
            adjusted_subtotal = subtotal
            
            if freight > 0 and adjusted_subtotal >= freight:
                adjusted_subtotal = adjusted_subtotal - freight

            unit_cost = 0.0
            if quantity > 0:
                unit_cost = adjusted_subtotal / quantity
            
            print(f"DEBUG CALC: ({subtotal} (Sub) - {freight} (Frt)) / {quantity} (Qty) = {unit_cost} (Unit Cost)")

            # 5. Item No Extraction
            item_no = po_raw
            if "-" in po_raw:
                item_no = re.sub(r"^\d+-", "", po_raw).strip()

            description = f"Inv{invoice_no}_{seed_type}_{po_raw}"
            line_amount = round(quantity * round(unit_cost, 5), 2)
            processed_line_total_sum += line_amount

            resource_lines.append({
                "Type": "Resource",
                "No": item_no,
                "Description": description,
                "Quantity": quantity,
                "DirectUnitCost": round(unit_cost, 5),
                "LineAmount": line_amount,
                "PO_Number": po_raw
            })

        # --- 3. Balancing G/L Line ---
        gl_amount = grand_total - processed_line_total_sum
        print(f"\nDEBUG: Balancing GL: {grand_total} (Total) - {processed_line_total_sum} (Sum) = {gl_amount}")
        
        if abs(gl_amount) >= 0.01:
            resource_lines.append({
                "Type": "G/L Account",
                "No": "609100",
                "Description": f"Inv{invoice_no}_Freight_Splits_Adj",
                "Quantity": 1,
                "DirectUnitCost": round(gl_amount, 2),
                "LineAmount": round(gl_amount, 2),
                "PO_Number": "G/L Adjustment"
            })

        # --- 4. Logging & Final Return ---
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
                line["VendorNo"] = "95257"
            
            grouped_results[filename] = resource_lines

    return grouped_results