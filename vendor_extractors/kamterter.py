import fitz  # PyMuPDF
import re
from db_logger import log_processing_event

def extract_kamterter_data_from_bytes(pdf_files: list[tuple[str, bytes]]) -> dict[str, list[dict]]:
    grouped_results = {}

    for filename, pdf_bytes in pdf_files:
        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
        full_text = "".join([page.get_text() for page in doc])
        page_count = doc.page_count
        doc.close()

        # --- 1. Global Metadata ---
        invoice_no = None
        if m := re.search(r"Invoice\s*#:\s*(\d+)", full_text):
            invoice_no = m.group(1)

        doc_date = None
        if m := re.search(r"Invoiced\s*Date:\s*(\d{2}/\d{2}/\d{4})", full_text):
            doc_date = m.group(1) # Keep as string for display

        grand_total = 0.0
        if m := re.search(r"Total:\s*\n\s*\$([\d,]+\.\d{2})", full_text):
            grand_total = float(m.group(1).replace(",", ""))

        # --- 2. Block Processing ---
        ktt_blocks = re.split(r"(?=KTT\s*#:)", full_text)
        
        resource_lines = []
        processed_line_total_sum = 0.0
        
        for block in ktt_blocks:
            if "KTT #:" not in block:
                continue

            po_raw = re.search(r"PO\s*#:\s*([^\n]+)", block).group(1).strip()
            
            # G/L Logic: Skip "Unprocessed" or Date-based POs for item creation
            # (These contribute to the balancing GL line via math, not explicit line items)
            if re.match(r"\d{2}/\d{2}/\d{4}", po_raw) or "left unprocessed" in block.lower():
                continue

            # Item Logic
            seed_type = "Unknown"
            if m_seed := re.search(r"Seed\s*Type:\s*([^\n]+)", block):
                seed_type = m_seed.group(1).strip()

            qty_match = re.search(r"Shipped\s*Weight:\s*([\d,]+\.\d{2})", block)
            quantity = float(qty_match.group(1).replace(",", "")) if qty_match else 0.0

            sub_match = re.search(r"Subtotal:\s*\$?([\d,]+\.\d{2})", block)
            subtotal = float(sub_match.group(1).replace(",", "")) if sub_match else 0.0

            freight_match = re.search(r"Freight:.*?\$\s*([\d,]+\.\d{2})", block, re.DOTALL)
            freight = float(freight_match.group(1).replace(",", "")) if freight_match else 0.0

            # Unit Cost = (Subtotal - Freight) / Quantity
            unit_cost = 0.0
            if quantity > 0:
                unit_cost = (subtotal - freight) / quantity

            # Extract Suffix for Item No (e.g. "32-ON-MEC" -> "ON-MEC")
            item_no = po_raw
            if "-" in po_raw:
                # Regex to take everything after the first hyphen
                item_no = re.sub(r"^\d+-", "", po_raw).strip()

            description = f"INV{invoice_no}_{seed_type}_{po_raw}"

            # Calculate line amount for balancing check
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

        # --- 3. Balancing G/L Line ---
        # Plug the difference between PDF Total and calculated Resource lines
        gl_amount = grand_total - processed_line_total_sum
        
        if abs(gl_amount) > 0.001:
            resource_lines.append({
                "Type": "G/L Account",
                "No": "609100",
                "Description": f"INV{invoice_no}_Freight_Splits_Adj",
                "Quantity": 1,
                "DirectUnitCost": round(gl_amount, 2),
                "LineAmount": round(gl_amount, 2)
            })

        # --- 4. Logging & Final Return ---
        log_processing_event(
            vendor='Kamterter',
            filename=filename,
            extraction_info={'method': 'PyMuPDF', 'page_count': page_count},
            po_number=None
        )

        if resource_lines:
            # Attach header data to every line for the template to access easily
            for line in resource_lines:
                line["VendorInvoiceNo"] = invoice_no
                line["DocumentDate"] = doc_date
                line["VendorNo"] = "95257"
            
            grouped_results[filename] = resource_lines

    return grouped_results