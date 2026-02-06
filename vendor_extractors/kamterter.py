import fitz  # PyMuPDF
import re
from db_logger import log_processing_event


def parse_currency(value_str):
    """Parses currency string, robust against spaces and OCR weirdness"""
    if not value_str:
        return 0.0

    clean = re.sub(r"[^\d\.,-]", "", value_str)
    clean = clean.replace(",", "")

    if clean.count(".") > 1:
        parts = clean.split(".")
        clean = "".join(parts[:-1]) + "." + parts[-1]

    try:
        return float(clean)
    except ValueError:
        print(f"    [DEBUG] parse_currency FAILED for input: '{value_str}'")
        return 0.0


def extract_kamterter_data_from_bytes(pdf_files: list[tuple[str, bytes]]) -> dict[str, list[dict]]:
    grouped_results = {}

    for filename, pdf_bytes in pdf_files:
        print(f"\n{'='*60}")
        print(f"🔎 STARTING DEBUG ANALYSIS: {filename}")
        print(f"{'='*60}")

        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
        full_text = "".join(page.get_text() for page in doc)
        page_count = doc.page_count
        doc.close()

        print(f"--- [DEBUG] RAW TEXT DUMP---")
        print(full_text)
        print(f"--- [DEBUG] END RAW TEXT DUMP ---\n")

        # --- 1. Global Metadata ---
        invoice_no = None
        if m := re.search(r"Invoice\s*(?:#|No\.?)[:\s]*(\d+)", full_text, re.IGNORECASE):
            invoice_no = m.group(1)
            print(f"[DEBUG] Invoice No Found: {invoice_no}")

        doc_date = None
        if m := re.search(r"Invoiced\s*Date[:\s]*(\d{1,2}/\d{1,2}/\d{4})", full_text, re.IGNORECASE):
            doc_date = m.group(1)
            print(f"[DEBUG] Date Found: {doc_date}")

        # --- 2. Extract Grand Total ---
        grand_total = 0.0
        all_prices_raw = re.findall(r"\$\s*([0-9,.]+)", full_text)
        all_prices = [parse_currency(p) for p in all_prices_raw]

        if all_prices:
            grand_total = max(all_prices)
            print(f"[DEBUG] GRAND TOTAL FOUND: {grand_total}")
        else:
            print("[DEBUG] ❌ GRAND TOTAL NOT FOUND")

        # --- 3. Block Processing ---
        split_pattern = r"(?=Lot\s*[:#])"
        ktt_blocks = re.split(split_pattern, full_text)
        print(f"\n[DEBUG] Split Logic: Found {len(ktt_blocks)} segments using pattern '{split_pattern}'")

        resource_lines = []
        processed_line_total_sum = 0.0
        skipped_numeric_amount = 0.0
        numeric_po_found = False

        for i, block in enumerate(ktt_blocks):
            if "KTT" not in block:
                continue

            print(f"\n--- BLOCK {i} ANALYSIS ---")

            # --- FINANCIALS ---
            subtotal = 0.0

            if m := re.search(r"(\$\s*[\d,.]+)\s*\n\s*Subtotal", block, re.IGNORECASE):
                subtotal = parse_currency(m.group(1))
                print(f"   Subtotal (Reverse): {subtotal}")
            elif m := re.search(r"Subtotal\s*[:\n]*\s*(\$\s*[\d,.]+)", block, re.IGNORECASE):
                subtotal = parse_currency(m.group(1))
                print(f"   Subtotal (Forward): {subtotal}")
            else:
                prices = re.findall(r"\$\s*([\d,.]+)", block)
                if prices:
                    subtotal = parse_currency(prices[-1])
                    print(f"   Subtotal (Fallback): {subtotal}")

            # --- FREIGHT LOGIC (UPDATED) ---
            freight = 0.0
            
            # 1. Reverse Match (Price matches "\n" Freight) - This matches your PDF format
            if m_frt_rev := re.search(r"(\$\s*[\d,.]+)\s*\n\s*Freight:\s*FedEx Priority Freight", block):
                freight = parse_currency(m_frt_rev.group(1))
                print(f"   Freight: FedEx Priority Freight Found (Reverse): {freight}")

            adjusted_subtotal = subtotal
            if freight > 0 and subtotal > freight:
                adjusted_subtotal = subtotal - freight
                print(
                    f"   Adjusted Subtotal: {adjusted_subtotal} "
                    f"(Subtotal {subtotal} - Freight {freight})"
                )

            # --- PO EXTRACTION ---
            po_match = re.search(r"PO\s*(?:#)?[:\s]*([^\n]+)", block, re.IGNORECASE)
            po_raw = po_match.group(1).strip() if po_match else "UNKNOWN"
            clean_po = re.sub(r"[\s\u00A0]+", "", po_raw)
            print(f"   PO Found: '{po_raw}' (Clean: '{clean_po}')")

            # --- US NUMERIC PO SKIP ---
            if re.match(r"^\d+$", clean_po):
                numeric_po_found = True

                # FIX: skip ONLY US item cost, keep freight for G/L
                skipped_numeric_amount += adjusted_subtotal

                print(
                    f"   👉 ACTION: SKIP (Numeric PO). "
                    f"Skipping US Item Cost: {adjusted_subtotal}, "
                    f"Keeping Freight: {freight}"
                )
                continue

            # --- Date / Unprocessed ---
            if re.match(r"\d{1,2}/\d{1,2}/\d{4}", po_raw) or "left unprocessed" in block.lower():
                print(
                    f"   👉 ACTION: SKIP (Date/Unprocessed). "
                    f"Amount {subtotal} will flow to G/L."
                )
                continue

            # --- ITEM DETAILS ---
            seed_type = "Unknown"
            if m := re.search(r"Seed\s*Type.*?:(.*?)(?:\n|$)", block, re.IGNORECASE):
                seed_type = m.group(1).strip()
                print(f"   Seed Type: '{seed_type}'")

            quantity = 0.0
            if m := re.search(r"Shipped\s*Weight[:\s]*([\d,]+\.\d{2})", block, re.IGNORECASE):
                quantity = parse_currency(m.group(1))
                print(f"   Quantity: {quantity}")

            unit_cost = 0.0
            if quantity > 0:
                unit_cost = adjusted_subtotal / quantity
                print(f"   Unit Cost: {unit_cost}")

            item_no = po_raw
            if "-" in po_raw:
                if m := re.search(r"^\d+-(.+)", po_raw):
                    item_no = m.group(1).strip()

            description = f"INV{invoice_no}_{seed_type}_{po_raw}"
            line_amount = round(quantity * round(unit_cost, 5), 2)

            processed_line_total_sum += line_amount
            print(f"   👉 ACTION: ADD LINE. Amount: {line_amount}")

            resource_lines.append({
                "Type": "Resource",
                "No": item_no,
                "Description": description,
                "Quantity": quantity,
                "DirectUnitCost": round(unit_cost, 5),
                "LineAmount": line_amount,
                "PO_Number": po_raw
            })

        # --- 4. G/L BALANCING ---
        print("\n" + "=" * 40)
        print("📊 FINAL G/L CALCULATION")
        print("=" * 40)
        print(f"Grand Total:        {grand_total}")
        print(f" - Processed Sum:   {processed_line_total_sum}")
        print(f" - Skipped Numeric: {skipped_numeric_amount}")
        print("-" * 20)

        gl_amount = grand_total - processed_line_total_sum - skipped_numeric_amount
        print(f" = G/L Result:      {gl_amount}")
        print("=" * 40 + "\n")

        if resource_lines and abs(gl_amount) >= 0.01:
            resource_lines.append({
                "Type": "G/L Account",
                "No": "609100",
                "Description": f"INV{invoice_no}_Unprocessed_WOSplit_Shipping",
                "Quantity": 1,
                "DirectUnitCost": round(gl_amount, 2),
                "LineAmount": round(gl_amount, 2),
                "PO_Number": "G/L Adjustment"
            })

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

        log_processing_event(
            vendor="Kamterter",
            filename=filename,
            extraction_info={"method": "PyMuPDF", "page_count": page_count},
            po_number=None
        )

        if resource_lines:
            for line in resource_lines:
                line["VendorInvoiceNo"] = invoice_no
                line["DocumentDate"] = doc_date
                line["BuyFromVendorName"] = "KAMTERTER II, LLC"

            grouped_results[filename] = resource_lines

    return grouped_results