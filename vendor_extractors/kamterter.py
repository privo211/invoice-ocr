import fitz  # PyMuPDF
import re
from db_logger import log_processing_event

def parse_currency(value_str):
    """Parses currency string, robust against spaces and OCR weirdness"""
    if not value_str:
        return 0.0
    # Remove $ and spaces, keep digits, commas, dots, hyphens (for negative adjustments if any)
    clean = re.sub(r"[^\d\.,-]", "", value_str)
    
    # Handle potential "26.056.66" (dot as thousand) vs "26,056.66"
    clean = clean.replace(",", "")
    if clean.count(".") > 1:
        parts = clean.split(".")
        clean = "".join(parts[:-1]) + "." + parts[-1]
        
    try:
        val = float(clean)
        return val
    except ValueError:
        print(f"    [DEBUG] parse_currency FAILED for input: '{value_str}' -> returning 0.0")
        return 0.0

def extract_kamterter_data_from_bytes(pdf_files: list[tuple[str, bytes]]) -> dict[str, list[dict]]:
    grouped_results = {}

    for filename, pdf_bytes in pdf_files:
        print(f"\n{'='*60}")
        print(f"🔎 STARTING DEBUG ANALYSIS: {filename}")
        print(f"{'='*60}")

        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
        # Standard extraction
        full_text = "".join([page.get_text() for page in doc])
        page_count = doc.page_count
        doc.close()

        # --- DEBUG LOGGING ---
        print(f"--- [DEBUG] RAW TEXT DUMP (First 500 chars) ---")
        print(full_text[:500] + "...") 
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
        # Robust regex finding all prices with potential spaces
        all_prices_raw = re.findall(r"\$\s*([0-9,.]+)", full_text)
        all_prices = [parse_currency(p) for p in all_prices_raw]
        
        if all_prices:
            # Grand Total is typically the maximum single value in the document
            grand_total = max(all_prices)
            print(f"[DEBUG] GRAND TOTAL FOUND: {grand_total}")
        else:
            print("[DEBUG] ❌ GRAND TOTAL NOT FOUND")

        # --- 3. Block Processing ---
        # FIX: Split by 'Lot #' instead of 'KTT'. 
        # This keeps the 'Seed Type' (which is above KTT) grouped with the correct item.
        split_pattern = r"(?=Lot\s*[:#])"
        ktt_blocks = re.split(split_pattern, full_text)
        print(f"\n[DEBUG] Split Logic: Found {len(ktt_blocks)} segments using pattern '{split_pattern}'")
        
        resource_lines = []
        processed_line_total_sum = 0.0
        skipped_numeric_amount = 0.0 
        numeric_po_found = False

        for i, block in enumerate(ktt_blocks):
            # Valid blocks must have a KTT number
            if "KTT" not in block:
                continue

            print(f"\n--- BLOCK {i} ANALYSIS ---")
            
            # --- FINANCIALS (Extract First) ---
            # We extract financials first so we can subtract them if we decide to SKIP this block.
            subtotal = 0.0
            
            # Method 1: Reverse match (Price preceding "Subtotal:")
            if m_sub_rev := re.search(r"(\$\s*[\d,.]+)\s*\n\s*Subtotal", block, re.IGNORECASE):
                subtotal = parse_currency(m_sub_rev.group(1))
                print(f"   Subtotal (Reverse): {subtotal}")
            # Method 2: Forward match ("Subtotal: Price")
            elif m_sub_fwd := re.search(r"Subtotal\s*[:\n]*\s*(\$\s*[\d,.]+)", block, re.IGNORECASE):
                subtotal = parse_currency(m_sub_fwd.group(1))
                print(f"   Subtotal (Forward): {subtotal}")
            # Method 3: Fallback (Last dollar amount in block)
            elif not subtotal:
                prices = re.findall(r"\$\s*([\d,.]+)", block)
                if prices:
                    subtotal = parse_currency(prices[-1])
                    print(f"   Subtotal (Fallback): {subtotal}")

            # Freight logic (to subtract from Subtotal if included)
            freight = 0.0
            if m_frt := re.search(r"Freight.*(\$\s*[\d,.]+)", block, re.IGNORECASE):
                 freight = parse_currency(m_frt.group(1))
                 print(f"   Freight Found: {freight}")

            # Calculate Net Item Cost (Subtotal often includes Freight)
            adjusted_subtotal = subtotal
            if freight > 0 and adjusted_subtotal > freight:
                adjusted_subtotal = adjusted_subtotal - freight
                print(f"   Adjusted Subtotal: {adjusted_subtotal} (Subtotal {subtotal} - Freight {freight})")

            # --- PO EXTRACTION & FILTERING ---
            po_match = re.search(r"PO\s*(?:#)?[:\s]*([^\n]+)", block, re.IGNORECASE)
            po_raw = po_match.group(1).strip() if po_match else "UNKNOWN"
            
            # Clean PO for numeric check (remove spaces)
            clean_po = re.sub(r"[\s\u00A0]+", "", po_raw)
            print(f"   PO Found: '{po_raw}' (Clean: '{clean_po}')")

            # 1. Skip Numeric POs (US Branch)
            if re.match(r"^\d+$", clean_po):
                numeric_po_found = True
               
                skipped_numeric_amount += adjusted_subtotal
                print(f"   👉 ACTION: SKIP (Numeric PO). Skipping amount: {adjusted_subtotal} (Freight {freight} left for G/L)")
                continue 

            # 2. Skip Date POs or Unprocessed
            if re.match(r"\d{1,2}/\d{1,2}/\d{4}", po_raw) or "left unprocessed" in block.lower():
                # These are usually small fees ($20) that should go to G/L, 
                # so we DO NOT add them to 'skipped_numeric_amount'. 
                # They will naturally fall into the G/L bucket.
                print(f"   👉 ACTION: SKIP (Date/Unprocessed). Amount {subtotal} will flow to G/L.")
                continue

            # --- ITEM DETAILS ---
            # Seed Type (Description)
            # Regex looks for "Seed Type" followed by anything until a newline
            seed_type = "Unknown"
            if m_seed := re.search(r"Seed\s*Type.*?:(.*?)(?:\n|$)", block, re.IGNORECASE):
                seed_type = m_seed.group(1).strip()
                print(f"   Seed Type: '{seed_type}'")

            # Quantity
            quantity = 0.0
            if m_qty := re.search(r"Shipped\s*Weight[:\s]*([\d,]+\.\d{2})", block, re.IGNORECASE):
                quantity = parse_currency(m_qty.group(1))
                print(f"   Quantity: {quantity}")

            unit_cost = 0.0
            if quantity > 0:
                unit_cost = adjusted_subtotal / quantity
                print(f"   Unit Cost: {unit_cost}")

            # Item No (Suffix of PO)
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

        # --- 4. Balancing G/L Line ---
        print("\n" + "="*40)
        print("📊 FINAL G/L CALCULATION")
        print("="*40)
        print(f"Grand Total:       {grand_total}")
        print(f" - Processed Sum:  {processed_line_total_sum}")
        print(f" - Skipped Numeric:{skipped_numeric_amount}")
        print("-" * 20)

        # G/L = GrandTotal - (Sum of processed items) - (Sum of explicitly skipped US items)
        gl_amount = grand_total - processed_line_total_sum - skipped_numeric_amount
        print(f" = G/L Result:     {gl_amount}")
        print("="*40 + "\n")
        
        # Add G/L line if remainder is significant AND we processed at least one line
        if resource_lines and abs(gl_amount) >= 0.01:
            print(f"[DEBUG] Adding Balancing G/L Line for amount: {gl_amount}")
            resource_lines.append({
                "Type": "G/L Account",
                "No": "609100",
                "Description": f"INV{invoice_no}_Unprocessed_WOSplit_Shipping",
                "Quantity": 1,
                "DirectUnitCost": round(gl_amount, 2),
                "LineAmount": round(gl_amount, 2),
                "PO_Number": "G/L Adjustment"
            })
        else:
            print("[DEBUG] No G/L line needed (difference negligible or no items processed).")
        
        # Handle case where ONLY US lines were found
        if not resource_lines and numeric_po_found:
             print("[DEBUG] No resource lines, but numeric POs found. Adding Warning.")
             resource_lines.append({
                "Type": "NOTE",
                "No": "",
                "Description": "The detected PO# Lines for this invoice belong to the US.",
                "Quantity": 0,
                "DirectUnitCost": 0,
                "LineAmount": 0,
                "IsUSWarning": True
            })

        # --- 5. Logging & Final Return ---
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