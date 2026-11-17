"""US Kamterter invoice extraction and assembly matching."""

from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
import re

import fitz

from db_logger import log_processing_event
from vendor_extractors.kamterter import _extract_freight_amount

US_KAMTERTER_VENDOR = "KAMTERTER II, LLC (M&T EDI)"
CENT = Decimal("0.01")
UNIT_COST_PRECISION = Decimal("0.00001")
AMOUNT = r"\$[ \t]*([\d,]+\.\d{2})"


def _po_key(value):
    value = str(value or "").strip().upper()
    if match := re.fullmatch(r"TR[ \t-]*(\d+)", value):
        return f"TR-{match[1]}"
    return value if value.isdigit() else ""


def _labeled_amount(text, label):
    horizontal = r"[ \t]*"
    prefix = rf"^{horizontal}{label}{horizontal}:{horizontal}"
    patterns = (
        rf"{prefix}{AMOUNT}{horizontal}$",
        rf"^{horizontal}{AMOUNT}{horizontal}\r?\n{prefix}$",
        rf"{prefix}\r?\n{horizontal}{AMOUNT}{horizontal}$",
    )
    for pattern in patterns:
        if matches := re.findall(pattern, text, re.IGNORECASE | re.MULTILINE):
            return Decimal(matches[-1].replace(",", ""))
    raise ValueError(f"The PDF is missing a readable {label.lower()}.")


def _invoice_line(line_type, number, description, quantity, amount, unit=""):
    unit_cost = (amount / quantity).quantize(UNIT_COST_PRECISION, rounding=ROUND_HALF_UP)
    return {
        "Type": line_type,
        "No": number,
        "Description": description,
        "Quantity": float(quantity),
        "Unit": unit,
        "DirectUnitCost": float(unit_cost),
        "LineAmount": float((quantity * unit_cost).quantize(CENT, rounding=ROUND_HALF_UP)),
    }


def _treated_line(block, invoice_no, po, amount, lookup_assembly):
    reference = _po_key(po)
    if not reference:
        raise ValueError(f"PO {po} is not a recognized assembly order number.")

    rows = lookup_assembly(reference)
    assemblies = {(row.get("AssemblyStatus"), row.get("AssemblyNo")) for row in rows}
    if len({status for status, _ in assemblies}) > 1:
        raise ValueError(f"PO {po} has both open and posted assemblies. Check for a partial posting.")
    if len(assemblies) != 1:
        raise ValueError(f"PO {po} needs one matching open or posted assembly in Business Central.")

    assembly_status, assembly_no = next(iter(assemblies))
    if not assembly_no:
        raise ValueError(f"The matching assembly for PO {po} has no document number.")
    assembly_label = f"{assembly_status} assembly {assembly_no}"
    resources = [row for row in rows if row.get("LineType") == "Resource"]
    if len(resources) != 1:
        raise ValueError(f"{assembly_label} needs exactly one Resource line.")

    resource_line = resources[0]
    resource = resource_line.get("ResourceNo")
    unit = resource_line.get("UnitOfMeasure")
    try:
        quantity = Decimal(str(resource_line.get("Quantity")))
        uses_order_quantity = (
            assembly_status == "Open" and quantity == 0 and bool(unit)
            and unit == resource_line.get("AssemblyUnitOfMeasure")
        )
        if uses_order_quantity:
            quantity = Decimal(str(resource_line.get("AssemblyQuantity")))
    except InvalidOperation as exc:
        raise ValueError(f"{assembly_label} has an invalid Resource quantity.") from exc
    if not resource or not unit or not quantity.is_finite() or quantity <= 0:
        raise ValueError(f"{assembly_label} needs a Resource number, quantity, and unit.")

    seed_match = re.search(r"^[ \t]*Seed\s*Type[ \t]*:[ \t]*(.+)$", block, re.IGNORECASE | re.MULTILINE)
    if not seed_match:
        raise ValueError(f"PO {po} is missing its seed type.")
    seed = seed_match[1].strip()
    if shipped_match := re.search(r"Shipped Weight[ \t]*:[ \t]*([\d,.]+)[ \t]*(\S+)", block, re.IGNORECASE):
        shipped_weight = f"{shipped_match[1]} {shipped_match[2]}"
    else:
        shipped_weight = ""

    line = _invoice_line("Resource", resource, f"INV{invoice_no}_{seed}_{reference}", quantity, amount, unit)
    line.update(
        AssemblyNo=assembly_no, AssemblyStatus=assembly_status,
        UsesOrderQuantity=uses_order_quantity, ShippedWeight=shipped_weight,
    )
    return line


def _invoice_lines(text, invoice_no, lookup_assembly):
    total = _labeled_amount(text, "Total")
    blocks = re.split(r"(?=^[ \t]*Lot[ \t]*[:#])", text, flags=re.IGNORECASE | re.MULTILINE)
    lines = []
    treated_orders = set()
    subtotals = Decimal(0)
    for block in blocks:
        if not re.search(r"^[ \t]*KTT[ \t]*#", block, re.IGNORECASE | re.MULTILINE):
            continue
        block = re.split(r"^[ \t]*Summary of Charges[ \t]*$", block, maxsplit=1,
                         flags=re.IGNORECASE | re.MULTILINE)[0]
        po_match = re.search(r"^[ \t]*PO[ \t]*#?[ \t]*:[ \t]*(.+)$", block, re.IGNORECASE | re.MULTILINE)
        po = po_match[1].strip() if po_match else ""
        subtotal = _labeled_amount(block, "Subtotal")
        freight = Decimal(str(_extract_freight_amount(block)))
        if freight > subtotal:
            raise ValueError(f"Freight exceeds the subtotal for PO {po or 'unknown'}.")
        subtotals += subtotal
        amount = subtotal - freight

        if re.search(r"\b(?:left unprocessed|unprocessed seed)\b", block, re.IGNORECASE):
            if amount:
                lines.append(_invoice_line("G/L Account", "609100",
                                           f"INV{invoice_no}_{po}_Unprocessed Seed", Decimal(1), amount))
        elif amount or not freight:
            if not po:
                raise ValueError("An invoice section is missing its PO number.")
            line = _treated_line(block, invoice_no, po, amount, lookup_assembly)
            if line["AssemblyNo"] in treated_orders:
                raise ValueError(f"Assembly {line['AssemblyNo']} appears in more than one treatment section.")
            treated_orders.add(line["AssemblyNo"])
            lines.append(line)
        if freight:
            lines.append(_invoice_line("G/L Account", "609100",
                                       f"INV{invoice_no}_{po}_Freight", Decimal(1), freight))

    if not lines or subtotals != total:
        raise ValueError("The extracted section subtotals must add up to the PDF invoice total.")
    return lines, total


def extract_kamterter_us_data_from_bytes(pdf_files, lookup_assembly):
    grouped_results = {}

    for filename, pdf_bytes in pdf_files:
        invoice_no = doc_date = ""
        page_count = 0
        try:
            with fitz.open(stream=pdf_bytes, filetype="pdf") as doc:
                page_count = doc.page_count
                text = "\n".join(page.get_text() for page in doc)
            invoice = re.search(r"Invoice\s*(?:#|No\.?)[ \t:]*([0-9]+)", text, re.IGNORECASE)
            date = re.search(r"Invoiced\s*Date[ \t:]*([0-9]{1,2}/[0-9]{1,2}/[0-9]{4})", text, re.IGNORECASE)
            invoice_no = invoice[1] if invoice else ""
            doc_date = date[1] if date else ""
            if not invoice_no or not doc_date:
                raise ValueError("The PDF is missing a readable invoice number or date.")
            lines, total = _invoice_lines(text, invoice_no, lookup_assembly)
            lines[0]["InvoiceTotal"] = str(total)
        except fitz.FileDataError:
            lines = [{"IsUSWarning": True, "Description": "This invoice PDF could not be opened."}]
        except ValueError as exc:
            lines = [{"IsUSWarning": True, "Description": str(exc)}]

        for line in lines:
            line.update(VendorInvoiceNo=invoice_no, DocumentDate=doc_date, BuyFromVendorName=US_KAMTERTER_VENDOR)
        grouped_results[filename] = lines
        log_processing_event(
            vendor="Kamterter US", filename=filename,
            extraction_info={"method": "PyMuPDF", "page_count": page_count}, po_number=None,
        )

    return grouped_results
