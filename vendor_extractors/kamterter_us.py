"""US Kamterter invoice extraction and posted assembly matching."""

from collections import defaultdict
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
import re
from xml.etree.ElementTree import ParseError
from zipfile import BadZipFile

import fitz
from openpyxl import load_workbook
from openpyxl.utils.exceptions import InvalidFileException

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


def _assembly_rows(workbook, sheet, fields):
    rows = workbook[sheet].values
    for row in rows:
        if all(field in row for field in fields):
            positions = [row.index(field) for field in fields]
            return ([record[i] for i in positions] for record in rows)
    raise ValueError("The assembly export is missing required columns.")


def load_kamterter_us_assemblies(stream):
    """Index Resource lines by the original PO in a TR-ASSEMBLY export."""
    if stream is None:
        return {}

    try:
        workbook = load_workbook(stream, read_only=True, data_only=True, keep_links=False)
    except (BadZipFile, InvalidFileException, OSError, ParseError, ValueError) as exc:
        raise ValueError("Select a valid TR-ASSEMBLY Excel export.") from exc

    try:
        resources = defaultdict(list)
        for document, line_type, number, quantity, unit in _assembly_rows(
            workbook, "911 Posted Assembly Line",
            ("Document No.", "Type", "No.", "Quantity", "Unit of Measure Code"),
        ):
            if document and line_type == "Resource":
                resources[str(document).strip()].append((str(number or "").strip(), quantity, unit))

        assemblies = defaultdict(dict)
        for document, order, customer_po, reversed_order in _assembly_rows(
            workbook, "910 Posted Assembly Header", ("No.", "Order No.", "CustomerPO", "Reversed"),
        ):
            if not document or str(reversed_order).strip().lower() in {"true", "yes", "1"}:
                continue
            document = str(document).strip()
            for key in {_po_key(order), _po_key(customer_po)} - {""}:
                assemblies[key][document] = resources[document]
        return assemblies
    except (KeyError, ParseError) as exc:
        raise ValueError("The export must include posted assembly headers and lines.") from exc
    finally:
        workbook.close()


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


def _treated_line(block, invoice_no, po, amount, assemblies):
    matches = assemblies.get(_po_key(po), {})
    if len(matches) != 1:
        raise ValueError(f"PO {po} needs one matching posted assembly in the export.")

    assembly_no, resources = next(iter(matches.items()))
    if len(resources) != 1:
        raise ValueError(f"Posted assembly {assembly_no} needs exactly one Resource line.")

    resource, raw_quantity, unit = resources[0]
    try:
        quantity = Decimal(str(raw_quantity).replace(",", ""))
    except InvalidOperation as exc:
        raise ValueError(f"Posted assembly {assembly_no} has an invalid Resource quantity.") from exc
    if not resource or not unit or not quantity.is_finite() or quantity <= 0:
        raise ValueError(f"Posted assembly {assembly_no} needs a Resource number, quantity, and unit.")

    seed_match = re.search(r"^[ \t]*Seed\s*Type[ \t]*:[ \t]*(.+)$", block, re.IGNORECASE | re.MULTILINE)
    if not seed_match:
        raise ValueError(f"PO {po} is missing its seed type.")
    seed = seed_match[1].strip()
    reference = _po_key(po)
    if shipped_match := re.search(r"Shipped Weight[ \t]*:[ \t]*([\d,.]+)[ \t]*(\S+)", block, re.IGNORECASE):
        shipped_weight = f"{shipped_match[1]} {shipped_match[2]}"
    else:
        shipped_weight = ""

    line = _invoice_line("Resource", resource, f"INV{invoice_no}_{seed}_{reference}", quantity, amount, unit)
    line.update(AssemblyNo=assembly_no, ShippedWeight=shipped_weight)
    return line


def _invoice_lines(text, invoice_no, assemblies):
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
            line = _treated_line(block, invoice_no, po, amount, assemblies)
            if line["AssemblyNo"] in treated_orders:
                raise ValueError(f"Posted assembly {line['AssemblyNo']} appears in more than one treatment section.")
            treated_orders.add(line["AssemblyNo"])
            lines.append(line)
        if freight:
            lines.append(_invoice_line("G/L Account", "609100",
                                       f"INV{invoice_no}_{po}_Freight", Decimal(1), freight))

    if not lines or subtotals != total:
        raise ValueError("The extracted section subtotals must add up to the PDF invoice total.")
    return lines, total


def extract_kamterter_us_data_from_bytes(pdf_files, assemblies=None):
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
            lines, total = _invoice_lines(text, invoice_no, assemblies or {})
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
