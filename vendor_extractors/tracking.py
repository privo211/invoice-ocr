"""Shared, conservative shipment tracking suggestions for the lot review screen."""

from dataclasses import dataclass
import logging
import os
import re
import time

import fitz
import requests


SUPPORTED_VENDORS = frozenset({"seminis", "syngenta", "nunhems"})
logger = logging.getLogger(__name__)
TRACKING_LABEL = re.compile(
    r"\b(?:Inbound\s+|Shipment\s+|Package\s+)?Tracking"
    r"(?:\s+(?:Numbers?|Nos?\.?|ID))?\s*#?\s*:?", re.I
)
PO_LABEL = re.compile(
    r"^\s*(?:Customer\s+)?(?:Purchase\s+Order|P\.?\s*O\.?)"
    r"(?:\s+(?:Number|No\.?))?\s*#?\s*:?", re.I
)
INVOICE_LABEL = re.compile(
    r"^\s*Invoice(?:\s+(?:Number|No\.?))?\s*#?\s*:?"
    r"(?:\s*/\s*Date)?", re.I
)
OTHER_FIELD_LABEL = re.compile(
    r"\b(?:BOL(?:\s*/\s*CMR)?|CMR|Delivery|Invoice|Shipment|Order|Carrier|"
    r"Purchase|Customer|Freight|Page|Lot|Batch|Ship\s+Date)\b", re.I
)


@dataclass
class TextLine:
    text: str
    x0: float
    y0: float
    x1: float
    y1: float


@dataclass
class PageText:
    page: int
    text: str
    lines: list[TextLine]


def _native_page(page, number):
    lines = []
    for block in page.get_text("dict").get("blocks", []):
        for line in block.get("lines", []):
            text = "".join(span.get("text", "") for span in line["spans"]).strip()
            if text:
                lines.append(TextLine(text, *line["bbox"]))
    return PageText(number, page.get_text(), lines)


def _extract_azure_pages(pdf_bytes):
    """Keep Azure line positions so a value under a table heading stays associated."""
    endpoint = os.getenv("AZURE_ENDPOINT", "").rstrip("/")
    key = os.getenv("AZURE_KEY")
    if not endpoint or not key:
        raise ValueError("Azure OCR credentials are not configured")
    headers = {"Ocp-Apim-Subscription-Key": key}
    response = requests.post(
        f"{endpoint}/formrecognizer/documentModels/prebuilt-layout:analyze",
        params={"api-version": "2023-07-31"},
        headers={**headers, "Content-Type": "application/pdf"},
        data=pdf_bytes, timeout=30,
    )
    response.raise_for_status()
    if response.status_code != 202:
        raise RuntimeError("Azure did not accept tracking OCR")
    operation_url = response.headers["Operation-Location"]
    for _ in range(30):
        result_response = requests.get(operation_url, headers=headers, timeout=15)
        result_response.raise_for_status()
        result = result_response.json()
        if result.get("status") == "failed":
            raise RuntimeError("Tracking OCR analysis failed")
        if result.get("status") == "succeeded":
            pages = []
            for page in result.get("analyzeResult", {}).get("pages", []):
                lines = []
                for line in page.get("lines", []):
                    polygon = line.get("polygon", [])
                    if len(polygon) < 8:
                        continue
                    # Normalize Azure's inch/pixel units to points for the proximity rules.
                    scale = 72 if page.get("unit") == "inch" else 1
                    xs = [x * scale for x in polygon[::2]]
                    ys = [y * scale for y in polygon[1::2]]
                    lines.append(TextLine(line["content"], min(xs), min(ys), max(xs), max(ys)))
                pages.append(PageText(
                    page["pageNumber"], "\n".join(line.text for line in lines), lines
                ))
            return pages
        time.sleep(1)
    raise TimeoutError("Tracking OCR timed out")


def _read_document(pdf_bytes):
    """OCR low-text pages; an incomplete document must not produce a suggestion."""
    try:
        with fitz.open(stream=pdf_bytes, filetype="pdf") as pdf:
            pages = [_native_page(page, i) for i, page in enumerate(pdf, 1)]
            missing_pages = {
                i for i, page in enumerate(pdf, 1)
                if ((len(pages[i - 1].text.strip()) < 50
                     and (page.get_images() or page.get_drawings()))
                    or sum((fitz.Rect(image["bbox"]) & page.rect).get_area()
                           for image in page.get_image_info()) > page.rect.get_area() * .1)
            }
    except Exception:
        pages, missing_pages = [], {1}
    if not missing_pages:
        return pages, False
    try:
        ocr_pages = _extract_azure_pages(pdf_bytes)
        if not pages:
            return ocr_pages, not bool(ocr_pages)
        by_number = {page.page: page for page in ocr_pages if page.lines}
        incomplete = not missing_pages.issubset(by_number)
        combined = []
        for page in pages:
            ocr = by_number.get(page.page) if page.page in missing_pages else None
            combined.append(PageText(page.page, page.text + "\n" + ocr.text,
                                     page.lines + ocr.lines) if ocr else page)
        return combined, incomplete
    except (requests.RequestException, ValueError, RuntimeError, TimeoutError, KeyError):
        # Failure affects suggestions only, never the existing invoice workflow.
        logger.warning("Tracking OCR unavailable; leaving tracking for manual review")
        return pages, True


def _field_values(page, pattern):
    """Read an inline value, or the closest aligned value beside/below its label."""
    values = []
    for line in page.lines:
        match = pattern.search(line.text)
        if not match:
            continue
        tail = line.text[match.end():].strip(" :#\t")
        if tail:
            values.append(tail)
            continue
        height = max(line.y1 - line.y0, 5)
        # Isolate the label when an OCR line contains several table headings.
        char_width = (line.x1 - line.x0) / max(len(line.text), 1)
        label_x0 = line.x0 + match.start() * char_width
        label_x1 = line.x0 + match.end() * char_width
        beside = []
        below = []
        for other in page.lines:
            if other is line:
                continue
            same_row = abs((other.y0 + other.y1 - line.y0 - line.y1) / 2) < height * .5
            if (same_row and 0 <= other.x0 - label_x1 <= 180
                    and re.search(r"\d", other.text) and not OTHER_FIELD_LABEL.search(other.text)):
                beside.append(other)
            gap = other.y0 - line.y1
            label_center = (label_x0 + label_x1) / 2
            other_center = (other.x0 + other.x1) / 2
            aligned = (other.x0 <= label_center <= other.x1
                       or label_x0 - height <= other_center <= label_x1 + height)
            if aligned and -height * .2 <= gap <= height * 2.5:
                below.append(other)
        if beside:
            values.append(min(beside, key=lambda other: other.x0).text)
        elif below:
            values.append(min(below, key=lambda other: (other.y0, abs(other.x0 - label_x0))).text)
    return values


def _tracking_numbers(value):
    numbers = []
    # Explicit labels establish meaning. A carrier-like number elsewhere never qualifies.
    tokens = re.split(r"[\s,;]+", value.upper().strip())
    if not tokens or any(not re.fullmatch(r"[A-Z0-9]{8,40}", token) for token in tokens):
        return []
    for token in tokens:
        if not re.search(r"\d", token):
            continue
        if token.startswith("1Z") and not re.fullmatch(r"1Z[A-Z0-9]{16}", token):
            continue
        if token.isdigit() and not 8 <= len(token) <= 34:
            continue
        numbers.append(token)
    return numbers


def _carrier(text, number):
    if number.startswith("1Z"):
        return "UPS"
    if re.search(r"FEDEX\s+FREIGHT", text, re.I):
        return "FedEx Freight"
    if re.search(r"\bFEDEX\b", text, re.I):
        return "FedEx"
    return ""


def extract_tracking_documents(pdf_files):
    """Return shipment evidence from every PDF, including non-invoice attachments."""
    documents = []
    for filename, pdf_bytes in pdf_files:
        pages, incomplete = _read_document(pdf_bytes)
        text = "\n".join(page.text for page in pages)
        numbers, sources, purchase_orders, invoice_numbers = set(), [], set(), set()
        for page in pages:
            for value in _field_values(page, TRACKING_LABEL):
                for number in _tracking_numbers(value):
                    numbers.add(number)
                    source = {"filename": filename, "page": page.page,
                              "carrier": _carrier(page.text, number)}
                    if source not in sources:
                        sources.append(source)
            for value in _field_values(page, PO_LABEL):
                purchase_orders.update(re.findall(r"(?<!\d)\d{5}(?!\d)", value))
            for value in _field_values(page, INVOICE_LABEL):
                if match := re.match(r"([A-Z0-9-]{6,40})(?![A-Z0-9])", value.upper()):
                    invoice_numbers.add(match[1])
        if numbers or incomplete:
            documents.append({
                "filename": filename, "text": text.upper(), "numbers": sorted(numbers),
                "sources": sources, "purchase_orders": purchase_orders,
                "invoice_numbers": invoice_numbers, "incomplete": incomplete,
            })
    return documents


def _identifier(value):
    return re.sub(r"\s+", "", str(value or "")).upper()


def _contains_identifier(text, identifier):
    return bool(identifier and re.search(
        rf"(?<![A-Z0-9_]){re.escape(identifier)}(?![A-Z0-9_])", text
    ))


def _contains_lot(text, lot, vendor):
    if _contains_identifier(text, lot):
        return True
    if vendor == "seminis" and re.fullmatch(r"\d{9,10}/\d{2,4}", lot):
        # A shipment may omit the invoice's packaging suffix; never accept a different suffix.
        base = lot.split("/")[0]
        return bool(re.search(rf"(?<![A-Z0-9_]){base}(?![A-Z0-9_/])", text))
    if vendor == "nunhems" and re.fullmatch(r"\d{11}(?:_\d{3})?", lot):
        base = lot.split("_")[0]
        return bool(re.search(rf"(?<![A-Z0-9_]){base}(?:_\d{{3}})?(?![A-Z0-9_])", text))
    return False


def _matches(item, document, vendor):
    lot = _identifier(item.get("VendorLot") or item.get("VendorLotNo") or item.get("VendorProductLot"))
    batch = _identifier(item.get("VendorBatch") or item.get("VendorBatchNo") or item.get("VendorBatchLot"))
    lot_matches = len(lot) >= 6 and _contains_lot(document["text"], lot, vendor)
    batch_matches = len(batch) >= 6 and _contains_identifier(document["text"], batch)
    if not (lot_matches or batch_matches):
        return False
    po = re.findall(r"(?<!\d)\d{5}(?!\d)", str(item.get("PurchaseOrder") or ""))
    if len(po) == 1 and document["purchase_orders"] and po[0] not in document["purchase_orders"]:
        return False
    invoice = _identifier(item.get("VendorInvoiceNo"))
    if invoice and document["invoice_numbers"] and invoice not in document["invoice_numbers"]:
        return False
    return True


def prefill_tracking_numbers(grouped_results, pdf_files, vendor):
    """Suggest one supported tracking value only when a shipment matches the lot/batch."""
    if vendor not in SUPPORTED_VENDORS:
        return grouped_results
    documents = extract_tracking_documents(pdf_files)
    for items in grouped_results.values():
        for item in items:
            item.update(TrackingPrefillEnabled=True, InboundTrackingNo="",
                        InboundTrackingSources=[], InboundTrackingWarning="")
            matches = [document for document in documents if _matches(item, document, vendor)]
            if not matches:
                continue
            numbers = {number for document in matches for number in document["numbers"]}
            sources = []
            for document in matches:
                for source in document["sources"]:
                    if source not in sources:
                        sources.append(source)
            item["InboundTrackingSources"] = sources
            if any(document["incomplete"] for document in matches):
                item["InboundTrackingWarning"] = "Shipment information could not be fully read. Enter tracking manually."
            elif any(len(document["purchase_orders"]) > 1 or len(document["invoice_numbers"]) > 1
                     for document in matches):
                item["InboundTrackingWarning"] = "This document contains several orders or invoices. Enter tracking manually."
            elif len(numbers) > 1:
                item["InboundTrackingWarning"] = "Several tracking numbers match this lot. Enter the correct tracking number manually."
            elif len(numbers) == 1:
                item["InboundTrackingNo"] = next(iter(numbers))
    return grouped_results


def merge_tracking_metadata(existing, incoming):
    """Keep financial aggregation intact, but never inherit tracking from only one line."""
    if not (existing.get("TrackingPrefillEnabled") or incoming.get("TrackingPrefillEnabled")):
        return
    existing["TrackingPrefillEnabled"] = True
    sources = existing.setdefault("InboundTrackingSources", [])
    for source in incoming.get("InboundTrackingSources", []):
        if source not in sources:
            sources.append(source)
    first, second = existing.get("InboundTrackingNo", ""), incoming.get("InboundTrackingNo", "")
    same_po = _identifier(existing.get("PurchaseOrder")) == _identifier(incoming.get("PurchaseOrder"))
    same_invoice = _identifier(existing.get("VendorInvoiceNo")) == _identifier(incoming.get("VendorInvoiceNo"))
    if first and first == second and same_po and same_invoice:
        return
    existing["InboundTrackingNo"] = ""
    warnings = [existing.get("InboundTrackingWarning"), incoming.get("InboundTrackingWarning")]
    existing["InboundTrackingWarning"] = next((warning for warning in warnings if warning),
        "Tracking could not be uniquely matched to every combined line. Enter tracking manually.")
