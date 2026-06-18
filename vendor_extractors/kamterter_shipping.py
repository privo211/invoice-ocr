import os
import re
from datetime import date, timedelta
import fitz  # PyMuPDF
import requests

try:
    from db_logger import log_processing_event
except Exception:  # ponytail: logging optional; ignore if unavailable
    def log_processing_event(*args, **kwargs):
        return None


def _is_business_day(d: date) -> bool:
    return d.weekday() < 5  # 0=Mon .. 6=Sun


def add_business_days_inclusive(shipped_date: date, days: int = 5) -> date:
    """Add business days counting the first business day on/after shipped_date as day 1.

    - Weekends (Sat/Sun) are skipped.
    - No holidays considered.
    """
    if days <= 1:
        # Return the first business day on/after shipped_date
        d = shipped_date
        while not _is_business_day(d):
            d += timedelta(days=1)
        return d

    # Find first business day on/after shipped_date and count as day 1
    d = shipped_date
    while not _is_business_day(d):
        d += timedelta(days=1)

    count = 1
    while count < days:
        d += timedelta(days=1)
        if _is_business_day(d):
            count += 1
    return d


# --- OCR SUPPORT (pattern reused from other vendors) ---
AZURE_ENDPOINT = os.getenv("AZURE_ENDPOINT")
AZURE_KEY = os.getenv("AZURE_KEY")


def _extract_text_with_azure_ocr(pdf_content: bytes) -> str:
    if not AZURE_ENDPOINT or not AZURE_KEY:
        raise ValueError("Azure OCR credentials (AZURE_ENDPOINT / AZURE_KEY) are not set.")

    headers = {"Ocp-Apim-Subscription-Key": AZURE_KEY, "Content-Type": "application/pdf"}
    resp = requests.post(
        f"{AZURE_ENDPOINT}formrecognizer/documentModels/prebuilt-layout:analyze?api-version=2023-07-31",
        headers=headers,
        data=pdf_content,
        timeout=30,
    )
    if resp.status_code != 202:
        raise RuntimeError(f"Azure OCR request failed: {resp.text}")

    op_url = resp.headers.get("Operation-Location")
    if not op_url:
        raise RuntimeError("Azure OCR missing Operation-Location header")

    # ponytail: simple poll; fine for small docs. Upgrade to event grid if needed.
    for _ in range(30):
        r = requests.get(op_url, headers={"Ocp-Apim-Subscription-Key": AZURE_KEY}, timeout=15)
        j = r.json()
        st = j.get("status")
        if st == "succeeded":
            lines = [
                ln.get("content", "").strip()
                for pg in j.get("analyzeResult", {}).get("pages", [])
                for ln in pg.get("lines", [])
                if ln.get("content", "").strip()
            ]
            return "\n".join(lines)
        if st == "failed":
            raise RuntimeError("Azure OCR analysis failed")
    raise TimeoutError("Azure OCR timed out")


def _extract_text_with_fallback(pdf_bytes: bytes):
    """Return (method, text, page_count) using PyMuPDF then OCR fallback.

    method = 'text' or 'ocr'
    """
    method = "text"
    text = ""
    page_count = 0
    try:
        with fitz.open(stream=pdf_bytes, filetype="pdf") as doc:
            texts: list[str] = []
            for page in doc:
                getter = getattr(page, "get_text", None)
                if callable(getter):
                    val = getter()
                    texts.append(str(val))
                else:
                    texts.append(str(page))
            text = "".join(texts)
            page_count = getattr(doc, "page_count", len(texts) if texts else 0)
    except Exception:
        # If PyMuPDF fails, try OCR directly
        try:
            text = _extract_text_with_azure_ocr(pdf_bytes)
            method = "ocr"
        except Exception:
            text = ""
            method = "text"
            page_count = 0
            return method, text, page_count

    # Low-searchable-text threshold triggers OCR
    if len(text.strip()) < 200:
        try:
            ocr_text = _extract_text_with_azure_ocr(pdf_bytes)
            if ocr_text.strip():
                text = ocr_text
                method = "ocr"
        except Exception:
            pass
    return method, text, page_count


def _replace_unicode_dashes(s: str) -> str:
    # Common Unicode dashes
    return s.replace("\u2013", "-").replace("\u2014", "-").replace("\u2212", "-")


def _parse_date_shipped(text: str) -> str | None:
    # ponytail: naive header scan; robust enough for current format. Consider layout parsing if needed later.
    m = re.search(r"Date\s*Shipped:?\s*(\d{1,2}/\d{1,2}/\d{4})", text, re.IGNORECASE)
    return m.group(1) if m else None


def _parse_customer_po(text: str) -> str | None:
    # Strategy: find header line containing 'Cust. PO#' and take the next non-empty line as the value.
    # ponytail: table-agnostic; upgrade to bbox-based extraction if this fails on variants.
    lines = [ln.strip() for ln in text.splitlines()]
    for i, ln in enumerate(lines):
        if re.search(r"Cust\.?\s*PO#", ln, re.IGNORECASE):
            # search next non-empty line(s)
            for j in range(i + 1, min(i + 10, len(lines))):
                val = lines[j].strip()
                if not val:
                    continue
                # Avoid picking another header-y line
                if re.search(r"^(Item|Description|Qty|Quantity|Ship|Subtotal|Total)\b", val, re.IGNORECASE):
                    continue
                # ponytail: skip known table header fragments seen after 'Cust. PO#' in shipping report
                # e.g., 'SD CNT/LB# PKG' or lines containing PKG/LB# tokens
                if re.search(r"(\bSD\b.*\bCNT\b)|\bLB#\b|\bPKG\b", val, re.IGNORECASE):
                    continue
                norm = _replace_unicode_dashes(val).upper().replace(" ", "")
                # Require canonical PO shape like 123-ABC-DE; otherwise keep scanning
                if re.fullmatch(r"\d{3}-[A-Z]{3}-[A-Z]{2}", norm):
                    return norm
            # If we didn't return from nearby lines, try a global search as fallback
            m = re.search(r"\b(\d{3})\s*[-\u2013]\s*([A-Za-z]{3})\s*[-\u2013]\s*([A-Za-z]{2})\b", text)
            if m:
                return f"{m.group(1)}-{m.group(2).upper()}-{m.group(3).upper()}"
    return None


def extract_kamterter_shipping_data_from_bytes(pdf_files):
    """Extract Kamterter shipping info per file.

    Returns a dict keyed by filename with fields:
      - date_shipped (MM/DD/YYYY)
      - customer_po (normalized)
      - est_date_from_treater (YYYY-MM-DD)
      - extraction_method ('text' | 'ocr')
      - page_count (int)
      - errors (list[str])
    """
    results = {}

    for filename, pdf_bytes in pdf_files:
        method, text, page_count = _extract_text_with_fallback(pdf_bytes)

        errors: list[str] = []
        date_shipped_str = _parse_date_shipped(text) if text else None
        customer_po = _parse_customer_po(text) if text else None

        est_iso = None
        if not date_shipped_str:
            errors.append("Missing 'Date Shipped'")
        if not customer_po:
            errors.append("Missing 'Cust. PO#'")

        if date_shipped_str and customer_po:
            # Compute 5 business days from shipped date (inclusive) and return ISO
            m, d, y = map(int, date_shipped_str.split("/"))
            shipped_dt = date(year=y, month=m, day=d)
            est_dt = add_business_days_inclusive(shipped_dt, 5)
            est_iso = est_dt.isoformat()

        # Assemble record
        rec = {
            "date_shipped": date_shipped_str,
            "customer_po": customer_po,
            "est_date_from_treater": est_iso,
            "extraction_method": method,
            "page_count": page_count,
            "errors": errors,
        }
        results[filename] = rec

        # Optional logging
        try:
            log_processing_event(
                vendor="kamterter_shipping",
                filename=filename,
                extraction_info={"method": ("Azure OCR" if method == "ocr" else "PyMuPDF"), "page_count": page_count},
                po_number=customer_po,
            )
        except Exception:
            pass

    return results
