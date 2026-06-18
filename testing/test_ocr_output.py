"""Diagnostic script: dump PyMuPDF text and Azure OCR text for each Sakata test PDF."""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import fitz
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

from vendor_extractors.sakata import _extract_text_with_azure_ocr

PDF_DIR = os.path.join(os.path.dirname(__file__), "sakata")

for fname in sorted(os.listdir(PDF_DIR)):
    if not fname.lower().endswith(".pdf"):
        continue
    fpath = os.path.join(PDF_DIR, fname)
    with open(fpath, "rb") as f:
        pdf_bytes = f.read()

    print(f"\n{'='*80}")
    print(f"FILE: {fname}")
    print(f"{'='*80}")

    with fitz.open(stream=pdf_bytes, filetype="pdf") as doc:
        pymupdf_text = "".join(page.get_text() for page in doc)
        text_len = len(pymupdf_text.strip())
        print(f"PyMuPDF text length: {text_len}")
        if text_len < 200:
            print("[SCANNED - needs OCR]")
            try:
                ocr_text = _extract_text_with_azure_ocr(pdf_bytes)
                print(f"--- OCR OUTPUT ({len(ocr_text)} chars) ---")
                print(ocr_text)
                print("--- END OCR OUTPUT ---")
            except Exception as e:
                print(f"OCR FAILED: {e}")
        else:
            print("[TEXT-SEARCHABLE]")
            print(f"--- PyMuPDF TEXT ---")
            print(pymupdf_text[:3000])
            print("--- END PyMuPDF TEXT ---")
