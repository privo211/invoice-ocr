# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What This Is

A Flask web app for extracting invoice data from PDF documents via OCR (Azure Document Intelligence) or text extraction (PyMuPDF). Extracted data is matched against Microsoft Dynamics 365 Business Central purchase orders. Used internally at Stokes Seeds.

## Running

```bash
# Install dependencies
pip install -r requirements.txt

# Run the app (requires .env with Azure/BC credentials)
python app.py
```

**Required environment variables:** `SECRET_KEY`, `AZURE_TENANT_ID`, `AZURE_CLIENT_ID`, `AZURE_CLIENT_SECRET`, `BC_COMPANY`, `BC_ENV` (optional, defaults to SANDBOX-25C), `AZURE_ENDPOINT`, `AZURE_KEY`

**Required services:** PostgreSQL on localhost (database: `invoice_ocr`)

## Architecture

- **`app.py`** — Main Flask app. Handles authentication (MSAL/Azure AD), file upload, BC API integration (fetching PO items, posting invoices), and routes for each vendor's results page. MSAL token cache is stored in PostgreSQL.

- **`vendor_extractors/`** — One module per vendor (sakata, syngenta, seminis, nunhems, hm_clause, kamterter). Each extracts structured invoice data from PDF bytes. They handle both text-based extraction (PyMuPDF) and OCR fallback (Azure Document Intelligence). Each extractor returns vendor-specific data structures and has its own parsing logic for that vendor's invoice format.

- **`db_logger.py`** — PostgreSQL logging. Tracks processing events (vendor, PO number, extraction method, page count) and maintains lifetime statistics. Tables: `processing_log` (last 100 entries, auto-pruned), `lifetime_stats` (running counters).

- **`templates/`** — One results template per vendor plus `index.html` (upload page) and `logs.html` (stats dashboard).

## Key Patterns

- Vendor extractors expose a consistent interface: `extract_<vendor>_data_from_bytes(pdf_bytes)` returns extracted fields. Some vendors also export a `find_best_<vendor>_package_description()` for fuzzy-matching item descriptions against BC.

- PDF processing tries PyMuPDF text extraction first; falls back to Azure OCR if text is insufficient. The extraction method is logged for stats tracking.

- BC API calls use client credentials flow via MSAL. The `get_bc_env()` function routes vendors to Production vs sandbox environments.

- All vendors currently route to Production for BC API calls.
