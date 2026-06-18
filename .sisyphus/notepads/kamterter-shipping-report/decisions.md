## Decisions

### 2026-06-18
- Added `pytest>=8.0` to requirements.txt — follows existing `>=` version style.
- Created `tests/test_kamterter_shipping.py` as the sole harness file.
- Implemented a `no_network` context manager and an optional `enable_no_network_via_monkeypatch(monkeypatch)` helper.
  - ponytail: kept scope to blocking `requests.*` only; upgrade to socket-level block if later tests require.

### 2026-06-18 (runtime)
- Return schema uses extraction_method 'text'/'ocr' while DB log records 'PyMuPDF'/'Azure OCR' for stats compatibility.
- Business-day rule: shipped date counts as day 1 on the next business day if weekend; holidays ignored.
- Logging is best-effort wrapped in try/except to keep offline tests green and prevent DB coupling.

### 2026-06-18 (UI)
- Added a new dropdown option value "kamterter_shipping" labeled "Kamterter: Shipping Report" adjacent to Kamterter invoice; form action unchanged.  
  ponytail: no conditional UI logic; routing will distinguish values server-side.
- Created isolated template results_kamterter_shipping.html without any invoice creation references.  
  ponytail: minimal inline fetch to POST JSON to /update-kamterter-shipping-report with customer_po and est_date_from_treater only; extend payload later if needed.
- Tests assert presence of the new option and absence of /create-purchase-invoice in the shipping template; keeps checks offline-only.

### 2026-06-18 (BC sandbox update)
- Route `/update-kamterter-shipping-report` is sandbox-only (SANDBOX-25C) and hard-codes Company('Stokes%20Seeds%20Limited'); never calls get_bc_env().
- Added helpers: `_odata_quote` (escape single quotes) and `_bc_error_message` (extract JSON error.message) to minimize duplication.
- Tests verify source markers (SANDBOX-25C, entity key path, If-Match, 412 mention) and validate `_odata_quote` via safe import with stubs.
