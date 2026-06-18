## Learnings

### 2026-06-18
- Pytest isn't in the repo by default; adding `pytest>=8.0` to requirements keeps style consistent (`>=` pins).
- A test module is discoverable without pytest.ini as long as it's under `tests/` and named `test_*.py`.
- Network guards can stay stdlib-only via `unittest.mock.patch`; avoids tying test helpers to pytest while still runnable under it.

### 2026-06-18 (runtime)
- Avoid importing vendor_extractors.sakata for OCR due to top-level BC token fetch; duplicated minimal Azure OCR helper locally.
- Chose low-text threshold of 200 chars to trigger OCR; matches existing extractor pattern.
- Parser keeps a naive header/next-line scan for Cust. PO#; good enough now. Upgrade: bbox/table parsing if formats vary.

### 2026-06-18 (UI)
- Templates share structure: bootstrap + dark-mode toggle + card/table layout. Reused this for shipping results to blend in.
- Offline tests should stick to string checks on templates to avoid Flask/env coupling.
- Keeping per-file record shape simple (dict) aligns with extractor output, avoids list handling in shipping UI.

### 2026-06-18 (sandbox OData route)
- OData string literals escape single quotes by doubling; a tiny helper `_odata_quote()` is enough for keys and filters.
- BC PATCH requires `If-Match` with the real `@odata.etag`; 412 means stale etag — a single refetch+retry covers it.
- Importing app.py in tests requires stubbing `db_logger` and `vendor_extractors` and seeding env vars; keeps tests offline and fast.
2026-06-18T15:34:31Z — QA run
- Sample PDF: Stokes Seeds, Ltd - Buffalo, NY - Shipping Report 6-16-2026 Shipment # 24329.pdf
- Extracted: date_shipped=06/16/2026, customer_po=174-CAR-RA, est_date_from_treater=2026-06-22
- Extraction method: text, page_count=1
- Minimal parser fix: skip header-like tokens after 'Cust. PO#' (e.g., 'SD CNT/LB# PKG', 'QUANTITY') and add fallback regex to capture PO pattern 123-ABC-DE.
- Tests: pytest tests/test_kamterter_shipping.py passed; py_compile passed.
\n+## 2026-06-18
- Added offline Flask test_client behavior tests for /update-kamterter-shipping-report.
- Reused _load_app_for_helpers() and stubbed MSAL/cache via build_msal_app/get_accounts to avoid DB/msal.
- Mocked requests.get/patch with simple DummyResp; covered happy path, validation, 0/2+ matches guard, and 412 etag retry.
- Asserted SANDBOX-25C and filter URL construction and If-Match header/payload correctness.
 
### 2026-06-18 (UI migration)
- Adopted card grid + multi-select modal for Kamterter Shipping.
- Reused Sakata card/dl fields and Seminis modal/select-all JS pattern.
- Disabled selection for errored cards by omitting them from the modal list.
