# Kamterter Shipping Report OCR & BC Sandbox Update

## TL;DR
> **Summary**: Add a separate Kamterter Shipping Report workflow to extract shipped date + customer PO, calculate the treater estimate date, and PATCH Business Central SANDBOX-25C Assembly Order data.
> **Deliverables**:
> - `vendor_extractors/kamterter_shipping.py` with PDF text/OCR extraction and 5-business-day calculation.
> - Existing upload dropdown option `Kamterter: Shipping Report` wired to a new results template.
> - Dedicated sandbox-only BC OData GET/PATCH route for `Est_Date_from_Treater`.
> - Minimal pytest coverage for parsing, date math, and BC request behavior.
> **Effort**: Medium
> **Parallel**: YES - 3 waves
> **Critical Path**: Task 1 → Task 3 → Task 4 → Final Verification

## Context
### Original Request
Implement automated OCR extraction and API integration for Kamterter Shipping Reports. Extract `Date Shipped` and `Cust. PO#`, calculate a 5-business-day estimated date, and update `Est_Date_from_Treater` in Business Central OData.

### Interview Summary
- Confirmed: do not modify `vendor_extractors/kamterter.py`.
- Confirmed: create `vendor_extractors/kamterter_shipping.py`.
- Confirmed: use existing dropdown with new option `Kamterter: Shipping Report`.
- Confirmed: add minimal pytest tests.
- Corrected: endpoint environment must be `SANDBOX-25C`, not Production.
- Date rule: shipped date counts as business day 1, so `06/16/2026` + 5 business days = `2026-06-22`.

### Metis Review (gaps addressed)
Metis delegation was unavailable for plan-family agents in this environment; Oracle gap review was used instead. Incorporated guardrails:
- Hard-pin this workflow to SANDBOX-25C; never use Production fallback.
- Escape single quotes in OData `$filter` values.
- Abort on 0 or >1 BC matches; do not guess.
- PATCH with `If-Match` using `@odata.etag`; on `412`, refetch once and retry once.
- Fail clearly on missing `Date Shipped` or `Cust. PO#`.

## Work Objectives
### Core Objective
Add the smallest separate Kamterter Shipping Report workflow that safely updates BC sandbox assembly order estimated treater dates.

### Deliverables
- New extractor module: `vendor_extractors/kamterter_shipping.py`.
- Modified upload routing in `app.py` for vendor value `kamterter_shipping`.
- New update route in `app.py`: `/update-kamterter-shipping-report`.
- New result template: `templates/results_kamterter_shipping.html`.
- Modified dropdown in `templates/index.html`.
- Minimal tests in one file: `tests/test_kamterter_shipping.py`.
- Add `pytest` to `requirements.txt` only if absent.

### Definition of Done (verifiable conditions with commands)
- `pytest tests/test_kamterter_shipping.py` passes.
- Running the sample PDF through the new dropdown extracts `Date Shipped = 06/16/2026`, `Cust. PO# = 174-CAR-RA`, `Est_Date_from_Treater = 2026-06-22`.
- BC update route sends GET to SANDBOX-25C `Assembly_Order_Excel` with `$filter=TMG_CustomerPO eq '174-CAR-RA'`.
- BC update route PATCHes exactly one matched record using `If-Match: <@odata.etag>` and payload `{"Est_Date_from_Treater":"2026-06-22"}`.
- Financial Kamterter invoice flow still imports `vendor_extractors/kamterter.py` and renders `templates/results_kamterter.html` unchanged in behavior.

### Must Have
- Use `SANDBOX-25C` in the URL for this workflow.
- Use `Company('Stokes%20Seeds%20Limited')/Assembly_Order_Excel`.
- Use stdlib date logic; skip Saturday/Sunday only.
- Count shipped date as business day 1.
- Escape OData filter string quotes.
- No live Production update path.

### Must NOT Have
- Must not modify `vendor_extractors/kamterter.py`.
- Must not reuse `/create-purchase-invoice` for shipping reports.
- Must not add a separate upload page.
- Must not add heavy test/mocking dependencies beyond pytest.
- Must not PATCH if BC GET returns 0 or multiple records.

## Verification Strategy
> ZERO HUMAN INTERVENTION - all verification is agent-executed.
- Test decision: tests-after with pytest.
- QA policy: Every task has agent-executed scenarios.
- Evidence: `.sisyphus/evidence/task-{N}-{slug}.{ext}`.
- Live/sandbox BC QA must use SANDBOX-25C only. If credentials are unavailable, mock route tests are sufficient and the final report must state sandbox live check was skipped due missing credentials.

## Execution Strategy
### Parallel Execution Waves
> Target: 5-8 tasks per wave. This project is intentionally small; fewer tasks avoid fake abstraction.

Wave 1: Task 5 pytest bootstrap, then Task 1 extractor/date logic and Task 2 UI template/dropdown shell can run in parallel.
Wave 2: Task 3 app upload branch, Task 4 BC update route; Task 4 depends on Task 1 output shape.
Wave 3: Task 6 sample/manual QA and regression checks.

### Dependency Matrix (full, all tasks)
- Task 1: blocked by Task 5 for test file setup; blocks Tasks 3, 4, 6.
- Task 2: blocked by Task 5 for test file setup; blocks Task 3 and browser QA.
- Task 3: blocked by Tasks 1, 2; blocks Task 6.
- Task 4: blocked by Task 1; blocks Task 6.
- Task 5: blocks feature-specific tests in Tasks 1-4.
- Task 6: blocked by Tasks 1-5.

### Agent Dispatch Summary
- Wave 1 → 3 tasks → quick, quick, visual-engineering.
- Wave 2 → 2 tasks → quick, unspecified-high.
- Wave 3 → 1 task → unspecified-high.

## TODOs
> Implementation + Test = ONE task. Never separate.
> EVERY task MUST have: Agent Profile + Parallelization + QA Scenarios.

- [x] 1. Add Kamterter Shipping extractor and business-day helper

  **What to do**:
  - Create `vendor_extractors/kamterter_shipping.py`.
  - Expose `extract_kamterter_shipping_data_from_bytes(pdf_files)`.
  - Accept the same `pdf_files` shape used by existing extractors in `app.py:620-889`.
  - Extract PDF text with PyMuPDF first. If text is empty/insufficient, use the same Azure OCR fallback style used by existing vendor extractors; do not create a new OCR client abstraction.
  - Parse `Date Shipped:` as `MM/DD/YYYY`; tolerate optional colon and single-digit month/day.
  - Parse first non-empty `Cust. PO#` value from the table; normalize by stripping spaces, uppercasing, and converting Unicode dashes to `-`.
  - Add helper `add_business_days_inclusive(shipped_date, days=5)` using stdlib `datetime`; shipped date is day 1 if it is a weekday; skip Saturday/Sunday; no holiday support.
  - Return grouped dict keyed by filename with fields: `date_shipped`, `customer_po`, `est_date_from_treater`, `extraction_method`, `page_count`, `errors`.
  - If date or PO is missing, return an error entry and do not invent values.
  - Add/extend `tests/test_kamterter_shipping.py` with parser and date helper tests for this task.

  **Must NOT do**:
  - Do not edit `vendor_extractors/kamterter.py`.
  - Do not add dependencies.
  - Do not support holidays.

  **Recommended Agent Profile**:
  - Category: `quick` - Reason: one new parser module with stdlib date logic.
  - Skills: [] - No browser/git skill needed.
  - Omitted: [`frontend-ui-ux`] - No UI work in this task.

  **Parallelization**: Can Parallel: YES | Wave 1 | Blocks: Tasks 3, 4, 6 | Blocked By: Task 5

  **References**:
  - Pattern: `vendor_extractors/kamterter.py:25-226` - extractor signature/return grouping style; do not modify this file.
  - Pattern: `vendor_extractors/sakata.py:750-751` - `extract_*_data_from_bytes` naming pattern.
  - Pattern: `db_logger.py:123-166` - optional processing log event shape; use vendor `kamterter_shipping` if logging.
  - Sample: `/Users/priyanshuvora/Downloads/Stokes Seeds, Ltd - Buffalo, NY - Shipping Report 6-16-2026 Shipment # 24329.pdf` - expected parse values.

  **Acceptance Criteria**:
  - [ ] `python - <<'PY'` import check succeeds for `extract_kamterter_shipping_data_from_bytes` and `add_business_days_inclusive`.
  - [ ] `add_business_days_inclusive(date(2026, 6, 16), 5).isoformat() == '2026-06-22'`.
  - [ ] Missing date or PO returns an error entry and no `est_date_from_treater` update value.

  **QA Scenarios**:
  ```
  Scenario: Happy path date math
    Tool: Bash
    Steps: Run a Python one-liner importing add_business_days_inclusive and asserting 2026-06-16 -> 2026-06-22.
    Expected: Process exits 0.
    Evidence: .sisyphus/evidence/task-1-extractor-date.txt

  Scenario: Missing fields fail safely
    Tool: Bash
    Steps: Run pytest test that monkeypatches PDF text extraction to text without Date Shipped or Cust. PO#.
    Expected: Result contains errors and no BC-ready estimated date.
    Evidence: .sisyphus/evidence/task-1-extractor-error.txt
  ```

  **Commit**: YES | Message: `feat(kamterter): add shipping report extractor` | Files: [`vendor_extractors/kamterter_shipping.py`, `tests/test_kamterter_shipping.py`]

- [x] 2. Add dropdown option and isolated shipping results template

  **What to do**:
  - In `templates/index.html`, add option value `kamterter_shipping` with label `Kamterter: Shipping Report` near the existing `kamterter` option.
  - Create `templates/results_kamterter_shipping.html`.
  - Template must display filename, extracted shipped date, customer PO, computed estimated date, extraction method, and errors.
  - Add one button per successful file: `Update Business Central`.
  - Button JS must POST JSON to `/update-kamterter-shipping-report` with `customer_po` and `est_date_from_treater`.
  - Do not reuse financial invoice fields/buttons from `results_kamterter.html`.
  - Add/extend `tests/test_kamterter_shipping.py` with dropdown/template endpoint guard assertions for this task.

  **Must NOT do**:
  - Do not create a new upload page.
  - Do not call `/create-purchase-invoice`.
  - Do not include invoice totals, G/L lines, or purchase invoice UI.

  **Recommended Agent Profile**:
  - Category: `visual-engineering` - Reason: small template/JS UI work.
  - Skills: [] - Existing HTML/JS only.
  - Omitted: [`frontend-ui-ux`] - Existing style should be copied, not redesigned.

  **Parallelization**: Can Parallel: YES | Wave 1 | Blocks: Tasks 3, 6 | Blocked By: Task 5

  **References**:
  - Pattern: `templates/index.html:198-221` - existing upload form/dropdown.
  - Pattern: `templates/results_kamterter.html:168-260` - results card/table style only.
  - Pattern: `templates/results_kamterter.html:492-500` - fetch POST pattern only; endpoint and payload must differ.

  **Acceptance Criteria**:
  - [ ] Dropdown contains both `Kamterter` and `Kamterter: Shipping Report` as distinct options.
  - [ ] Shipping template posts only `customer_po` and `est_date_from_treater` plus any filename/display context.
  - [ ] No string `/create-purchase-invoice` appears in `templates/results_kamterter_shipping.html`.

  **QA Scenarios**:
  ```
  Scenario: Shipping option visible
    Tool: Bash
    Steps: Run pytest/Flask client GET / and assert response HTML contains option value="kamterter_shipping" and label Kamterter: Shipping Report.
    Expected: Assertion passes.
    Evidence: .sisyphus/evidence/task-2-dropdown.txt

  Scenario: Wrong invoice endpoint absent
    Tool: Bash
    Steps: Run a Python check reading templates/results_kamterter_shipping.html and asserting '/create-purchase-invoice' not in file.
    Expected: Process exits 0.
    Evidence: .sisyphus/evidence/task-2-endpoint-guard.txt
  ```

  **Commit**: YES | Message: `feat(ui): add kamterter shipping report view` | Files: [`templates/index.html`, `templates/results_kamterter_shipping.html`, `tests/test_kamterter_shipping.py`]

- [x] 3. Wire upload branch in Flask app

  **What to do**:
  - In `app.py`, import `extract_kamterter_shipping_data_from_bytes` from `vendor_extractors.kamterter_shipping`.
  - In the existing `/` POST vendor chain, add `elif vendor == "kamterter_shipping"`.
  - Call `extract_kamterter_shipping_data_from_bytes(pdf_files)` and render `results_kamterter_shipping.html` with extraction results.
  - Keep existing `vendor == "kamterter"` branch unchanged.
  - Keep temp PDF save behavior only if already generic enough; shipping does not need invoice attachment temp save unless required by existing upload flow.
  - Add/extend `tests/test_kamterter_shipping.py` with Flask upload branch regression tests for this task.

  **Must NOT do**:
  - Do not change financial Kamterter branch behavior.
  - Do not route shipping into invoice creation.

  **Recommended Agent Profile**:
  - Category: `quick` - Reason: small route branch/import.
  - Skills: [] - No special skill needed.
  - Omitted: [`frontend-ui-ux`] - UI already handled.

  **Parallelization**: Can Parallel: NO | Wave 2 | Blocks: Task 6 | Blocked By: Tasks 1, 2

  **References**:
  - Pattern: `app.py:620-889` - existing upload route and vendor branching.
  - Pattern: `app.py:874-876` - current Kamterter financial branch to keep separate.

  **Acceptance Criteria**:
  - [ ] POST `/` with `vendor=kamterter_shipping` calls the new extractor and renders `results_kamterter_shipping.html`.
  - [ ] POST `/` with `vendor=kamterter` still calls `extract_kamterter_data_from_bytes` and renders `results_kamterter.html`.
  - [ ] No import or call in `vendor_extractors/kamterter.py` is changed.

  **QA Scenarios**:
  ```
  Scenario: Shipping upload routes to shipping template
    Tool: Bash
    Steps: Run pytest with Flask test_client monkeypatching extract_kamterter_shipping_data_from_bytes to return one parsed row; POST multipart to / with vendor=kamterter_shipping.
    Expected: Response status 200 and contains 174-CAR-RA and 2026-06-22.
    Evidence: .sisyphus/evidence/task-3-upload-shipping.txt

  Scenario: Financial Kamterter still separate
    Tool: Bash
    Steps: Run pytest monkeypatching financial extractor and POST vendor=kamterter.
    Expected: Financial extractor called; shipping extractor not called.
    Evidence: .sisyphus/evidence/task-3-kamterter-regression.txt
  ```

  **Commit**: YES | Message: `feat(app): route kamterter shipping uploads` | Files: [`app.py`, `tests/test_kamterter_shipping.py`]

- [x] 4. Add sandbox-only BC OData GET/PATCH update route

  **What to do**:
  - In `app.py`, add route `/update-kamterter-shipping-report` accepting POST JSON.
  - Required JSON: `customer_po`, `est_date_from_treater`.
  - Validate `est_date_from_treater` is `YYYY-MM-DD` using stdlib `datetime.date.fromisoformat`.
  - Normalize/strip PO; escape single quotes for OData filter by replacing `'` with `''`.
  - Acquire BC token using existing MSAL/session pattern from `app.py:899-909` or closest existing helper.
  - Use hard-coded base URL for this route only:
    `https://api.businesscentral.dynamics.com/v2.0/33b1b67a-786c-4b46-9372-c4e492d15cf1/SANDBOX-25C/ODataV4/Company('Stokes%20Seeds%20Limited')/Assembly_Order_Excel`
  - GET with params `$filter=TMG_CustomerPO eq '{escaped_po}'`.
  - Require exactly one `value` record. If 0 or >1, return non-2xx JSON error and do not PATCH.
  - Extract `Document_Type`, `No`, and `@odata.etag`.
  - PATCH the entity URL using key values for `Document_Type` and `No`, with `If-Match` set to the etag and JSON `{"Est_Date_from_Treater": est_date}`.
  - If PATCH returns `412`, refetch once and retry once with the new etag. If still failing, return error.
  - Return JSON including `success`, `customer_po`, `document_type`, `no`, and `est_date_from_treater`.
  - Add/extend `tests/test_kamterter_shipping.py` with BC route tests for this task.

  **Must NOT do**:
  - Do not call `get_bc_env()` for this route; it would risk Production fallback.
  - Do not PATCH on multiple matches.
  - Do not use `If-Match: *` for this route.

  **Recommended Agent Profile**:
  - Category: `unspecified-high` - Reason: external API update path needs careful error handling.
  - Skills: [] - No extra dependencies; use existing app patterns.
  - Omitted: [`playwright`] - API route task, not browser work.

  **Parallelization**: Can Parallel: NO | Wave 2 | Blocks: Task 6 | Blocked By: Task 1

  **References**:
  - Pattern: `app.py:69-123` - MSAL cache/helpers.
  - Pattern: `app.py:138-152` - timed request wrappers.
  - Pattern: `app.py:891-1117` - BC route token/session handling; do not reuse invoice semantics.
  - Pattern: `app.py:1119-1287` - OData URL/header style.
  - Endpoint: `https://api.businesscentral.dynamics.com/v2.0/33b1b67a-786c-4b46-9372-c4e492d15cf1/SANDBOX-25C/ODataV4/Company('Stokes%20Seeds%20Limited')/Assembly_Order_Excel`

  **Acceptance Criteria**:
  - [ ] Route rejects missing PO/date with 400 JSON.
  - [ ] Route rejects invalid ISO date with 400 JSON.
  - [ ] GET URL contains `SANDBOX-25C` and never `Production`.
  - [ ] OData filter escapes single quotes.
  - [ ] 0 matches and >1 matches do not PATCH.
  - [ ] Exactly one match PATCHes with `If-Match` equal to `@odata.etag` and payload field `Est_Date_from_Treater`.
  - [ ] 412 response triggers one refetch and one retry, then stops.

  **QA Scenarios**:
  ```
  Scenario: Happy path BC sandbox update
    Tool: Bash
    Steps: Run pytest monkeypatching requests.get/patch and MSAL token acquisition; POST JSON customer_po=174-CAR-RA, est_date_from_treater=2026-06-22.
    Expected: GET called with SANDBOX-25C filter; PATCH called with If-Match from etag and payload Est_Date_from_Treater=2026-06-22; route returns success JSON.
    Evidence: .sisyphus/evidence/task-4-bc-happy.txt

  Scenario: Multiple matches fail closed
    Tool: Bash
    Steps: Run pytest monkeypatching GET to return two records.
    Expected: Route returns error JSON and requests.patch is not called.
    Evidence: .sisyphus/evidence/task-4-bc-multiple.txt
  ```

  **Commit**: YES | Message: `feat(bc): update kamterter shipping estimate` | Files: [`app.py`, `tests/test_kamterter_shipping.py`]

- [x] 5. Bootstrap minimal pytest harness

  **What to do**:
  - Add `pytest` to `requirements.txt` if not already present.
  - Create `tests/test_kamterter_shipping.py` only; no pytest.ini unless pytest discovery fails.
  - Add shared no-network guard/helper fixtures if needed by later task tests.
  - Use pytest `monkeypatch`; do not add `responses`, `requests-mock`, or VCR.
  - Tests must not call Azure, Business Central, PostgreSQL, or read `.env` secrets.

  **Must NOT do**:
  - Do not create broad test scaffolding.
  - Do not add CI in this task.
  - Do not run live BC tests from pytest.

  **Recommended Agent Profile**:
  - Category: `quick` - Reason: one focused test file.
  - Skills: [] - pytest only.
  - Omitted: [`deep`] - No broad test architecture needed.

  **Parallelization**: Can Parallel: NO | Wave 1 | Blocks: Tasks 1, 2, 3, 4, 6 | Blocked By: none

  **References**:
  - Existing absence: no pytest config found; keep setup minimal.
  - Manual scripts: `testing/test_ocr_output.py`, `test_bc_post.py` - do not depend on these for automated tests.
  - Target files: `vendor_extractors/kamterter_shipping.py`, `app.py`.

  **Acceptance Criteria**:
  - [ ] `pytest tests/test_kamterter_shipping.py` exits 0.
  - [ ] Test run does not require `.env`, Azure credentials, BC credentials, or PostgreSQL.
  - [ ] Test file exists and can be discovered by pytest.
  - [ ] Later task tests can use monkeypatch without adding extra mocking dependencies.

  **QA Scenarios**:
  ```
  Scenario: Test suite passes offline
    Tool: Bash
    Steps: Run pytest tests/test_kamterter_shipping.py.
    Expected: All tests pass without network calls.
    Evidence: .sisyphus/evidence/task-5-pytest.txt

  Scenario: No external services touched
    Tool: Bash
    Steps: Run pytest with monkeypatch guards that fail if requests reaches real network for BC route tests.
    Expected: No real network call occurs.
    Evidence: .sisyphus/evidence/task-5-offline-guard.txt
  ```

  **Commit**: YES | Message: `test(kamterter): add pytest harness` | Files: [`requirements.txt`, `tests/test_kamterter_shipping.py`]

- [x] 6. Run sample pipeline and regression checks

  **What to do**:
  - Run the new extractor against `/Users/priyanshuvora/Downloads/Stokes Seeds, Ltd - Buffalo, NY - Shipping Report 6-16-2026 Shipment # 24329.pdf`.
  - Verify extracted `Date Shipped: 06/16/2026`, `Cust. PO#: 174-CAR-RA`, estimated date `2026-06-22`.
  - Run `pytest tests/test_kamterter_shipping.py`.
  - Run Flask route tests from the pytest file.
  - If credentials/session are available and safe, run one SANDBOX-25C GET for `TMG_CustomerPO eq '174-CAR-RA'` and confirm match `No == TR-01347`; only PATCH if explicitly enabled by test harness/operator and still sandbox-only.
  - Confirm `vendor_extractors/kamterter.py` diff is empty.

  **Must NOT do**:
  - Do not hit Production.
  - Do not PATCH if sandbox GET returns anything other than exactly one record.
  - Do not mark final verification complete without user approval after reviews.

  **Recommended Agent Profile**:
  - Category: `unspecified-high` - Reason: end-to-end QA and sandbox safety checks.
  - Skills: [] - Bash/pytest; browser only if manual UI verification is chosen.
  - Omitted: [`git-master`] - No commit orchestration requested unless user asks.

  **Parallelization**: Can Parallel: NO | Wave 3 | Blocks: Final Verification | Blocked By: Tasks 1-5

  **References**:
  - Sample PDF: `/Users/priyanshuvora/Downloads/Stokes Seeds, Ltd - Buffalo, NY - Shipping Report 6-16-2026 Shipment # 24329.pdf`.
  - Expected OData reference: `/Users/priyanshuvora/Downloads/Assembly_Order_OData.xlsx`.
  - Existing financial extractor guard: `vendor_extractors/kamterter.py`.

  **Acceptance Criteria**:
  - [ ] Sample extractor output exactly matches expected values.
  - [ ] Pytest suite passes.
  - [ ] No diff exists in `vendor_extractors/kamterter.py`.
  - [ ] Any sandbox live check evidence includes URL with `SANDBOX-25C`.

  **QA Scenarios**:
  ```
  Scenario: Sample PDF extraction
    Tool: Bash
    Steps: Run a Python script invoking extract_kamterter_shipping_data_from_bytes on the sample PDF path.
    Expected: date_shipped=06/16/2026, customer_po=174-CAR-RA, est_date_from_treater=2026-06-22.
    Evidence: .sisyphus/evidence/task-6-sample-extraction.txt

  Scenario: Production guard
    Tool: Bash
    Steps: Run pytest assertion or source check that update route URL contains SANDBOX-25C and not Production.
    Expected: Assertion passes.
    Evidence: .sisyphus/evidence/task-6-production-guard.txt
  ```

  **Commit**: YES | Message: `chore(kamterter): verify shipping report flow` | Files: [no source files unless fixes are required]

- [x] 7. Refine Kamterter Shipping results UI with card multi-select update flow

  **What to do**:
  - Replace `templates/results_kamterter_shipping.html` table UI with the same refined Bootstrap card field layout used by other result pages.
  - Use a `row row-cols-1 row-cols-md-2 g-4` card grid if multiple shipping records are present.
  - Each card title/header must show the filename, like the other results pages.
  - Each card body must show only these visible fields: `Shipped Date`, `Customer PO`, and `Estimated Date From Treater`.
  - Rename visible label `Estimated Date` to `Estimated Date From Treater`.
  - Remove visible `Extraction Method`, `Errors`, and `Action` table/column UI.
  - Do not render a per-card update button.
  - Add one global `Update Selected in BC` button after all cards.
  - Add a Bootstrap multi-select modal like `results_seminis.html` uses for lot creation: checkbox list, select/deselect all checkbox, confirm button, cancel button.
  - Modal list rows should use card data attributes and show a concise label such as `{filename} — {customer_po} — {est_date_from_treater}`.
  - Cards with extraction errors may remain visible, but must not be selectable/updatable; do not show an error column. Use disabled modal checkbox or omit those cards from the modal list.
  - Confirm button should iterate selected cards and POST each selected payload to `/update-kamterter-shipping-report` using existing `customer_po` and `est_date_from_treater` fields.
  - Preserve existing success/failure behavior: show an in-card status badge when an update succeeds and keep failed update messages actionable.
  - Update `tests/test_kamterter_shipping.py` template assertions to require the new UI contract.

  **Must NOT do**:
  - Do not change `app.py`, BC route behavior, or extractor logic.
  - Do not reintroduce table markup in `results_kamterter_shipping.html`.
  - Do not show `Extraction Method` or an `Errors` field/column in the visible card layout.
  - Do not add dependencies.
  - Do not perform live BC PATCH during UI tests.

  **Recommended Agent Profile**:
  - Category: `visual-engineering` - Reason: UI refinement using existing card/modal patterns.
  - Skills: [`frontend-ui-ux`] - Use existing refined results page visual language; no redesign beyond requested layout.
  - Omitted: [`playwright`] - Browser QA belongs in final/manual verification unless app can be launched safely.

  **Parallelization**: Can Parallel: NO | Wave 4 | Blocks: Final Verification | Blocked By: Task 6

  **References**:
  - Pattern: `templates/results_sakata.html:265-331` - card grid and `dl` field layout.
  - Pattern: `templates/results_seminis.html:499-513` - multi-select modal markup with select/deselect all.
  - Pattern: `templates/results_seminis.html:810-858` - JS populates modal from cards and processes selected checkboxes.
  - Current target: `templates/results_kamterter_shipping.html:71-128` - replace table/per-row action UI.
  - Current JS target: `templates/results_kamterter_shipping.html:157-207` - adapt from per-button listener to global modal selected-card processing.

  **Acceptance Criteria**:
  - [ ] `templates/results_kamterter_shipping.html` contains no `<table` markup.
  - [ ] Visible label `Estimated Date From Treater` appears.
  - [ ] Visible labels/text `Extraction Method` and `Errors` do not appear.
  - [ ] Only one global update trigger exists for all cards.
  - [ ] Multi-select modal includes select/deselect all, selected item checkboxes, confirm, and cancel controls.
  - [ ] Confirming selected items POSTs to `/update-kamterter-shipping-report` with per-card `customer_po` and `est_date_from_treater`.
  - [ ] `python3 -m pytest -q tests/test_kamterter_shipping.py` passes.

  **QA Scenarios**:
  ```
  Scenario: Shipping template uses refined card layout
    Tool: Bash
    Steps: Run pytest assertions reading `templates/results_kamterter_shipping.html`.
    Expected: No `<table`; label `Estimated Date From Treater` present; `Extraction Method` and `Errors` absent; modal/select-all/global update controls present.
    Evidence: .sisyphus/evidence/task-7-card-ui.txt

  Scenario: Modal update payload remains correct
    Tool: Bash
    Steps: Run pytest/source assertions that template JS still posts to `/update-kamterter-shipping-report` and uses `customer_po` plus `est_date_from_treater` from selected card data.
    Expected: Assertions pass and no `/create-purchase-invoice` appears in shipping template.
    Evidence: .sisyphus/evidence/task-7-modal-payload.txt
  ```

  **Commit**: YES | Message: `feat(ui): refine kamterter shipping results` | Files: [`templates/results_kamterter_shipping.html`, `tests/test_kamterter_shipping.py`]

## Final Verification Wave (MANDATORY — after ALL implementation tasks)
> 4 review agents run in PARALLEL. ALL must APPROVE. Present consolidated results to user and get explicit "okay" before completing.
> **Do NOT auto-proceed after verification. Wait for user's explicit approval before marking work complete.**
> **Never mark F1-F4 as checked before getting user's okay.** Rejection or user feedback -> fix -> re-run -> present again -> wait for okay.
- [ ] F1. Plan Compliance Audit — oracle
- [ ] F2. Code Quality Review — unspecified-high
- [ ] F3. Real Manual QA — unspecified-high (+ playwright if UI)
- [ ] F4. Scope Fidelity Check — deep

## Commit Strategy
- Commit per task only if user asks execution agent to commit.
- Keep commits small and scoped.
- Never commit `.env`, uploads, evidence files, or downloaded sample PDFs.

## Success Criteria
- Shipping workflow is distinct from financial Kamterter invoices.
- Sample extraction and date calculation match expected values.
- BC update route is sandbox-only and fail-closed.
- Minimal pytest suite covers the money/API-risk logic without external services.
- Final verification agents approve and user explicitly says okay.
