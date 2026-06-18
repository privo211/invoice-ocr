## Issues
\n+## 2026-06-18
- Route relies on MSAL cache functions at runtime; tests must stub load_cache/build_msal_app to avoid psycopg2 and live MSAL. Current helper pattern works without touching app.py.
