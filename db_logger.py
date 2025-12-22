# # db_logger.py
# import psycopg2
# import os
# from flask import g

# # Database configuration
# DB_CONFIG = {
#     'host': 'localhost',
#     'database': 'invoice_ocr',
#     'user': 'priyanshu',
#     'password': 'reorg0211',
# }

# def get_db():
#     """Opens a new database connection if there is none for the current context."""
#     if 'db' not in g:
#         g.db = psycopg2.connect(**DB_CONFIG)
#     return g.db

# def close_db(e=None):
#     """Closes the database connection."""
#     db = g.pop('db', None)
#     if db is not None:
#         db.close()

# def init_db():
#     """Initializes the database: log table and persistent stats table."""
#     db = get_db()
#     cur = db.cursor()
    
#     # 1. Processing Log (Pruned to last 100)
#     cur.execute("""
#         CREATE TABLE IF NOT EXISTS processing_log (
#             id SERIAL PRIMARY KEY,
#             timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
#             vendor VARCHAR(50),
#             po_number VARCHAR(100),
#             filename VARCHAR(255),
#             extraction_method VARCHAR(50),
#             page_count INTEGER
#         );
#     """)

#     # 2. Lifetime Stats (Persistent Counters)
#     # Stores running totals so we don't lose stats when pruning logs
#     cur.execute("""
#         CREATE TABLE IF NOT EXISTS lifetime_stats (
#             metric_key VARCHAR(50) PRIMARY KEY,
#             metric_value INTEGER DEFAULT 0
#         );
#     """)
    
#     # Initialize stats with 0 if they don't exist
#     cur.execute("""
#         INSERT INTO lifetime_stats (metric_key, metric_value)
#         VALUES 
#             ('total_documents', 0),
#             ('ocr_count', 0),
#             ('text_count', 0),
#             ('total_pages', 0)
#         ON CONFLICT (metric_key) DO NOTHING;
#     """)
    
#     db.commit()
#     cur.close()
#     close_db()

# def log_processing_event(vendor, filename, extraction_info, po_number=None):
#     """Logs event, updates lifetime stats, and prunes old logs."""
#     db = get_db()
#     cur = db.cursor()
#     try:
#         method = extraction_info.get('method', 'Unknown')
#         pages = extraction_info.get('page_count', 0)

#         # 1. Update Lifetime Stats (Running Totals)
#         cur.execute("UPDATE lifetime_stats SET metric_value = metric_value + 1 WHERE metric_key = 'total_documents';")
#         cur.execute("UPDATE lifetime_stats SET metric_value = metric_value + %s WHERE metric_key = 'total_pages';", (pages,))
        
#         if method == 'Azure OCR':
#             cur.execute("UPDATE lifetime_stats SET metric_value = metric_value + 1 WHERE metric_key = 'ocr_count';")
#         elif method == 'PyMuPDF':
#             cur.execute("UPDATE lifetime_stats SET metric_value = metric_value + 1 WHERE metric_key = 'text_count';")

#         # 2. Insert new detailed log entry
#         cur.execute("""
#             INSERT INTO processing_log (vendor, po_number, filename, extraction_method, page_count)
#             VALUES (%s, %s, %s, %s, %s);
#         """, (vendor, po_number, filename, method, pages))
        
#         # 3. Prune: Keep only last 100 entries in the log table
#         cur.execute("""
#             DELETE FROM processing_log 
#             WHERE id NOT IN (
#                 SELECT id FROM processing_log 
#                 ORDER BY timestamp DESC 
#                 LIMIT 100
#             );
#         """)
        
#         db.commit()
#     except Exception as e:
#         db.rollback()
#         print(f"Database log failed: {e}")
#     finally:
#         cur.close()

# def get_log_stats():
#     """Retrieves aggregated statistics from the lifetime_stats table."""
#     db = get_db()
#     cur = db.cursor()
    
#     # Fetch all stats into a dictionary
#     cur.execute("SELECT metric_key, metric_value FROM lifetime_stats;")
#     rows = cur.fetchall()
#     stats_map = {row[0]: row[1] for row in rows}
#     cur.close()
    
#     # Safe retrieval with defaults
#     total = stats_map.get('total_documents', 0)
#     ocr = stats_map.get('ocr_count', 0)
#     text = stats_map.get('text_count', 0)
#     total_pages = stats_map.get('total_pages', 0)
    
#     # Calculate derived stats
#     avg_pages = round(total_pages / total, 1) if total > 0 else 0
#     ocr_percent = round((ocr / total) * 100, 1) if total > 0 else 0
    
#     return {
#         'total': total,
#         'ocr': ocr,
#         'text': text,
#         'avg_pages': avg_pages,
#         'ocr_percent': ocr_percent
#     }

# def get_paginated_logs(page=1, per_page=50):
#     """Retrieves a paginated list of log entries (limited to the stored last 100)."""
#     offset = (page - 1) * per_page
#     db = get_db()
#     cur = db.cursor()
    
#     cur.execute("""
#         SELECT timestamp, vendor, po_number, filename, extraction_method, page_count
#         FROM processing_log
#         ORDER BY timestamp DESC
#         LIMIT %s OFFSET %s;
#     """, (per_page, offset))
#     logs = cur.fetchall()
    
#     cur.execute("SELECT COUNT(*) FROM processing_log;")
#     total = cur.fetchone()[0]
    
#     cur.close()
#     return logs, total

# def init_app(app):
#     """Register database functions with the Flask app."""
#     with app.app_context():
#         init_db()
#     app.teardown_appcontext(close_db)

# db_logger.py
import psycopg2
import os
from flask import g

# Database configuration
DB_CONFIG = {
    'host': 'localhost',
    'database': 'invoice_ocr',
    'user': 'priyanshu',
    'password': 'reorg0211',
}

def get_db():
    """Opens a new database connection if there is none for the current context."""
    if 'db' not in g:
        g.db = psycopg2.connect(**DB_CONFIG)
    return g.db

def close_db(e=None):
    """Closes the database connection."""
    db = g.pop('db', None)
    if db is not None:
        db.close()

def _backfill_stats_from_logs(cur):
    """
    Helper to calculate stats from the existing processing_log table.
    Used to initialize lifetime_stats if they are found to be empty.
    """
    print("Backfilling lifetime_stats from existing logs...")
    cur.execute("""
        SELECT 
            COUNT(*) AS total_docs,
            COALESCE(SUM(page_count), 0) AS total_pages,
            COUNT(*) FILTER (WHERE extraction_method = 'Azure OCR') AS ocr_docs,
            COALESCE(SUM(page_count) FILTER (WHERE extraction_method = 'Azure OCR'), 0) AS ocr_pages,
            COUNT(*) FILTER (WHERE extraction_method = 'PyMuPDF') AS text_docs,
            COALESCE(SUM(page_count) FILTER (WHERE extraction_method = 'PyMuPDF'), 0) AS text_pages
        FROM processing_log;
    """)
    row = cur.fetchone()
    if row:
        total_docs, total_pages, ocr_docs, ocr_pages, text_docs, text_pages = row
        
        updates = [
            ('total_documents', total_docs),
            ('total_pages', total_pages),
            ('ocr_count', ocr_docs),
            ('ocr_pages', ocr_pages),
            ('text_count', text_docs),
            ('text_pages', text_pages)
        ]
        
        for key, val in updates:
            cur.execute("INSERT INTO lifetime_stats (metric_key, metric_value) VALUES (%s, %s) ON CONFLICT (metric_key) DO UPDATE SET metric_value = EXCLUDED.metric_value;", (key, val))

def init_db():
    """Initializes the database: log table and persistent stats table."""
    db = get_db()
    cur = db.cursor()
    
    # 1. Processing Log
    cur.execute("""
        CREATE TABLE IF NOT EXISTS processing_log (
            id SERIAL PRIMARY KEY,
            timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            vendor VARCHAR(50),
            po_number VARCHAR(100),
            filename VARCHAR(255),
            extraction_method VARCHAR(50),
            page_count INTEGER
        );
    """)

    # 2. Lifetime Stats (Persistent Counters)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS lifetime_stats (
            metric_key VARCHAR(50) PRIMARY KEY,
            metric_value INTEGER DEFAULT 0
        );
    """)
    
    # Initialize keys with 0 if they don't exist
    keys = ['total_documents', 'ocr_count', 'text_count', 'total_pages', 'ocr_pages', 'text_pages']
    for key in keys:
        cur.execute("INSERT INTO lifetime_stats (metric_key, metric_value) VALUES (%s, 0) ON CONFLICT (metric_key) DO NOTHING;", (key,))
    
    # 3. Check if we need to backfill (if total_documents is 0 but processing_log has data)
    cur.execute("SELECT metric_value FROM lifetime_stats WHERE metric_key = 'total_documents';")
    res = cur.fetchone()
    current_total = res[0] if res else 0
    
    if current_total == 0:
        cur.execute("SELECT COUNT(*) FROM processing_log;")
        log_count = cur.fetchone()[0]
        if log_count > 0:
            _backfill_stats_from_logs(cur)

    db.commit()
    cur.close()
    close_db()

def log_processing_event(vendor, filename, extraction_info, po_number=None):
    """Logs event, updates lifetime stats (pages & docs), and prunes old logs."""
    db = get_db()
    cur = db.cursor()
    try:
        method = extraction_info.get('method', 'Unknown')
        pages = extraction_info.get('page_count', 0)

        # 1. Update Lifetime Stats (Running Totals)
        cur.execute("UPDATE lifetime_stats SET metric_value = metric_value + 1 WHERE metric_key = 'total_documents';")
        cur.execute("UPDATE lifetime_stats SET metric_value = metric_value + %s WHERE metric_key = 'total_pages';", (pages,))
        
        if method == 'Azure OCR':
            cur.execute("UPDATE lifetime_stats SET metric_value = metric_value + 1 WHERE metric_key = 'ocr_count';")
            cur.execute("UPDATE lifetime_stats SET metric_value = metric_value + %s WHERE metric_key = 'ocr_pages';", (pages,))
        elif method == 'PyMuPDF':
            cur.execute("UPDATE lifetime_stats SET metric_value = metric_value + 1 WHERE metric_key = 'text_count';")
            cur.execute("UPDATE lifetime_stats SET metric_value = metric_value + %s WHERE metric_key = 'text_pages';", (pages,))

        # 2. Insert new detailed log entry
        cur.execute("""
            INSERT INTO processing_log (vendor, po_number, filename, extraction_method, page_count)
            VALUES (%s, %s, %s, %s, %s);
        """, (vendor, po_number, filename, method, pages))
        
        # 3. Prune: Keep only last 100 entries in the log table
        cur.execute("""
            DELETE FROM processing_log 
            WHERE id NOT IN (
                SELECT id FROM processing_log 
                ORDER BY timestamp DESC 
                LIMIT 100
            );
        """)
        
        db.commit()
    except Exception as e:
        db.rollback()
        print(f"Database log failed: {e}")
    finally:
        cur.close()

def get_log_stats():
    """Retrieves aggregated statistics from the lifetime_stats table."""
    db = get_db()
    cur = db.cursor()
    
    cur.execute("SELECT metric_key, metric_value FROM lifetime_stats;")
    rows = cur.fetchall()
    stats_map = {row[0]: row[1] for row in rows}
    cur.close()
    
    total_docs = stats_map.get('total_documents', 0)
    total_pages = stats_map.get('total_pages', 0)
    ocr_pages = stats_map.get('ocr_pages', 0)
    text_pages = stats_map.get('text_pages', 0)
    
    # Calculate Percentage based on PAGES
    ocr_percent_pages = round((ocr_pages / total_pages) * 100, 1) if total_pages > 0 else 0
    
    return {
        'total': total_docs,
        'total_pages': total_pages,
        'ocr_pages': ocr_pages,
        'text_pages': text_pages,
        'ocr_percent': ocr_percent_pages
    }

def get_paginated_logs(page=1, per_page=50):
    """Retrieves a paginated list of log entries."""
    offset = (page - 1) * per_page
    db = get_db()
    cur = db.cursor()
    
    cur.execute("""
        SELECT timestamp, vendor, po_number, filename, extraction_method, page_count
        FROM processing_log
        ORDER BY timestamp DESC
        LIMIT %s OFFSET %s;
    """, (per_page, offset))
    logs = cur.fetchall()
    
    cur.execute("SELECT COUNT(*) FROM processing_log;")
    total = cur.fetchone()[0]
    
    cur.close()
    return logs, total

def init_app(app):
    """Register database functions with the Flask app."""
    with app.app_context():
        init_db()
    app.teardown_appcontext(close_db)