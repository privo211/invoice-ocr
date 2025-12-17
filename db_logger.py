# # db_logger.py
# import psycopg2
# import os
# from flask import g

# # Database configuration, assuming it's consistent with your app
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
#     """Initializes the database and creates the log table if it doesn't exist."""
#     db = get_db()
#     cur = db.cursor()
#     cur.execute("""
#         CREATE TABLE IF NOT EXISTS processing_log (
#             id SERIAL PRIMARY KEY,
#             timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
#             vendor VARCHAR(50),
#             po_number VARCHAR(100),
#             filename VARCHAR(255),
#             extraction_method VARCHAR(50),
#             page_count INTEGER,
#             line_count INTEGER
#         );
#     """)
#     db.commit()
#     cur.close()
#     close_db()
    
# def log_processing_event(vendor, filename, extraction_info, po_number=None):
#     """Logs a single document processing event and prunes old logs (keeps last 100)."""
#     db = get_db()
#     cur = db.cursor()
#     try:
#         # 1. Insert new log
#         insert_sql = """
#             INSERT INTO processing_log (vendor, po_number, filename, extraction_method, page_count)
#             VALUES (%s, %s, %s, %s, %s);
#         """
#         cur.execute(insert_sql, (
#             vendor,
#             po_number,
#             filename,
#             extraction_info.get('method', 'Unknown'),
#             extraction_info.get('page_count', 0)
#         ))
        
#         # 2. Prune: Delete all but the latest 100 entries
#         prune_sql = """
#             DELETE FROM processing_log 
#             WHERE id NOT IN (
#                 SELECT id FROM processing_log 
#                 ORDER BY timestamp DESC 
#                 LIMIT 100
#             );
#         """
#         cur.execute(prune_sql)
        
#         db.commit()
#     except Exception as e:
#         db.rollback()
#         print(f"Database log failed: {e}")
#     finally:
#         cur.close()

# def get_log_stats():
#     """Retrieves aggregated statistics from the processing log."""
#     db = get_db()
#     cur = db.cursor()
#     cur.execute("""
#         SELECT
#             COUNT(*) AS total_documents,
#             COUNT(*) FILTER (WHERE extraction_method = 'Azure OCR') AS ocr_count,
#             COUNT(*) FILTER (WHERE extraction_method = 'PyMuPDF') AS text_count,
#             AVG(page_count) AS avg_pages
#         FROM processing_log;
#     """)
#     stats = cur.fetchone()
#     cur.close()
    
#     total = stats[0] if stats[0] else 0
#     ocr = stats[1] if stats[1] else 0
#     text = stats[2] if stats[2] else 0
#     avg_pages = round(stats[3], 1) if stats[3] else 0
    
#     return {
#         'total': total,
#         'ocr': ocr,
#         'text': text,
#         'avg_pages': avg_pages,
#         'ocr_percent': round((ocr / total) * 100, 1) if total > 0 else 0
#     }

# def get_paginated_logs(page=1, per_page=50):
#     """Retrieves a paginated list of log entries."""
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
#     # Ensure the DB table exists when the app starts
#     with app.app_context():
#         init_db()
#     # Close the DB connection when the app context is torn down
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

def init_db():
    """Initializes the database: log table and persistent stats table."""
    db = get_db()
    cur = db.cursor()
    
    # 1. Processing Log (Pruned to last 100)
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
    # Stores running totals so we don't lose stats when pruning logs
    cur.execute("""
        CREATE TABLE IF NOT EXISTS lifetime_stats (
            metric_key VARCHAR(50) PRIMARY KEY,
            metric_value INTEGER DEFAULT 0
        );
    """)
    
    # Initialize stats with 0 if they don't exist
    cur.execute("""
        INSERT INTO lifetime_stats (metric_key, metric_value)
        VALUES 
            ('total_documents', 0),
            ('ocr_count', 0),
            ('text_count', 0),
            ('total_pages', 0)
        ON CONFLICT (metric_key) DO NOTHING;
    """)
    
    db.commit()
    cur.close()
    close_db()

def log_processing_event(vendor, filename, extraction_info, po_number=None):
    """Logs event, updates lifetime stats, and prunes old logs."""
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
        elif method == 'PyMuPDF':
            cur.execute("UPDATE lifetime_stats SET metric_value = metric_value + 1 WHERE metric_key = 'text_count';")

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
    
    # Fetch all stats into a dictionary
    cur.execute("SELECT metric_key, metric_value FROM lifetime_stats;")
    rows = cur.fetchall()
    stats_map = {row[0]: row[1] for row in rows}
    cur.close()
    
    # Safe retrieval with defaults
    total = stats_map.get('total_documents', 0)
    ocr = stats_map.get('ocr_count', 0)
    text = stats_map.get('text_count', 0)
    total_pages = stats_map.get('total_pages', 0)
    
    # Calculate derived stats
    avg_pages = round(total_pages / total, 1) if total > 0 else 0
    ocr_percent = round((ocr / total) * 100, 1) if total > 0 else 0
    
    return {
        'total': total,
        'ocr': ocr,
        'text': text,
        'avg_pages': avg_pages,
        'ocr_percent': ocr_percent
    }

def get_paginated_logs(page=1, per_page=50):
    """Retrieves a paginated list of log entries (limited to the stored last 100)."""
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