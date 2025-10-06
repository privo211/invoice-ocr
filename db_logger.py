# db_logger.py
import psycopg2
import os
from flask import g

# Database configuration, assuming it's consistent with your app
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
    """Initializes the database and creates the log table if it doesn't exist."""
    db = get_db()
    cur = db.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS processing_log (
            id SERIAL PRIMARY KEY,
            timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            vendor VARCHAR(50),
            po_number VARCHAR(100),
            filename VARCHAR(255),
            extraction_method VARCHAR(50),
            page_count INTEGER,
            line_count INTEGER
        );
    """)
    db.commit()
    cur.close()
    close_db()

def log_processing_event(vendor, filename, extraction_info, po_number=None):
    """Logs a single document processing event to the database."""
    sql = """
        INSERT INTO processing_log (vendor, po_number, filename, extraction_method, page_count, line_count)
        VALUES (%s, %s, %s, %s, %s, %s);
    """
    db = get_db()
    cur = db.cursor()
    try:
        cur.execute(sql, (
            vendor,
            po_number,
            filename,
            extraction_info.get('method'),
            extraction_info.get('page_count'),
            len(extraction_info.get('lines', []))
        ))
        db.commit()
    except Exception as e:
        db.rollback()
        print(f"Database log failed: {e}")
    finally:
        cur.close()

def get_log_stats():
    """Retrieves aggregated statistics from the processing log."""
    db = get_db()
    cur = db.cursor()
    cur.execute("""
        SELECT
            COUNT(*) AS total_documents,
            COUNT(*) FILTER (WHERE extraction_method = 'Azure OCR') AS ocr_count,
            COUNT(*) FILTER (WHERE extraction_method = 'PyMuPDF') AS text_count,
            AVG(page_count) AS avg_pages
        FROM processing_log;
    """)
    stats = cur.fetchone()
    cur.close()
    
    total = stats[0] if stats[0] else 0
    ocr = stats[1] if stats[1] else 0
    text = stats[2] if stats[2] else 0
    avg_pages = round(stats[3], 1) if stats[3] else 0
    
    return {
        'total': total,
        'ocr': ocr,
        'text': text,
        'avg_pages': avg_pages,
        'ocr_percent': round((ocr / total) * 100, 1) if total > 0 else 0
    }

def get_paginated_logs(page=1, per_page=20):
    """Retrieves a paginated list of all log entries."""
    offset = (page - 1) * per_page
    db = get_db()
    cur = db.cursor()
    cur.execute("""
        SELECT timestamp, vendor, po_number, filename, extraction_method, page_count, line_count
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
    # Ensure the DB table exists when the app starts
    with app.app_context():
        init_db()
    # Close the DB connection when the app context is torn down
    app.teardown_appcontext(close_db)