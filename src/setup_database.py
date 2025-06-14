"""
Database setup script for FRED data storage.
"""
import sqlite3
import os

DB_DIR = os.path.join(os.path.dirname(__file__), '..', 'data')
os.makedirs(DB_DIR, exist_ok=True)
DB_PATH = os.path.join(DB_DIR, 'fred_data.db')

def setup_database():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # Table for series metadata
    cur.execute("""
    CREATE TABLE IF NOT EXISTS series_metadata (
        id TEXT PRIMARY KEY,
        title TEXT,
        frequency TEXT,
        units TEXT,
        last_updated TEXT
    )
    """)

    # Table for extraction logs
    cur.execute("""
    CREATE TABLE IF NOT EXISTS extraction_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        series_id TEXT,
        extracted_at TEXT,
        status TEXT,
        message TEXT
    )
    """)

    # Table for long-format FRED data
    cur.execute("""
    CREATE TABLE IF NOT EXISTS fred_data_long (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        series_id TEXT,
        date TEXT,
        value REAL,
        FOREIGN KEY(series_id) REFERENCES series_metadata(id),
        UNIQUE(series_id, date)
    )
    """)

    # Table for wide-format FRED data
    cur.execute("""
    CREATE TABLE IF NOT EXISTS fred_data_wide (
        date TEXT PRIMARY KEY
        -- series columns will be added dynamically
    )
    """)

    # Table for date shell (all calendar dates from 2018-01-01 to 2030-12-31)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS date_shell (
        date TEXT PRIMARY KEY
    )
    """)

    # Populate date_shell if empty
    cur.execute("SELECT COUNT(*) FROM date_shell")
    if cur.fetchone()[0] == 0:
        from datetime import date, timedelta
        start = date(2018, 1, 1)
        end = date(2030, 12, 31)
        days = (end - start).days + 1
        dates = [(start + timedelta(days=i)).isoformat() for i in range(days)]
        cur.executemany("INSERT INTO date_shell (date) VALUES (?)", [(d,) for d in dates])
        print(f"✅ Populated date_shell with {days} dates from {start} to {end}")

    conn.commit()
    conn.close()
    print(f"✅ Database setup complete! Location: {DB_PATH}")

if __name__ == "__main__":
    setup_database()
