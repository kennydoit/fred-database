import os
import sys
import yaml
import sqlite3
from dotenv import load_dotenv
from fredapi import Fred

# Load environment variables from .env
load_dotenv()
api_key = os.getenv("FRED_API_KEY")
if not api_key:
    print("❌ FRED_API_KEY not found in .env file.")
    sys.exit(1)

# Load config.yaml
with open(os.path.join(os.path.dirname(__file__), '..', 'config.yaml'), 'r') as f:
    config = yaml.safe_load(f)

start_date = config.get("start_date", "1950-01-01")
common_series = config.get("common_series", {})

fred = Fred(api_key=api_key)

DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'fred_data.db')

def upsert_series_metadata(cur, series_id):
    try:
        info = fred.get_series_info(series_id)
        cur.execute("""
            INSERT OR REPLACE INTO series_metadata (id, title, frequency, units, last_updated)
            VALUES (?, ?, ?, ?, ?)
        """, (
            series_id,
            info.title,
            info.frequency,
            info.units,
            info.last_updated
        ))
    except Exception as e:
        print(f"⚠️  Could not fetch metadata for {series_id}: {e}")

def insert_observations(cur, series_id, data):
    for date, value in data.items():
        cur.execute("""
            INSERT OR REPLACE INTO fred_data_long (series_id, date, value)
            VALUES (?, ?, ?)
        """, (series_id, str(date.date()), float(value) if value is not None else None))

def log_extraction(cur, series_id, status, message):
    from datetime import datetime, timezone
    cur.execute("""
        INSERT INTO extraction_log (series_id, extracted_at, status, message)
        VALUES (?, ?, ?, ?)
    """, (series_id, datetime.now(timezone.utc).isoformat(), status, message))

def get_latest_date(cur, series_id):
    cur.execute("SELECT MAX(date) FROM fred_data_long WHERE series_id = ?", (series_id,))
    result = cur.fetchone()
    return result[0] if result and result[0] else None

def extract_and_store(series_id, default_start_date, cur):
    try:
        latest_date = get_latest_date(cur, series_id)
        # If we have data, start from the next day; else use default_start_date
        if latest_date:
            from datetime import datetime, timedelta
            start_date = (datetime.strptime(latest_date, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
        else:
            start_date = default_start_date
        data = fred.get_series(series_id, observation_start=start_date)
        upsert_series_metadata(cur, series_id)
        insert_observations(cur, series_id, data)
        log_extraction(cur, series_id, "success", f"{len(data)} records")
        print(f"✅ {series_id}: {len(data)} records stored from {start_date}")
    except Exception as e:
        log_extraction(cur, series_id, "error", str(e))
        print(f"❌ {series_id}: {e}")

def main():
    print(f"Extracting FRED series from {start_date} as defined in config.yaml...\n")
    conn = sqlite3.connect(DB_PATH, timeout=30)
    cur = conn.cursor()
    for category, series_list in common_series.items():
        print(f"\nCategory: {category}")
        for item in series_list:
            # Remove comments from YAML items if present
            series_id = str(item).split()[0]
            extract_and_store(series_id, start_date, cur)
            conn.commit()
    conn.close()
    print("\n✅ All data extracted and stored in the database.")

if __name__ == "__main__":
    main()