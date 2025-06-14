import os
import sqlite3
import pandas as pd

DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'fred_data.db')

def fetch_observations():
    conn = sqlite3.connect(DB_PATH)
    query = """
        SELECT date, series_id, value
        FROM fred_data_long
    """
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df

def fetch_date_shell():
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query("SELECT date FROM date_shell ORDER BY date", conn)
    conn.close()
    return df

def denormalize_observations(df, date_shell_df):
    # Remove duplicates, keeping the last value for each (date, series_id)
    df = df.drop_duplicates(subset=["date", "series_id"], keep="last")
    wide_df = df.pivot(index="date", columns="series_id", values="value")
    wide_df.columns = [f"fred_{col}" for col in wide_df.columns]
    wide_df = wide_df.reindex(date_shell_df['date'])
    wide_df.index.name = "date"
    wide_df = wide_df.sort_index()
    wide_df = wide_df.ffill().loc[:pd.Timestamp.today().strftime('%Y-%m-%d')]
    return wide_df

def ensure_columns(conn, columns):
    cur = conn.cursor()
    cur.execute("PRAGMA table_info(fred_data_wide)")
    existing_cols = {row[1] for row in cur.fetchall()}
    for col in columns:
        if col not in existing_cols:
            cur.execute(f'ALTER TABLE fred_data_wide ADD COLUMN "{col}" REAL')
    conn.commit()

def upsert_wide_table(conn, wide_df):
    ensure_columns(conn, wide_df.columns)
    cur = conn.cursor()
    for date, row in wide_df.iterrows():
        cols = ['date'] + list(row.index)
        vals = [date] + [row[col] if pd.notnull(row[col]) else None for col in row.index]
        placeholders = ','.join(['?'] * len(cols))
        update_clause = ', '.join([f'"{col}"=excluded."{col}"' for col in row.index])
        sql = f'''
            INSERT INTO fred_data_wide ({",".join('"' + c + '"' for c in cols)})
            VALUES ({placeholders})
            ON CONFLICT(date) DO UPDATE SET {update_clause}
        '''
        cur.execute(sql, vals)
    conn.commit()

def main():
    print("🔄 Fetching and transforming FRED observations...")
    df = fetch_observations()
    date_shell_df = fetch_date_shell()
    if df.empty or date_shell_df.empty:
        print("❌ No data found in fred_data_long or date_shell table.")
        return
    wide_df = denormalize_observations(df, date_shell_df)
    conn = sqlite3.connect(DB_PATH)
    upsert_wide_table(conn, wide_df)
    conn.close()
    print("✅ Denormalized data written to fred_data_wide table in the database.")

if __name__ == "__main__":
    main()