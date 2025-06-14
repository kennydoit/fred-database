"""
Database operations for FRED data storage.
"""
import sqlite3
import pandas as pd
from pathlib import Path
from datetime import datetime
import logging
from typing import List, Dict, Any, Optional
from config import Config

logger = logging.getLogger(__name__)

class FREDDatabase:
    """Database manager for FRED data storage."""
    
    def __init__(self, db_path: str = None):
        """Initialize database connection."""
        self.db_path = db_path or Config.DATABASE_PATH
        self.connection = None
        self._ensure_database_exists()
    
    def _ensure_database_exists(self):
        """Ensure database file and directory exist."""
        db_file = Path(self.db_path)
        db_file.parent.mkdir(parents=True, exist_ok=True)
    
    def connect(self):
        """Establish database connection."""
        if self.connection is None:
            self.connection = sqlite3.connect(self.db_path)
            self.connection.row_factory = sqlite3.Row
        return self.connection
    
    def close(self):
        """Close database connection."""
        if self.connection:
            self.connection.close()
            self.connection = None
    
    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
    
    def create_tables(self):
        """Create database tables if they don't exist."""
        conn = self.connect()
        cursor = conn.cursor()
        
        # Series metadata table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS series_metadata (
                id TEXT PRIMARY KEY,
                title TEXT,
                observation_start DATE,
                observation_end DATE,
                frequency TEXT,
                frequency_short TEXT,
                units TEXT,
                units_short TEXT,
                seasonal_adjustment TEXT,
                seasonal_adjustment_short TEXT,
                last_updated TIMESTAMP,
                popularity INTEGER,
                notes TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Observations table for raw data
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS observations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                series_id TEXT,
                date DATE,
                value REAL,
                realtime_start DATE,
                realtime_end DATE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (series_id) REFERENCES series_metadata (id),
                UNIQUE (series_id, date, realtime_start, realtime_end)
            )
        ''')
        
        # Extraction log table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS extraction_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                series_id TEXT,
                extraction_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                status TEXT,
                records_extracted INTEGER,
                error_message TEXT,
                FOREIGN KEY (series_id) REFERENCES series_metadata (id)
            )
        ''')
        
        # Create indexes for better performance
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_observations_series_date ON observations (series_id, date)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_observations_date ON observations (date)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_extraction_log_series ON extraction_log (series_id)')
        
        conn.commit()
        logger.info("Database tables created successfully")
    
    def insert_series_metadata(self, series_data: Dict[str, Any]) -> bool:
        """Insert or update series metadata."""
        conn = self.connect()
        cursor = conn.cursor()
        
        try:
            cursor.execute('''
                INSERT OR REPLACE INTO series_metadata 
                (id, title, observation_start, observation_end, frequency, frequency_short,
                 units, units_short, seasonal_adjustment, seasonal_adjustment_short,
                 last_updated, popularity, notes, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ''', (
                series_data.get('id'),
                series_data.get('title'),
                series_data.get('observation_start'),
                series_data.get('observation_end'),
                series_data.get('frequency'),
                series_data.get('frequency_short'),
                series_data.get('units'),
                series_data.get('units_short'),
                series_data.get('seasonal_adjustment'),
                series_data.get('seasonal_adjustment_short'),
                series_data.get('last_updated'),
                series_data.get('popularity'),
                series_data.get('notes')
            ))
            conn.commit()
            logger.info(f"Series metadata inserted/updated: {series_data.get('id')}")
            return True
        except Exception as e:
            logger.error(f"Error inserting series metadata: {e}")
            conn.rollback()
            return False
    
    def insert_observations(self, series_id: str, observations: List[Dict[str, Any]]) -> int:
        """Insert observations data."""
        conn = self.connect()
        cursor = conn.cursor()
        
        inserted_count = 0
        for obs in observations:
            try:
                cursor.execute('''
                    INSERT OR IGNORE INTO observations 
                    (series_id, date, value, realtime_start, realtime_end)
                    VALUES (?, ?, ?, ?, ?)
                ''', (
                    series_id,
                    obs.get('date'),
                    obs.get('value') if obs.get('value') != '.' else None,
                    obs.get('realtime_start'),
                    obs.get('realtime_end')
                ))
                if cursor.rowcount > 0:
                    inserted_count += 1
            except Exception as e:
                logger.error(f"Error inserting observation: {e}")
        
        conn.commit()
        logger.info(f"Inserted {inserted_count} new observations for series {series_id}")
        return inserted_count
    
    def log_extraction(self, series_id: str, status: str, records_extracted: int = 0, error_message: str = None):
        """Log extraction activity."""
        conn = self.connect()
        cursor = conn.cursor()
        
        try:
            cursor.execute('''
                INSERT INTO extraction_log (series_id, status, records_extracted, error_message)
                VALUES (?, ?, ?, ?)
            ''', (series_id, status, records_extracted, error_message))
            conn.commit()
        except Exception as e:
            logger.error(f"Error logging extraction: {e}")
    
    def get_series_list(self) -> List[str]:
        """Get list of all series IDs in the database."""
        conn = self.connect()
        cursor = conn.cursor()
        cursor.execute('SELECT DISTINCT id FROM series_metadata ORDER BY id')
        return [row[0] for row in cursor.fetchall()]
    
    def get_series_data(self, series_id: str, start_date: str = None, end_date: str = None) -> pd.DataFrame:
        """Retrieve series data as pandas DataFrame."""
        conn = self.connect()
        
        query = '''
            SELECT date, value FROM observations 
            WHERE series_id = ?
        '''
        params = [series_id]
        
        if start_date:
            query += ' AND date >= ?'
            params.append(start_date)
        
        if end_date:
            query += ' AND date <= ?'
            params.append(end_date)
        
        query += ' ORDER BY date'
        
        df = pd.read_sql_query(query, conn, params=params)
        if not df.empty:
            df['date'] = pd.to_datetime(df['date'])
            df.set_index('date', inplace=True)
        
        return df
    
    def get_database_stats(self) -> Dict[str, Any]:
        """Get database statistics."""
        conn = self.connect()
        cursor = conn.cursor()
        
        # Count series
        cursor.execute('SELECT COUNT(*) FROM series_metadata')
        series_count = cursor.fetchone()[0]
        
        # Count observations
        cursor.execute('SELECT COUNT(*) FROM observations')
        obs_count = cursor.fetchone()[0]
        
        # Get date range
        cursor.execute('SELECT MIN(date), MAX(date) FROM observations')
        date_range = cursor.fetchone()
        
        return {
            'series_count': series_count,
            'observations_count': obs_count,
            'date_range': {
                'start': date_range[0],
                'end': date_range[1]
            }
        }
