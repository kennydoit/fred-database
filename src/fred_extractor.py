"""
FRED data extraction and storage functionality.
"""
import requests
import time
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime
import pandas as pd
from fredapi import Fred

from config import Config
from database import FREDDatabase

logger = logging.getLogger(__name__)

class FREDExtractor:
    """FRED data extractor and database manager."""
    
    def __init__(self, api_key: str = None):
        """Initialize FRED extractor."""
        self.api_key = api_key or Config.FRED_API_KEY
        if not self.api_key:
            raise ValueError("FRED API key is required")
        
        self.fred = Fred(api_key=self.api_key)
        self.database = FREDDatabase()
        self.session = requests.Session()
        
        # Setup logging
        logging.basicConfig(level=getattr(logging, Config.LOG_LEVEL))
    
    def _make_request(self, endpoint: str, params: Dict[str, Any]) -> Dict[str, Any]:
        """Make API request with rate limiting."""
        url = f"{Config.FRED_BASE_URL}/{endpoint}"
        params['api_key'] = self.api_key
        params['file_type'] = 'json'
        
        time.sleep(Config.RATE_LIMIT_DELAY)
        
        try:
            response = self.session.get(url, params=params, timeout=Config.REQUEST_TIMEOUT)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"API request failed: {e}")
            raise
    
    def get_series_info(self, series_id: str) -> Dict[str, Any]:
        """Get series metadata from FRED API."""
        try:
            data = self._make_request('series', {'series_id': series_id})
            if 'seriess' in data and len(data['seriess']) > 0:
                return data['seriess'][0]
            else:
                raise ValueError(f"Series {series_id} not found")
        except Exception as e:
            logger.error(f"Error getting series info for {series_id}: {e}")
            raise
    
    def get_series_observations(self, series_id: str, start_date: str = None, end_date: str = None) -> List[Dict[str, Any]]:
        """Get series observations from FRED API."""
        params = {'series_id': series_id}
        if start_date:
            params['observation_start'] = start_date
        if end_date:
            params['observation_end'] = end_date
        
        try:
            data = self._make_request('series/observations', params)
            if 'observations' in data:
                return data['observations']
            else:
                return []
        except Exception as e:
            logger.error(f"Error getting observations for {series_id}: {e}")
            raise
    
    def extract_series(self, series_id: str, start_date: str = None, end_date: str = None, force_update: bool = False) -> bool:
        """Extract a single series and store in database."""
        logger.info(f"Starting extraction for series: {series_id}")
        
        try:
            with self.database as db:
                # Check if series already exists
                existing_series = db.get_series_list()
                if series_id in existing_series and not force_update:
                    logger.info(f"Series {series_id} already exists. Use force_update=True to refresh.")
                    return True
                
                # Get series metadata
                logger.info(f"Fetching metadata for {series_id}")
                series_info = self.get_series_info(series_id)
                
                # Insert/update series metadata
                if db.insert_series_metadata(series_info):
                    logger.info(f"Metadata stored for {series_id}")
                else:
                    logger.error(f"Failed to store metadata for {series_id}")
                    db.log_extraction(series_id, 'failed', 0, 'Failed to store metadata')
                    return False
                
                # Get observations
                logger.info(f"Fetching observations for {series_id}")
                observations = self.get_series_observations(series_id, start_date, end_date)
                
                if observations:
                    # Insert observations
                    records_inserted = db.insert_observations(series_id, observations)
                    db.log_extraction(series_id, 'success', records_inserted)
                    logger.info(f"Successfully extracted {records_inserted} observations for {series_id}")
                    return True
                else:
                    logger.warning(f"No observations found for {series_id}")
                    db.log_extraction(series_id, 'success', 0, 'No observations found')
                    return True
                    
        except Exception as e:
            error_msg = f"Error extracting series {series_id}: {e}"
            logger.error(error_msg)
            with self.database as db:
                db.log_extraction(series_id, 'failed', 0, str(e))
            return False
    
    def extract_multiple_series(self, series_ids: List[str], start_date: str = None, end_date: str = None, force_update: bool = False) -> Dict[str, bool]:
        """Extract multiple series."""
        results = {}
        
        logger.info(f"Starting extraction for {len(series_ids)} series")
        
        for i, series_id in enumerate(series_ids, 1):
            logger.info(f"Processing series {i}/{len(series_ids)}: {series_id}")
            results[series_id] = self.extract_series(series_id, start_date, end_date, force_update)
            
            # Add delay between series to respect rate limits
            if i < len(series_ids):
                time.sleep(Config.RATE_LIMIT_DELAY * 2)
        
        # Summary
        successful = sum(1 for success in results.values() if success)
        logger.info(f"Extraction complete: {successful}/{len(series_ids)} series extracted successfully")
        
        return results
    
    def search_series(self, search_text: str, limit: int = 100) -> List[Dict[str, Any]]:
        """Search for series by text."""
        try:
            data = self._make_request('series/search', {
                'search_text': search_text,
                'limit': limit
            })
            return data.get('seriess', [])
        except Exception as e:
            logger.error(f"Error searching series: {e}")
            return []
    
    def get_popular_series(self, limit: int = 100) -> List[Dict[str, Any]]:
        """Get popular series from FRED."""
        try:
            data = self._make_request('series/search', {
                'search_text': '*',
                'order_by': 'popularity',
                'sort_order': 'desc',
                'limit': limit
            })
            return data.get('seriess', [])
        except Exception as e:
            logger.error(f"Error getting popular series: {e}")
            return []
    
    def setup_database(self):
        """Initialize database tables."""
        with self.database as db:
            db.create_tables()
        logger.info("Database setup complete")
    
    def get_database_summary(self) -> Dict[str, Any]:
        """Get summary of database contents."""
        with self.database as db:
            return db.get_database_stats()
    
    def get_series_data(self, series_id: str, start_date: str = None, end_date: str = None) -> pd.DataFrame:
        """Get series data as pandas DataFrame."""
        with self.database as db:
            return db.get_series_data(series_id, start_date, end_date)
