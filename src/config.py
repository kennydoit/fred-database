"""
Configuration settings for FRED database application.
"""
import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

class Config:
    """Configuration class for FRED database settings."""
    
    # API Configuration
    FRED_API_KEY = os.getenv('FRED_API_KEY')
    FRED_BASE_URL = 'https://api.stlouisfed.org/fred'
    
    # Database Configuration
    DATABASE_PATH = os.getenv('DATABASE_PATH', 'data/fred_data.db')
    
    # Logging Configuration
    LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
    
    # Data extraction settings
    DEFAULT_FILE_TYPE = 'json'
    REQUEST_TIMEOUT = 30
    RATE_LIMIT_DELAY = 0.1  # seconds between requests
    
    @classmethod
    def validate_config(cls):
        """Validate that required configuration is present."""
        if not cls.FRED_API_KEY:
            raise ValueError(
                "FRED_API_KEY not found. Please set it in your .env file. "
                "Get your API key from https://fred.stlouisfed.org/docs/api/api_key.html"
            )
        
        # Ensure data directory exists
        db_dir = Path(cls.DATABASE_PATH).parent
        db_dir.mkdir(parents=True, exist_ok=True)
        
        return True

# Validate configuration on import
Config.validate_config()
