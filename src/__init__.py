"""
FRED Database Package

A Python package for extracting Federal Reserve Economic Data (FRED) 
and storing it in a database for analysis.
"""

from .fred_extractor import FREDExtractor
from .database import FREDDatabase
from .config import Config

__version__ = "1.0.0"
__author__ = "FRED Database Project"

__all__ = ["FREDExtractor", "FREDDatabase", "Config"]
