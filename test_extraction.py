"""
Test script for FRED data extraction.
"""
import sys
import os

# Add the src directory to the Python path
sys.path.insert(0, 'src')

from fred_extractor import FREDExtractor

def main():
    print('Testing FRED data extraction...')
    
    extractor = FREDExtractor()
    
    # Test extracting GDP data
    print('Extracting GDP data...')
    result = extractor.extract_series('GDP', start_date='2020-01-01')
    print(f'GDP extraction result: {result}')
    
    # Test extracting unemployment rate
    print('Extracting unemployment rate...')
    result = extractor.extract_series('UNRATE', start_date='2020-01-01')
    print(f'UNRATE extraction result: {result}')
    
    # Show database stats
    stats = extractor.get_database_summary()
    print(f'\nDatabase Statistics:')
    print(f'- Series: {stats["series_count"]}')
    print(f'- Observations: {stats["observations_count"]}')
    if stats["date_range"]["start"]:
        print(f'- Date range: {stats["date_range"]["start"]} to {stats["date_range"]["end"]}')

if __name__ == "__main__":
    main()
