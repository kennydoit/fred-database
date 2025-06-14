# FRED Database

A Python-based application for extracting Federal Reserve Economic Data (FRED) and storing it in a database for analysis.

## Features

- Extract data from FRED API
- Store raw, untransformed data in SQLite database
- Support for multiple data series
- Data quality validation
- Configurable data extraction settings

## Setup

1. Install dependencies with uv:
   ```
   uv sync
   ```

2. Set up your FRED API key:
   - Get an API key from https://fred.stlouisfed.org/docs/api/api_key.html
   - Create a `.env` file and add your API key:
     ```
     FRED_API_KEY=your_api_key_here
     ```

3. Run the database setup:
   ```
   uv run python src/setup_database.py
   ```

## Usage

### Using the Python API

```python
from src.fred_extractor import FREDExtractor

# Initialize extractor
extractor = FREDExtractor()

# Extract a single series
extractor.extract_series('GDPC1')  # Real GDP

# Extract multiple series
series_list = ['GDPC1', 'UNRATE', 'CPIAUCSL']
extractor.extract_multiple_series(series_list)
```

### Using the command line scripts

```bash
# Set up the database
uv run python src/setup_database.py

# Extract common economic indicators
uv run python scripts/extract_common_series.py

# Run example usage
uv run python scripts/example_usage.py
```

## Database Schema

- `series_metadata`: Information about each FRED series
- `observations`: Raw data points for each series
- `extraction_log`: Log of data extraction activities

## Project Structure

```
fred-database/
├── src/
│   ├── fred_extractor.py    # Main data extraction logic
│   ├── database.py          # Database operations
│   ├── config.py           # Configuration settings
│   └── setup_database.py   # Database initialization
├── data/
│   └── fred_data.db        # SQLite database
├── scripts/
│   └── extract_common_series.py  # Script for common economic indicators
├── requirements.txt
├── .env.example
└── README.md
```
