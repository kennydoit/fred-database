import os
from dotenv import load_dotenv
from fredapi import Fred

# Load environment variables from .env
load_dotenv()
api_key = os.getenv("FRED_API_KEY")

if not api_key:
    print("❌ FRED_API_KEY not found in .env file.")
    exit(1)

fred = Fred(api_key=api_key)

try:
    # Try fetching a well-known series
    data = fred.get_series('UNRATE', observation_start='2020-01-01')
    print("✅ Successfully fetched UNRATE series from FRED.")
    print(data.head())
except Exception as e:
    print(f"❌ Failed to fetch data: {e}")