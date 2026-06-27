# test_twelve_data.py
# One-off standalone script to verify Twelve Data's free tier actually
# returns full 5-year daily historical depth for a single ticker, before
# committing to replacing yfinance in ingest.py.
#
# Run from project root: python test_twelve_data.py

import os
from datetime import date, timedelta

import requests
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("TWELVE_DATA_API_KEY")
SYMBOL = "AAPL"
YEARS = 5

if not API_KEY:
    raise RuntimeError("TWELVE_DATA_API_KEY not found. Check your .env file.")

end = date.today()
start = end - timedelta(days=365 * YEARS)

url = "https://api.twelvedata.com/time_series"
params = {
    "symbol": SYMBOL,
    "interval": "1day",
    "start_date": start.isoformat(),
    "end_date": end.isoformat(),
    "outputsize": 5000,  # max allowed; ~1260 trading days expected for 5yrs
    "apikey": API_KEY,
}

response = requests.get(url, params=params)
data = response.json()

if "values" not in data:
    print("ERROR or unexpected response:")
    print(data)
else:
    values = data["values"]
    print(f"Status: {data.get('status')}")
    print(f"Requested range: {start} to {end}")
    print(f"Rows returned: {len(values)}")
    print(f"Earliest date in response: {values[-1]['datetime']}")
    print(f"Latest date in response:   {values[0]['datetime']}")
    print("\nSample row:")
    print(values[0])
