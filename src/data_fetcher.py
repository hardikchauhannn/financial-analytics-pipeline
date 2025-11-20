"""
data_fetcher.py - Fetch stock market data from Alpha Vantage API
"""

import os
import requests
import time
from datetime import datetime
from dotenv import load_dotenv
import logging

load_dotenv()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class StockDataFetcher:
    def __init__(self):
        self.api_key = os.getenv('ALPHA_VANTAGE_API_KEY')
        if not self.api_key:
            raise ValueError("ALPHA_VANTAGE_API_KEY not found in .env file!")
        
        self.base_url = "https://www.alphavantage.co/query"
        logger.info("Stock data fetcher initialized")
    
    def fetch_daily_data(self, symbol, outputsize='compact'):
        try:
            params = {
                'function': 'TIME_SERIES_DAILY',
                'symbol': symbol,
                'outputsize': outputsize,
                'apikey': self.api_key
            }
            
            logger.info(f"Fetching data for {symbol}...")
            response = requests.get(self.base_url, params=params)
            response.raise_for_status()
            data = response.json()
            
            if 'Error Message' in data:
                logger.error(f"API Error: {data['Error Message']}")
                return None
            
            if 'Note' in data:
                logger.warning(f"API Rate Limit: {data['Note']}")
                return None
            
            if 'Time Series (Daily)' not in data:
                logger.error(f"Unexpected response for {symbol}")
                return None
            
            time_series = data['Time Series (Daily)']
            logger.info(f"[SUCCESS] Fetched {len(time_series)} days of data for {symbol}")
            return time_series
            
        except Exception as e:
            logger.error(f"Error fetching {symbol}: {e}")
            return None
    
    def parse_stock_data(self, symbol, time_series_data):
        parsed_data = []
        
        for date_str, values in time_series_data.items():
            try:
                record = {
                    'symbol': symbol,
                    'date': datetime.strptime(date_str, '%Y-%m-%d'),
                    'open_price': float(values['1. open']),
                    'high_price': float(values['2. high']),
                    'low_price': float(values['3. low']),
                    'close_price': float(values['4. close']),
                    'volume': int(values['5. volume']),
                    'created_at': datetime.now()
                }
                parsed_data.append(record)
            except (KeyError, ValueError) as e:
                logger.error(f"Error parsing {symbol} on {date_str}: {e}")
                continue
        
        logger.info(f"Parsed {len(parsed_data)} records for {symbol}")
        return parsed_data
    
    def fetch_multiple_symbols(self, symbols, delay=12):
        all_data = {}
        
        for i, symbol in enumerate(symbols):
            logger.info(f"Processing {symbol} ({i+1}/{len(symbols)})...")
            
            time_series = self.fetch_daily_data(symbol)
            
            if time_series:
                parsed_data = self.parse_stock_data(symbol, time_series)
                all_data[symbol] = parsed_data
                
                if i < len(symbols) - 1:
                    logger.info(f"Waiting {delay} seconds...")
                    time.sleep(delay)
            else:
                logger.warning(f"Skipping {symbol}")
                all_data[symbol] = []
        
        return all_data


if __name__ == "__main__":
    print("Testing Stock Data Fetcher...")
    print("-" * 50)
    
    try:
        fetcher = StockDataFetcher()
        print("[SUCCESS] Data fetcher initialized")
        
        test_symbol = 'AAPL'
        print(f"\nFetching data for {test_symbol}...")
        
        time_series = fetcher.fetch_daily_data(test_symbol, outputsize='compact')
        
        if time_series:
            print(f"[SUCCESS] Fetched {len(time_series)} days of data")
            
            parsed_data = fetcher.parse_stock_data(test_symbol, time_series)
            print(f"[SUCCESS] Parsed {len(parsed_data)} records")
            
            if parsed_data:
                print("\n[STATS] Sample Record:")
                sample = parsed_data[0]
                print(f"   Symbol: {sample['symbol']}")
                print(f"   Date: {sample['date']}")
                print(f"   Close: ${sample['close_price']:.2f}")
                print(f"   Volume: {sample['volume']:,}")
        else:
            print("[ERROR] Failed to fetch data")
            print("Check your API key in .env file")
        
        print("\n" + "=" * 50)
        print("[SUCCESS] Data fetcher test complete!")
        print("=" * 50)
        
    except Exception as e:
        print(f"\n[ERROR] {e}")
        exit(1)
