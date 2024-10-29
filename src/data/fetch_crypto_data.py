import requests
import pandas as pd
from datetime import datetime, timedelta
import psycopg2
import time
from typing import List, Dict
import pytz

class BinanceFetcher:
    def __init__(self):
        self.base_url = "https://api.binance.com/api/v3"
    
    def fetch_klines_data(self, symbol: str, interval: str = '1d', 
                         limit: int = 1000) -> List[Dict]:
        """
        Fetch OHLCV data from Binance
        
        Args:
            symbol: Trading pair (e.g., 'BTCUSDT', 'ETHUSDT')
            interval: Timeframe ('1d' for daily, '1h' for hourly, etc.)
            limit: Number of candles to fetch (max 1000)
        """
        endpoint = f"{self.base_url}/klines"
        
        params = {
            'symbol': symbol,
            'interval': interval,
            'limit': limit
        }
        
        try:
            response = requests.get(endpoint, params=params)
            response.raise_for_status()
            data = response.json()
            
            # Convert Binance data to our format
            ohlc_data = []
            for entry in data:
                timestamp = datetime.fromtimestamp(entry[0]/1000, tz=pytz.UTC)
                
                ohlc_data.append({
                    'symbol': symbol,
                    'timestamp': timestamp,
                    'open': float(entry[1]),
                    'high': float(entry[2]),
                    'low': float(entry[3]),
                    'close': float(entry[4]),
                    'volume': float(entry[5])
                })
            
            print(f"Fetched {len(ohlc_data)} records for {symbol}")
            return ohlc_data
            
        except requests.exceptions.RequestException as e:
            print(f"Error fetching data for {symbol}: {e}")
            return []
    
    def get_exchange_info(self) -> List[str]:
        """
        Get list of all available trading pairs
        """
        endpoint = f"{self.base_url}/exchangeInfo"
        
        try:
            response = requests.get(endpoint)
            response.raise_for_status()
            data = response.json()
            
            # Filter for USDT pairs only
            symbols = [symbol['symbol'] for symbol in data['symbols'] 
                      if symbol['symbol'].endswith('USDT')
                      and symbol['status'] == 'TRADING']
            
            return symbols
            
        except requests.exceptions.RequestException as e:
            print(f"Error fetching exchange info: {e}")
            return []

class DatabaseHandler:
    def __init__(self):
        self.conn_params = {
            'dbname': 'crypto_market_data',
            'user': 'username',
            'password': 'password',
            'host': 'localhost',
            'port': '5432'
        }
    
    def store_data(self, data: List[Dict]) -> None:
        """
        Store OHLC data in PostgreSQL database
        """
        if not data:
            return
            
        insert_query = """
        INSERT INTO ohlc_data (symbol, timestamp, open, high, low, close, volume)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (symbol, timestamp) 
        DO UPDATE SET
            open = EXCLUDED.open,
            high = EXCLUDED.high,
            low = EXCLUDED.low,
            close = EXCLUDED.close,
            volume = EXCLUDED.volume;
        """
        
        try:
            with psycopg2.connect(**self.conn_params) as conn:
                with conn.cursor() as cur:
                    for row in data:
                        cur.execute(insert_query, (
                            row['symbol'],
                            row['timestamp'],
                            row['open'],
                            row['high'],
                            row['low'],
                            row['close'],
                            row['volume']
                        ))
                    print(f"Successfully stored {len(data)} records")
        except Exception as e:
            print(f"Error storing data: {e}")
            raise

def main():
    # Initialize classes
    fetcher = BinanceFetcher()
    db_handler = DatabaseHandler()
    
    # List of popular trading pairs
    symbols = [
        'BTCUSDT',
        'ETHUSDT',
        'BNBUSDT',
        'ADAUSDT',
        'SOLUSDT'
    ]
    
    # Alternatively, fetch all available USDT pairs
    # symbols = fetcher.get_exchange_info()
    
    # Fetch and store data for each symbol
    for symbol in symbols:
        print(f"\nProcessing {symbol}...")
        
        try:
            # Fetch data (default: last 1000 daily candles)
            data = fetcher.fetch_klines_data(symbol)
            
            # Store data
            if data:
                db_handler.store_data(data)
            
            # Sleep to respect rate limits
            time.sleep(1)  # Binance allows 1200 requests per minute
            
        except Exception as e:
            print(f"Error processing {symbol}: {e}")
            continue

def list_available_pairs():
    """
    Utility function to list all available USDT trading pairs
    """
    fetcher = BinanceFetcher()
    symbols = fetcher.get_exchange_info()
    
    print("Available USDT trading pairs:")
    for symbol in symbols[:10]:  # Print first 10 pairs as example
        print(symbol)
    print(f"\nTotal available USDT pairs: {len(symbols)}")

if __name__ == "__main__":
    # Uncomment to see available trading pairs
    # list_available_pairs()
    
    main()