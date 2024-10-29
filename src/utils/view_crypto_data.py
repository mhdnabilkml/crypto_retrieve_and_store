import psycopg2
import pandas as pd
from datetime import datetime, timedelta
import pytz
from tabulate import tabulate

class CryptoDataViewer:
    def __init__(self):
        self.conn_params = {
            'dbname': 'crypto_market_data',
            'user': 'username',
            'password': 'password',
            'host': 'localhost',
            'port': '5432'
        }
    
    def get_available_symbols(self):
        """List all symbols in the database"""
        query = """
        SELECT DISTINCT symbol 
        FROM ohlc_data 
        ORDER BY symbol;
        """
        
        with psycopg2.connect(**self.conn_params) as conn:
            df = pd.read_sql_query(query, conn)
            return df['symbol'].tolist()
    
    def get_date_range(self, symbol):
        """Get the date range for a specific symbol"""
        query = """
        SELECT 
            MIN(timestamp) as earliest_date,
            MAX(timestamp) as latest_date,
            COUNT(*) as total_records
        FROM ohlc_data
        WHERE symbol = %s;
        """
        
        with psycopg2.connect(**self.conn_params) as conn:
            df = pd.read_sql_query(query, conn, params=[symbol])
            return df.iloc[0]
    
    def view_recent_data(self, symbol, limit=10):
        """View most recent data for a symbol"""
        query = """
        SELECT 
            timestamp,
            open,
            high,
            low,
            close,
            volume
        FROM ohlc_data
        WHERE symbol = %s
        ORDER BY timestamp DESC
        LIMIT %s;
        """
        
        with psycopg2.connect(**self.conn_params) as conn:
            df = pd.read_sql_query(query, conn, params=[symbol, limit])
            # Format timestamp
            df['timestamp'] = df['timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')
            return df
    
    def get_data_by_daterange(self, symbol, start_date, end_date):
        """Get data for a specific date range"""
        query = """
        SELECT 
            timestamp,
            open,
            high,
            low,
            close,
            volume
        FROM ohlc_data
        WHERE symbol = %s
        AND timestamp BETWEEN %s AND %s
        ORDER BY timestamp DESC;
        """
        
        with psycopg2.connect(**self.conn_params) as conn:
            return pd.read_sql_query(query, conn, params=[symbol, start_date, end_date])
    
    def get_database_stats(self):
        """Get general statistics about the database"""
        query = """
        SELECT 
            symbol,
            COUNT(*) as record_count,
            MIN(timestamp) as earliest_date,
            MAX(timestamp) as latest_date,
            ROUND(AVG(volume), 2) as avg_volume
        FROM ohlc_data
        GROUP BY symbol
        ORDER BY record_count DESC;
        """
        
        with psycopg2.connect(**self.conn_params) as conn:
            return pd.read_sql_query(query, conn)

def main():
    viewer = CryptoDataViewer()
    
    while True:
        print("\nCrypto Market Data Viewer")
        print("1. List available symbols")
        print("2. View recent data for a symbol")
        print("3. View date range for a symbol")
        print("4. View data by date range")
        print("5. View database statistics")
        print("6. Exit")
        
        choice = input("\nEnter your choice (1-6): ")
        
        if choice == '1':
            symbols = viewer.get_available_symbols()
            print("\nAvailable symbols:")
            for symbol in symbols:
                print(symbol)
            print(f"\nTotal symbols: {len(symbols)}")
        
        elif choice == '2':
            symbol = input("Enter symbol (e.g., BTCUSDT): ").upper()
            limit = int(input("Enter number of records to view: "))
            df = viewer.view_recent_data(symbol, limit)
            print(f"\nMost recent {limit} records for {symbol}:")
            print(tabulate(df, headers='keys', tablefmt='psql', floatfmt=".2f"))
        
        elif choice == '3':
            symbol = input("Enter symbol (e.g., BTCUSDT): ").upper()
            info = viewer.get_date_range(symbol)
            print(f"\nData range for {symbol}:")
            print(f"Earliest date: {info['earliest_date']}")
            print(f"Latest date: {info['latest_date']}")
            print(f"Total records: {info['total_records']}")
        
        elif choice == '4':
            symbol = input("Enter symbol (e.g., BTCUSDT): ").upper()
            start_date = input("Enter start date (YYYY-MM-DD): ")
            end_date = input("Enter end date (YYYY-MM-DD): ")
            df = viewer.get_data_by_daterange(symbol, start_date, end_date)
            print(f"\nData for {symbol} from {start_date} to {end_date}:")
            print(tabulate(df, headers='keys', tablefmt='psql', floatfmt=".2f"))
        
        elif choice == '5':
            stats = viewer.get_database_stats()
            print("\nDatabase Statistics:")
            print(tabulate(stats, headers='keys', tablefmt='psql', floatfmt=".2f"))
        
        elif choice == '6':
            print("Goodbye!")
            break
        
        else:
            print("Invalid choice. Please try again.")
        
        input("\nPress Enter to continue...")

if __name__ == "__main__":
    main()