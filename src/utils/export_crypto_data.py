import psycopg2
import pandas as pd
from datetime import datetime, timedelta
import os

class CryptoDataExporter:
    def __init__(self):
        self.conn_params = {
            'dbname': 'crypto_market_data',
            'user': 'username',
            'password': 'password',
            'host': 'localhost',
            'port': '5432'
        }
        
        # Create exports directory if it doesn't exist
        self.export_dir = 'crypto_exports'
        if not os.path.exists(self.export_dir):
            os.makedirs(self.export_dir)
    
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
    
    def export_single_symbol(self, symbol, start_date=None, end_date=None):
        """Export data for a single symbol to Excel"""
        # Base query
        query = """
        SELECT 
            timestamp,
            symbol,
            open,
            high,
            low,
            close,
            volume
        FROM ohlc_data
        WHERE symbol = %s
        """
        params = [symbol]
        
        # Add date range if specified
        if start_date and end_date:
            query += " AND timestamp BETWEEN %s AND %s"
            params.extend([start_date, end_date])
        
        query += " ORDER BY timestamp DESC"
        
        # Fetch data
        with psycopg2.connect(**self.conn_params) as conn:
            df = pd.read_sql_query(query, conn, params=params)
        
        # Format timestamp
        df['timestamp'] = pd.to_datetime(df['timestamp']).dt.strftime('%Y-%m-%d %H:%M:%S')
        
        # Create Excel file
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"{self.export_dir}/{symbol}_data_{timestamp}.xlsx"
        
        # Create Excel writer with datetime formatting
        with pd.ExcelWriter(filename, engine='xlsxwriter') as writer:
            # Write data
            df.to_excel(writer, sheet_name='OHLCV Data', index=False)
            
            # Get workbook and worksheet objects
            workbook = writer.book
            worksheet = writer.sheets['OHLCV Data']
            
            # Add formats
            header_format = workbook.add_format({
                'bold': True,
                'text_wrap': True,
                'valign': 'top',
                'border': 1,
                'bg_color': '#D9D9D9'
            })
            
            number_format = workbook.add_format({
                'num_format': '#,##0.00000000'
            })
            
            volume_format = workbook.add_format({
                'num_format': '#,##0.00'
            })
            
            # Apply formats
            for col_num, value in enumerate(df.columns.values):
                worksheet.write(0, col_num, value, header_format)
                
            # Set column widths and formats
            worksheet.set_column('A:A', 20)  # Timestamp
            worksheet.set_column('B:B', 12)  # Symbol
            worksheet.set_column('C:F', 15, number_format)  # OHLC
            worksheet.set_column('G:G', 15, volume_format)  # Volume
            
            # Freeze top row
            worksheet.freeze_panes(1, 0)
            
        print(f"Data exported to {filename}")
        return filename
    
    def export_all_symbols(self, start_date=None, end_date=None):
        """Export data for all symbols to separate sheets in one Excel file"""
        symbols = self.get_available_symbols()
        
        # Create Excel file
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"{self.export_dir}/all_crypto_data_{timestamp}.xlsx"
        
        with pd.ExcelWriter(filename, engine='xlsxwriter') as writer:
            workbook = writer.book
            
            # Add formats
            header_format = workbook.add_format({
                'bold': True,
                'text_wrap': True,
                'valign': 'top',
                'border': 1,
                'bg_color': '#D9D9D9'
            })
            
            number_format = workbook.add_format({
                'num_format': '#,##0.00000000'
            })
            
            volume_format = workbook.add_format({
                'num_format': '#,##0.00'
            })
            
            for symbol in symbols:
                # Fetch data for each symbol
                query = """
                SELECT 
                    timestamp,
                    symbol,
                    open,
                    high,
                    low,
                    close,
                    volume
                FROM ohlc_data
                WHERE symbol = %s
                """
                params = [symbol]
                
                if start_date and end_date:
                    query += " AND timestamp BETWEEN %s AND %s"
                    params.extend([start_date, end_date])
                
                query += " ORDER BY timestamp DESC"
                
                with psycopg2.connect(**self.conn_params) as conn:
                    df = pd.read_sql_query(query, conn, params=params)
                
                # Format timestamp
                df['timestamp'] = pd.to_datetime(df['timestamp']).dt.strftime('%Y-%m-%d %H:%M:%S')
                
                # Write to Excel
                sheet_name = symbol[:31]  # Excel sheet names limited to 31 chars
                df.to_excel(writer, sheet_name=sheet_name, index=False)
                
                # Get worksheet object
                worksheet = writer.sheets[sheet_name]
                
                # Apply formats
                for col_num, value in enumerate(df.columns.values):
                    worksheet.write(0, col_num, value, header_format)
                
                # Set column widths and formats
                worksheet.set_column('A:A', 20)  # Timestamp
                worksheet.set_column('B:B', 12)  # Symbol
                worksheet.set_column('C:F', 15, number_format)  # OHLC
                worksheet.set_column('G:G', 15, volume_format)  # Volume
                
                # Freeze top row
                worksheet.freeze_panes(1, 0)
                
        print(f"All data exported to {filename}")
        return filename

def main():
    exporter = CryptoDataExporter()
    
    while True:
        print("\nCrypto Data Excel Exporter")
        print("1. Export single symbol")
        print("2. Export all symbols")
        print("3. Export single symbol with date range")
        print("4. Export all symbols with date range")
        print("5. List available symbols")
        print("6. Exit")
        
        choice = input("\nEnter your choice (1-6): ")
        
        if choice == '1':
            symbol = input("Enter symbol (e.g., BTCUSDT): ").upper()
            exporter.export_single_symbol(symbol)
        
        elif choice == '2':
            exporter.export_all_symbols()
        
        elif choice == '3':
            symbol = input("Enter symbol (e.g., BTCUSDT): ").upper()
            start_date = input("Enter start date (YYYY-MM-DD): ")
            end_date = input("Enter end date (YYYY-MM-DD): ")
            exporter.export_single_symbol(symbol, start_date, end_date)
        
        elif choice == '4':
            start_date = input("Enter start date (YYYY-MM-DD): ")
            end_date = input("Enter end date (YYYY-MM-DD): ")
            exporter.export_all_symbols(start_date, end_date)
        
        elif choice == '5':
            symbols = exporter.get_available_symbols()
            print("\nAvailable symbols:")
            for symbol in symbols:
                print(symbol)
        
        elif choice == '6':
            print("Goodbye!")
            break
        
        else:
            print("Invalid choice. Please try again.")

if __name__ == "__main__":
    main()