import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

def create_database():
    # Connect to default postgres database first
    conn_params = {
        'dbname': 'postgres',
        'user': 'username',
        'password': 'password',
        'host': 'localhost',
        'port': '5432'
    }
    
    try:
        # Connect to default postgres database
        conn = psycopg2.connect(**conn_params)
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = conn.cursor()
        
        # Create new database
        db_name = 'crypto_market_data'
        cur.execute(f"SELECT 1 FROM pg_catalog.pg_database WHERE datname = '{db_name}'")
        exists = cur.fetchone()
        
        if not exists:
            cur.execute(f'CREATE DATABASE {db_name}')
            print(f"Database '{db_name}' created successfully")
        else:
            print(f"Database '{db_name}' already exists")
            
        cur.close()
        conn.close()
        
        # Connect to the new database and create tables
        conn_params['dbname'] = db_name
        conn = psycopg2.connect(**conn_params)
        cur = conn.cursor()
        
        # Create OHLC table with cryptocurrency specific considerations
        create_table_query = """
        CREATE TABLE IF NOT EXISTS ohlc_data (
            id SERIAL PRIMARY KEY,
            symbol VARCHAR(20) NOT NULL,  -- Increased size for longer crypto symbols
            timestamp TIMESTAMP WITH TIME ZONE NOT NULL,  -- Using timestamptz for UTC times
            open DECIMAL(18,8) NOT NULL,  -- Increased precision for crypto prices
            high DECIMAL(18,8) NOT NULL,
            low DECIMAL(18,8) NOT NULL,
            close DECIMAL(18,8) NOT NULL,
            volume DECIMAL(24,8),  -- Changed to decimal for crypto volumes
            created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(symbol, timestamp)
        );
        
        -- Create index for faster queries
        CREATE INDEX IF NOT EXISTS idx_symbol_timestamp 
        ON ohlc_data(symbol, timestamp);
        
        -- Create index for timestamp queries
        CREATE INDEX IF NOT EXISTS idx_timestamp 
        ON ohlc_data(timestamp);
        """
        
        cur.execute(create_table_query)
        conn.commit()
        print("OHLC table created successfully")
        
    except Exception as e:
        print(f"Error: {e}")
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

if __name__ == "__main__":
    create_database()