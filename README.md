# Crypto Data Pipeline

A Python application that fetches cryptocurrency OHLCV (Open, High, Low, Close, Volume) historical data from Binance, stores it in a PostgreSQL database, and provides tools to view and export the data to Excel for analysis.

## Setup

1. Clone the repository
2. Install requirements: `pip install -r requirements.txt`
3. Copy `.env.example` to `.env` and update with your database credentials
4. Run database setup: `python src/database/create_db.py`
5. Fetch data: `python src/data/binance_fetcher.py`
6. View or export data using the utility scripts in `src/utils/`

## Features
- Fetches historical cryptocurrency data from Binance
- Stores data in PostgreSQL database
- View data through command-line interface
- Export data to formatted Excel files