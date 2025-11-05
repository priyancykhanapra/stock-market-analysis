import pandas as pd
import yfinance as yf
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_batch
import argparse
import time
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_db_connection():
    """Establishes and returns a PostgreSQL database connection."""
    try:
        conn = psycopg2.connect(
            dbname="stock_20_25",
            user="postgres",
            password="root",
            host="127.0.0.1",
            port="5433"
        )
        return conn
    except psycopg2.OperationalError as e:
        logging.error(f"Database connection failed: {e}")
        return None

def fetch_and_insert_metadata(conn, ticker):
    """Fetches and inserts stock metadata for a single ticker."""
    try:
        ticker_info = yf.Ticker(ticker).info
        if ticker_info:
            name = ticker_info.get('longName')
            sector = ticker_info.get('sector')
            industry = ticker_info.get('industry')

            with conn.cursor() as cur:
                cur.execute(
                    sql.SQL("INSERT INTO stock_metadata (ticker, name, sector, industry) VALUES (%s, %s, %s, %s) ON CONFLICT (ticker) DO NOTHING;"),
                    (ticker, name, sector, industry)
                )
            conn.commit()
            logging.info(f"Metadata for {ticker} inserted.")
    except Exception as e:
        logging.warning(f"Could not fetch metadata for {ticker}: {e}")

def fetch_and_insert_daily_data(conn, ticker, start_date, end_date):
    """Fetches historical daily data for a ticker and inserts it into the database."""
    try:
        data = yf.download(ticker, start=start_date, end=end_date, progress=False)
        if data.empty:
            logging.warning(f"No data fetched for {ticker} from {start_date} to {end_date}")
            return

        # Prepare data for batch insertion
        data = data.reset_index()
        data.rename(columns={
            'Date': 'time',
            'Open': 'open',
            'High': 'high',
            'Low': 'low',
            'Close': 'close',
            'Volume': 'volume'
        }, inplace=True)
        data['ticker'] = ticker

        # Convert DataFrame to a list of tuples
        data_to_insert = [tuple(row) for row in data[['time', 'ticker', 'open', 'high', 'low', 'close', 'volume']].itertuples(index=False)]

        with conn.cursor() as cur:
            # Upsert logic to handle potential duplicate entries
            execute_batch(
                cur,
                sql.SQL("""
                    INSERT INTO stock_data_daily (time, ticker, open, high, low, close, volume)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (time, ticker) DO UPDATE SET
                        open = EXCLUDED.open,
                        high = EXCLUDED.high,
                        low = EXCLUDED.low,
                        close = EXCLUDED.close,
                        volume = EXCLUDED.volume;
                """),
                data_to_insert
            )
        conn.commit()
        logging.info(f"Successfully inserted {len(data_to_insert)} rows for {ticker}.")

    except Exception as e:
        logging.error(f"Error processing data for {ticker}: {e}")
        if conn:
            conn.rollback()

def main(start_date, end_date, instruments_file):
    """Main function to orchestrate the data fetching and insertion process."""
    try:
        instruments = pd.read_csv(instruments_file)['ticker'].tolist()
    except FileNotFoundError:
        logging.error(f"Instruments file '{instruments_file}' not found.")
        return

    conn = get_db_connection()
    if not conn:
        return

    try:
        for ticker in instruments:
            logging.info(f"Processing ticker: {ticker}")
            
            # Fetch and insert metadata
            fetch_and_insert_metadata(conn, ticker)
            
            # Fetch and insert daily data
            fetch_and_insert_daily_data(conn, ticker, start_date, end_date)
            
            # Add a small delay to avoid hitting API rate limits
            time.sleep(1)

    finally:
        if conn:
            conn.close()
            logging.info("Database connection closed.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch historical stock data and insert into TimescaleDB.")
    parser.add_argument("start_date", help="Start date (YYYY-MM-DD)")
    parser.add_argument("end_date", help="End date (YYYY-MM-DD)")
    parser.add_argument("--instruments_file", default="instruments.csv", help="Path to the CSV file with tickers")
    args = parser.parse_args()

    main(args.start_date, args.end_date, args.instruments_file)