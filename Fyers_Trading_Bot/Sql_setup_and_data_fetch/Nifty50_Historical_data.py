import datetime
import time
import mysql.connector as mdb
from fyers_apiv3 import fyersModel
import os
from dotenv import load_dotenv
load_dotenv()

# ✅ Fyers API Credentials
app_id = os.getenv("client_id")
access_token = os.getenv("FYERS_ACCESS_TOKEN")

# ✅ Initialize Fyers API
fyers = fyersModel.FyersModel(client_id=app_id, token=access_token, is_async=False)

# ✅ MySQL Database Connection
db_config = {
    "host": "localhost",
    "user": "sec_user",
    "password": os.getenv("db_password"),
    "database": "Algo_trading"
}
con = mdb.connect(**db_config)
cur = con.cursor()

# ✅ Fetch Tickers from Database
def get_nifty50_tickers():
    """Fetch Nifty 50 tickers and symbol IDs from the database."""
    cur.execute("SELECT id, ticker, name FROM symbol")
    return cur.fetchall()

# ✅ Get Last Available Date for a Symbol
def get_last_available_date(symbol_id):
    """Fetch the most recent date for which data is available in MySQL."""
    cur.execute(
        "SELECT MAX(price_date) FROM daily_price WHERE symbol_id = %s",
        (symbol_id,)
    )
    result = cur.fetchone()
    return result[0] if result and result[0] else None

# ✅ Fetch Historical Data from Fyers API
def fetch_historical_data(symbol, start_date, end_date):
    """Fetches historical stock data from Fyers API in chunks."""
    all_prices = []
    batch_size = 100  # Fetch 100 days per request
    
    while start_date <= end_date:
        batch_end = min(start_date + datetime.timedelta(days=batch_size - 1), end_date)
        payload = {
            "symbol": f"NSE:{symbol}-EQ",
            "resolution": "D",
            "date_format": "1",
            "range_from": start_date.strftime('%Y-%m-%d'),
            "range_to": batch_end.strftime('%Y-%m-%d'),
            "cont_flag": "0"
        }
        
        for attempt in range(3):  # Retry mechanism
            response = fyers.history(data=payload)
            if response and "candles" in response and response["candles"]:
                break
            print(f"⚠️ Retry {attempt+1}/3 for {symbol} ({start_date} to {batch_end})...")
            time.sleep(2)
        
        if response and "candles" in response and response["candles"]:
            all_prices.extend([
                (datetime.datetime.fromtimestamp(d[0]), d[1], d[2], d[3], d[4], d[5])
                for d in response["candles"]
            ])
        else:
            print(f"❌ No data found for {symbol} ({start_date} to {batch_end})")
        
        start_date = batch_end + datetime.timedelta(days=1)  # Ensure continuous data fetch
    
    return all_prices

# ✅ Insert Data into MySQL
def insert_into_db(data_vendor_id, symbol_id, stock_name, price_data):
    """Inserts historical stock data into MySQL."""
    if not price_data:
        print(f"⚠️ No new data to insert for {stock_name}")
        return

    now = datetime.datetime.now(datetime.timezone.utc)
    insert_values = [
        (data_vendor_id, symbol_id, stock_name, d[0], now, now, d[1], d[2], d[3], d[4], d[5])
        for d in price_data
    ]
    
    # 🔥 Debug: Print first 5 rows before inserting
    print(f"🛠️ Preparing to insert {len(price_data)} rows into daily_price:")
    print(insert_values[:5])  

    query = """
    INSERT INTO daily_price (
        data_vendor_id, symbol_id, stock_name, price_date, created_date,
        last_updated_date, open_price, high_price, low_price,
        close_price, volume
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    
    try:
        cur.executemany(query, insert_values)
        con.commit()
        print(f"✅ Successfully inserted {len(price_data)} records for {stock_name}")
    except mdb.Error as e:
        print(f"❌ MySQL Error for {stock_name}: {e}")
        con.rollback()

# ✅ Main Execution
if __name__ == "__main__":
    tickers = get_nifty50_tickers()
    total_tickers = len(tickers)
    today = datetime.date.today()
    end_date = today - datetime.timedelta(days=1)  # ✅ Fetch data only until yesterday
    
    for i, (symbol_id, ticker, stock_name) in enumerate(tickers):
        print(f"📊 Checking last available date for {stock_name} ({ticker})...")
        
        last_available_date = get_last_available_date(symbol_id)
        
        if last_available_date:
            start_date = last_available_date.date() + datetime.timedelta(days=1)
            print(f"📅 Last available date: {last_available_date} → Fetching from {start_date} to {end_date}")
        else:
            start_date = datetime.date(2024, 1, 1)
            print(f"🆕 No data found in DB → Fetching from {start_date} to {end_date}")
        
        # Fetch data if there are missing dates
        if start_date <= end_date:
            historical_data = fetch_historical_data(ticker, start_date, end_date)
            
            if historical_data:
                insert_into_db(1, symbol_id, stock_name, historical_data)
                print(f"✅ Inserted {len(historical_data)} rows for {stock_name} ({ticker})")
            else:
                print(f"⚠️ No new data found for {stock_name} ({ticker})")
        else:
            print(f"✅ Data is already up-to-date for {stock_name} ({ticker})")
    
    print("🎉 Data fetching complete!")