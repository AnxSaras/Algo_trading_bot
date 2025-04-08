import datetime
import pandas as pd
from fyers_apiv3 import fyersModel
from sqlalchemy import create_engine
from sqlalchemy import text
import urllib.parse

# 🔹 Encode Password for MySQL Connection
DB_USER = "sec_user"
DB_PASS = "Apple@1331"
DB_NAME = "Algo_trading"
DB_HOST = "localhost"

ENCODED_PASS = urllib.parse.quote_plus(DB_PASS)
SQL_CONNECTION_STRING = f"mysql+pymysql://{DB_USER}:{ENCODED_PASS}@{DB_HOST}/{DB_NAME}"

# 🔹 Read API Credentials
app_id = open("fyers_appid.txt", 'r').read().strip()
access_token = open("fyers_token.txt", 'r').read().strip()

# 🔹 Initialize Fyers API
fyers = fyersModel.FyersModel(client_id=app_id, token=access_token)

# 🔹 NIFTY 50 Symbols List
NIFTY_50_TICKERS = [
    "RELIANCE", "TCS", "HDFCBANK", "INFY", "ICICIBANK", "HINDUNILVR", "ITC",
    "SBIN", "BHARTIARTL", "KOTAKBANK", "LT", "HCLTECH", "ASIANPAINT", "AXISBANK",
    "MARUTI", "SUNPHARMA", "TITAN", "ULTRACEMCO", "BAJFINANCE", "WIPRO",
    "ONGC", "M&M", "POWERGRID", "NTPC", "NESTLEIND", "JSWSTEEL", "TECHM",
    "TATAMOTORS", "INDUSINDBK", "HDFCLIFE", "DRREDDY", "BAJAJFINSV", "HINDALCO",
    "GRASIM", "CIPLA", "ADANIPORTS", "SBILIFE", "TATASTEEL", "DIVISLAB",
    "BRITANNIA", "COALINDIA", "BPCL", "EICHERMOT", "UPL", "HEROMOTOCO",
    "APOLLOHOSP", "BAJAJ-AUTO", "SHREECEM"
]

# 🔹 Fetch Nifty 50 Symbols from Fyers API
def fetch_nifty50_symbols():
    """Fetch NIFTY 50 stock details from Fyers API."""
    symbols = []
    now = datetime.datetime.now()

    for ticker in NIFTY_50_TICKERS:
        fyers_symbol = f"NSE:{ticker}-EQ"
        data = {"symbols": fyers_symbol}

        try:
            response = fyers.quotes(data)

            if 'd' in response and response['d']:
                stock_info = response['d'][0]

                name = stock_info.get("name", ticker)
                sector = stock_info.get("sector", "Unknown")
                industry = stock_info.get("industry", "Unknown")
                isin = stock_info.get("isin", None)  # Ensure None for NULL in DB

                symbols.append((1, ticker, name, sector, industry, isin, now, now))
            else:
                print(f"⚠️ No data found for {ticker}")

        except Exception as e:
            print(f"❌ Error fetching data for {ticker}: {e}")

    print(f"✅ Symbols fetched: {len(symbols)}")  # Debug log
    return symbols

# 🔹 Insert Data into MySQL
def insert_symbols_to_db(symbols):
    """Insert symbols into MySQL, avoiding duplicates and handling NULL ISINs."""
    if not symbols:
        print("⚠️ No symbols to insert.")
        return

    # ✅ Create SQLAlchemy Engine
    engine = create_engine(SQL_CONNECTION_STRING)

    # ✅ Debugging: Check Symbols List Format
    print("🔍 Checking symbols format before inserting:")
    for sym in symbols[:5]:  # Print only first 5 for debugging
        print(sym)

    # ✅ Convert Data to DataFrame
    column_names = ["exchange_id", "ticker", "name", "sector", "industry", "isin", "created_date", "last_updated_date"]
    df = pd.DataFrame(symbols, columns=column_names)

    # ✅ Ensure `None` values are converted to `NULL`
    df = df.where(pd.notnull(df), None)

    try:
        # ✅ Insert into MySQL using dictionaries (SQLAlchemy)
        with engine.begin() as conn:
            sql_query = text("""  
                INSERT INTO symbol (exchange_id, ticker, name, sector, industry, isin, created_date, last_updated_date)  
                VALUES (:exchange_id, :ticker, :name, :sector, :industry, :isin, :created_date, :last_updated_date)  
                ON DUPLICATE KEY UPDATE  
                    name = VALUES(name),  
                    sector = VALUES(sector),  
                    industry = VALUES(industry),  
                    last_updated_date = VALUES(last_updated_date);  
            """)

            for _, row in df.iterrows():
                conn.execute(sql_query, row.to_dict())  # ✅ Use text() wrapped query

        print(f"✅ Successfully inserted/updated {len(symbols)} NIFTY 50 symbols into MySQL.")

    except Exception as e:
        print(f"❌ Error inserting into MySQL: {e}")

# 🔹 Run Fetch & Store Process
if __name__ == "__main__":
    symbols = fetch_nifty50_symbols()
    insert_symbols_to_db(symbols)
