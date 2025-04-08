import time
import requests
import pandas as pd
import mysql.connector as mdb
from urllib.parse import quote_plus
from sqlalchemy import create_engine
from fyers_api import fyersModel
from datetime import datetime, timedelta
from openpyxl import Workbook
import os
import logging
from collections import deque
import requests
import json
from dotenv import load_dotenv

# ✅ Configure Telegram Bot
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

def send_telegram_alert(message):
    """Send alert to Telegram"""
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        payload = {"chat_id": TELEGRAM_CHAT_ID, "text": message}
        response = requests.post(url, json=payload)
        if response.status_code == 200:
            logging.info(f"✅ Telegram alert sent: {message}")
        else:
            logging.error(f"❌ Telegram alert failed: {response.text}")
    except Exception as e:
        logging.error(f"❌ Telegram API Error: {e}")


# FYERS API Credentials
client_id = os.getenv("client_id")
access_token = os.getenv("FYERS_ACCESS_TOKEN")

# Database Configuration
db_config = {
    "host": "localhost",
    "user": "sec_user",
    "password": os.getenv("db_password"),
    "database": "Algo_trading"
}

# ✅ Initialize Fyers API
fyers = fyersModel.FyersModel(client_id=client_id, token=access_token, is_async=False, log_path="")

# ✅ Connect to MySQL
def connect_db():
    return mdb.connect(**db_config)

# ✅ Log signal to MySQL (BUY/SELL)
def log_signal_to_db(symbol, signal_type, price):
    try:
        con = connect_db()
        cursor = con.cursor()

        signal_color = "green" if signal_type == "BUY" else "red"
        strategy = "MW_RSI"

        query = """
            INSERT INTO signal_log (symbol, signal_type, signal_color, signal_time, strategy, price)
            VALUES (%s, %s, %s, NOW(), %s, %s)
        """
        cursor.execute(query, (symbol, signal_type, signal_color, strategy, price))
        con.commit()
        cursor.close()
        con.close()

    except Exception as e:
        print(f"❌ Failed to log signal for {symbol}: {e}")

# ✅ Fetch Historical Data from MySQL
def fetch_historical_data(symbol, days=14):
    """Fetch last `days` of historical data for a stock from MySQL"""
    try:
        con = connect_db()
        query = f"""
            SELECT date, close 
            FROM daily_price 
            WHERE symbol = '{symbol}' 
            ORDER BY date DESC 
            LIMIT {days}
        """
        df = pd.read_sql(query, con)
        con.close()
        
        if df.empty:
            return None
        
        df["date"] = pd.to_datetime(df["date"])
        df.set_index("date", inplace=True)
        return df.sort_index()
    
    except Exception as e:
        return None

# ✅ Fetch Live Market Data from Fyers API
def get_market_data(symbols):
    """Fetch live prices for multiple symbols from Fyers API."""
    try:
        symbols_str = ",".join([f"NSE:{symbol}-EQ" for symbol in symbols])
        response = fyers.quotes({"symbols": symbols_str})

        if response.get("s") == "ok":
            return {item["n"].split(":")[-1]: item["v"].get("lp", 0) for item in response["d"]}
    
    except Exception:
        return {}

# ✅ Calculate RSI
def calculate_rsi(df, period=14):
    """Calculate RSI using rolling average method"""
    delta = df["close"].diff()
    gain = delta.where(delta > 0, 0)
    loss = -delta.where(delta < 0, 0)

    avg_gain = gain.rolling(window=period).mean()
    avg_loss = loss.rolling(window=period).mean()

    rs = avg_gain / (avg_loss + 1e-10)  # Avoid division by zero
    df["RSI"] = 100 - (100 / (1 + rs))
    return df

# ✅ Trading Strategy + Signal Logging
def check_trade_signals(symbol, df, live_price):
    df = calculate_rsi(df)
    df["RSI_Previous"] = df["RSI"].shift(1)

    buy_signal = df.iloc[-1]["RSI"] < 30 and df.iloc[-2]["RSI"] >= 30
    sell_signal = df.iloc[-1]["RSI"] > 70 and df.iloc[-2]["RSI"] <= 70

    if buy_signal:
        message = f"🚀 BUY {symbol} at ₹{live_price}"
        send_telegram_alert(message)
        log_signal_to_db(symbol, "BUY", live_price)

    elif sell_signal:
        message = f"⚠️ SELL {symbol} at ₹{live_price}"
        send_telegram_alert(message)
        log_signal_to_db(symbol, "SELL", live_price)


# ✅ Auto Trading Logic
def auto_trade(symbols):
    while True:
        try:
            # Get live market prices
            live_prices = get_market_data(symbols)

            for symbol in symbols:
                df = fetch_historical_data(symbol)
                if df is not None and symbol in live_prices:
                    df.loc[datetime.now()] = live_prices[symbol]  # Append live price to historical data
                    check_trade_signals(symbol, df, live_prices[symbol])

            time.sleep(5)  # Reduce API call frequency

        except Exception:
            continue  # Skip errors and retry

# Run Trading Bot
if __name__ == "__main__":
   stock_list = [
        "ADANIENT", "ADANIPORTS", "APOLLOHOSP", "ASIANPAINT", "AXISBANK",
        "BAJAJ-AUTO", "BAJFINANCE", "BAJAJFINSV", "BEL", "BPCL",
        "BHARTIARTL", "BRITANNIA", "CIPLA", "COALINDIA", "DRREDDY",
        "EICHERMOT", "GRASIM", "HCLTECH", "HDFCBANK", "HDFCLIFE",
        "HEROMOTOCO", "HINDALCO", "HINDUNILVR", "ICICIBANK", "INDUSINDBK",
        "INFY", "ITC", "JSWSTEEL", "KOTAKBANK", "LT",
        "M&M", "MARUTI", "NESTLEIND", "NTPC", "ONGC",
        "POWERGRID", "RELIANCE", "SBILIFE", "SHRIRAMFIN", "SBIN",
        "SUNPHARMA", "TCS", "TATACONSUM", "TATAMOTORS", "TATASTEEL",
        "TECHM", "TITAN", "TRENT", "ULTRACEMCO", "WIPRO"
]
auto_trade(stock_list)
