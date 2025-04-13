import time
import requests
import pandas as pd
from urllib.parse import quote_plus
from sqlalchemy import create_engine
from fyers_api import fyersModel
from datetime import datetime, timedelta
from openpyxl import Workbook
import os
import logging
from collections import deque
import json
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# ✅ Configure Telegram Bot
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

def send_telegram_alert(message):
    """Send alert to Telegram"""
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        payload = {"chat_id": TELEGRAM_CHAT_ID, "text": message}
        response = requests.post(url, json=payload, timeout=10)
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
db_user = "sec_user"
db_password = os.getenv("db_password")
db_host = "localhost"
db_name = "Algo_trading"

# SQLAlchemy connection string for MySQL
db_password_encoded = quote_plus(db_password)
db_url = f"mysql+mysqlconnector://{db_user}:{db_password_encoded}@{db_host}/{db_name}"

# Create engine
engine = create_engine(db_url)

# ✅ Initialize Fyers API
fyers = fyersModel.FyersModel(client_id=client_id, token=access_token, is_async=False, log_path="")

# ✅ Capital Management
MAX_CAPITAL = 10000  # Initial capital
STOP_LOSS_PERCENT = 0.02  # 2% stop loss
TAKE_PROFIT_PERCENT = 0.05  # 5% take profit
current_capital = MAX_CAPITAL  # Track available capital
current_position = None  # Track open position: {"symbol": str, "quantity": int, "entry_price": float, "order_id": str}

# ✅ Create Trade Log Table
def init_trade_log_table():
    """Initialize trade_log table if it doesn't exist"""
    try:
        create_table_query = """
            CREATE TABLE IF NOT EXISTS trade_log (
                id INT AUTO_INCREMENT PRIMARY KEY,
                symbol VARCHAR(50),
                entry_time DATETIME,
                entry_price FLOAT,
                quantity INT,
                exit_time DATETIME,
                exit_price FLOAT,
                profit_loss FLOAT,
                capital_after_trade FLOAT
            )
        """
        with engine.begin() as connection:
            connection.execute(create_table_query)
        logging.info("✅ Trade log table initialized")
    except Exception as e:
        logging.error(f"❌ Failed to initialize trade_log table: {e}")

# ✅ Log signal to MySQL (BUY/SELL)
def log_signal_to_db(symbol, signal_type, price):
    try:
        signal_color = "green" if signal_type == "BUY" else "red"
        strategy = "MW_RSI"

        insert_query = """
            INSERT INTO signal_log (symbol, signal_type, signal_color, signal_time, strategy, price)
            VALUES (%s, %s, %s, NOW(), %s, %s)
        """

        with engine.begin() as connection:
            connection.execute(
                insert_query,
                (symbol, signal_type, signal_color, strategy, price)
            )

    except Exception as e:
        logging.error(f"❌ Failed to log signal: {e}")

# ✅ Log trade outcome to MySQL
def log_trade_to_db(symbol, entry_time, entry_price, quantity, exit_time, exit_price, profit_loss):
    global current_capital
    try:
        insert_query = """
            INSERT INTO trade_log (symbol, entry_time, entry_price, quantity, exit_time, exit_price, profit_loss, capital_after_trade)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        with engine.begin() as connection:
            connection.execute(
                insert_query,
                (symbol, entry_time, entry_price, quantity, exit_time, exit_price, profit_loss, current_capital)
            )
        logging.info(f"✅ Logged trade for {symbol}: P/L = {profit_loss}, Capital = {current_capital}")
    except Exception as e:
        logging.error(f"❌ Failed to log trade: {e}")

# ✅ Place Bracket Order
def place_bracket_order(symbol, live_price, quantity):
    global current_position
    try:
        # Calculate stop loss and take profit
        stop_loss_price = live_price * (1 - STOP_LOSS_PERCENT)
        take_profit_price = live_price * (1 + TAKE_PROFIT_PERCENT)

        # Round prices to 2 decimal places as per exchange requirements
        stop_loss_price = round(stop_loss_price, 2)
        take_profit_price = round(take_profit_price, 2)

        # Fyers API bracket order payload
        order_data = {
            "symbol": f"NSE:{symbol}-EQ",
            "qty": quantity,
            "type": 2,  # Limit order
            "side": 1,  # Buy
            "productType": "BO",  # Bracket Order
            "limitPrice": live_price,
            "stopPrice": 0,
            "validity": "DAY",
            "stopLoss": round(live_price - stop_loss_price, 2),
            "takeProfit": round(take_profit_price - live_price, 2),
            "disclosedQty": 0,
            "offlineOrder": False
        }

        response = fyers.place_order(order_data)
        if response.get("s") == "ok":
            order_id = response.get("id")
            logging.info(f"✅ Bracket order placed for {symbol}: Qty={quantity}, Entry={live_price}, SL={stop_loss_price}, TP={take_profit_price}")
            send_telegram_alert(f"🚀 BUY {symbol}: Qty={quantity} @ ₹{live_price}, SL=₹{stop_loss_price}, TP=₹{take_profit_price}")
            # Update current position
            current_position = {
                "symbol": symbol,
                "quantity": quantity,
                "entry_price": live_price,
                "entry_time": datetime.now(),
                "order_id": order_id
            }
            return True
        else:
            logging.error(f"❌ Failed to place bracket order: {response}")
            send_telegram_alert(f"❌ Failed to place order for {symbol}: {response.get('message')}")
            return False

    except Exception as e:
        logging.error(f"❌ Error placing bracket order for {symbol}: {e}")
        send_telegram_alert(f"❌ Error placing order for {symbol}: {e}")
        return False

# ✅ Check Order Status and Update Capital
def check_order_status():
    global current_position, current_capital
    if not current_position:
        return

    try:
        symbol = current_position["symbol"]
        order_id = current_position["order_id"]
        orders = fyers.orderbook().get("orderBook", [])
        for order in orders:
            if order.get("id") == order_id:
                status = order.get("status")
                if status in [2, 4]:  # 2 = Complete, 4 = Cancelled
                    # Assume filled at stop loss or take profit
                    exit_price = order.get("tradedPrice", current_position["entry_price"])
                    quantity = current_position["quantity"]
                    profit_loss = (exit_price - current_position["entry_price"]) * quantity
                    current_capital += profit_loss

                    # Log trade
                    log_trade_to_db(
                        symbol=symbol,
                        entry_time=current_position["entry_time"],
                        entry_price=current_position["entry_price"],
                        quantity=quantity,
                        exit_time=datetime.now(),
                        exit_price=exit_price,
                        profit_loss=profit_loss
                    )
                    send_telegram_alert(
                        f"🏁 Trade Closed: {symbol}, P/L=₹{profit_loss:.2f}, New Capital=₹{current_capital:.2f}"
                    )

                    # Clear position
                    current_position = None
                    logging.info(f"✅ Position closed for {symbol}, New Capital = {current_capital}")
                break

    except Exception as e:
        logging.error(f"❌ Error checking order status: {e}")

# ✅ Fetch Historical Data from MySQL
def fetch_historical_data(symbol, days=14):
    """Fetch last `days` of historical data for a stock using SQLAlchemy"""
    try:
        query = """
            SELECT price_date, close_price AS close 
            FROM daily_price 
            WHERE stock_name = %s 
            ORDER BY price_date DESC 
            LIMIT %s
        """
        df = pd.read_sql_query(query, engine, params=(symbol, days))

        if df.empty:
            return None

        df["price_date"] = pd.to_datetime(df["price_date"])
        df.set_index("price_date", inplace=True)
        return df.sort_index()

    except Exception as e:
        logging.error(f"❌ Error fetching historical data for {symbol}: {e}")
        return None

# ✅ Fetch Live Market Data from Fyers API
def get_market_data(symbols):
    """Fetch live prices for multiple symbols from Fyers API."""
    try:
        symbols_str = ",".join([f"NSE:{symbol}-EQ" for symbol in symbols])
        response = fyers.quotes({"symbols": symbols_str})

        if response.get("s") == "ok":
            market_data = {}
            for item in response["d"]:
                symbol = item["n"].split(":")[-1].split("-")[0]
                live_price = item["v"].get("lp", 0)
                market_data[symbol] = {"live_price": live_price}
            return market_data
        else:
            logging.error(f"❌ Market data fetch failed: {response}")
            return {}

    except Exception as e:
        logging.error(f"❌ Error fetching live market data: {e}")
        return {}

# ✅ Calculate RSI
def calculate_rsi(df, period=14):
    """Calculate RSI using rolling average method"""
    delta = df["close"].diff()
    gain = delta.where(delta > 0, 0)
    loss = -delta.where(delta < 0, 0)

    avg_gain = gain.rolling(window=period).mean()
    avg_loss = loss.rolling(window=period).mean()

    rs = avg_gain / (avg_loss + 1e-10)
    df["RSI"] = 100 - (100 / (1 + rs))
    return df

# ✅ Detect M/W Patterns
def detect_mw_pattern(df, min_diff_percent=0.5):
    """Detect M or W pattern based on closing prices"""
    closes = df["close"].values[-5:]
    if len(closes) < 5:
        return None

    avg_price = sum(closes) / len(closes)
    min_diff = avg_price * (min_diff_percent / 100)

    # W Pattern
    if (closes[0] > closes[1] + min_diff and
        closes[1] < closes[2] - min_diff and
        closes[2] > closes[3] + min_diff and
        closes[3] < closes[4] - min_diff):
        return "W"

    # M Pattern
    elif (closes[0] < closes[1] - min_diff and
          closes[1] > closes[2] + min_diff and
          closes[2] < closes[3] - min_diff and
          closes[3] > closes[4] + min_diff):
        return "M"

    return None

# ✅ Trading Strategy + Signal Logging
def check_trade_signals(symbol, df, live_price):
    global current_position
    df = calculate_rsi(df)
    df = df.dropna(subset=["RSI"])
    latest_rsi = df.iloc[-1]["RSI"]
    pattern = detect_mw_pattern(df)

    logging.info(f"Checking {symbol} — RSI: {latest_rsi:.2f}, Pattern: {pattern}")

    if current_position:
        return  # Skip if a position is already open

    if pattern == "W" and latest_rsi < 30:
        # Calculate quantity based on available capital
        quantity = int(current_capital / live_price)
        if quantity > 0:
            logging.info(f"[SIGNAL] BUY {symbol} at ₹{live_price}, Qty={quantity}")
            log_signal_to_db(symbol, "BUY", live_price)
            place_bracket_order(symbol, live_price, quantity)

# ✅ Auto Trading Logic
def auto_trade(symbols):
    init_trade_log_table()  # Initialize trade log table
    while True:
        try:
            # Check status of open position
            check_order_status()

            # Get live market prices
            live_prices = get_market_data(symbols)

            for symbol in symbols:
                if current_position:
                    break  # Skip if a position is open

                df = fetch_historical_data(symbol)
                if df is not None and symbol in live_prices:
                    live_time = datetime.now()
                    live_close = live_prices[symbol]["live_price"]

                    # Append live price to historical data
                    new_row = pd.DataFrame({"close": [live_close]}, index=[live_time])
                    df = pd.concat([df, new_row])

                    # Check for trade signals
                    check_trade_signals(symbol, df, live_close)

            time.sleep(5)

        except Exception as e:
            logging.error(f"❌ Error in auto_trade loop: {e}")
            time.sleep(10)
            continue

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