import time
import json
import pymysql
import requests
import pandas as pd
from fyers_api import fyersModel
from concurrent.futures import ThreadPoolExecutor

# MySQL connection
DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "yourpassword",
    "database": "Algo_Trading"
}

# FYERS API Configuration
FYERS_CLIENT_ID = "YOUR_CLIENT_ID"
FYERS_ACCESS_TOKEN = "YOUR_ACCESS_TOKEN"

fyers = fyersModel.FyersModel(client_id=FYERS_CLIENT_ID, token=FYERS_ACCESS_TOKEN, log_path="")

# Telegram Bot Configuration
TELEGRAM_BOT_TOKEN = "YOUR_TELEGRAM_BOT_TOKEN"
TELEGRAM_CHAT_ID = "YOUR_CHAT_ID"

# Trading Parameters
STOP_LOSS_PERCENT = 1.5  # 1.5% SL
TAKE_PROFIT_PERCENT = 3.0  # 3% TP

# List of NIFTY50 Stocks
NIFTY50_STOCKS = ["RELIANCE", "TCS", "INFY", "HDFCBANK", "ICICIBANK", "HINDUNILVR", "SBIN", "KOTAKBANK"]

# Send Telegram Alert
def send_telegram_alert(message):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": message}
    requests.post(url, data=payload)

# Fetch market data from FYERS
def fetch_market_data(symbol):
    try:
        response = fyers.quotes({"symbols": f"NSE:{symbol}"})
        if response["s"] == "ok":
            data = response["d"][0]
            return {
                "price": data["v"]["lp"],
                "open": data["v"]["o"],
                "high": data["v"]["h"],
                "low": data["v"]["l"],
                "close": data["v"]["c"]
            }
    except Exception as e:
        print(f"Error fetching data for {symbol}: {str(e)}")
    return None

# Fetch and update historical data in MySQL
def fetch_historical_data(symbol, days=50):
    try:
        connection = pymysql.connect(**DB_CONFIG)
        with connection.cursor() as cursor:
            # Fetch last 50 days' closing prices
            query = f"SELECT date, close FROM daily_price WHERE symbol = %s ORDER BY date DESC LIMIT %s"
            cursor.execute(query, (symbol, days))
            rows = cursor.fetchall()

            # Convert to DataFrame
            df = pd.DataFrame(rows, columns=["date", "close"]).sort_values(by="date", ascending=True)

            # Fetch latest market price
            market_data = fetch_market_data(symbol)
            if market_data:
                latest_price = market_data["price"]
                df = df.append({"date": pd.Timestamp.now(), "close": latest_price}, ignore_index=True)

                # Insert the latest price into MySQL
                insert_query = "INSERT INTO daily_price (symbol, date, close) VALUES (%s, NOW(), %s)"
                cursor.execute(insert_query, (symbol, latest_price))
                connection.commit()

        connection.close()
        return df
    except Exception as e:
        print(f"Error updating historical data: {str(e)}")
        return None

# Compute RSI
def calculate_rsi(data, period=14):
    delta = data["close"].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    rs = gain / loss
    return 100 - (100 / (1 + rs))

# Compute MACD
def calculate_macd(data, short_window=12, long_window=26, signal_window=9):
    short_ema = data["close"].ewm(span=short_window, adjust=False).mean()
    long_ema = data["close"].ewm(span=long_window, adjust=False).mean()
    data["macd"] = short_ema - long_ema
    data["signal"] = data["macd"].ewm(span=signal_window, adjust=False).mean()

# Fetch available funds from Fyers
def get_available_funds():
    try:
        response = fyers.funds()
        if response["s"] == "ok":
            return float(response["fund_limit"][0]["equityAmount"])
    except Exception as e:
        print(f"Error fetching funds: {str(e)}")
    return 0

# Place a Bracket Order (BO) utilizing max funds
def place_trade(symbol, side, price):
    available_funds = get_available_funds()
    if available_funds <= 0:
        send_telegram_alert(f"Insufficient funds for trading {symbol}")
        return None

    qty = int(available_funds / price)  # Calculate max quantity
    if qty <= 0:
        send_telegram_alert(f"Insufficient funds to buy even 1 unit of {symbol}")
        return None

    stop_loss = round(price * (1 - STOP_LOSS_PERCENT / 100), 2)
    take_profit = round(price * (1 + TAKE_PROFIT_PERCENT / 100), 2)

    try:
        order = {
            "symbol": f"NSE:{symbol}",
            "qty": qty,
            "type": 2,  # Market order
            "side": 1 if side == "BUY" else -1,
            "productType": "BO",  # Bracket Order
            "limitPrice": 0,
            "stopPrice": stop_loss,  # SL for BO
            "takeProfit": take_profit,  # TP for BO
            "validity": "DAY",
            "orderTag": "AlgoTrade"
        }
        response = fyers.place_order(order)

        # Send Telegram alert
        send_telegram_alert(f"Trade executed: {side} {qty} units of {symbol} at {price}. SL: {stop_loss}, TP: {take_profit}")

        return response
    except Exception as e:
        print(f"Error placing trade: {str(e)}")
        return None

# Strategy Execution
def execute_strategy(symbol):
    historical_data = fetch_historical_data(symbol)
    if historical_data is None or historical_data.empty:
        return

    # Compute indicators
    historical_data["rsi"] = calculate_rsi(historical_data)
    calculate_macd(historical_data)

    last_rsi = historical_data["rsi"].iloc[-1]
    last_macd = historical_data["macd"].iloc[-1]
    last_signal = historical_data["signal"].iloc[-1]

    market_data = fetch_market_data(symbol)
    if not market_data:
        return

    # Buy condition
    if last_rsi < 30 and last_macd > last_signal:
        place_trade(symbol, "BUY", market_data["price"])

    # Sell condition
    elif last_rsi > 70 and last_macd < last_signal:
        place_trade(symbol, "SELL", market_data["price"])

# Run trading bot for multiple stocks in parallel
def run_trading_bot():
    with ThreadPoolExecutor(max_workers=5) as executor:
        while True:
            executor.map(execute_strategy, NIFTY50_STOCKS)
            time.sleep(60)

if __name__ == "__main__":
    run_trading_bot()
