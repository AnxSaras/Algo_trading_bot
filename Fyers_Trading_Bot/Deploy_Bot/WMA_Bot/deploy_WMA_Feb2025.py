import numpy as np
import pandas as pd
import time
import json
from fyers_apiv3 import fyersModel
from datetime import datetime, timedelta
from sqlalchemy import create_engine
from urllib.parse import quote_plus
import os

# ✅ Manually Enter Fyers API Credentials
CLIENT_ID = os.getenv("FYERS_APP_ID") # Replace with your actual Fyers App ID
ACCESS_TOKEN = os.getenv("FYERS_ACCESS_TOKEN")

# ✅ Database Connection
DB_HOST = "localhost"
DB_USER = "sec_user"
DB_PASS = os.getenv("db_password")
DB_NAME = "securities_master"

ENCODED_PASS = quote_plus(DB_PASS)
ENGINE = create_engine(f"mysql+mysqlconnector://{DB_USER}:{ENCODED_PASS}@{DB_HOST}/{DB_NAME}")

# ✅ Initialize Fyers API
fyers = fyersModel.FyersModel(client_id=CLIENT_ID, token=ACCESS_TOKEN, is_async=False)


# ✅ Trading Parameters
INITIAL_CAPITAL = 4000
RISK_PER_TRADE = 0.02  # 2% of available capital
BROKERAGE_FEE = 0.001  # 0.1% brokerage


# ✅ Load Nifty 50 Stocks
def get_nifty50_stocks():
    query = "SELECT ticker FROM symbol"
    df = pd.read_sql(query, con=ENGINE)
    return df["ticker"].tolist()


# ✅ Fetch Real-Time Data from Fyers
def fetch_realtime_data(stock_ticker):
    symbol = f"NSE:{stock_ticker}-EQ"
    data = fyers.history({
        "symbol": symbol,
        "resolution": "D",
        "date_format": "1",
        "range_from": (datetime.today() - timedelta(days=60)).strftime('%Y-%m-%d'),
        "range_to": datetime.today().strftime('%Y-%m-%d'),
        "cont_flag": "1"
    })

    if "candles" in data and data["candles"]:
        df = pd.DataFrame(data["candles"], columns=["timestamp", "open", "high", "low", "close", "volume"])
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="s")
        df.set_index("timestamp", inplace=True)
        return df
    else:
        print(f"❌ No data available for {stock_ticker}.")
        return None


# ✅ Weighted Moving Average (WMA)
def weighted_moving_average(prices, period):
    weights = np.arange(1, period + 1)
    wma = np.convolve(prices, weights / weights.sum(), mode="valid")
    return np.concatenate((np.full(period - 1, np.nan), wma))


# ✅ Generate Trading Signals
def generate_signals(df, short_window=10, long_window=30):
    if len(df) < long_window:
        return None  

    df = df.copy()
    df["WMA_Short"] = weighted_moving_average(df["close"], short_window)
    df["WMA_Long"] = weighted_moving_average(df["close"], long_window)
    df.dropna(inplace=True)
    df["Signal"] = np.where(df["WMA_Short"] > df["WMA_Long"], 1, 0)
    df["Position"] = df["Signal"].diff()
    return df


# ✅ Place Market Order with Fyersexit;

def place_order(stock, action, qty, is_intraday=True):
    fyers_symbol = f"NSE:{stock}-EQ"
    order_type = 1  # ✅ Market Order
    side = 1 if action == "BUY" else -1
    product_type = "INTRADAY" if is_intraday else "CNC"  # ✅ Use valid productType

    # ✅ Fetch latest market price from Fyers to use as limitPrice
    market_data = fyers.quotes({"symbols": fyers_symbol})
    print("Market Data Response:", market_data)  # Debugging line

    try:
        last_traded_price = market_data.get("d", [{}])[0].get("v", {}).get("ltp", 
                               market_data.get("d", [{}])[0].get("v", {}).get("lp"))
        if last_traded_price is None:
            print(f"⚠️ Warning: Neither 'ltp' nor 'lp' found in response for {stock}")
            return None
    except (IndexError, KeyError, TypeError) as e:
        print(f"❌ Error retrieving market data: {e}")
        return None

    print(f"🔹 Fyers Symbol: {fyers_symbol}, Action: {action}, Qty: {qty}, Product: {product_type}")

    order = {
        "symbol": fyers_symbol,
        "qty": qty,
        "type": order_type,  # ✅ Market Order
        "side": side,
        "productType": product_type,  # ✅ Fixed Product Type
        "validity": "DAY",
        "disclosedQty": 0,
        "offlineOrder": False,
        "limitPrice": last_traded_price,  # ✅ Use LTP instead of 0
    }

    response = fyers.place_order(order)
    print(f"📤 Order Request Sent: {order}")
    print(f"🔍 Fyers Response: {response}")

    if response.get("s") == "ok":
        print(f"✅ Order Placed: {stock} - {action}, Qty: {qty}, Product: {product_type}")
        return response.get("id")
    else:
        print(f"❌ Order Failed for {stock}. Error: {response}")
        return None


# ✅ Fetch & Save Pending Orders
def save_pending_orders():
    response = fyers.orderbook()

    if "orderBook" in response:
        df = pd.DataFrame(response["orderBook"])
        df.to_csv("pending_orders.csv", index=False)
        print("✅ Pending orders saved to 'pending_orders.csv'")
    else:
        print("❌ Error fetching pending orders:", response)


# ✅ Live Trading Execution
def live_trading():
    stock_list = get_nifty50_stocks()
    capital = INITIAL_CAPITAL
    trade_log = []

    for stock in stock_list:
        df = fetch_realtime_data(stock)
        if df is None:
            continue

        df_signals = generate_signals(df)
        if df_signals is None:
            continue

        latest = df_signals.iloc[-1]
        trade_size = max(int((RISK_PER_TRADE * capital) / latest["close"]), 1)

        if latest["Position"] == 1:  # Buy Signal
            order_id = place_order(stock, "BUY", trade_size)
            if order_id:
                entry = latest["close"]
                exit_price = entry * 1.05  
                trade_fee = (entry * trade_size) * BROKERAGE_FEE
                profit = (exit_price - entry) * trade_size
                net_profit = profit - trade_fee
                trade_log.append(
                    [datetime.now(), stock, "BUY", entry, trade_size, exit_price, profit, trade_fee, net_profit])

        elif latest["Position"] == -1:  # Sell Signal
            order_id = place_order(stock, "SELL", trade_size)
            if order_id:
                trade_log.append([datetime.now(), stock, "SELL", latest["close"], trade_size, "-", "-", "-", "-"])

        time.sleep(1)  # Avoid API Rate Limits

    # ✅ Save Live Trades to CSV
    trade_df = pd.DataFrame(trade_log,
                            columns=["Timestamp", "Stock", "Action", "Entry Price", "Qty", "Exit Price", "Trade P/L",
                                     "Fees", "Net Profit"])
    trade_df.to_csv("live_trades.csv", index=False)
    print("✅ Live trades saved to 'live_trades.csv'")


# ✅ Run Live Trading
if __name__ == "__main__":
    while True:
        print("\n🔄 Running WMA Strategy...")
        live_trading()
        save_pending_orders()  # ✅ Fetch & Save Pending Orders
        print("⏳ Waiting for next cycle...\n")
        time.sleep(300)  # Run every 5 minutes
