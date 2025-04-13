import numpy as np
import pandas as pd
import mysql.connector as mdb
import matplotlib.pyplot as plt
import mplfinance as mpf
from itertools import product
from urllib.parse import quote_plus
from sqlalchemy import create_engine
from matplotlib.backends.backend_pdf import PdfPages
import os


# ✅ Database Connection Details
DB_HOST = "localhost"
DB_USER = "sec_user"
DB_PASS = "v"
DB_NAME = "securities_master"

# ✅ Encode Password for URL
ENCODED_PASS = quote_plus(DB_PASS)
ENGINE = create_engine(f"mysql+mysqlconnector://{DB_USER}:{ENCODED_PASS}@{DB_HOST}/{DB_NAME}")


# ✅ Fetch Nifty 50 Stocks
def get_nifty50_stocks():
    try:
        con = mdb.connect(host=DB_HOST, user=DB_USER, password=DB_PASS, database=DB_NAME)
        cur = con.cursor()
        cur.execute("SELECT ticker FROM symbol")
        stocks = [row[0] for row in cur.fetchall()]
        con.close()
        return stocks
    except Exception as e:
        print(f"❌ Error fetching stock list: {e}")
        return []


# ✅ Load Historical Data
def load_data(stock_ticker, start_date="2024-02-21", end_date="2025-02-21"):
    query = """
        SELECT dp.price_date AS timestamp, dp.open_price AS open, dp.high_price AS high, 
               dp.low_price AS low, dp.close_price AS close, dp.volume AS volume
        FROM daily_price AS dp
        INNER JOIN symbol AS sym ON dp.symbol_id = sym.id
        WHERE sym.ticker = %s
        AND dp.price_date BETWEEN %s AND %s
        ORDER BY dp.price_date ASC
    """
    try:
        df = pd.read_sql(query, con=ENGINE, params=(stock_ticker, start_date, end_date))
        if df.empty:
            return None
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        df.set_index("timestamp", inplace=True)
        return df
    except Exception as e:
        print(f"❌ Error loading data for {stock_ticker}: {e}")
        return None


# ✅ Weighted Moving Average (WMA)
def weighted_moving_average(prices, period):
    weights = np.arange(1, period + 1)
    return np.convolve(prices, weights / weights.sum(), mode="valid")


# ✅ Generate Trading Signals
def generate_signals(df, short_window, long_window):
    df = df.copy()
    df["WMA_Short"] = np.nan
    df["WMA_Long"] = np.nan

    if len(df) >= long_window:
        df.loc[df.index[short_window - 1:], "WMA_Short"] = weighted_moving_average(df["close"], short_window)
        df.loc[df.index[long_window - 1:], "WMA_Long"] = weighted_moving_average(df["close"], long_window)

    df.dropna(inplace=True)
    df["Signal"] = np.where(df["WMA_Short"] > df["WMA_Long"], 1, 0)
    df["Position"] = df["Signal"].diff()
    return df


# ✅ Optimize WMA Parameters
def optimize_wma_parameters(df, short_range=range(5, 16, 5), long_range=range(20, 41, 10)):
    best_params = None
    best_performance = float('-inf')

    for short, long in product(short_range, long_range):
        if short >= long:
            continue
        df_test = generate_signals(df.copy(), short, long)
        trades, final_balance = backtest(df_test)
        net_profit = final_balance - 5000

        if net_profit > best_performance:
            best_performance = net_profit
            best_params = (short, long)

    return best_params


def backtest(df, initial_balance=5000, risk_per_trade=0.02, brokerage_fee=0.001):
    balance = initial_balance
    positions = 0
    entry_price = 0
    trades = []
    balance_over_time = []

    for index, row in df.iterrows():
        # If we hold a position, check Stop Loss & Take Profit
        if positions > 0:
            stop_loss_price = entry_price * (1 - 0.02)  # 2% Stop Loss
            take_profit_price = entry_price * (1 + 0.05)  # 5% Take Profit

            if row["close"] <= stop_loss_price:
                balance += positions * row["close"]
                balance -= balance * brokerage_fee
                trades.append((index, "STOP LOSS", row["close"], balance))
                positions = 0  # Close position properly

            elif row["close"] >= take_profit_price:
                balance += positions * row["close"]
                balance -= balance * brokerage_fee
                trades.append((index, "TAKE PROFIT", row["close"], balance))
                positions = 0  # Close position properly

        # ✅ Buy Signal
        if row["Position"] == 1 and positions == 0:
            trade_value = max(risk_per_trade * balance, 500)  # Ensure minimum trade value
            trade_size = max(int(trade_value / row["close"]), 1)  # Ensure at least 1 share

            if trade_size > 0:
                positions = trade_size
                entry_price = row["close"]
                balance -= positions * entry_price
                balance -= balance * brokerage_fee
                trades.append((index, "BUY", entry_price, balance))

        # ✅ Sell Signal
        elif row["Position"] == -1 and positions > 0:
            balance += positions * row["close"]
            balance -= balance * brokerage_fee
            trades.append((index, "SELL", row["close"], balance))
            positions = 0

        balance_over_time.append(balance)

    df["Balance"] = balance_over_time
    return trades, balance


# ✅ Save All Charts to One PDF
def save_all_charts_to_pdf(all_trades):
    with PdfPages("Nifty50_Backtest_Charts.pdf") as pdf:
        for stock, (df, trades) in all_trades.items():
            fig, (ax1, ax2) = plt.subplots(2, figsize=(12, 6), gridspec_kw={'height_ratios': [3, 1]}, sharex=True)

            mpf.plot(df, type='candle', style='yahoo', ax=ax1, volume=ax2)

            for trade in trades:
                date, action, price, _ = trade
                if date in df.index:
                    color, marker = ("green", "^") if action == "BUY" else ("red", "v")
                    ax1.scatter(date, price, color=color, marker=marker, s=100, label=action)

            ax1.set_title(f"{stock} - Candlestick Chart with Trades")
            ax1.legend()

            pdf.savefig(fig)
            plt.close(fig)

    print("✅ All charts saved in Nifty50_Backtest_Charts.pdf")


# ✅ Run Backtest for Multiple Stocks and Save to CSV
def backtest_multiple_stocks():
    stock_list = get_nifty50_stocks()
    results = []
    all_trades = {}

    for stock in stock_list:
        df = load_data(stock)
        if df is None:
            continue

        best_params = optimize_wma_parameters(df)
        if best_params is None:
            continue

        best_short, best_long = best_params
        df_signals = generate_signals(df, best_short, best_long)
        trades, final_balance = backtest(df_signals)
        net_profit = final_balance - 5000

        results.append([stock, best_short, best_long, final_balance, net_profit])
        all_trades[stock] = (df_signals, trades)

    results_df = pd.DataFrame(results, columns=["Stock", "Short WMA", "Long WMA", "Final Balance", "Net Profit"])
    results_df.to_csv("backtest_results.csv", index=False)
    print("✅ Results saved to backtest_results.csv")

    save_all_charts_to_pdf(all_trades)


# ✅ Run Strategy
if __name__ == "__main__":
    backtest_multiple_stocks()
