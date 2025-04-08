import numpy as np
import pandas as pd
import mysql.connector as mdb
import os
from urllib.parse import quote_plus
from sqlalchemy import create_engine
from openpyxl import load_workbook
from openpyxl.styles import PatternFill

# Database Configuration
db_config = {
    "host": "localhost",
    "user": "Algo_Trading",
    "password": "",
    "database": "Historical_data_2024"
}

# Connect to MySQL
def connect_db():
    try:
        con = mdb.connect(**db_config)
        print("✅ Database connection successful!")
        return con
    except mdb.Error as e:
        print(f"❌ Database connection error: {e}")
        return None

con = connect_db()
ENCODED_PASS = quote_plus(db_config["password"])
ENGINE = create_engine(f"mysql+mysqlconnector://{db_config['user']}:{ENCODED_PASS}@{db_config['host']}/{db_config['database']}")

# Load Historical Data
def load_data_from_db():
    query = """
        SELECT sym.ticker, dp.price_date AS timestamp, dp.open_price AS open, dp.high_price AS high, 
               dp.low_price AS low, dp.close_price AS close, dp.volume AS volume
        FROM daily_price AS dp
        INNER JOIN symbol AS sym ON dp.symbol_id = sym.id
        ORDER BY sym.ticker, dp.price_date ASC
    """
    with ENGINE.connect() as conn:
        df = pd.read_sql(query, conn)
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    symbols = df["ticker"].unique()
    print(f"Processing {len(symbols)} stocks: {symbols}")
    return df.set_index(["ticker", "timestamp"])


# Calculate RSI with SMA(14,2)

def calculate_rsi(data, period=14, smoothing=2):
    """
    Computes the Relative Strength Index (RSI) and a smoothed RSI SMA.

    Parameters:
        data (pd.DataFrame): DataFrame with MultiIndex (ticker, date) and a 'close' column.
        period (int): Lookback period for RSI calculation.
        smoothing (int): Lookback period for RSI SMA calculation.

    Returns:
        pd.DataFrame: Original DataFrame with added 'RSI' and 'RSI_SMA' columns.
    """

    # Ensure the DataFrame is sorted
    data = data.sort_values(by="timestamp")  # ✅ Ensure chronological order


    # Compute price change
    delta = data["close"].groupby(level=0).diff()

    # Compute gains and losses
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)

    # Compute EMA of gains and losses
    avg_gain = gain.groupby(level=0).ewm(span=period, adjust=False).mean()
    avg_loss = loss.groupby(level=0).ewm(span=period, adjust=False).mean()

    # Compute RSI (avoiding division by zero)
    rs = avg_gain / (avg_loss + 1e-10)
    rsi = 100 - (100 / (1 + rs))

    # ✅ Fix index misalignment before assignment
    data = data.copy()  # Avoid modifying original
    data["RSI"] = rsi.reset_index(level=0, drop=True)

    # ✅ Compute RSI SMA (Smoothed RSI) and fix indexing
    data["RSI_SMA"] = (
        data["RSI"]
        .groupby(level=0)
        .rolling(window=smoothing, min_periods=1)
        .mean()
        .reset_index(level=0, drop=True)
    )

    return data

# Generate Buy/Sell Signals based on M & W Patterns
def generate_signals(data):
    data["Signal"] = 0
    rsi_shifted = data["RSI_SMA"].groupby(level=0).shift(1)  # Avoid lookahead bias

    # Vectorized Buy/Sell Signal Logic
    buy_signal = (rsi_shifted < 30) & (data["RSI_SMA"] > 30)
    sell_signal = (rsi_shifted > 70) & (data["RSI_SMA"] < 70)

    data.loc[buy_signal, "Signal"] = 1
    data.loc[sell_signal, "Signal"] = -1
    
    return data

# Backtest Strategy - One Trade at a Time
def backtest(data, initial_balance=10000, brokerage_fee=20):
    all_trades = []
    serial_no = 1
    balance = initial_balance  # Start with initial capital
    position = None  # Track open position

    total_trades, profitable_trades, loss_trades = 0, 0, 0
    total_profit, total_loss = 0, 0
    max_drawdown, peak_balance = 0, balance
    
    for (ticker, timestamp), row in data.iterrows():
        # SELL Condition
        if position and row["Signal"] == -1 and position["ticker"] == ticker:
            exit_price = row["close"]
            sell_quantity = position["shares"]
            purchase_value = position["entry_price"] * sell_quantity
            gross_profit = (exit_price - position["entry_price"]) * sell_quantity
            gross_profit_pct = (gross_profit / purchase_value) * 100 if purchase_value > 0 else 0
            
            # ✅ Fees Calculation
            stamp_fee = 0.0005 * sell_quantity * exit_price
            gst = 0.18 * brokerage_fee
            stcg = 0.2 * gross_profit if gross_profit > 0 else 0  # 20% tax on profit only
            fees = brokerage_fee + stamp_fee + gst + stcg

            net_profit = gross_profit - fees
            net_profit_pct = (net_profit / purchase_value) * 100 if purchase_value > 0 else 0

            # Track performance metrics
            if net_profit > 0:
                profitable_trades += 1
                total_profit += net_profit 
            else:
                loss_trades += 1
                total_loss += net_profit

            balance += sell_quantity * exit_price - fees  # ✅ Balance rolls over
            max_drawdown = min(max_drawdown, balance - peak_balance)
            peak_balance = max(peak_balance, balance)

            all_trades.append([
                serial_no, ticker, timestamp, purchase_value, position["entry_price"], sell_quantity,
                exit_price, sell_quantity, gross_profit, gross_profit_pct, fees, net_profit, balance, net_profit_pct
            ])
            serial_no += 1
            total_trades += 1
            position = None  # Reset after selling

        # BUY Condition
        elif not position and row["Signal"] == 1:
        # ✅ First trade should always use ₹100,000
            if serial_no == 1:
                trade_funds = 100000  
    else:
        trade_funds = balance  # ✅ Use updated balance from net P&L

    buy_quantity = int((trade_funds - brokerage_fee) / row["close"])  # ✅ Adjusted buy quantity
    if buy_quantity > 0 and trade_funds >= buy_quantity * row["close"] + brokerage_fee:
        position = {"ticker": ticker, "shares": buy_quantity, "entry_price": row["close"]}
        balance -= buy_quantity * row["close"] + brokerage_fee  # ✅ Deduct from balance


    # Performance Metrics
    win_rate = (profitable_trades / total_trades * 100) if total_trades > 0 else 0
    profit_factor = round((total_profit / abs(total_loss)), 2) if total_loss != 0 else "∞"
    summary = [
        ["Total Trades", total_trades],
        ["Profitable Trades", profitable_trades],
        ["Loss Trades", loss_trades],
        ["Win Rate (%)", round(win_rate, 2)],
        ["Profit Factor", profit_factor],
        ["Max Drawdown", round(max_drawdown, 2)],
        ["Final Balance", round(balance, 2)]
    ]
    return all_trades, summary


def save_results_to_files(all_trades, summary, stock_data, trades_filename="backtest_trades.csv", summary_filename="backtest_summary.txt"):
    """
    Saves backtest results to CSV, text, and Excel files.

    Parameters:
        all_trades (list): List of executed trades.
        summary (list): Performance summary.
        stock_data (pd.DataFrame): DataFrame containing all stock data.
        trades_filename (str): CSV filename for trade data.
        summary_filename (str): TXT filename for summary.
    """
    # Save trades to CSV
    if all_trades:
        trade_log = pd.DataFrame(all_trades, columns=[
            "Serial No", "Ticker", "Timestamp", "Purchase Value", "Entry Price", "Shares",
            "Exit Price", "Sell Quantity", "Gross Profit", "Gross Profit %", "Fees", "Net Profit", "Balance", "Net Profit %"
        ])
        trade_log.to_csv(trades_filename, index=False)
        print(f"✅ Trade history saved to {trades_filename}")
    else:
        trade_log = pd.DataFrame()  # Empty DataFrame to avoid errors in Excel export

    # Save summary to text file
    with open(summary_filename, "w") as f:
        for item in summary:
            f.write(f"{item[0]}: {item[1]}\n")
    
    print(f"✅ Summary saved to {summary_filename}")

    # Convert summary list to DataFrame for Excel export
    summary_df = pd.DataFrame(summary, columns=["Metric", "Value"])

    # Save everything to Excel
    with pd.ExcelWriter("Backtest_Results.xlsx", engine="xlsxwriter") as writer:
        stock_data.to_excel(writer, sheet_name="Stock Data", index=False)
        trade_log.to_excel(writer, sheet_name="Trade Log", index=False)
        summary_df.to_excel(writer, sheet_name="Performance Summary", index=False)
    
    print("✅ Excel file generated: Backtest_Results.xlsx")

# Backtest Strategy - M&W RSI Pattern

def backtest_strategy(df, initial_funds=100000):
    trades_log = []
    trade_signals = []
    total_buy_trades = 0
    total_sell_trades = 0
    net_pnl = 0
    
    for ticker, data in df.groupby(level=0):
        data = data.copy().reset_index()
        print(f"🔄 Processing {ticker}...")

        if "RSI" not in data.columns:
            print(f"❌ RSI not found for {ticker}, skipping...")
            continue

        data = data.dropna(subset=["RSI"])
        position = False
        entry_price = None
        entry_date = None
        buy_quantity = 0

        data = data.copy().reset_index(drop=True)  # ✅ Reset index
        for i in range(2, len(data)):
            rsi_1, rsi_2, rsi_3 = data.iloc[i-2]["RSI"], data.iloc[i-1]["RSI"], data.iloc[i]["RSI"]
            timestamp, close_price = data.loc[i, "timestamp"], data.loc[i, "close"]

            # Detect W Pattern (Bullish Reversal)
            if not position and rsi_1 < 30 and rsi_3 > rsi_1 and rsi_3 > rsi_2:
                position = True
                entry_price = close_price
                entry_date = timestamp
                buy_quantity = initial_funds // entry_price
                trade_signals.append([ticker, timestamp, "BUY", close_price])
                print(f"✅ Buy Signal: {ticker} on {entry_date} at {entry_price}")
                total_buy_trades += 1

            # Detect M Pattern (Bearish Reversal)
            elif position and rsi_1 > 70 and rsi_3 < rsi_1 and rsi_3 < rsi_2:
                position = False
                exit_price = close_price
                exit_date = timestamp
                gross_pnl = (exit_price - entry_price) * buy_quantity
                gross_pnl_percent = (gross_pnl / initial_funds) * 100
                fees = 20 + (0.001 * gross_pnl) + (0.18 * 20) + (0.2 * gross_pnl if gross_pnl > 0 else 0)
                net_pnl = gross_pnl - fees  # ✅ Corrected Net P&L Calculation
                initial_funds += net_pnl  # ✅ Update Initial Funds for Next Trade
                net_pnl_percent = (net_pnl / initial_funds) * 100
                
                trades_log.append([ticker, entry_date, initial_funds, buy_quantity, entry_price, exit_date, buy_quantity, exit_price, 
                                   round(gross_pnl, 2), round(gross_pnl_percent, 2), round(fees, 2), round(net_pnl, 2), round(net_pnl_percent, 2)])
                
                trade_signals.append([ticker, timestamp, "SELL", close_price])
                print(f"❌ Sell Signal: {ticker} on {exit_date} at {exit_price}")
                total_sell_trades += 1

    total_net_pnl = sum(trade[11] for trade in trades_log)  # Summing Net P&L column
    summary = [[len(trade_signals), total_buy_trades, total_sell_trades, round(total_net_pnl, 2)]]

    save_to_excel(trades_log, trade_signals, summary)

# Function to Save Data to Excel

def save_to_excel(trades_log, trade_signals, summary):
    filename = "Backtest_MW_RSI_strat_Results.xlsx"
    with pd.ExcelWriter(filename, engine="xlsxwriter") as writer:
        df_trades = pd.DataFrame(trades_log, columns=[
            "Ticker", "Entry Date", "Initial Funds", "Buy Qty", "Entry Price", 
            "Exit Date", "Sell Qty", "Exit Price", "Gross P&L", "Gross P&L %",
            "Fees", "Net P&L", "Net P&L %"
        ])
        df_trades.to_excel(writer, sheet_name="Trades Executed", index=False)

        df_signals = pd.DataFrame(trade_signals, columns=["Ticker", "Date", "Signal Type", "Price"])
        df_signals.to_excel(writer, sheet_name="Trade Signals", index=False)

        total_net_pnl = df_trades["Net P&L"].sum()  # ✅ Calculate total net P&L
        summary[-1][3] = round(total_net_pnl, 2)  # ✅ Corrected assignment

        df_summary = pd.DataFrame(summary, columns=["Total Signals", "Total Buy Trades", "Total Sell Trades", "Net P&L"])
        df_summary.to_excel(writer, sheet_name="Summary", index=False)  # ✅ Write summary sheet

        # Apply Formatting
        workbook = writer.book
        worksheet = writer.sheets["Trades Executed"]
        format_green = workbook.add_format({"bg_color": "#C6EFCE", "font_color": "#006100", "bold": True})
        format_red = workbook.add_format({"bg_color": "#FFC7CE", "font_color": "#9C0006", "bold": True})
        header_format = workbook.add_format({"bold": True, "bg_color": "#D3D3D3", "border": 1})

        # ✅ Apply Conditional Formatting for Net P&L (L column)
        worksheet.conditional_format("L2:L1000", {"type": "cell", "criteria": ">", "value": 0, "format": format_green})
        worksheet.conditional_format("L2:L1000", {"type": "cell", "criteria": "<", "value": 0, "format": format_red})

        # ✅ Format Headers
        for col_num, value in enumerate(df_trades.columns.values):
            worksheet.write(0, col_num, value, header_format)

        # ✅ Add Grand Total Row
        grand_total_row = len(df_trades) + 2  # ✅ Fixed row position
        worksheet.write_row(
            grand_total_row, 0, ["Grand Total", "", "", "", "", "", "", "", 
                                 df_trades["Gross P&L"].sum(), "", df_trades["Fees"].sum(), df_trades["Net P&L"].sum(), ""], 
            workbook.add_format({"bold": True, "bg_color": "#FFFF99", "border": 1})
        )

    print(f"✅ Excel file generated: {filename}")


# Main Execution
if __name__ == "__main__":
    print("🔄 Starting script execution...")
    df = load_data_from_db()
    if df is None or df.empty:
        print("❌ No data loaded. Exiting...")
        exit()
    print(f"✅ Loaded {len(df)} rows from the database.")
    df = calculate_rsi(df)
    print("✅ RSI calculation complete.")
    df = generate_signals(df)
    print("✅ Signal generation complete.")
    print(f"🔍 Buy signals: {df['Signal'].eq(1).sum()}, Sell signals: {df['Signal'].eq(-1).sum()}")
    print("✅ Running backtest...")
    trades = backtest_strategy(df)
    print("✅ Backtest completed.")