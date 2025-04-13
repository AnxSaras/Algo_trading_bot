import datetime
import holidays
import os
import subprocess
from dotenv import load_dotenv
load_dotenv()

def is_trading_day():
    today = datetime.date.today()
    ist = datetime.timezone(datetime.timedelta(hours=5, minutes=30))
    now = datetime.datetime.now(ist)
    
    # Check if Monday–Friday
    if today.weekday() >= 5:
        return False
    
    # Check if within 09:00–16:00 IST
    if not (9 <= now.hour < 16):
        return False
    
    # Check NSE/BSE holidays (India)
    india_holidays = holidays.India(years=today.year)
    return today not in india_holidays

def main():
    if is_trading_day():
        print("Running trading scripts...")
        # Generate token
        subprocess.run(["python", "Fyers_Trading_Bot/Fyers_API_setup/fyer_token_generator.py"])
        # Update database
        subprocess.run(["python", "Fyers_Trading_Bot/Sql_setup_and_data_fetch/Nifty50_Historical_data.py"])
        # Run trading
        subprocess.run(["python", "STRATEGY_3_MW_RSI/mw_rsi_live_trading.py"])
    else:
        print("Not a trading day or outside trading hours. Skipping.")

if __name__ == "__main__":
    main()