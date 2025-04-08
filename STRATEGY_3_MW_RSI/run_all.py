import os
import subprocess
import webbrowser
import time
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError
from urllib.parse import quote_plus
from dotenv import load_dotenv
load_dotenv()

# --- MySQL connection test ---
username = 'sec_user'
#db_password = os.getenv("db_password")
password = quote_plus(db_password)
#password = quote_plus('v')  # Escaped
host = 'localhost'
port = 3306
database = 'Algo_trading'

connection_str = f"mysql+pymysql://{username}:{password}@{host}:{port}/{database}"
print(f"DEBUG connection string: {connection_str}")

try:
    engine = create_engine(connection_str)
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))
    print("✅ Connected successfully!")
except OperationalError as e:
    print("❌ Database connection failed:")
    print(e)

# --- Path to live trading script and dashboard ---
live_trading_script = "/Users/ankursaraswat/Fyers_API_trading_bot/STRATEGY_3_MW_RSI/mw_rsi_live_trading.py"
dashboard_script = "/Users/ankursaraswat/Fyers_API_trading_bot/STRATEGY_3_MW_RSI/Strat3_streamlit_dashboard.py"

# --- Start live trading script in subprocess ---
subprocess.Popen(["python", live_trading_script])

# --- Start Streamlit dashboard ---
subprocess.Popen(["streamlit", "run", dashboard_script])

# --- Wait then open browser ---
time.sleep(5)
webbrowser.open("http://localhost:8501")
