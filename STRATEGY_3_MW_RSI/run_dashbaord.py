import urllib.parse
import subprocess
import webbrowser
import time
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError
import os

# DB Credentials
username = 'sec_user'
db_password = os.getenv("Apple@1331")
password = urllib.parse.quote_plus(db_password)  # Encode special characters
host = 'localhost'
port = 3306
database = 'Algo_trading'

try:
    conn_str = f"mysql+pymysql://{username}:{password}@{host}:{port}/{database}"
    print("DEBUG connection string:", conn_str)

    engine = create_engine(conn_str)
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))  # ✅ Correct way in SQLAlchemy 2.0+
    print("✅ Connected successfully!")

except OperationalError as e:
    print("❌ Connection failed:")
    print(e)

# Path to Streamlit dashboard
streamlit_app_path = "/Users/ankursaraswat/Fyers_API_trading_bot/STRATEGY_3_MW_RSI/Strat3_streamlit_dashboard.py"

# Start Streamlit
subprocess.Popen(["streamlit", "run", streamlit_app_path])
time.sleep(5)
webbrowser.open("http://localhost:8501")
