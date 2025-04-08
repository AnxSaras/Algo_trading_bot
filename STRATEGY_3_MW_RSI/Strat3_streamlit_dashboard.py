import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError
from urllib.parse import quote_plus


# --- App Title ---
st.set_page_config(page_title="MW RSI Strategy Dashboard", layout="wide")
st.title("📊 MW RSI Strategy - Signal Monitor")

# --- DB credentials ---
username = 'root'                # ← change to your MySQL username
password = quote_plus('Apple@1331')  # ✅ URL-encoded
host = 'localhost'
port = 3306
database = 'Algo_Trading'

connection_str = f"mysql+pymysql://{username}:{password}@{host}:{port}/{database}"

# --- Create SQLAlchemy engine ---
try:
    engine = create_engine(connection_str)
    # Test connection
    with engine.connect() as conn:
        result = conn.execute("SELECT 1")

    # --- Read latest signals ---
    query = "SELECT * FROM signal_log ORDER BY timestamp DESC LIMIT 50"
    signal_df = pd.read_sql(query, con=engine)

    # --- Display in dashboard ---
    st.subheader("📌 Latest Trade Signals")
    st.dataframe(signal_df, use_container_width=True)

except OperationalError as e:
    st.error("❌ Could not connect to MySQL database.")
    st.code(str(e))

except Exception as e:
    st.error("⚠️ An unexpected error occurred.")
    st.code(str(e))
