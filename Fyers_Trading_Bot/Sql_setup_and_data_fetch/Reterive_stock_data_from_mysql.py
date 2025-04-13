#!/usr/bin/python
# -*- coding: utf-8 -*-
# retrieving_data.py

import pandas as pd
import mysql.connector as mdb

# ✅ Database Connection Details
db_host = "localhost"
db_user = "sec_user"
db_pass = "Apple@1234"  # Change if needed
db_name = "securities_master"

# ✅ Connect to MySQL Database
con = mdb.connect(host=db_host, user=db_user, password=db_pass, database=db_name)
cur = con.cursor()

# ✅ Define the Stock Symbol to Retrieve
stock_ticker = "TCS"  # Change as needed

# ✅ SQL Query to Fetch OHLC Data for the Stock
query = """
SELECT dp.price_date, sym.name, dp.open_price, dp.high_price, dp.low_price, dp.close_price, dp.volume
FROM symbol AS sym
INNER JOIN daily_price AS dp
ON dp.symbol_id = sym.id
WHERE sym.ticker = %s
ORDER BY dp.price_date DESC
LIMIT 10;
"""

# ✅ Execute Query & Load Data into Pandas DataFrame
cur.execute(query, (stock_ticker,))
data = cur.fetchall()

# ✅ Convert to Pandas DataFrame
df = pd.DataFrame(data, columns=["price_date", "stock_name", "open_price", "high_price", "low_price", "close_price", "volume"])

# ✅ Close the database connection
cur.close()
con.close()

# ✅ Display the last 10 rows of data
print(df)
