from fyers_apiv3.FyersWebsocket import data_ws
from fyers_apiv3 import fyersModel
from datetime import date, datetime, timedelta
import math
import pandas as pd

# MySQL Connection String
SQL_SERVER_CONNECTION_STRING = 'mysql+pymysql://root:Apple@1234@localhost/securities_master'

# Read API credentials from files
app_id = open("fyers_appid.txt", 'r').read().strip()
access_token = open("fyers_token.txt", 'r').read().strip()

# Initialize the fyersModel instance
fyers = fyersModel.FyersModel(client_id=app_id, token=access_token)

# DataFrame to store historical data
histdata = pd.DataFrame()


def get_history_data(start_date, end_date, resolution):
    """
    Fetches historical data from Fyers API and appends it to the global DataFrame.
    """
    global histdata

    data = {
        "symbol": "NSE:SBIN-EQ",
        "resolution": str(resolution),
        "date_format": "1",
        "range_from": start_date.strftime("%Y-%m-%d"),
        "range_to": end_date.strftime("%Y-%m-%d"),
        "cont_flag": "6"
    }

    try:
        response = fyers.history(data=data)
        if 'candles' not in response:
            print(f"Error fetching data: {response}")
            return

        df = pd.DataFrame(response['candles'], columns=['datetime', 'open', 'high', 'low', 'close', 'volume'])
        df['datetime'] = pd.to_datetime(df['datetime'], unit="s")
        df['datetime'] = df['datetime'].dt.tz_localize('UTC').dt.tz_convert('Asia/Kolkata')
        df['datetime'] = df['datetime'].dt.tz_localize(None)
        df = df.set_index('datetime')

        # Append to global DataFrame
        histdata = pd.concat([histdata, df], axis=0)

    except Exception as e:
        print(f"Exception occurred while fetching data: {e}")


if __name__ == "__main__":
    start_date = date(2023, 1, 1)
    end_date = start_date + timedelta(days=100)

    total_days = (date.today() - start_date).days
    iterations = math.ceil(total_days / 100)

    for _ in range(iterations):
        get_history_data(start_date, end_date, 1)

        # Move start_date and end_date forward by 100 days
        start_date = end_date + timedelta(days=1)
        end_date = start_date + timedelta(days=100)

    # Save to CSV
    histdata.to_csv("sbi_data.csv")
    print("Historical data saved to sbi_data.csv")
