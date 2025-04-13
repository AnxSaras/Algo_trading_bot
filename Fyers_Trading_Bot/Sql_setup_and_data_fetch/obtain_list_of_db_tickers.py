import datetime
import mysql.connector as mdb
import requests
import pandas as pd


# Database connection function
def get_db_connection():
    return mdb.connect(
        host='localhost',
        user='sec_user',
        password='Apple@1234',
        database='securities_master'
    )


# Get the data_vendor_id based on the vendor name (e.g., 'Fyers API')
def get_data_vendor_id(vendor_name):
    """
    Returns the data_vendor_id for a given vendor name.
    """
    try:
        con = get_db_connection()
        with con.cursor() as cur:
            cur.execute("SELECT id FROM data_vendor WHERE name = %s", (vendor_name,))
            result = cur.fetchone()
            con.close()
            if result:
                return result[0]
            else:
                print(f"Data vendor {vendor_name} not found!")
                return None
    except Exception as e:
        print(f"Error getting data vendor ID: {e}")
        return None


# Function to obtain list of tickers from the symbol table
def obtain_list_of_db_tickers():
    """
    Obtains a list of the ticker symbols in the database.
    """
    try:
        con = get_db_connection()
        with con.cursor() as cur:
            cur.execute("SELECT id, ticker FROM symbol")
            data = cur.fetchall()
        con.close()
        return [(d[0], d[1]) for d in data]
    except Exception as e:
        print(f"Error obtaining tickers: {e}")
        return []


# Function to get historical data from Fyers API
def get_daily_historic_data_fyers(ticker, start_date=(2000, 1, 1), end_date=datetime.date.today().timetuple()[0:3]):
    """
    Obtains historical data from Fyers API and returns a list of tuples.
    ticker: Fyers API ticker symbol (e.g., 'NSE:RELIANCE')
    start_date: Start date in (YYYY, M, D) format
    end_date: End date in (YYYY, M, D) format
    """
    try:
        # Fyers API endpoint for historical data
        endpoint = f"https://api.fyers.in/api/v2/data-rest/v2/marketdata/history/"

        # Prepare the request parameters
        params = {
            'symbol': ticker,
            'resolution': 1,  # 1-minute data; you can change this based on requirement
            'from': int(datetime.datetime(start_date[0], start_date[1], start_date[2]).timestamp()),
            # convert start date to UNIX timestamp
            'to': int(datetime.datetime(end_date[0], end_date[1], end_date[2]).timestamp()),
            # convert end date to UNIX timestamp
        }

        # Send the request to Fyers API
        response = requests.get(endpoint, params=params,
                                headers={'Authorization': 'Bearer <YOUR_ACCESS_TOKEN>'})  # Add your access token here

        if response.status_code == 200:
            data = response.json()
            if data['s'] == 'ok':
                # Process the data into the desired format (list of tuples)
                prices = []
                for d in data['candles']:
                    prices.append((
                        datetime.datetime.utcfromtimestamp(d[0] / 1000).date(),  # price_date
                        d[1],  # open_price
                        d[2],  # high_price
                        d[3],  # low_price
                        d[4],  # close_price
                        d[4],  # adj_close_price (same as close for simplicity)
                        d[5]  # volume
                    ))

                return prices
            else:
                print(f"Error: {data['errmsg']}")
        else:
            print(f"Error fetching data from Fyers: {response.status_code}")

        return []
    except Exception as e:
        print(f"Could not download Fyers data for {ticker}: {e}")
        return []


# Function to insert data into the daily_price table
def insert_daily_data_into_db(data_vendor_id, symbol_id, daily_data):
    """
    Takes a list of tuples of daily data and adds it to the MySQL database.
    daily_data: List of tuples of the OHLC data (with adj_close and volume)
    """
    try:
        con = get_db_connection()
        now = datetime.datetime.now(datetime.timezone.utc)  # Use UTC aware datetime

        # Amend the data to include the vendor ID and symbol ID
        daily_data = [
            (data_vendor_id, symbol_id, d[0], now, now, d[1], d[2], d[3], d[4], d[5], d[6])
            for d in daily_data
        ]

        column_str = """data_vendor_id, symbol_id, price_date, created_date, last_updated_date, open_price, high_price, low_price, close_price, volume, adj_close_price"""
        insert_str = ("%s, " * 11)[:-2]
        final_str = "INSERT INTO daily_price (%s) VALUES (%s)" % (column_str, insert_str)

        with con.cursor() as cur:
            # Execute the insert operation using executemany
            cur.executemany(final_str, daily_data)
            con.commit()
        print(f"Successfully added data for symbol {symbol_id}")
    except Exception as e:
        print(f"Error inserting data for symbol {symbol_id}: {e}")
    finally:
        con.close()


# Main execution
if __name__ == "__main__":
    try:
        # Step 1: Get the data vendor ID (assuming Fyers API is your vendor)
        data_vendor_id = get_data_vendor_id('Fyers API')

        if data_vendor_id:
            print(f"Data Vendor ID for Fyers API: {data_vendor_id}")

            # Step 2: Get list of tickers from the database
            tickers = obtain_list_of_db_tickers()
            total_tickers = len(tickers)

            # Step 3: Loop through tickers and insert data into the database
            for i, t in enumerate(tickers):
                print(f"Adding data for {t[1]}: {i + 1} out of {total_tickers}")
                fyers_data = get_daily_historic_data_fyers(t[1])
                if fyers_data:
                    insert_daily_data_into_db(data_vendor_id, t[0], fyers_data)
                else:
                    print(f"No data available for {t[1]}")
        else:
            print("Data vendor not found!")
    except Exception as e:
        print(f"Error in main execution: {e}")
