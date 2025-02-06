# So, the intent of this ETL script is to hit the coinAPI... uhh... API in order to get the yesterday's USD value of various crypto
# coins in a 30 minute intervals. IE: 00:29:00, 00:59:00, 01:29:00, 01:59:00, etc...
# To accomplish this I needed to get today's date use relative date to get yesterday's date and format it into YYYY-mm-ddT00:00:00 format
# using f-string formatting.
# Next, I used a dictionary to create a key-value pair for the top 10 crypto coins on the list found at:
# https://www.forbes.com/advisor/investing/cryptocurrency/top-10-cryptocurrencies/
# Don't get mad at me if your crypto coin isn't on the list, blame Forbes.
# Then, I used a while loop based on the length of the dictionary to use f-string literals to fomat the URL to have the correct:
#   Exchange ID, Start Date, and End Date
# Each time the while loop is run for each crypto coin, the values are taken from the JSON response, added to a temp dataframe
# and then written to the final dataframe using the pd.concat method.
# Finally, the dataframe that has each crytpo coin value in 30 minute increments is written to a PostgreSQL db table.

from dotenv import load_dotenv
import os
import requests
import json
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import exc, create_engine
import time

load_dotenv()
coinapi_key = os.getenv('coinapi_key')

conn_string = os.getenv('db_conn_string')

db = create_engine(conn_string)
conn = db.connect()

# Get the date for today and yesterday and convert them to YYYY-mm-ddT00:00:00 format for API call
yesterday_date = datetime.now() - timedelta(days = 1)
yesterday = f"{yesterday_date.strftime('%Y-%m-%d')}T00:00:00"
today = f"{datetime.now().strftime('%Y-%m-%d')}T00:00:00"

# Create an empty Dataframe to hold the values after each run of the loop
daily_df = pd.DataFrame()

i = 1

# Get yesterdays crypto coin values
crypto_coins = {0:"BTC", 1:'ETH', 2:'USDT', 3:"USDC", 4:"BNB", 5:"BUSD", 6:"XRP", 7:"ADA", 8:"DOGE", 9:"MATIC"}
# while i <= len(crypto_coins):
for i in range(len(crypto_coins)):
    exchange_id = crypto_coins[i]

    url = f'https://rest.coinapi.io/v1/exchangerate/{exchange_id}/USD/history?period_id=30MIN&time_start={yesterday}&time_end={today}'
    try:
        yesterdays_response = requests.get(f'{url}&apikey={coinapi_key}')
    
        daily_text = json.dumps(yesterdays_response.json(), sort_keys=True, indent=4)
        print(daily_text)
        if "status" not in daily_text:
            temp_df = pd.read_json(daily_text)
            temp_df['exchange_id'] = exchange_id
            daily_df = pd.concat([daily_df, temp_df])
            
            time.sleep(3)
        else: 
            time.sleep(15)
    except requests.HTTPError as e:
        print(f'There was an error: {e}')


daily_df = daily_df[['exchange_id','rate_close','rate_high','rate_low','rate_open','time_close','time_open','time_period_end','time_period_start']]

print(daily_df)

# try:    
#     daily_df.to_sql(name = 'daily_crypto_data', con=conn, if_exists='append', index=False)

#     print('data loaded to table')
# except exc.SQLAlchemyError as e:
#     print(f'There was an error: {e}')
