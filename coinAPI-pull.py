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
from datetime import datetime, timedelta, timezone
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
import time

load_dotenv()
conn_string = os.getenv('db_conn_string')
coindesk_key = os.getenv('coindesk_key')

db = create_engine(conn_string)
conn = db.connect()

# Get the date for today and yesterday and convert them to YYYY-mm-ddT00:00:00 format for API call
yesterday_date = datetime.now() - timedelta(days = 1)
yesterday_start = f"{yesterday_date.strftime('%Y-%m-%d')}T00:00:00"
start_datetime_object = datetime.strptime(yesterday_start, "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc)

yesterday_end = f"{yesterday_date.strftime('%Y-%m-%d')}T23:59:59"
end_datetime_object = datetime.strptime(yesterday_end, "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc)
end_timestamp = int(end_datetime_object.timestamp())

# Create an empty Dataframe to hold the values after each run of the loop
daily_df = pd.DataFrame()

# Get yesterdays crypto coin values
# crypto_coins = {0:"BTC-USD"}
crypto_coins = {0:"BTC-USD", 1:'ETH-USD', 2:'USDT-USD', 3:"USDC-USD", 4:"BNB-USD", 5:"XRP-USD", 6:"ADA-USD", 7:"DOGE-USD", 8:"MATIC-USD"}
for i in range(len(crypto_coins)):
    exchange_id = crypto_coins[i]

    try:
        # yesterdays_response = requests.get(f'{url}&apikey={coindesk_key}')
        yesterdays_response = requests.get('https://data-api.coindesk.com/index/cc/v1/historical/hours',
            params={"market":"cadli","instrument":exchange_id,"limit":24,"aggregate":1,"fill":"true","apply_mapping":"true","response_format":"JSON","to_ts":end_timestamp,"api_key":coindesk_key},
            headers={"Content-type":"application/json; charset=UTF-8"}
        )
        
        daily_text = json.dumps(yesterdays_response.json()['Data'], sort_keys=True, indent=4)
        if "status" not in daily_text:
            temp_df = pd.read_json(daily_text)
            temp_df['exchange_id'] = exchange_id.replace('-USD','')
            daily_df = pd.concat([daily_df, temp_df], ignore_index=True)
            
            time.sleep(3)
        else: 
            time.sleep(15)
    except requests.HTTPError as e:
        print(f'There was an error: {e}')
        
daily_df = daily_df[['exchange_id','FIRST_MESSAGE_TIMESTAMP', 'LAST_MESSAGE_TIMESTAMP', 'OPEN', 'HIGH_MESSAGE_VALUE', 'LOW_MESSAGE_VALUE', 'CLOSE']]

daily_df['FIRST_MESSAGE_TIMESTAMP'] = pd.to_datetime(daily_df['FIRST_MESSAGE_TIMESTAMP'], unit='s', utc=True)
daily_df['LAST_MESSAGE_TIMESTAMP'] = pd.to_datetime(daily_df['LAST_MESSAGE_TIMESTAMP'], unit='s', utc=True)

daily_df['time_period_start'] = start_datetime_object
daily_df['time_period_end'] = end_datetime_object

daily_df.rename(columns={'FIRST_MESSAGE_TIMESTAMP' : 'time_close', 'LAST_MESSAGE_TIMESTAMP': 'time_open', 'OPEN': 'rate_open', 'HIGH_MESSAGE_VALUE': 'rate_high', 'LOW_MESSAGE_VALUE': 'rate_low', 'CLOSE': 'rate_close'}, inplace=True)

print(daily_df)
daily_df = daily_df[['exchange_id','rate_close','rate_high','rate_low','rate_open','time_close','time_open','time_period_end','time_period_start']]
daily_df = daily_df.drop_duplicates().sort_values(by='time_period_start', ignore_index=True)

try:    
    daily_df.to_sql(name = 'daily_crypto_prod', schema='crypto_data', con=conn, if_exists='append', index=False)
        
    print('data loaded to table')
except SQLAlchemyError as e:
    print(f'There was an error: {e}')
