import requests
import schedule
import requests
import datetime
from pymongo import MongoClient
from params import SYMBOLS , URL , X_RAPIDAPI_KEY , X_RAPIDAPI_HOST , MONGO_HOST , MONGO_DB , MONGO_COLLECTION


historical_stock_data = {}

# get prices data from data source
def get_stock_data() -> dict :

    headers = {
        'x-rapidapi-key': X_RAPIDAPI_KEY,
        'x-rapidapi-host': X_RAPIDAPI_HOST
    }

    for symbol in SYMBOLS:
        querystring = {"symbol": symbol, "region": "US"}
        response = requests.get(URL, headers=headers, params=querystring)
        if response.status_code == 200:
            historical_stock_data[symbol] = []
            for record in response.json()['prices']:
                try:
                    # Convert timestamp and add datetime_utc field
                    transformed_record = transform_data(record,symbol)
                except (KeyError, TypeError):
                    # Skip records with missing 'date' or conversion errors
                    pass
                historical_stock_data[symbol].append(transformed_record)
    return historical_stock_data

# transform timestamp to datetime
def transform_data(record,symbol):
    utc_datetime = datetime.datetime.fromtimestamp(record['date'])
    record['datetime_utc'] = utc_datetime.strftime("%Y-%m-%d")
    record['symbol']=symbol
    return record

#load data to mongo db (stg area)
def load_data_to_db():

    _MONGO_HOST = MONGO_HOST
    _MONGO_DB = MONGO_DB
    _MONGO_COLLECTION = MONGO_COLLECTION
 
    client = MongoClient(_MONGO_HOST)
    db = client[_MONGO_DB]
    collection = db[_MONGO_COLLECTION]

    data = get_stock_data()  # Call get_stock_data() to retrieve data
    existing_dates = set(collection.distinct("date"))  # Get existing dates efficiently

    # Filter records with dates not found in existing_dates
    new_records = [record for symbol, records in data.items() for record in records if record['date'] not in existing_dates]

    if new_records:
        # Insert only new records using insert_many (bulk insertion)
        collection.insert_many(new_records)
        print(f"Inserted {len(new_records)} new records.")
    else:
        print("No new records found for insertion.") 

# Schedule the function to run every day
schedule.every().minute.at(':15').do(load_data_to_db)
while True:
    schedule.run_pending()