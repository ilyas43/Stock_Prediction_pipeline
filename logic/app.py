"""
this script runs at the same day as prediction script and before it
"""



from model.functions import clean_text
from model.functions import get_model
from model.functions import get_vecrtor
from model.functions import save_to_mongodb
from model.functions import update_is_new_flag
from model.functions import get_spark_session
from model.functions import train_LSTM_model

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, date_add
from pyspark.sql import Row
from collections import defaultdict
from datetime import datetime, date

import csv
import os
import math
import pymongo
import shutil
from params import MONGO_HOST , MONGO_DB , HISTORICAL_NEWS_COLLECTION , HISTORICAL_PRICES_COLLECTION , HISTORICAL_DWH_NEWS
# Initialize SparkSession
spark = get_spark_session(MONGO_DB,HISTORICAL_NEWS_COLLECTION)

# Read data from MongoDB
df = spark.read.format("com.mongodb.spark.sql.DefaultSource").load()

# getting news data that has is_new=1 from mongo db
def get_news_data():


    # Initialize an empty list to store symbol and corresponding new news
    symbol_news_list = []

    # Iterate through each record in the DataFrame
    for row in df.collect():
        today_date_str = datetime.today().strftime("%Y-%m-%d")
        today =  datetime.strptime(today_date_str, "%Y-%m-%d")

        # Initialize an empty list to store new news with is_new=1 (not preproceesed before) and news published before today
        new_news_list = [news for news in row['listOfNews'] if news['is_new']==1 and today > datetime.strptime(news['_pubDate'], "%Y-%m-%d")]

        # Append the symbol and corresponding new news to symbol_news_list
        symbol_news_list.append({'symbol': row['symbol'], 'new_news_list': new_news_list})
    
    # Stop SparkSession
    spark.stop()

    return symbol_news_list

# Perform text preprocessing on the news
# You can call your preprocessing functions here
def clean_and_analyse_text_sentiment():

    # Initialize an empty list to store cleaned news
    cleaned_news_list = []

    new_arrived_data = get_news_data()

    v = get_vecrtor()
    model = get_model()

    # Loop over each news list
    for symbol_news in new_arrived_data:

        # Initialize an empty list to store cleaned news for this symbol
        cleaned_symbol_news_list = []

        symbol = symbol_news['symbol']
        new_news_list = symbol_news['new_news_list']

        if len(new_news_list) == 0:
            print("No new news arrived for symbol: ", symbol)
            continue
        else:
            # Loop over each news in the new_news_list
            for row in new_news_list:

                news = row.asDict()

                # Get the text of the news
                text = news['_TextNew']
                
                # Clean the text
                cleaned_text = clean_text(text)

                #vectorize the texts
                vectorized_text = v.transform([cleaned_text])

                # give vectore of the text to the Nave bais model to get the sentiment prediction
                predicted_sentiment = model.predict(vectorized_text.reshape(1, -1))
                sentiment = "positive" if predicted_sentiment == 1 else "negative"

                # Create a new field named 'sentiment' and assign it the sentiment of the text predicted by NB
                news['sentiment'] = sentiment

                updated_news = Row(**news)

                # Append the cleaned and analysed news to the cleaned_symbol_news_list
                cleaned_symbol_news_list.append(updated_news)

        # Append the symbol and corresponding cleaned news to cleaned_news_list
        cleaned_news_list.append({'symbol': symbol, 'new_news_list': cleaned_symbol_news_list})

    # Get the directory of the current script
    script_dir = os.path.dirname(os.path.abspath(__file__))

    # Change the current working directory to the script's directory
    os.chdir(script_dir)
    return cleaned_news_list

# agregate news based on date and symbol and calculate some metrics , and save the result to DWH
def ODS_TO_DWH_news():

    cleaned_new_data = clean_and_analyse_text_sentiment()
    
    final_dataset = []

    for symbol_news in cleaned_new_data:
        symbol = symbol_news['symbol']
        symbol_news_list = symbol_news['new_news_list']

        if len(symbol_news_list) == 0:
            continue
        else:
            # Group news by date
            grouped_by_date = defaultdict(lambda: {'Pp': 0, 'Pn': 0})

            for news in symbol_news_list:
                pub_date = news['_pubDate']
                sentiment = news['sentiment']

                # Increment positive or negative count based on sentiment
                if sentiment == 'positive':
                    grouped_by_date[pub_date]['Pp'] += 1
                else:
                    grouped_by_date[pub_date]['Pn'] += 1

            # Calculate bullish indicators and sentiment for each date
            for date, counts in grouped_by_date.items():
                Pp = counts['Pp']
                Pn = counts['Pn']

                # Calculate bullish indicator
                if Pp + Pn != 0:
                    bullish_indicator = (Pp - Pn) / (Pp + Pn)
                else:
                    bullish_indicator = 0

                # Determine sentiment
                sentiment = "positive" if bullish_indicator > 0 else "negative" if bullish_indicator < 0 else "neutral"

                # calculate β∗t = βt * ln(1 + P°all/t)
                bullish_indicator_all_posts = bullish_indicator * math.log(1 + (Pp + Pn))

                # calculate agreement indicator : αt = 1 - V- 1-B°2t  
                agreement_indicator = 1 - math.sqrt(1 - bullish_indicator ** 2 ) 

                # Create news sentiment object
                news_sentiment = {
                    '_pubDate': date,
                    'symbol': symbol,
                    'sentiment': sentiment,
                    'bullish_indicator': bullish_indicator,
                    'bullish_indicator_all_posts': bullish_indicator_all_posts,
                    'agreement_indicator': agreement_indicator,
                    'is_matched':0
                }

                final_dataset.append(news_sentiment)
    if len(final_dataset) != 0 :
        # save this dataset in the database 
        save_to_mongodb(final_dataset)

        # call function to make is_new flag = 0
        update_is_new_flag(final_dataset)

        return 1
    else :
        return None

# matching with prices , by symbol and date and create a dataset . in dataframe df
def match_with_prices():

    # Initialize SparkSession
    spark = SparkSession.builder \
        .appName("Read MongoDB Collections") \
        .getOrCreate()
    
    client = pymongo.MongoClient(MONGO_HOST)

    # Select database and collection
    db = client["stock_data"]
    price_collection = db[HISTORICAL_PRICES_COLLECTION]
    news_collection = db[HISTORICAL_DWH_NEWS]

    # Fetch data from collection 1, excluding the _id field
    price_data = price_collection.find({}, {"_id": 0})

    # Fetch data from collection 2, excluding the _id field
    news_data = news_collection.find({}, {"_id": 0})

    # Explicitly cast the data types of columns to ensure compatibility
    price_data = [{k: float(v) if isinstance(v, int) else v for k, v in row.items()} for row in price_data]
    news_data = [{k: float(v) if isinstance(v, int) else v for k, v in row.items()} for row in news_data]


    # Create Spark DataFrame for data from collection 1
    price_df = spark.createDataFrame(price_data)

    # Create Spark DataFrame for data from collection 2
    news_df = spark.createDataFrame(news_data)
        


        
 

    # Convert pubDate to date object
    news_df = news_df.withColumn("_pubDate", to_date(col("_pubDate"), "yyyy-MM-dd"))

    # Add a day for news day, add one day to each day, to match each price day with yesterday news
    news_df = news_df.withColumn("yesterday_pubDate", date_add(col("_pubDate"), 1))

    # Join news_df with price_df on symbol and date
    joined_df = news_df.join(price_df, (news_df.symbol == price_df.symbol) & (news_df.yesterday_pubDate == price_df.datetime_utc), "inner")

    # Filter out the records where is_matched is 0
    joined_df = joined_df.filter(joined_df.is_matched == 0)

    # Select relevant columns
    matched_dataset = joined_df.select(
        news_df._pubDate.alias("_pubDate"),
        news_df.symbol.alias("symbol"),
        news_df.sentiment.alias("sentiment"),
        news_df.bullish_indicator.alias("bullish_indicator"),
        news_df.bullish_indicator_all_posts.alias("bullish_indicator_all_posts"),
        news_df.agreement_indicator.alias("agreement_indicator"),
        price_df.open.alias("open"),
        price_df.high.alias("high"),
        price_df.low.alias("low"),
        price_df.close.alias("close"),
        price_df.volume.alias("volume"),
    ).collect()
    
    # Convert DataFrame rows to JSON strings
    matched_json = [row.asDict() for row in matched_dataset]
    # Convert datetime.date objects to strings
    for item in matched_json:
        item['_pubDate'] = item['_pubDate'].strftime("%Y-%m-%d")

    # Update records in the news_collection where is_matched is 0 and present in matched_json
    for record in matched_json:
        pub_date = record['_pubDate']
        symbol = record['symbol']
        news_collection.update_many(
            {'_pubDate': pub_date, 'symbol': symbol, 'is_matched': 0},
            {'$set': {'is_matched': 1}}
        )
    
    # Stop SparkSession
    spark.stop()
    
    # Return matched dataset in JSON format
    return matched_json

#save to csv
def store_dataset_to_csv(matched_dataset):
  """
  Stores the JSON data in matched_dataset to a CSV file.

  Args:
      matched_dataset (list): A list of dictionaries containing news and prices.
  """

  # Get today's date and create folder name
  today_str = date.today().strftime("%Y-%m-%d")
  folder_name = f"data/input"

  # Create the folder if it doesn't exist
  os.makedirs(folder_name, exist_ok=True)

  # Construct the CSV filename
  csv_filename = os.path.join(folder_name, f"{today_str}.csv")

  # Open the CSV file in write mode with UTF-8 encoding
  with open(csv_filename, 'w', newline='', encoding='utf-8') as csvfile:
    csv_writer = csv.writer(csvfile)

    column_names = None  # Initialize as None to check if it's assigned

    # Iterate through each dictionary in the list
    for item in matched_dataset:
      # Extract column names from the first dictionary
      if column_names is None:
        column_names = list(item.keys())  # Extract only once
        csv_writer.writerow(column_names)  # Write header row

      # Write a CSV row for each dictionary's values
      csv_writer.writerow(item.values())

  print(f"Dataset successfully stored to CSV file: {csv_filename}")

# send the dataset to the database
def Train_model():
    """load data from dataset if it exist,  and read it and train the model ,  
    after training the model , 
    copy this file to archive folder , and delete the dataset file """

    # Source and destination paths
    file_path = f'data/input/{date.today().strftime("%Y-%m-%d")}.csv'
    destination_path = 'data/archive'

    # read file
    # Read the content of the file
    with open(file_path, 'r') as file:
        file_content = file.read()

    # copy it to archive
    # Check if the source file exists
    if os.path.exists(file_path):
        # Copy the file to the destination folder
        shutil.copy(file_path, destination_path)
        print("File copied successfully.")
    else:
        print("Source file does not exist.")

    # train model , use partial_fit
    train_LSTM_model(file_content)
    
    # delete file from data/input
    # Check if the file exists before attempting to delete it
    if os.path.exists(file_path):
        # Delete the file
        os.remove(file_path)
        print("File deleted successfully.")
    else:
        print("File does not exist.")

    return

executed = ODS_TO_DWH_news()
if executed != None:
    matched_dataset = match_with_prices()
    # save the result to csv file in 'dataset'  folder if folder do not exist , create it , file name should be with date exemple 21_01_2024.csv 
    store_dataset_to_csv(matched_dataset)
    print('json format of dataset : ',matched_dataset)
    Train_model()
else:
    print("no data was arrived nor matched ! ")