import joblib
import pickle
import nltk
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
from nltk.stem import PorterStemmer
from textblob import TextBlob
from pyspark.sql import SparkSession
nltk.data.path.append("/opt/bitnami/spark/app/model/nltk_data")
import pymongo
# nltk.download('punkt', download_dir="/opt/bitnami/spark/app/model/nltk_data")
# nltk.download('stopwords',download_dir="/opt/bitnami/spark/app/model/nltk_data")

import os  # Import os module

# Get the directory of the current script
script_dir = os.path.dirname(os.path.abspath(__file__))

# Change the current working directory to the script's directory
os.chdir(script_dir)


# Function to vectorize the text data

def get_vecrtor():
  
  # Filepath of the saved vectorizer (same as before)
  vectorizer_file = "my_vectorizer.pkl"

  # Load the vectorizer from the pickle file
  with open(vectorizer_file, 'rb') as f:
      v = pickle.load(f)
  print("CountVectorizer loaded from:", vectorizer_file)
  return v


def get_model():

    # Load the saved Naive Bayes model
    loaded_model = joblib.load('sentiment_NB_model.pkl')
    print("model loaded ")
    return loaded_model

# cleaning function
def clean_text(text):
    print("start cleaning text")
    # Tokenization
    tokens = word_tokenize(text)
    
    # Lowercasing
    tokens = [token.lower() for token in tokens]
    
    # Removing stopwords
    stop_words = set(stopwords.words('english'))
    tokens = [token for token in tokens if token not in stop_words]
    
    # Remove punctuation marks
    tokens = [token for token in tokens if token.isalnum()]
    
    # Filter out tokens starting with "http://" or "https://"
    tokens = [token for token in tokens if not token.startswith(('http://', 'https://','http'))]

    # Stemming
    stemmer = PorterStemmer()
    tokens = [stemmer.stem(token) for token in tokens]

    #correct spelling
    tokens = [str(TextBlob(token).correct()) for token in tokens]

    # return tokens as one string
    return " ".join(tokens)

from pyspark.sql import SparkSession

def get_spark_session(database_name, collection_name):

    # Create SparkSession with MongoDB integration
    spark = SparkSession.builder \
        .appName("MongoDB Integration") \
        .config("spark.mongodb.input.uri", f"mongodb://host.docker.internal:27017/{database_name}.{collection_name}") \
        .getOrCreate()

    return spark

def save_to_mongodb(final_dataset):
    # Connect to MongoDB
    client = pymongo.MongoClient("mongodb://host.docker.internal:27017/")

    # Select database and collection
    db = client["stock_data"]
    collection = db["historical_dwh_news"]

    # Insert final_dataset into the collection
    collection.insert_many(final_dataset)

    # Close the MongoDB connection
    client.close()

def update_is_new_flag(final_dataset):
    # Connect to MongoDB
    client = pymongo.MongoClient("mongodb://host.docker.internal:27017/")

    # Select database and collection
    db = client["stock_data"]
    collection = db["historical_news"]

    # Update records in the collection
    for record in final_dataset:
        pub_date = record['_pubDate']
        symbol = record['symbol']
        # Update documents where symbol matches and _pubDate is in listOfNews array
        collection.update_many(
            {'symbol': symbol, 'listOfNews._pubDate': pub_date},
            {'$set': {'listOfNews.$[].is_new': 0}}
        )

    # Close the MongoDB connection
    client.close()

def update_is_matched_to_1(final_dataset):
    # Connect to MongoDB
    client = pymongo.MongoClient("mongodb://localhost:27017/")

    # Select database and collection
    db = client["stock_data"]
    collection = db["historical_dwh_news"]

    # Update records in the collection
    for record in final_dataset:
        pub_date = record['_pubDate']
        symbol = record['symbol']
        collection.update_one(
            {'_pubDate': pub_date, 'symbol': symbol},
            {'$set': {'is_matched': 1}}
        )

    # Close the MongoDB connection
    client.close()

# train LSTM Model
def train_LSTM_model(file_content):
    
    return
