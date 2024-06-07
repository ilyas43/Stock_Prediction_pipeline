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
from tensorflow.keras.models import load_model , model_from_json
from sklearn.preprocessing import MinMaxScaler
from sklearn.model_selection import train_test_split
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
def train_LSTM_model(data):
    # Get the directory of the current script
    script_dir = os.path.dirname(os.path.abspath(__file__))

    # Change the current working directory to the script's directory
    os.chdir(script_dir)
    # Convert "sentiment" column to binary (1 for positive, 0 for negative)
    data["sentiment"] = data["sentiment"].apply(lambda x: 1 if x == "positive" else 0)
    
    # Define features and target
    features = ["sentiment", "bullish_indicator", "bullish_indicator_all_posts", "agreement_indicator", "open"]
    target = ["high", "low", "close", "volume"]
    
    # Split features and target
    X = data[features]
    y = data[target]
    
    # Normalize features
    scaler = MinMaxScaler()
    X_scaled = scaler.fit_transform(X)
    
    # Reshape data for LSTM
    X_train = X_scaled.reshape((X_scaled.shape[0], 1, X_scaled.shape[1]))
    
    # load json and create model
    json_file = open('model.json', 'r')
    loaded_model_json = json_file.read()
    json_file.close()
    loaded_model = model_from_json(loaded_model_json)
    # load weights into new model
    loaded_model.load_weights("model.h5")
    print("Loaded model from disk")
        
    # Compile the model before training
    loaded_model.compile(optimizer='adam', loss='mean_squared_error')
    
    # Fit the model
    loaded_model.fit(X_train, y, epochs=100, batch_size=1, verbose=1)
    
    # Save the model
    # serialize model to JSON
    model_json = loaded_model.to_json()
    with open("model.json", "w") as json_file:
        json_file.write(model_json)
    # serialize weights to HDF5
    loaded_model.save_weights("model.h5")
    print("Saved model to disk")


# Make prediction
def predict_LSTM_model(features):
    # Get the directory of the current script
    script_dir = os.path.dirname(os.path.abspath(__file__))

    # Change the current working directory to the script's directory
    os.chdir(script_dir)
    # Load the model
    # load json and create model
    json_file = open('model.json', 'r')
    loaded_model_json = json_file.read()
    json_file.close()
    loaded_model = model_from_json(loaded_model_json)
    # load weights into new model
    loaded_model.load_weights("model.h5")
    print("Loaded model from disk")

    # Compile the model before training
    loaded_model.compile(optimizer='adam', loss='mean_squared_error')

    # Normalize features
    scaler = MinMaxScaler()
    features_scaled = scaler.fit_transform(features)

    # Reshape the input data for LSTM
    features_reshaped = features_scaled.reshape((features_scaled.shape[0], 1, features_scaled.shape[1]))

    # Make prediction
    loaded_predictions = loaded_model.predict(features_reshaped)

    # serialize model to JSON
    model_json = loaded_model.to_json()
    with open("model.json", "w") as json_file:
        json_file.write(model_json)
    # serialize weights to HDF5
    loaded_model.save_weights("model.h5")
    print("Saved model to disk")

    # Return the result
    return loaded_predictions