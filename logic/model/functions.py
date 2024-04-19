import joblib
import pickle
import nltk
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
from nltk.stem import PorterStemmer
from textblob import TextBlob
nltk.data.path.append("/opt/bitnami/spark/app/model/nltk_data")
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