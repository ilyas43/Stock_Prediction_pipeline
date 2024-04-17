from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
from nltk.stem import PorterStemmer
from textblob import TextBlob
from nltk.util import ngrams
nltk.download('punkt')
nltk.download('stopwords')
import joblib
import pickle
import nltk



# Function to vectorize the text data

def vecrtorize(text):
  # Filepath of the saved vectorizer (same as before)
  vectorizer_file = "my_vectorizer.pkl"

  # Load the vectorizer from the pickle file
  with open(vectorizer_file, 'rb') as f:
      v = pickle.load(f)

  print("CountVectorizer loaded from:", vectorizer_file)

  vectorised_text = v.transform([text])

  return vectorised_text


def predict_sentiment(input):
    # Load the saved Naive Bayes model
    loaded_model = joblib.load('sentiment_model.pkl')

    # Predict the sentiment of the example text data using the loaded model
    predicted_sentiment = loaded_model.predict([input])

    sentiment = "positive" if predict_sentiment == 1 else "negative"

    # Print the predicted sentiment
    return sentiment

# cleaning function
def clean_text(text):
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