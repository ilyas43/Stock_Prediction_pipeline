from pyspark.sql import SparkSession
from logic.model.functions import clean_text
from logic.model.functions import predict_sentiment
from logic.model.functions import vecrtorize
# Initialize SparkSession
spark = SparkSession.builder \
    .appName("MongoDB Integration") \
    .config("spark.mongodb.input.uri", "mongodb://host.docker.internal:27017/stock_data_dev.historical_news") \
    .getOrCreate()

    # Read data from MongoDB
df = spark.read.format("com.mongodb.spark.sql.DefaultSource").load()

# getting news data that has is_new=1 from mongo db
def get_news_data():


    # Initialize an empty list to store symbol and corresponding new news
    symbol_news_list = []

    # Iterate through each record in the DataFrame
    for row in df.collect():

        # Initialize an empty list to store new news with is_new=1
        new_news_list = [news for news in row['listOfNews'] if news['is_new']==1]

        # Append the symbol and corresponding new news to symbol_news_list
        symbol_news_list.append({'symbol': row['symbol'], 'new_news_list': new_news_list})

    # Loop through the symbol_news_list and print symbol and its new news
    for entry in symbol_news_list:
        print("Symbol:", entry['symbol'])
        print("News:", entry['new_news_list'])
    
    # Stop SparkSession
    spark.stop()

    return symbol_news_list

# Perform text preprocessing on the news
# You can call your preprocessing functions here
def clean_and_analyse_text_sentiment():

    # Initialize an empty list to store cleaned news
    cleaned_news_list = []
    
    # Loop over each news list
    for symbol_news in df.collect():
        symbol = symbol_news['symbol']
        new_news_list = symbol_news['new_news_list']

        # Initialize an empty list to store cleaned news for this symbol
        cleaned_symbol_news_list = []

        # Loop over each news in the new_news_list
        for news in new_news_list:
            # Get the text of the news
            text = news['_TextNew']
            
            # Clean the text
            cleaned_text = clean_text(text)

            #vectorize the texts
            vectorized_text = vecrtorize(cleaned_text)

            # give vectore of the text to the Nave bais model to get the sentiment prediction
            sentiment = predict_sentiment(vectorized_text)
            
            # Create a new field named 'sentiment' and assign it the sentiment of the text predicted by NB
            news['sentiment'] = sentiment

            # add some metrics to new dataset
            
            
            # Append the cleaned and analysed news to the cleaned_symbol_news_list
            cleaned_symbol_news_list.append(news)

        # Append the symbol and corresponding cleaned news to cleaned_news_list
        cleaned_news_list.append({'symbol': symbol, 'new_news_list': cleaned_symbol_news_list})

    return cleaned_news_list





# matching with prices , by symbol and date and create a dataset . in dataframe df


# update the news extracted to make is_new = 0 in the data base


# send the dataset to the database
