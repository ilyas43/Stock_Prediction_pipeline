from pyspark.sql import SparkSession
from model.functions import clean_text
from model.functions import get_model
from model.functions import get_vecrtor
from pyspark.sql import Row
from collections import defaultdict
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

    # # Loop through the symbol_news_list and print symbol and its new news
    # for entry in symbol_news_list:
    #     print("Symbol:", entry['symbol'])
    #     print("News:", entry['new_news_list'])
    
    # Stop SparkSession
    spark.stop()

    return symbol_news_list

# Perform text preprocessing on the news
# You can call your preprocessing functions here
def clean_and_analyse_text_sentiment():

    # Initialize an empty list to store cleaned news
    cleaned_news_list = []
    
    v = get_vecrtor()
    model = get_model()

    # Loop over each news list
    for symbol_news in get_news_data():
        symbol = symbol_news['symbol']
        new_news_list = symbol_news['new_news_list']

        # Initialize an empty list to store cleaned news for this symbol
        cleaned_symbol_news_list = []

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

    return cleaned_news_list


# agregate news based on date and symbol

def calculate_bullish_indicators(cleaned_news_list):
    final_dataset = []

    for symbol_news in cleaned_news_list:
        symbol = symbol_news['symbol']
        symbol_news_list = symbol_news['new_news_list']

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
            sentiment = "positive" if bullish_indicator > 0 else "negative"

            # Create news sentiment object
            news_sentiment = {
                'pubDate': date,
                'bullish_indicators': bullish_indicator,
                'sentiment': sentiment,
                'symbol': symbol
            }

            final_dataset.append(news_sentiment)

    return final_dataset

# Example usage:
cleaned_news_list = clean_and_analyse_text_sentiment()
final_dataset = calculate_bullish_indicators(cleaned_news_list)
print(final_dataset)


# matching with prices , by symbol and date and create a dataset . in dataframe df


# update the news extracted to make is_new = 0 in the data base


# send the dataset to the database
