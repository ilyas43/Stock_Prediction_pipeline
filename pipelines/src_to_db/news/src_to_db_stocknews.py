from datetime import datetime
import requests
from bs4 import BeautifulSoup
from pymongo import MongoClient
from params import SYMBOLS , URL , X_RAPIDAPI_KEY , X_RAPIDAPI_HOST, MONGO_HOST , MONGO_DB , MONGO_COLLECTION

# get the list of news for each symbol
def get_stockNews_data()->list:
    listOfTtextNews = []
    listOfsymbols = []
    payload = "Pass in the value of uuids field returned right in this endpoint to load the next page, or leave empty to load first page"
    headers = {
        "content-type": "text/plain",
        'x-rapidapi-key': X_RAPIDAPI_KEY,
        'x-rapidapi-host': X_RAPIDAPI_HOST
    }

    for symbol in SYMBOLS:
        querystring = {"region":"US","snippetCount":"28","s":symbol}
        res = requests.post(URL, data=payload, headers=headers, params=querystring)
        if res.status_code == 200:
            response = res.json()
            stock_news = response['data']['main']['stream']
            for stock_new in stock_news:
                click_through_url = stock_new['content'].get('clickThroughUrl')
                url = click_through_url.get('url') if click_through_url and 'url' in click_through_url else None
                preview_url = stock_new['content'].get('previewUrl') if 'previewUrl' in stock_new['content'] else None            
                
                news_element = {
                    '_pubDate' :stock_new['content']['pubDate'],
                    '_title' :stock_new['content']['title'],
                    '_symbol':symbol,
                    '_TextNew': scrap_news_for_each_symbol(url if url else preview_url) if url or preview_url else None
                }
                listOfTtextNews.append(news_element)
        new_symbol={
            'symbol':symbol,
            'listOfNews':listOfTtextNews
        }
        listOfsymbols.append(new_symbol)
    return listOfsymbols

# scrap news data for each given , returns a text new
def scrap_news_for_each_symbol(url:str)->str:
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')
    text_content = soup.get_text()
    return text_content
    
def load_to_database():
    _MONGO_HOST = MONGO_HOST
    _MONGO_DB = MONGO_DB
    _MONGO_COLLECTION = MONGO_COLLECTION
    
    # Connect to MongoDB
    client = MongoClient(_MONGO_HOST, connectTimeoutMS=30000)
    db = client[_MONGO_DB]
    collection = db[_MONGO_COLLECTION]
    
    # Get new data
    new_data = get_stockNews_data()
    
    # Iterate over symbols
    for symbol_data in new_data:
        symbol = symbol_data['symbol']
        news_list = symbol_data['listOfNews']
        
        # Check if the symbol already exists in the collection
        existing_symbol = collection.find_one({'symbol': symbol})
        
        # If the symbol doesn't exist, insert it with its list of news
        if not existing_symbol:
            collection.insert_one(symbol_data)
            print(f"Inserted new symbol {symbol} with news.")
        else:
            # If the symbol exists, update its list of news
            existing_news_list = existing_symbol['listOfNews']
            for news_element in news_list:
                # Check if the news already exists in the symbol's list of news
                if news_element not in existing_news_list:
                    # Add the new news to the symbol's list of news
                    existing_news_list.append(news_element)
                    print(f"Inserted new news for symbol {symbol}.")
            # Update the symbol document in the collection with the updated list of news
            collection.update_one({'symbol': symbol}, {'$set': {'listOfNews': existing_news_list}})
    
    client.close()


load_to_database()
