from datetime import datetime
import requests
from bs4 import BeautifulSoup
import hashlib
from pymongo import MongoClient
from params import SYMBOLS , URL , X_RAPIDAPI_KEY , X_RAPIDAPI_HOST, MONGO_HOST , MONGO_DB , MONGO_COLLECTION

# get the list of news for each symbol
def get_stockNews_data()->list:
    listOfsymbols = []
    payload = "Pass in the value of uuids field returned right in this endpoint to load the next page, or leave empty to load first page"
    headers = {
        "content-type": "text/plain",
        'x-rapidapi-key': X_RAPIDAPI_KEY,
        'x-rapidapi-host': X_RAPIDAPI_HOST
    }

    for symbol in SYMBOLS:
        listOfTtextNews = []
        querystring = {"region":"US","snippetCount":"28","s":symbol}
        res = requests.post(URL, data=payload, headers=headers, params=querystring)
        if res.status_code == 200:
            response = res.json()
            stock_news = response['data']['main']['stream']
            for stock_new in stock_news:
                click_through_url = stock_new['content'].get('clickThroughUrl')
                url = click_through_url.get('url') if click_through_url and 'url' in click_through_url else None
                preview_url = stock_new['content'].get('previewUrl') if 'previewUrl' in stock_new['content'] else None            
                
                #elements to make guid , for unicity assurance
                title = stock_new['content']['title']
                pubDate = datetime.strptime(stock_new['content']['pubDate'], "%Y-%m-%dT%H:%M:%SZ").strftime("%Y-%m-%d")

                #gather the elements into one variable
                combined_guid_elements=f"{title}|{symbol}|{pubDate}"

                #make new element
                news_element = {
                    # add id to  make it unique , and to iedentify this element to not insert it twice
                    'guid' : hashlib.sha256(combined_guid_elements.encode()).hexdigest(),
                    '_pubDate' :pubDate,
                    '_title' :title,
                    '_symbol':symbol,
                    '_TextNew': scrape_yahoo_finance_article(url if url else preview_url) if url or preview_url else None,
                    #add is_new to know if this new is already used in dataset for model input or not
                    'is_new':1,
                }
                if news_element['_TextNew'] is not None:
                    listOfTtextNews.append(news_element)
        new_symbol={
            'symbol':symbol,
            'listOfNews':listOfTtextNews
        }
        listOfsymbols.append(new_symbol)
    return listOfsymbols

# scrap news data for each given , returns a text new
def scrape_yahoo_finance_article(url):
    try:
        # Send a GET request to the URL
        response = requests.get(url)
        
        # Check if the request was successful (status code 200)
        if response.status_code == 200:
            # Parse the HTML content of the page
            soup = BeautifulSoup(response.content, 'html.parser')
            # Find the main article content
            article_content = soup.find('div', class_='caas-body')

            
            # Initialize an empty string to store the extracted text
            extracted_text = ""
            
            # Extract the text from each paragraph in the article content
            if article_content:
                paragraphs = article_content.find_all('p')

                for p in paragraphs:
                    extracted_text += p.get_text() + "\n"

            return extracted_text
        else:
            print("Failed to retrieve the webpage.for url ",url)
            return None
    except Exception as e:
        print(f"An error occurred while scraping the webpage: {e}")
        return None
    
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
                if news_element['guid'] not in [existing_element['guid'] for existing_element in existing_news_list]:
                    # Add the new news to the symbol's list of news
                    existing_news_list.append(news_element)
                    print(f"Inserted new news for symbol {symbol}.")
            # Update the symbol document in the collection with the updated list of news
            collection.update_one({'symbol': symbol}, {'$set': {'listOfNews': existing_news_list}})
    
    client.close()


load_to_database()
