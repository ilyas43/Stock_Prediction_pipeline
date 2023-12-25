from dotenv import load_dotenv
import os
import requests
from datetime import date
from utils.functions import *
load_dotenv("../.env")

def API_TO_KAFKA():
    url = os.getenv('URL')
    querystring = {"symbol":"AMZN","region":"US"}

    headers = {
        "X-RapidAPI-Key": os.getenv('RAPID_API_KEY'),
        "X-RapidAPI-Host": os.getenv('RAPID_API_HOST')
    }

    response = requests.get(url, headers=headers, params=querystring)
    res = response.json()
    details = res['summaryDetail']
    '''
    
        we must modify this code in order to retrieve the historical data of (yesterday) , with these caracteristics and close
        at the first time , we should choose the first time of the data ex: 2001 , 
        and then we will get the data of only yesterday everyday 
        choose the start date of the data , and the end date will be yesterday , 
        the same thing with scrapping news

        - for oneshoot producer to learn model at the first time: 
            stock price : get stock info from 01/01/2002 to 21/12/2023 EXEMPLE
            Stock News  : get the news from 01/01/2002 to 21/12/2023 EXEMPLE !!! from historic API !!!!

        
    - for historical data (yesterday) to learn model: 
        stock price (API): at 8:00 , get stock info of yesterday
        Stock News (database)  : get the news of yesterday from database 
    - for realtime data (today) for real time dashboard and prediction :
        stock price (API) : get the realtime data , show stock infos (open current hight , current low ...) in dashboard
        Stock News (scraping)  : get the news at 18:00 of today's datetime 
                      and stock them to use them tomorrow to learn the moled 
                      and also give them to model to predict 
    '''
    #fields
    previousClose=details['previousClose']['fmt']
    open=details['open']['fmt']
    dayLow=details['dayLow']['fmt']
    dayHigh=details['dayHigh']['fmt']
    volume=details['volume']['fmt']

    regularMarketOpen=details['regularMarketOpen']['fmt']
    regularMarketDayHigh=details['regularMarketDayHigh']['fmt']
    regularMarketPreviousClose=details['regularMarketPreviousClose']['fmt']
    
    regularMarketDayLow=details['regularMarketDayLow']['fmt']
    priceHint=details['priceHint']['fmt']
    currency=details['currency']
    trailingPE=details['trailingPE']['fmt']
    regularMarketVolume=details['regularMarketVolume']['fmt']
    averageVolume=details['averageVolume']['fmt']
    
    ask=details['ask']['fmt']# minimum amount a seller is willing to accept when selling a stock.
    askSize=details['askSize']['fmt'] # number of shares available at the ask price
    
    
    symbol=details['symbol']['fmt']
    stock_date=date.today()-1
    print(stock_date)

# get data from API
API_TO_KAFKA()