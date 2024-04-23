# Stock_Prediction_pipeline
## introduction
A data engineering pipeline that ingests data from various sources in order to visualize the data , and train LSTM model to predict stock price
## details
before you start , add params.py file to each pipieline folder that contains :
SYMBOLS=
URL = 
X_RAPIDAPI_KEY
X_RAPIDAPI_HOST=

MONGO_HOST=
MONGO_DB=
MONGO_COLLECTION=

## how it works : 
input data :  text senetiments infos of yesterday + previous close + previous onpen , + previous high and low , + previous volume , + previous close for n period ... + today's open