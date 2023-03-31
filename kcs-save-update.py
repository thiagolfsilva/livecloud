import base64
import functions_framework
import requests
import timestamp
import json
import datetime
import pandas as pd
from google.cloud import bigquery
import pandas_gbq

project_id = 'data-warehouse-course-ps'
dataset_id = 'test_set'
table_loans = 'kcs_loans'
table_prices = 'kcs_prices'
BASE_KUCOIN='https://api.kucoin.com'

def get_currencies():
    response = requests.get('https://api.kucoin.com/api/v1/currencies')
    data=response.json()
    currencies = []
    for datapoint in data['data']:
        if datapoint['isMarginEnabled']:
            currencies.append(datapoint['currency'])

    currencies.sort()
    return currencies

def save_rates(coinset):
     
    timestamp = datetime.datetime.now().timestamp()
    print(pd.to_datetime)

    df_rates = get_loan_data(coinset, timestamp)    
    df_prices = get_prices(coinset, timestamp)

    df_rates.to_gbq(destination_table='{}.{}'.format(dataset_id, table_loans), project_id=project_id,if_exists='append')
    df_prices.to_gbq(destination_table='{}.{}'.format(dataset_id, table_prices), project_id=project_id,if_exists='append')


def get_loan_data(coinset, timestamp, showprogress=True):
    for coin in coinset:
        if showprogress:
            print(coin)
        response = requests.get(BASE_KUCOIN+'/api/v1/margin/market?currency='+coin)
        data = pd.json_normalize(response.json()["data"])
        data['coin'] = coin
        data['timestamp'] = pd.to_datetime(timestamp*10**9).floor('S')
        data['hrtimestamp'] = pd.to_datetime(timestamp*10**9).floor('H')
        
        try:
            df = pd.concat([df, data], ignore_index=True)

        except:
            df = data
    return df


def get_prices(currencies, timestamp):
    response = requests.get('https://api.kucoin.com/api/v1/market/allTickers')
    data=response.json()
    prices = []

    for datapoint in data['data']['ticker']:
        base, quote = datapoint['symbol'].split('-')
        if quote=='USDT' and base in currencies:
            midprice = (float(datapoint['buy'])+float(datapoint['sell']))/2
            prices.append([base, midprice])
    if 'USDT' in currencies:
        prices.append(['USDT', 1])
    df=pd.DataFrame(prices, columns=['coin','price'])
    df['timestamp'] = pd.to_datetime(timestamp*10**9).floor('S')
    df['hrtimestamp'] = pd.to_datetime(timestamp*10**9).floor('H')
    return df
    

@functions_framework.cloud_event
def hello_pubsub(cloud_event):
    currencies = get_currencies()
    save_rates(currencies)
    return "success"