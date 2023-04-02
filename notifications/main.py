import os
import json
from google.cloud import firestore
import requests

bot_token = os.environ['TELEGRAM_BOT_TOKEN']
api_url = f'https://api.telegram.org/bot{bot_token}'
data_url = "https://3qfclxl7ne2ttf4jxzpreebb3i0popbm.lambda-url.eu-west-3.on.aws/"

db = firestore.Client()

def telegram_notifications(request, context):
    # Call the web API and get the data
    response = requests.get(data_url)
    print('response')
    print(response)

    # Parse the response as JSON
    result = json.loads(response.content)
    print('result')
    print(result)


    # Baseline cost of trading fees. An arbitrage is only profitable if the price difference is bigger than this percentage
    threshold = 0.2

    # Add to the message all coins that are profitable to arbitrage using taker orders
    message = f'Coins with profitable arbitrage opportunities:\n'
    
    for item in result:
        if item['profit']>threshold:
            message+= f"{item['coin']} - Buy on {item['lowest_ask_venue']} for ${item['lowest_ask']:.4f}, Sell on {item['highest_bid_venue']} for ${item['highest_bid']:.4f}, instant {(item['profit']-threshold):.2f}% profit\n"
    message+='Considering 0.1% trading fees on each side'

    # Get the list of subscribers from Firestore
    subscribers_ref = db.collection('subscribers')
    subscribers = [doc.to_dict()['chat_id'] for doc in subscribers_ref.stream()]

    # Send messages to all subscribers
    for chat_id in subscribers:
        send_message(chat_id, message)

    return 'Messages sent successfully.'

def send_message(chat_id, text):
    data = {'chat_id': chat_id, 'text': text}
    requests.post(f'{api_url}/sendMessage', json=data)

if __name__ == '__main__':
    telegram_notifications(0,0)