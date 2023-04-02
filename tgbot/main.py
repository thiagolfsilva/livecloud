import os
import json
from google.cloud import firestore
import requests
from flask import make_response

bot_token = os.environ['TELEGRAM_BOT_TOKEN']
api_url = f'https://api.telegram.org/bot{bot_token}'
db = firestore.Client()

def telegram_webhook(request):
    message = request.json.get('message')
    chat_id = message.get('chat', {}).get('id')
    text = message.get('text')

    if text == '/subscribe':
        subscribe(chat_id)
    elif text == '/unsubscribe':
        unsubscribe(chat_id)
    elif text == '/help':
        show_help(chat_id)
    elif text == '/start':
        start(chat_id)
    else:
        default_response(chat_id)

    return make_response('', 200)

def start(chat_id):
    message = "Welcome to the Coinsight Notifications Bot, every 10 minutes we look through the exchanges Order Book for arbitrage opportunities, and notify you when we find them! Use /subscribe to start receiving notifications"
    send_message(chat_id, message)

def show_help(chat_id):
    message = 'This bot allows you to subscribe and unsubscribe from Coinsight Notifications.\n\n'
    message += 'To receive notifications, send /subscribe.\n'
    message += 'To stop receiving notifications, send /unsubscribe.\n'
    message += 'Or head to our documentation page to know more at https://apex-6.gitbook.io/coinsight/'
    send_message(chat_id, message)

def subscribe(chat_id):
    doc_ref = db.collection('subscribers').document(str(chat_id))
    doc = doc_ref.get()
    if doc.exists:
        send_message(chat_id, 'You are already subscribed!')
    else:
        doc_ref.set({'chat_id': chat_id})
        send_message(chat_id, 'You have successfully subscribed!')

def unsubscribe(chat_id):
    db.collection('subscribers').document(str(chat_id)).delete()
    send_message(chat_id, 'You have successfully unsubscribed!')

def default_response(chat_id):
    send_message(chat_id, "I'm sorry, I don't understand that command. Please send /help to see the list of available commands.")

def send_message(chat_id, text):
    data = {'chat_id': chat_id, 'text': text}
    requests.post(f'{api_url}/sendMessage', json=data)
