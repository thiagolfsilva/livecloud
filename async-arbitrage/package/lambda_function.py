import json
import asyncio
import aiohttp
from collections import Counter


async def get_binance_coins():
    url = 'https://data.binance.com/api/v3/exchangeInfo'

    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            if response.status == 200:
                data = await response.json()
                symbols = [symbol['symbol'] for symbol in data['symbols'] if symbol['quoteAsset'] == 'USDT' and not symbol['baseAsset'].endswith(('UP','DOWN','BEAR','BULL'))]
                coins = [symbol[:-len('USDT')] for symbol in symbols]
                return coins
            else:
                raise Exception(f'Failed to retrieve coins from Binance. Status code: {response.status}, Response text: {await response.text()}')


async def best_binance_bid_ask(coin):
    symbol = coin+'USDT'
    limit = 1
    url = f'https://data.binance.com/api/v3/depth?symbol={symbol}&limit={limit}'

    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            if response.status == 200:
                data = await response.json()
                return data['bids'][0][0], data['asks'][0][0]
            else:
                raise Exception(f'Failed to retrieve bid/ask for {coin} from Binance. Status code: {response.status}, Response text: {await response.text()}')


async def get_binance_futures_coins():
    url = 'https://fapi.binance.com/fapi/v1/exchangeInfo'

    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            if response.status == 200:
                data = await response.json()
                symbols = [symbol['symbol'] for symbol in data['symbols'] if symbol['quoteAsset'] == 'USDT'
                           and not '_' in symbol['symbol']]
                coins = [symbol[:-len('USDT')] for symbol in symbols]
                return coins
            else:
                raise Exception('Failed to retrieve coins from Binance Futures')


async def best_binance_futures_bid_ask(symbol):
    symbol = symbol+'USDT'
    limit = 5
    url = f'https://fapi.binance.com/fapi/v1/depth?symbol={symbol}&limit={limit}'

    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            if response.status == 200:
                data = await response.json()
                return data['bids'][0][0], data['asks'][0][0]
            else:
                raise Exception(f'Failed to retrieve bid/ask for {symbol} from Binance Futures. Status code: {response.status}, Response text: {await response.text()}')


async def get_kucoin_coins():

    url = 'https://api.kucoin.com/api/v1/symbols'

    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            if response.status == 200:
                data = await response.json()
                symbols = [symbol['name'] for symbol in data['data'] if symbol['quoteCurrency'] == 'USDT'
                           and '-' in symbol['name']]
                coins = [symbol[:-len('-USDT')] for symbol in symbols]
                return coins
            else:
                raise Exception('Failed to retrieve coins from Kucoin')


async def best_kucoin_bid_ask(symbol):
    symbol = symbol+'-USDT'
    limit = 20
    url = f'https://api.kucoin.com/api/v1/market/orderbook/level2_{limit}?symbol={symbol}'

    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            if response.status == 200:
                data = await response.json()
                return data['data']['bids'][0][0], data['data']['asks'][0][0]
            else:
                raise Exception(f'Failed to retrieve bid/ask for {symbol} from Kucoin. Status code: {response.status}, Response text: {await response.text()}')

async def get_best_bid_ask(coin, sources):
    best_bid = -float('inf')
    best_ask = float('inf')
    best_bid_source = None
    best_ask_source = None
    tasks = [func(coin) for func in sources.values()]
    results = await asyncio.gather(*tasks)
    print(results)

    for source, (bid, ask) in zip(sources.keys(), results):
        bid = float(bid)
        ask = float(ask)

        if bid > best_bid:
            best_bid = bid
            best_bid_source = source

        if ask < best_ask:
            best_ask = ask
            best_ask_source = source

    return best_bid, best_ask, best_bid_source, best_ask_source

async def get_arbitrage(coin_sources, price_sources, threshold=False, show=True):
    # Arbitrage needs to be between two or more venues, so we exclude coins that only trade in one
    print("get_arbitrage")
    all_coins = []
    for func in coin_sources.values():
        all_coins.extend(await func())
    counter = Counter(all_coins)
    result = [item for item, count in counter.items() if count >= 2]
    arr = []

    for coin in result:
        try:
            highest_bid, lowest_ask, highest_bid_source, lowest_ask_source = await get_best_bid_ask(coin, price_sources)
            profit = (highest_bid - lowest_ask) / lowest_ask * 100
            if profit >= threshold or threshold is False:
                profit_str = "{:.2f}%".format(profit)
                arr.append({
                    'coin': coin,
                    'profit': profit,
                    'highest_bid': highest_bid,
                    'highest_bid_venue': highest_bid_source,
                    'lowest_ask': lowest_ask,
                    'lowest_ask_venue': lowest_ask_source
                })
                string = f"{coin}: Profit:{profit_str}, Highest bid: {highest_bid} from {highest_bid_source}, Lowest ask: {lowest_ask} from {lowest_ask_source}"
                if show:
                    print(string)
        except:
            pass

    # Sort the results by profit in descending order
    arr.sort(key=lambda x: x['profit'], reverse=True)

    return arr

async def main_function():
    price_sources_spot = {
    "Binance": best_binance_bid_ask,
    "Kucoin": best_kucoin_bid_ask
    }

    coin_sources_spot = {
    "Binance": get_binance_coins,
    "Kucoin": get_kucoin_coins
    }

    price_sources_futures = {
        "Binance Futures": best_binance_futures_bid_ask,
    }

    coin_sources_futures = {
        "Binance Futures": get_binance_futures_coins
    }

    price_sources = {**price_sources_spot, **price_sources_futures}
    coin_sources = {**coin_sources_spot, **coin_sources_futures}

    result = await get_arbitrage(coin_sources, price_sources, show=True)

    return result

def lambda_handler(event, context):
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            result = loop.run_until_complete(main_function())
        finally:
            loop.close()
        return {
            'statusCode': 200,
            'body': json.dumps(result)
        }
    except Exception as e:
        return {
            'statusCode': 500,
            'body': str(e)
        }

if __name__ == '__main__':
    print(lambda_handler(0,0))


