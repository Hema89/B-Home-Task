#!/usr/bin/env python

from binance.spot import Spot as Client
from prometheus_client import start_http_server, Gauge
import pandas as pd
import time


class BinanceTest:

    def __init__(self, client):
        self.client = client
        self.prom_gauge = Gauge('absolute_delta_value',
                                'Absolute Delta Value of Price Spread', ['symbol'])

    def get_top_BTC_vol(self):
        #Print the top 5 symbols with quote asset BTC and the highest volume over the last 24 hours in descending order.
        f = pd.DataFrame(self.client.exchange_info()['symbols'])
        BTC_symbols = f['symbol'].where(f['quoteAsset'] == 'BTC').dropna()
        sym_vol_int =pd.DataFrame(self.client.ticker_24hr())
        sym_vol = sym_vol_int[['symbol','volume']]
        vol = pd.merge(BTC_symbols, sym_vol, on="symbol")
        vol['volume']=vol['volume'].astype('float')
        final_vol=vol.sort_values('volume',ascending = False).head(5)
        return final_vol.reset_index(drop=True)

    #Print the top 5 symbols with quote asset USDT and the highest number of trades over the last 24 hours in descending order.
    def trade_count(self, x):
        trade_data = pd.DataFrame(self.client.trades(x))
        current_time = round(time.time() * 1000)
        trade_df = trade_data[ current_time-trade_data.time <= 86400000 ].count()
        return trade_df.qty

    def get_top_USDT_symbol(self):
        f = pd.DataFrame(self.client.exchange_info()['symbols'])
        USDT_symbols = f['symbol'].where(f['quoteAsset'] == 'USDT').dropna().to_frame()
        USDT_symbols['number_of_trades'] = USDT_symbols.symbol.apply(lambda x: self.trade_count(x))
        final_trades = USDT_symbols.sort_values('number_of_trades', ascending=False).head(5)
        return final_trades.reset_index(drop=True)

    def get_notional(self,final_vol):
    #Using the symbols from Q1, what is the total notional value of the top 200 bids and asks currently on each order book
        notional_list = {}
        for s in final_vol['symbol']:
            f = self.client.depth(s, limit=200)
            for col in ["bids", "asks"]:
                df = pd.DataFrame(data=f[col], columns=["price", "quantity"], dtype=float)
                df['notional'] = df['price'] * df['quantity']
                df['notional'].sum()
                notional_list[s + '_' + col] = df['notional'].sum()
        return (notional_list)

    #What is the price spread for each of the symbols from Q2?

    def getSpreadList(self,final_trades):
        spread_list = {}

        for s in final_trades['symbol']:
            price_spread = self.client.book_ticker(s)
            spread_list[s] = float(price_spread['askPrice']) - float(price_spread['bidPrice'])

        return  spread_list


    def get_spread_delta(self, final_trades):

        delta = {}
        old_spread = self.getSpreadList(final_trades)

        time.sleep(10)
        new_spread =  self.getSpreadList(final_trades)

        for key in old_spread:
            delta[key] = abs(old_spread[key] - new_spread[key])

        for key in delta:
            self.prom_gauge.labels(key).set(delta[key])

        return delta


if __name__ == "__main__":
    # Start up the server to expose the metrics.

    key = "fVllVIwDvYdOCSTo1GZfVPvB9KImxr7idNzOwSBvJ0959UamHvldJpD3iNdjJ168"
    secret = "74vcBVnfRdPcHIcnjP11yB3Ji1JnR66zD99KuFNUoYWXsU8ah6OWzs8Pkg8nJkhq"

    client = Client(key, secret, base_url="https://api.binance.com")
    test = BinanceTest(client)
    #Q1:
    BTC_vol= test.get_top_BTC_vol()
    print(BTC_vol)
    print(200*'=')
    #Q2:
    USDT_trade=test.get_top_USDT_symbol()
    print(USDT_trade)
    print(200*'=')
    #Q3:
    notional = test.get_notional(BTC_vol)
    print(notional)
    print(200*'=')
    #Q4/5/6:
    start_http_server(8080)
    while True:
        spread_delta=test.get_spread_delta(USDT_trade)
        print(spread_delta)





