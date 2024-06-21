# -*- coding: UTF-8 -*-

# import
import requests
import pandas as pd
import os
from tqdm import tqdm
from alpha_vantage.timeseries import TimeSeries
import sys

# globals

# functions/classes

class AlphaVantage(object):
    _query_url = "https://www.alphavantage.co/query"
    def __init__(self, api_key: str = None,
                 function: str = "TIME_SERIES_DAILY_ADJUSTED",
                 ):
        self.api_key = api_key
        self.function = function

    def request_stock_price_hist(self, symbol: str) -> pd.DataFrame:

        meta_data = {"function": self.function,
                "symbol": symbol,
                "outputsize": "full",
                "datatype": "json",
                "apikey": self.api_key,
                }

        print("Retrieving stock price data from Alpha Vantage (This may take a while)...")
        r = requests.get(self._query_url, meta_data)
        print("Data has been successfully downloaded...")
        date = []
        column_names = list(range(0, 7))
        df = pd.DataFrame(columns = column_names)
        print("Sorting the retrieved data into a dataframe...")
        for i in tqdm(r.json()['Time Series (Daily)'].keys()):
            date.append(i)
            row = pd.DataFrame.from_dict(r.json()['Time Series (Daily)'][i],
                                         orient='index').reset_index().T[1:]
            df = pd.concat([df, row], ignore_index=True)
        df.columns = ["open", "high", "low", "close", "adjusted close", "volume", "dividend amount", "split cf"]
        df['date'] = date
        return df


    def demo1(self, symbol:str) -> pd.DataFrame:

        meta_data = {"function": self.function,
                "symbol": symbol,
                "outputsize": "full",
                "datatype": "json",
                "apikey": self.api_key}

        response = requests.get(self._query_url, meta_data)
        response_json = response.json()  # maybe redundant

        data:pd.DataFrame = pd.DataFrame.from_dict(response_json['Time Series (Daily)'],
                                                   orient='index').sort_index(axis=1)
        data = data.rename(columns={'1. open': 'Open', '2. high': 'High', '3. low': 'Low', '4. close': 'Close',
                                    '5. volume': 'Volume'}  # '5. adjusted close': 'AdjClose'
                           )
        data = data[['Open', 'High', 'Low', 'Close', 'Volume']]  # 'AdjClose'
        return data

    def demo2(self) -> None:

        symbols = ['QCOM', 'INTC', 'PDD']

        for symbol in symbols:
            meta_data = {"function": self.function,
                         "symbol": symbol,
                         "interval": "60min",
                         "datatype": "json",
                         "apikey": self.api_key}
            response = requests.get(self._query_url, meta_data)
            data = response.json()
            print(symbol)
            a = (data['Time Series (60min)'])
            keys = (a.keys())
            for key in keys:
                print(a[key]['2. high'] + " " + a[key]['5. volume'])

    def demo3(self, symbol:str) -> pd.DataFrame:
        """
        Retrieve daily stock price data from Alpha Vantage using the
        TimeSeries class from the alpha_vantage library.
        :param symbol: Ticker symbol of the stock
        :type symbol: str
        :return: stock data in a pandas dataframe
        :rtype: pd.DataFrame
        """

        ts = TimeSeries(key=self.api_key,
                        output_format="pandas",
                        indexing_type="date")

        try:
            data, meta_data = ts.get_daily(symbol=symbol, outputsize="full")
        except Exception as e:
            print(e)
            sys.exit(1)

        # Rename columns
        data = data.rename(columns={'1. open': 'Open', '2. high': 'High', '3. low': 'Low', '4. close': 'Close',
                                    '5. volume': 'Volume'}  # '5. adjusted close': 'AdjClose'
                           )

        # Keep only the columns we are interested in
        data = data[['Open', 'High', 'Low', 'Close', 'Volume']]  # 'AdjClose'
        return data


if __name__ == '__main__':

    api_key = os.getenv('ALPHA_VANTAGE_KEY', 'WUH8OLXGVSQRQLD1')

    alpha_wrapper = AlphaVantage(api_key=api_key, function="TIME_SERIES_DAILY")

    data = alpha_wrapper.demo3(symbol='NVDA')
    print(data.tail())  # check OK or not
