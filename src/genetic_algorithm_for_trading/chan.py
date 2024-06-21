# -*- coding: UTF-8 -*-
# Last Change: 2020-06-29  15:45:00
# Description:
# Source: https://medium.com/towards-artificial-intelligence/genetic-algorithm-for-trading-strategy-optimization-in-python-614eb660990d

# import
import pandas as pd
import numpy as np
import datetime
from pathlib import Path
from alpha_vantage.timeseries import TimeSeries
import sys
import os
from typing import Tuple
import requests
from tqdm import tqdm


# globals

# functions

class AlphaVantage(object):
    _query_url = "https://www.alphavantage.co/query"
    def __init__(self, api_key: str = None,
                 function: str = "TIME_SERIES_DAILY",
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

    def query_alpha_vantage(self, ticker: str) -> Tuple[pd.DataFrame, dict]:
        """Given a key to Alpha Vantage and a valid ticker, this function will
        query alpha vantage for the daily adjusted time series data and return
        a pandas dataframe and the metadata

        """
        # Wrapper für die Alpha Vantage API via requests
        ts = TimeSeries(key=self.api_key,
                        output_format="pandas",
                        indexing_type="date",
                        )

        try:
            data, meta_data = ts.get_daily(symbol=ticker, outputsize="full")
        except Exception as e:
            print(e)
            sys.exit(1)

        # Rename columns to match the other data sources
        data = data.rename(columns={'1. open': 'Open',
                                    '2. high': 'High',
                                    '3. low': 'Low',
                                    '4. close': 'Close',
                                    '5. volume': 'Volume',
                                    })

        # keep only the columns we need             )
        data = data[['Open', 'High', 'Low', 'Close', 'Volume']]

        # extend the data with the adjusted close
        data['Adjustment'] = round(data['Close'].shift(-1) / data['Open'])
        data[['Adjusted Close', 'Adjusted Open', 'Adjusted High', 'Adjusted Low']] = (
                data[['Close', 'Open', 'High', 'Low']] * data['Adjustment'])

        return data, meta_data


    def read_alpha_vantage(self, ticker: str, data_path: Path = None) -> pd.DataFrame:
        """If the ticker's csv has been downloaded with `get_alpha_vantage`,
        this function will return a pandas dataframe of adjusted open, adjusted
        high, adjusted low, adjusted close and volume rounded to 4 decimal places
        """
        if data_path is None:
            raise ValueError("data_path cannot be None")

        data = data_path / f"{ticker}.csv"
        if not data.exists():
            raise FileNotFoundError(f"{ticker}.csv not found in {data_path}")

        df = pd.read_csv(
                data, index_col=0, parse_dates=True
        ).sort_index()
        df = df.rename(
                columns={
                    "1. open": "Open",
                    "2. high": "High",
                    "3. low": "Low",
                    "4. close": "Close",
                    "5. adjusted close": "Adjusted Close",
                    "6. volume": "Volume",
                    "7. dividend amount": "Dividend",
                    "8. split coefficient": "Split Coefficient",
                }
        )
        df["Unadjusted Open"] = df["Open"]
        df["Open"] = df["Close"] * df["Adjusted Close"] / df["Open"]
        df["High"] = df["High"] * df["Open"] / df["Unadjusted Open"]
        df["Low"] = df["Low"] * df["Open"] / df["Unadjusted Open"]
        df["Close"] = df["Adjusted Close"]
        return df[["Open", "High", "Low", "Close", "Volume"]].round(4)


if __name__ == '__main__':

    data_path = Path("../data/alpha_vantage")
    if not data_path.exists():
        data_path.mkdir(parents=True)

    alpha = AlphaVantage(api_key=os.getenv('ALPHA_VANTAGE_KEY', 'WUH8OLXGVSQRQLD1'))

    ticker = "NVDA"
    cached_ts = data_path / f"{ticker}.csv"

    if not cached_ts.exists():
        time_series, metadata = alpha.query_alpha_vantage(ticker=ticker)

        # TODO Vor Speicherung in die korrekte Form bringen
        time_series.to_csv(cached_ts)
        print(f"{ticker} has been saved to {cached_ts}")
    else:
        time_series = alpha.read_alpha_vantage(ticker=ticker, data_path=data_path)

    print(time_series.head())
