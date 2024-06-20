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

# globals

# functions

def get_alpha_vantage(key: str, ticker: str) -> Tuple[pd.DataFrame, dict]:
    """Given a key to Alpha Vantage and a valid ticker, this function will
    query alpha vantage for the daily adjusted time series data and return
    """
    ts = TimeSeries(key=key, output_format="pandas", indexing_type="date")

    try:
        data, meta_data = ts.get_daily_adjusted(symbol=ticker) # , outputsize="full")
    except Exception as e:
        print(e)
        sys.exit(1)

    return data, meta_data


def read_alpha_vantage(ticker: str, data_path: Path = None) -> pd.DataFrame:
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

    ALPHA_ADVANTAGE_KEY = os.getenv('ALPHA_ADVANTAGE_KEY', 'WUH8OLXGVSQRQLD1')
    ticker = "EUR"
    cached_ts = data_path / f"{ticker}.csv"

    if not cached_ts.exists():
        time_series, metadata = get_alpha_vantage(key=ALPHA_ADVANTAGE_KEY,
                                                  ticker=ticker,
                                                  )

        # TODO Vor Speicherung in die korrekte Form bringen
        time_series.to_csv(cached_ts)
        print(f"{ticker} has been saved to {cached_ts}")
    else:
        time_series = read_alpha_vantage(ticker=ticker, data_path=data_path)

    print(time_series.head())
