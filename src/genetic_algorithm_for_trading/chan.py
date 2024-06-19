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

# globals

# functions

def get_alpha_vantage(key: str, ticker: str, data_path: Path) -> None:
    """Given a key to Alpha Vantage and a valid ticker, this function will
    query alpha vantage and save the dataset into a csv in a predefined
    directory using ticker as the filename.
    """
    if data_path is None:
        raise ValueError("data_path cannot be None")

    ts = TimeSeries(key=key, output_format="pandas", indexing_type="date")
    out = data_path / f"{ticker}.csv"

    try:
        data, meta_data = ts.get_daily_adjusted(symbol='i', outputsize="full")
        data.to_csv(out)
        print(f"{ticker} has been downloaded to {out}")
    except:
        print(f"{ticker} Not found.")


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

    ALPHA_VANTAGE_DIR_PATH = Path("../data/alpha_vantage")
    if not ALPHA_VANTAGE_DIR_PATH.exists():
        ALPHA_VANTAGE_DIR_PATH.mkdir(parents=True)

    SECRET = "WUH8OLXGVSQRQLD1"

    get_alpha_vantage(key=SECRET, ticker="NVDA", data_path=ALPHA_VANTAGE_DIR_PATH)
    df = read_alpha_vantage(ticker="NVDA", data_path=ALPHA_VANTAGE_DIR_PATH)

    print(df.head())
