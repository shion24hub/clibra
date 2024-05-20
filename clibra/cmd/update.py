from typing import Final
import pandas as pd
import datetime
import os
import numpy as np
import typer
from urllib.error import HTTPError
from rich.progress import track

from .config import WORKING_DIR, IMPLEMENTED_EXCHANGES_MAPPER
from .exchange import Exchange, Bybit
from .utils import generate_save_path, validate_datetime_format, validate_exchange


def make_1s_candle(df: pd.DataFrame):
    """
    convert trading data to ohlcv data.
    required columns of df: ['datetime', 'side', 'size', 'price']

    df:
    - datetime(pd.datetime64[ns]): timestamp of the trade
    - side(str): 'Buy' or 'Sell'
    - size(float): size of the trade
    - price(float): price of the trade
    """

    df = df[["datetime", "side", "size", "price"]]

    df.loc[:, ["buySize"]] = np.where(df["side"] == "Buy", df["size"], 0)
    df.loc[:, ["sellSize"]] = np.where(df["side"] == "Sell", df["size"], 0)
    df.loc[:, ["datetime"]] = df["datetime"].dt.floor("1s")

    df = df.groupby("datetime").agg(
        {
            "price": ["first", "max", "min", "last"],
            "size": "sum",
            "buySize": "sum",
            "sellSize": "sum",
        }
    )

    # multiindex to single index
    df.columns = ["_".join(col) for col in df.columns]
    df = df.rename(
        columns={
            "price_first": "open",
            "price_max": "high",
            "price_min": "low",
            "price_last": "close",
            "size_sum": "volume",
            "buySize_sum": "buyVolume",
            "sellSize_sum": "sellVolume",
        }
    )

    return df


def update(exchange: str, symbol: str, begin: str, end: str) -> None:
    """update

    update data directory of 1-second candlestick data.

    """

    timer = datetime.datetime.now()

    # validate the exchange
    l_exchange = validate_exchange(exchange)

    # validate the date format
    bdt = validate_datetime_format(begin)
    edt = validate_datetime_format(end)

    # main process
    # 1. generate the date range
    # 2. check if the data already exists
    # 3. download the data
    # 4. data processing
    # 5. save the data

    date_range = pd.date_range(bdt, edt, freq="D")
    ex: Exchange = IMPLEMENTED_EXCHANGES_MAPPER[l_exchange]()

    for date in track(date_range, description='Update'):

        target_dir, target_file = generate_save_path(l_exchange, symbol, date)
        target = os.path.join(target_dir, target_file)
        print(f"Target: {target}")

        # check if the data already exists
        if os.path.exists(target):
            print(f"    - Already exists.")
            continue

        # download the data
        url = ex.generate_url(symbol, date)
        try:
            df = ex.download(url)
        except HTTPError:
            print(f"    - Failed to download {url}.")
            continue
        except Exception as e:
            print(f"    - An error occurred: {e}.")
            continue
        print(f"    - Downloaded {url}.")

        # data processing
        df = ex.mold(df)
        df = make_1s_candle(df)
        print(f"    - Processed the data. {df.shape[0]} rows.")

        # save the data
        os.makedirs(target_dir, exist_ok=True)
        df.to_csv(target, compression="gzip")
        print(f"    - Saved the data.")

    print("All processes are completed.")
    print(f"Elapsed time: {datetime.datetime.now() - timer}")


def update_from(file_path: str):
    """ update_from

    ``` procedure.txt

    bybit BTCUSDT 20240101 20240104
    bybit ETHUSDT 20240101 20240104

    ```
    
    """

    # validate the file
    if not os.path.exists(file_path):
        err = f"{file_path} does not exist."
        raise typer.BadParameter(err)
    
    with open(file_path, "r") as f:
        lines = f.readlines()
        for line in lines:
            # if the line is empty, skip
            if line == "\n":
                continue
            args = line.split()
            update(*args)