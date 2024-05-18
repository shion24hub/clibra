import datetime
import os
from pathlib import Path
from urllib.error import HTTPError

import numpy as np
import pandas as pd
import typer
from loguru import logger
from rich import print

from .exchange import (
    Exchange,
    Bybit,
)


# Constants
WORKING_DIR = f"{Path.home()}/.clibra"
IMPLEMENTED_EXCHANGES_MAPPER = {'bybit': Bybit}

logger.remove()
logger.add(f"{WORKING_DIR}/log/app.log", level="INFO", format="{time} {level} {message}")
app = typer.Typer()

# Utility Functions

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

def generate_save_path(exchange: str, symbol: str, date: datetime.datetime) -> str:
    """
    Define the storage path.
    """

    save_path = os.path.join(
        WORKING_DIR,
        'candles',
        exchange,
        symbol,
    )
    filename = f'{date.strftime("%Y-%m-%d")}.csv.gz'

    return save_path, filename

def validate_datetime_format(date: str) -> datetime.datetime:
    """
    Validate the date format.
    """

    try:
        dt = datetime.datetime.strptime(date, "%Y%m%d")
    except ValueError:
        err = "Invalid date format. Please use YYYYMMDD."
        raise typer.BadParameter(err)

    return dt

def validate_exchange(exchange: str) -> str:
    """
    Validate the exchange.
    """

    l_exchange = exchange.lower()
    if l_exchange not in IMPLEMENTED_EXCHANGES_MAPPER.keys():
        err = f"{exchange} is not supported."
        raise typer.BadParameter(err)

    return l_exchange


# CLI Commands

@app.callback(help="A CLI tool for managing the crypto candlestick data.")
def callback() -> None:
    """ callback
    initialize the working directory.
    """

    os.makedirs(WORKING_DIR, exist_ok=True)


@app.command(help="Update the `clibra` strage.")
def update(
    exchange: str = typer.Argument(help="The exchange to update"),
    symbol: str = typer.Argument(help="The symbol to download"),
    begin: str = typer.Argument(help="The begin date (YYYYMMDD)"),
    end: str = typer.Argument(help="The end date (YYYYMMDD)"),
) -> None:
    """ update

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

    for date in date_range:

        target_dir, target_file = generate_save_path(l_exchange, symbol, date)
        target = os.path.join(target_dir, target_file)
        print(f'Target: {target}')

        # check if the data already exists
        if os.path.exists(target):
            print(f'    - Already exists.')
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
    
    print('All processes are completed.')
    print(f'Elapsed time: {datetime.datetime.now() - timer}')


@app.command(help="Generate the csv.gz file of T seconds OHLCV.")
def generate(
    exchange: str = typer.Argument(help="The exchange to generate"),
    symbol: str = typer.Argument(help="The symbol to download"),
    begin: str = typer.Argument(help="The begin date (YYYYMMDD)"),
    end: str = typer.Argument(help="The end date (YYYYMMDD)"),
    interval: int = typer.Argument(help="The interval in seconds"),
    output_dir: str = typer.Option("./", help="The output directory"),
) -> None:
    """ generate

    generate the database of 1-second candlestick data.

    """

    timer = datetime.datetime.now()

    # validate the exchange
    l_exchange = validate_exchange(exchange)

    # validate the date format
    bdt = validate_datetime_format(begin)
    edt = validate_datetime_format(end)

    # main process
    # 1. generate the date range
    # 2. check if the data exists
    # 3. read the data and convert its type
    # 4. re-structure the data
    # 5. save the data

    date_range = pd.date_range(bdt, edt, freq="D")
    dfs: list[pd.DataFrame] = [] # https://github.com/microsoft/pylance-release/issues/5630

    for date in date_range:
        
        target_dir, target_file = generate_save_path(l_exchange, symbol, date)
        target = os.path.join(target_dir, target_file)
        print(f'Target: {target}')

        # check if the data exists
        if not os.path.exists(target):
            print('    - does not exist.')
            continue
        
        # read the data and convert its type
        df = pd.read_csv(target)
        df["datetime"] = pd.to_datetime(df["datetime"])
        df["open"] = df["open"].astype(float)
        df["high"] = df["high"].astype(float)
        df["low"] = df["low"].astype(float)
        df["close"] = df["close"].astype(float)
        df["volume"] = df["volume"].astype(float)
        df["buyVolume"] = df["buyVolume"].astype(float)
        df["sellVolume"] = df["sellVolume"].astype(float)

        # re-structure the data
        df = df.set_index("datetime")
        df = df.resample(f"{interval}s").agg(
            {
                "open": "first",
                "high": "max",
                "low": "min",
                "close": "last",
                "volume": "sum",
                "buyVolume": "sum",
                "sellVolume": "sum",
            }
        )

        df = df.dropna()
        dfs.append(df)
        print(f"    - OK.")

    # save
    if len(dfs) == 0:
        print('No data.')
        return
    
    ans = pd.concat(dfs)
    file_name = f"{exchange}_{symbol}_{begin}_{end}_{interval}.csv.gz"
    output_path = os.path.join(output_dir, file_name)
    ans.to_csv(output_path, compression="gzip")

    print(f'Saved the data to {output_path}.')
    print(f'All processes are completed.')
    print(f'Elapsed time: {datetime.datetime.now() - timer}')


@app.command(help='Remove data for a specified period.')
def remove(
    exchange: str = typer.Argument(help="The exchange to remove"),
    symbol: str = typer.Argument(help="The symbol to remove"),
    begin: str = typer.Argument(help="The begin date (YYYYMMDD)"),
    end: str = typer.Argument(help="The end date (YYYYMMDD)"),
) -> None:
    """ remove

    Delete data stored

    """

    timer = datetime.datetime.now()
    
    # validate the exchange
    l_exchange = validate_exchange(exchange)
    
    # validate the date format
    bdt = validate_datetime_format(begin)
    edt = validate_datetime_format(end)

    # confirm
    y_or_n = typer.prompt(f"Do you really want to remove {bdt}-{edt} data for {symbol}? (y/n)")
    if y_or_n != "y":
        print("Canceled.")
        return

    # main process
    # 1. generate the date range
    # 2. remove the data

    date_range = pd.date_range(bdt, edt, freq="D")
    
    for date in date_range:
        
        target_dir, target_file = generate_save_path(l_exchange, symbol, date)
        target = os.path.join(target_dir, target_file)
        print(f'Target: {target}')

        if os.path.exists(target):
            os.remove(target)
            print(f"    - removed.")
        else:
            print(f"    - does not exist.")
    
    print('All processes are completed.')
    print(f'Elapsed time: {datetime.datetime.now() - timer}')


@app.command(help='Show the available symbols and dates.')
def show() -> None:
    """ show

    show the available symbols and dates.

    """

    # print working directory's size
    total_size = 0
    for root, _, files in os.walk(WORKING_DIR):
        for file in files:
            total_size += os.path.getsize(os.path.join(root, file))
    
    print(f"Total size: {total_size / 1024 / 1024:.2f} MB")

    # main process
    exchanges = os.listdir(f"{WORKING_DIR}/candles")
    exchanges = [x for x in exchanges if not x.startswith(".")]

    for exchange in exchanges:
            
        symbols = os.listdir(f"{WORKING_DIR}/candles/{exchange}")
        symbols = [x for x in symbols if not x.startswith(".")]

        for symbol in symbols:
            
            dates = os.listdir(f"{WORKING_DIR}/candles/{exchange}/{symbol}")
            dates = [x.split(".")[0] for x in dates]
            dates = [datetime.datetime.strptime(x, "%Y-%m-%d") for x in dates]

            mindt = min(dates)
            maxdt = max(dates)
            missings = [x for x in pd.date_range(mindt, maxdt, freq="D") if x not in dates]

            message = "{}: {} from {} to {}, {} missing dates".format(
                exchange, symbol, mindt.strftime("%Y-%m-%d"), maxdt.strftime("%Y-%m-%d"), len(missings)
            )
            print(message)

