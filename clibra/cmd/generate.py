
import os
import datetime
import pandas as pd
from rich.progress import track
import typer

from .config import WORKING_DIR, IMPLEMENTED_EXCHANGES_MAPPER
from .utils import generate_save_path, validate_datetime_format, validate_exchange



def generate(exchange: str, symbol: str, begin: str, end: str, interval: int, output_dir: str) -> None:
    """generate

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
    dfs: list[pd.DataFrame] = (
        []
    )  # https://github.com/microsoft/pylance-release/issues/5630

    for date in track(date_range, description='Generate'):

        target_dir, target_file = generate_save_path(l_exchange, symbol, date)
        target = os.path.join(target_dir, target_file)
        print(f"Target: {target}")

        # check if the data exists
        if not os.path.exists(target):
            print("    - does not exist.")
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
        print("No data.")
        return

    ans = pd.concat(dfs)
    file_name = f"{exchange}_{symbol}_{begin}_{end}_{interval}.csv.gz"
    output_path = os.path.join(output_dir, file_name)
    ans.to_csv(output_path, compression="gzip")

    print(f"Saved the data to {output_path}.")
    print(f"All processes are completed.")
    print(f"Elapsed time: {datetime.datetime.now() - timer}")


def generate_from(file_path: str):
    """ generate_from

    ``` procedure.txt

    bybit BTCUSDT 20240101 20240104 3600
    bybit ETHUSDT 20240101 20240104 3600

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
            generate(*args)

