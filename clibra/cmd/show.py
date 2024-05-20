import os
import datetime
import pandas as pd

from .config import WORKING_DIR


def show() -> None:
    """show

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

            # to avoid empty list
            if dates == []:
                continue

            mindt = min(dates)
            maxdt = max(dates)
            missings = [
                x for x in pd.date_range(mindt, maxdt, freq="D") if x not in dates
            ]

            message = "{}: {} from {} to {}, {} missing dates".format(
                exchange,
                symbol,
                mindt.strftime("%Y-%m-%d"),
                maxdt.strftime("%Y-%m-%d"),
                len(missings),
            )
            print(message)