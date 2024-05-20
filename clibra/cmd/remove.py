import datetime
import os
import typer
import pandas as pd
from .utils import generate_save_path, validate_datetime_format, validate_exchange


def remove(exchange: str, symbol: str, begin: str, end: str) -> None:
    """remove

    Delete data stored

    """

    timer = datetime.datetime.now()

    # validate the exchange
    l_exchange = validate_exchange(exchange)

    # validate the date format
    bdt = validate_datetime_format(begin)
    edt = validate_datetime_format(end)

    # confirm
    y_or_n = typer.prompt(
        f"Do you really want to remove {bdt}-{edt} data for {symbol}? (y/n)"
    )
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
        print(f"Target: {target}")

        if os.path.exists(target):
            os.remove(target)
            print(f"    - removed.")
        else:
            print(f"    - does not exist.")

    print("All processes are completed.")
    print(f"Elapsed time: {datetime.datetime.now() - timer}")