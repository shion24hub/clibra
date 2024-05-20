import datetime
import os
import typer
from .config import WORKING_DIR, IMPLEMENTED_EXCHANGES_MAPPER


def generate_save_path(exchange: str, symbol: str, date: datetime.datetime) -> str:
    """
    Define the storage path.
    """

    save_path = os.path.join(
        WORKING_DIR,
        "candles",
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