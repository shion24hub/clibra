from typing import Annotated
import os
import typer

from .cmd.config import WORKING_DIR
from .cmd.update import update as update_impl
from .cmd.update import update_from as update_from_impl
from .cmd.generate import generate as generate_impl
from .cmd.generate import generate_from as generate_from_impl
from .cmd.remove import remove as remove_impl
from .cmd.show import show as show_impl

app = typer.Typer()

# CLI Commands

@app.callback(help="A CLI tool for managing the crypto candlestick data.")
def callback() -> None:
    """callback
    initialize the working directory.
    """

    os.makedirs(WORKING_DIR, exist_ok=True)


@app.command(help="Update the `clibra` strage.")
def update(
    exchange: Annotated[str, typer.Argument(help="The exchange to update")],
    symbol: Annotated[str, typer.Argument(help="The symbol to download")],
    begin: Annotated[str, typer.Argument(help="The begin date (YYYYMMDD)")],
    end: Annotated[str, typer.Argument(help="The end date (YYYYMMDD)")],
) -> None:
    """update

    update data directory of 1-second candlestick data.

    """

    update_impl(exchange, symbol, begin, end)


@app.command(help="Update the `clibra` storage from a .txt file. The file should contain the following format: `exchange symbol begin end`.")
def update_from(file_path: Annotated[str, typer.Argument(help="The file path")]):
    """ update_from

    ``` procedure.txt

    bybit BTCUSDT 20240101 20240104
    bybit ETHUSDT 20240101 20240104

    ```
    
    """

    update_from_impl(file_path)


@app.command(help="Generate the csv.gz file of T seconds OHLCV.")
def generate(
    exchange: Annotated[str, typer.Argument(help="The exchange to generate")],
    symbol: Annotated[str, typer.Argument(help="The symbol to download")],
    begin: Annotated[str, typer.Argument(help="The begin date (YYYYMMDD)")],
    end: Annotated[str, typer.Argument(help="The end date (YYYYMMDD)")],
    interval: Annotated[int, typer.Argument(help="The interval in seconds")],
    output_dir: Annotated[str, typer.Option(help="The output directory")] = './',
) -> None:
    """generate

    generate the database of 1-second candlestick data.

    """

    generate_impl(exchange, symbol, begin, end, interval, output_dir)


@app.command()
def generate_from(file_path: Annotated[str, typer.Argument(help="The file path")]):
    """ generate_from

    ``` procedure.txt

    bybit BTCUSDT 20240101 20240104 3600
    bybit ETHUSDT 20240101 20240104 3600

    ```
    
    """

    generate_from_impl(file_path)


@app.command(help="Remove data for a specified period.")
def remove(
    exchange: Annotated[str, typer.Argument(help="The exchange to remove")],
    symbol: Annotated[str, typer.Argument(help="The symbol to remove")],
    begin: Annotated[str, typer.Argument(help="The begin date (YYYYMMDD)")],
    end: Annotated[str, typer.Argument(help="The end date (YYYYMMDD)")],
) -> None:
    """remove

    Delete data stored

    """

    remove_impl(exchange, symbol, begin, end)


@app.command(help="Show the available symbols and dates.")
def show() -> None:
    """show

    show the available symbols and dates.

    """

    show_impl()


