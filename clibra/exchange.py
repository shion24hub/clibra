from abc import ABCMeta, abstractmethod
import datetime
import os
from urllib.error import HTTPError

import pandas as pd


class Exchange(metaclass=ABCMeta):
    
    @abstractmethod
    def generate_url(self, symbol: str, date: datetime.datetime) -> str:
        pass

    @abstractmethod
    def download(self, url: str) -> None:
        pass

    @abstractmethod
    def mold(self, df: pd.DataFrame) -> pd.DataFrame:
        pass


class Bybit(Exchange):
    
    def __init__(self) -> None:
        self.base_url = 'https://public.bybit.com/trading/'

    def generate_url(self, symbol: str, date: datetime.datetime) -> str:
        """ generate_url

        add later.
        
        """

        filename = f"{symbol}{date.strftime('%Y-%m-%d')}.csv.gz"
        url = os.path.join(self.base_url, symbol, filename)

        return url
    
    def download(self, url: str) -> pd.DataFrame | None:
        """ download

        add later.

        """

        try:
            df = pd.read_csv(url, compression="gzip")
        except HTTPError as e:
            raise e
        except Exception as e:
            raise e

        return df
    
    def mold(self, df: pd.DataFrame) -> pd.DataFrame:
        """ mold

        add later.
        
        """

        df.loc[:, ["datetime"]] = pd.to_datetime(df["timestamp"], unit="s")

        return df
