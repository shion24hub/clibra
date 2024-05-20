from typing import Final
from pathlib import Path

from .exchange import Bybit

WORKING_DIR: Final = f"{Path.home()}/.clibra"
IMPLEMENTED_EXCHANGES_MAPPER: Final = {"bybit": Bybit}