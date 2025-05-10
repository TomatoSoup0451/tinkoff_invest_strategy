from abc import ABC, abstractmethod
from datetime import datetime
from typing import Optional, Union
import pandas as pd


class AbstractCandleProvider(ABC):
    @abstractmethod
    def get_minute_candles(
        self,
        ticker: Union[str, list[str], None] = None,
        from_dt: Optional[datetime] = None,
        to_dt: Optional[datetime] = None,
    ) -> pd.DataFrame:
        """
        Return minute candles, optionally filtered by ticker(s) and time range.
        - ticker: str → single contract
        - list[str] → multiple contracts
        - None → all contracts
        """

    @abstractmethod
    def get_hourly_candles(
        self,
        ticker: Union[str, list[str], None] = None,
        from_dt: Optional[datetime] = None,
        to_dt: Optional[datetime] = None,
    ) -> pd.DataFrame:
        """
        Return hourly candles aggregated from minute data,
        optionally filtered by ticker(s) and time range.
        """

    @abstractmethod
    def get_available_tickers(self) -> list[str]:
        """Return list of available contract codes (tickers) in dataset."""
