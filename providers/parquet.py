import re
from pathlib import Path
from typing import Optional, Union
from datetime import datetime

import pandas as pd
from .base import AbstractCandleProvider


class ParquetCandleProvider(AbstractCandleProvider):
    def __init__(self, data_dir: Path):
        self.data_dir = data_dir
        self._minute_cache: Optional[pd.DataFrame] = None

    def _extract_contract_code(self, filename: str) -> str:
        match = re.match(r"(FUTRTS\d{6})", filename)
        return match.group(1) if match else "UNKNOWN"

    def _load_minute_data(self) -> pd.DataFrame:
        if self._minute_cache is None:
            all_files = sorted(self.data_dir.glob("*_minute.parquet"))
            dfs = []
            for file in all_files:
                df = pd.read_parquet(file)
                df = df.copy()
                df["datetime"] = pd.to_datetime(df["datetime"], unit="ns")
                df["contract_code"] = self._extract_contract_code(file.name)
                dfs.append(df)
            self._minute_cache = pd.concat(dfs, ignore_index=True)
        return self._minute_cache

    def _normalize_tickers(self, ticker: Union[str, list[str], None]) -> Optional[list[str]]:
        if ticker is None:
            return None
        if isinstance(ticker, str):
            return [ticker]
        return ticker  # assume already list[str]

    def _filter(self, df: pd.DataFrame, tickers: Optional[list[str]], from_dt: Optional[datetime], to_dt: Optional[datetime]) -> pd.DataFrame:
        if tickers:
            df = df[df["contract_code"].isin(tickers)]
        if from_dt:
            df = df[df["datetime"] >= from_dt]
        if to_dt:
            df = df[df["datetime"] <= to_dt]
        return df.sort_values("datetime")

    def get_minute_candles(
        self,
        ticker: Union[str, list[str], None] = None,
        from_dt: Optional[datetime] = None,
        to_dt: Optional[datetime] = None
    ) -> pd.DataFrame:
        tickers = self._normalize_tickers(ticker)
        df = self._load_minute_data()
        return self._filter(df.copy(), tickers, from_dt, to_dt)

    def get_hourly_candles(
        self,
        ticker: Union[str, list[str], None] = None,
        from_dt: Optional[datetime] = None,
        to_dt: Optional[datetime] = None
    ) -> pd.DataFrame:
        minute_df = self.get_minute_candles(ticker, from_dt, to_dt)
        resampled = (
            minute_df.set_index("datetime")
            .resample("1H")
            .agg({
                "open": "first",
                "high": "max",
                "low": "min",
                "close": "last",
                "volume": "sum",
                "contract_code": "first"
            })
            .dropna(subset=["open", "high", "low", "close"])
            .reset_index()
        )
        return resampled

    def get_available_tickers(self) -> list[str]:
        df = self._load_minute_data()
        return sorted(df["contract_code"].unique())
