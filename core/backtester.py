from pathlib import Path
from typing import Optional, Type
import pandas as pd
from datetime import timedelta
from core.logger import get_logger
log = get_logger(__name__)
from providers.base import DataProvider


class BacktestRunner:
    def __init__(
        self,
        strategy_class: Type,
        data_provider: DataProvider,
        window_days: Optional[int] = None,
        stride_days: Optional[int] = None,
        exclude_days_start: int = 0,
        exclude_days_end: int = 0
    ):
        self.exclude_days_end = exclude_days_end
        self.exclude_days_start = exclude_days_start
        self.strategy_class = strategy_class
        self.data_provider = data_provider
        self.window_days = window_days
        self.stride_days = stride_days
        self.results = []

    def run(self):
        log.info("🚀 Запуск BacktestRunner...")
        for source, df in self.data_provider.get_dataframes():

            df = df.copy()
            df["datetime"] = pd.to_datetime(df["datetime"], utc=True)
            df = df.sort_values("datetime")

            if self.window_days:
                self._run_rolling_windows(df, source)
            else:
                self._run_full(df, source)

    def _run_full(self, df: pd.DataFrame, source: str):
        strategy = self.strategy_class()
        self.strategy_id = strategy.strategy_id

        fut_code = source.split("_")[0]
        contract_name = f"{fut_code} (full)"
        start = df["datetime"].min() + timedelta(days=self.exclude_days_start)
        end = df["datetime"].max() - timedelta(days=self.exclude_days_end)

        result = self._make_result(df, strategy, source, contract_name, start, end)
        self.results.append(result)

    def _run_rolling_windows(self, df: pd.DataFrame, source: str):
        start = df["datetime"].min() + timedelta(days=self.exclude_days_start)
        end = df["datetime"].max() - timedelta(days=self.exclude_days_end)
        window = timedelta(days=self.window_days)
        stride = timedelta(days=self.stride_days or self.window_days)

        current = start
        while current + window <= end:
            window_df = df[(df["datetime"] >= current) & (df["datetime"] < current + window)]
            if window_df.empty:
                current += stride
                continue

            strategy = self.strategy_class()
            self.strategy_id = strategy.strategy_id

            fut_code = source.split("_")[0]
            contract_name = f"{fut_code} ({current.date()} → {(current + window).date()})"

            result = self._make_result(window_df, strategy, source, contract_name, current, current + window)
            self.results.append(result)
            current += stride

    def _make_result(
            self,
            df: pd.DataFrame,
            strategy,
            source: str,
            contract_name: str,
            start,
            end
    ) -> dict:
        result = strategy.run(df)
        result["contract"] = contract_name
        result["strategy"] = strategy
        result["strategy_id"] = strategy.strategy_id
        result["source"] = source
        result["start"] = start
        result["end"] = end
        return result

