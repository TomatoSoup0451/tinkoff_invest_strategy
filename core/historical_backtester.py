from pathlib import Path
from typing import Optional, Type
import pandas as pd
from datetime import timedelta

class HistoricalBacktester:
    def __init__(
        self,
        strategy_class: Type,
        data_dir: Path,
        window_days: Optional[int] = None,
        stride_days: Optional[int] = None
    ):
        self.strategy_class = strategy_class
        self.data_dir = data_dir
        self.window_days = window_days
        self.stride_days = stride_days
        self.results = []

    def run(self):
        parquet_files = sorted(self.data_dir.glob("*.parquet"))
        for file in parquet_files:
            print(f"\n📁 Testing on file: {file.name}")
            df = pd.read_parquet(file)
            df["datetime"] = pd.to_datetime(df["datetime"], utc=True)
            df = df.sort_values("datetime")

            if self.window_days:
                self._run_rolling_windows(df, file.name)
            else:
                self._run_full(df, file.name)

    def _run_full(self, df: pd.DataFrame, source: str):
        strategy = self.strategy_class()
        result = strategy.run(df)
        fut_code = source.split("_")[0]
        result["Актив"] = f"{fut_code} (full)"
        result["strategy"] = strategy
        result["source"] = source
        result["start"] = df["datetime"].min()
        result["end"] = df["datetime"].max()
        self.results.append(result)

    def _run_rolling_windows(self, df: pd.DataFrame, source: str):
        start = df["datetime"].min()
        end = df["datetime"].max()
        window = timedelta(days=self.window_days)
        stride = timedelta(days=self.stride_days or self.window_days)

        current = start
        while current + window <= end:
            window_df = df[(df["datetime"] >= current) & (df["datetime"] < current + window)]
            if len(window_df) == 0:
                current += stride
                continue
            strategy = self.strategy_class()
            result = strategy.run(window_df)
            fut_code = source.split("_")[0]  # FUTRTS032200
            name = f"{fut_code} ({current.date()} → {(current + window).date()})"
            result["Актив"] = name
            result["strategy"] = strategy
            result["source"] = source
            result["start"] = current
            result["end"] = current + window

            self.results.append(result)
            current += stride
