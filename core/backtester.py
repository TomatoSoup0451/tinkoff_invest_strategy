from typing import Optional, Type, Union
from datetime import timedelta
import pandas as pd

from core.logger import get_logger
from providers.base import AbstractCandleProvider

log = get_logger(__name__)


class BacktestRunner:
    def __init__(
        self,
        strategy_class: Type,
        data_provider: AbstractCandleProvider,
        window_days: Optional[int] = None,
        stride_days: Optional[int] = None,
        exclude_days_start: int = 0,
        exclude_days_end: int = 0,
        tickers: Optional[Union[str, list[str]]] = None
    ):
        self.exclude_days_end = exclude_days_end
        self.exclude_days_start = exclude_days_start
        self.strategy_class = strategy_class
        self.data_provider = data_provider
        self.window_days = window_days
        self.stride_days = stride_days
        self.tickers = [tickers] if isinstance(tickers, str) else tickers
        self.results = []

    def run(self):
        log.info("🚀 Запуск BacktestRunner...")

        strategy = self.strategy_class()
        is_rollover = getattr(strategy.simulator, "rollover_aware", False)

        tickers_to_run = self.tickers or self.data_provider.get_available_tickers()

        # Rollover работает как один общий контракт
        if is_rollover:
            ticker = tickers_to_run  # список или один тикер
            raw_hourly = self.data_provider.get_hourly_candles(ticker=ticker)
            if raw_hourly.empty:
                log.warning("⚠️ Нет данных для Rollover стратегии")
                return

            start = raw_hourly["datetime"].min() + timedelta(days=self.exclude_days_start)
            end = raw_hourly["datetime"].max() - timedelta(days=self.exclude_days_end)

            minute_df = self.data_provider.get_minute_candles(ticker=ticker, from_dt=start, to_dt=end)
            hourly_df = self.data_provider.get_hourly_candles(ticker=ticker, from_dt=start, to_dt=end)

            if self.window_days:
                self._run_rolling_windows(minute_df, hourly_df, "Rollover")
            else:
                self._run_full(minute_df, hourly_df, "Rollover")

            return

        # Обычные стратегии по каждому тикеру
        for ticker in tickers_to_run:
            raw_hourly = self.data_provider.get_hourly_candles(ticker=ticker)
            if raw_hourly.empty:
                log.warning(f"⚠️ Нет данных для контракта {ticker}")
                continue

            start = raw_hourly["datetime"].min() + timedelta(days=self.exclude_days_start)
            end = raw_hourly["datetime"].max() - timedelta(days=self.exclude_days_end)

            minute_df = self.data_provider.get_minute_candles(ticker=ticker, from_dt=start, to_dt=end)
            hourly_df = self.data_provider.get_hourly_candles(ticker=ticker, from_dt=start, to_dt=end)

            if minute_df.empty or hourly_df.empty:
                log.warning(f"⚠️ Нет свечей в окне {start} → {end} для контракта {ticker}")
                continue

            if self.window_days:
                self._run_rolling_windows(minute_df, hourly_df, ticker)
            else:
                self._run_full(minute_df, hourly_df, ticker)

    def _run_full(self, minute_df: pd.DataFrame, hourly_df: pd.DataFrame, contract_code: str):
        strategy = self.strategy_class()
        self.strategy_id = strategy.strategy_id

        contract_name = f"{contract_code} (full)"
        start = hourly_df["datetime"].min()
        end = hourly_df["datetime"].max()

        result = self._make_result(minute_df, hourly_df, strategy, contract_code, contract_name, start, end)
        self.results.append(result)

    def _run_rolling_windows(self, minute_df: pd.DataFrame, hourly_df: pd.DataFrame, contract_code: str):
        start = hourly_df["datetime"].min()
        end = hourly_df["datetime"].max()
        window = timedelta(days=self.window_days)
        stride = timedelta(days=self.stride_days or self.window_days)

        current = start
        while current + window <= end:
            min_window = minute_df[(minute_df["datetime"] >= current) & (minute_df["datetime"] < current + window)]
            hour_window = hourly_df[(hourly_df["datetime"] >= current) & (hourly_df["datetime"] < current + window)]

            if hour_window.empty or min_window.empty:
                current += stride
                continue

            strategy = self.strategy_class()
            self.strategy_id = strategy.strategy_id

            contract_name = f"{contract_code} ({current.date()} → {(current + window).date()})"

            result = self._make_result(min_window, hour_window, strategy, contract_code, contract_name, current, current + window)
            self.results.append(result)
            current += stride

    def _make_result(
        self,
        minute_df: pd.DataFrame,
        hourly_df: pd.DataFrame,
        strategy,
        contract_code: str,
        contract_name: str,
        start,
        end
    ) -> dict:
        result = strategy.run(hourly_df, minute_df=minute_df)
        result["contract"] = contract_name
        result["strategy"] = strategy
        result["strategy_id"] = strategy.strategy_id
        result["source"] = contract_code
        result["start"] = start
        result["end"] = end
        return result
