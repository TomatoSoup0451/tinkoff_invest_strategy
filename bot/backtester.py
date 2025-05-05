from datetime import timedelta
from typing import Dict, Optional
import pandas as pd
from datetime import datetime
import os

from data_loader import get_futures_spec, load_candles
from indicators import calculate_indicators, add_signals
from analyzer import analyze_trades
from report import save_markdown_table, save_summary_table

def apply_position(df):
    df["position"] = 0
    for i in range(1, len(df)):
        if df["signal"].iloc[i] != 0:
            df.iloc[i, df.columns.get_loc("position")] = df["signal"].iloc[i]
        else:
            df.iloc[i, df.columns.get_loc("position")] = df["position"].iloc[i - 1]
    return df


def calculate_pnl(df, step_price: float, lot: float, commission_rate=0.0004):
    trades = []
    entry_price = entry_time = None
    for i in range(1, len(df)):
        prev_pos = df["position"].iloc[i - 1]
        curr_pos = df["position"].iloc[i]
        if prev_pos == 0 and curr_pos != 0:
            entry_price = df["open"].iloc[i]
            entry_time = df.index[i]
        elif prev_pos != 0 and curr_pos != prev_pos:
            exit_price = df["open"].iloc[i]
            exit_time = df.index[i]
            direction = prev_pos
            price_diff = (exit_price - entry_price) * direction
            pnl_raw = price_diff * step_price * lot
            commission = (entry_price + exit_price) * step_price * commission_rate * lot
            pnl_net = pnl_raw - commission
            trades.append({
                "entry_time": entry_time,
                "exit_time": exit_time,
                "side": "long" if direction == 1 else "short",
                "entry_price": entry_price,
                "exit_price": exit_price,
                "pnl_raw": pnl_raw,
                "pnl_net": pnl_net,
            })
            if curr_pos != 0:
                entry_price, entry_time = exit_price, exit_time
            else:
                entry_price = entry_time = None
    return pd.DataFrame(trades)


class BacktestRunner:
    def __init__(
            self,
            tickers: Dict[str, str],
            days: int = 365,
            window_days: Optional[int] = None,
            stride_days: Optional[int] = None,
            save_reports: bool = True,
            force_refresh: bool = False,
            report_dir: Optional[str] = None# ⬅️ Новый параметр
    ):
        self.tickers = tickers
        self.days = days
        self.window_days = window_days
        self.stride_days = stride_days
        self.save_reports = save_reports
        self.data_cache = {}
        self.force_refresh = force_refresh
        if report_dir is None:
            timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
            self.report_dir = os.path.join("reports", timestamp)
        else:
            self.report_dir = report_dir
        os.makedirs(self.report_dir, exist_ok=True)

    async def preload_data(self):
        for ticker in self.tickers:
            spec = await get_futures_spec(ticker)
            if not spec:
                continue
            df = await load_candles(spec["figi"], days=self.days, force_refresh=self.force_refresh)
            self.data_cache[ticker] = {
                "df": df,
                "spec": spec
            }

    def run_strategy_on_df(self, df: pd.DataFrame, ticker: str, name: str):
        df = calculate_indicators(df.copy())
        df = add_signals(df)
        df = apply_position(df)

        spec = self.data_cache[ticker]["spec"]
        trades_df = calculate_pnl(df, step_price=spec["step_price"], lot=spec["lot"])
        stats = analyze_trades(trades_df, name)

        if self.save_reports:
            save_markdown_table(trades_df, name, self.report_dir)

        return stats

    def run_on_windows(self, df: pd.DataFrame, name: str, ticker: str):
        results = []
        start = df.index.min()
        end = df.index.max()
        delta = timedelta(days=self.window_days)
        step = timedelta(days=self.stride_days)

        current = start
        while current + delta <= end:
            sliced = df.loc[current:current + delta]
            if len(sliced) < 50:
                current += step
                continue

            sliced_name = f"{name}_{current.date()}"
            stats = self.run_strategy_on_df(sliced, ticker, sliced_name)
            if stats:
                results.append(stats)

            current += step

        return results

    async def run_all(self):
        await self.preload_data()
        summary_stats = []

        for ticker, name in self.tickers.items():
            cached = self.data_cache.get(ticker)
            if not cached:
                continue

            df = cached["df"]

            if self.window_days and self.stride_days:
                results = self.run_on_windows(df, name, ticker)
                summary_stats.extend(results)
            else:
                stats = self.run_strategy_on_df(df, ticker, name)
                if stats:
                    summary_stats.append(stats)

        if self.save_reports:
            save_summary_table(summary_stats, self.report_dir)




