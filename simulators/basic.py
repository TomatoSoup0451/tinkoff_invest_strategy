import pandas as pd
from simulators.base import TradeSimulatorBase
from core.logger import get_logger

log = get_logger(__name__)


class BasicTradeSimulator(TradeSimulatorBase):
    def __init__(self, commission_rate: float = 0.0004, slippage: float = 10):
        self.commission_rate = commission_rate
        self.slippage = slippage
        self.trades = []

    def simulate(self, hourly_df: pd.DataFrame, signals: pd.Series, minute_df: pd.DataFrame = None) -> pd.DataFrame:
        self.trades = []

        if "contract_code" not in hourly_df.columns:
            log.warning("⚠️ contract_code отсутствует — симуляция будет выполнена без группировки.")
            return self._simulate_one_contract(hourly_df, signals)

        for contract_code, group_df in hourly_df.groupby("contract_code"):
            group_df = group_df.copy()
            group_signals = signals.loc[group_df.index]

            trades_df = self._simulate_one_contract(group_df, group_signals, contract_code)
            self.trades.extend(trades_df.to_dict(orient="records"))

        return pd.DataFrame(self.trades)

    def _simulate_one_contract(self, hourly_df: pd.DataFrame, signals: pd.Series, contract_code: str = "UNKNOWN") -> pd.DataFrame:
        trades = []
        in_position = False
        direction = 0
        entry_price = 0
        entry_time = None

        for i in range(1, len(hourly_df)):
            row = hourly_df.iloc[i]
            signal = signals.iloc[i - 1]

            # --- Открытие позиции ---
            if not in_position and signal != 0:
                in_position, direction, entry_price, entry_time = self._open_trade(signal, row)

            # --- Перезаход в другую сторону ---
            elif in_position and signal != 0 and signal != direction:
                trades.append(self._close_trade(row, direction, entry_price, entry_time, contract_code, "signal_change"))
                in_position, direction, entry_price, entry_time = self._open_trade(signal, row)

        return pd.DataFrame(trades)

    def _open_trade(self, signal, row):
        return True, signal, row["open"], row["datetime"]

    def _close_trade(self, row, direction, entry_price, entry_time, contract_code, exit_reason):
        exit_price = row["open"]
        exit_time = row["datetime"]

        gross_pnl = (exit_price - entry_price) * direction
        commission = (abs(entry_price) + abs(exit_price)) * self.commission_rate
        slippage_cost = self.slippage * 2
        net_pnl = gross_pnl - commission - slippage_cost

        return {
            "entry_time": entry_time,
            "exit_time": exit_time,
            "side": "long" if direction == 1 else "short",
            "entry_price": entry_price,
            "exit_price": exit_price,
            "pnl_raw": gross_pnl,
            "commission": commission,
            "slippage": slippage_cost,
            "pnl_net": net_pnl,
            "contract_code": contract_code,
            "exit_reason": exit_reason
        }
