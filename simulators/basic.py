import pandas as pd
from simulators.base import TradeSimulatorBase
from core.logger import get_logger

log = get_logger(__name__)

class BasicTradeSimulator(TradeSimulatorBase):
    def __init__(self, commission_rate: float = 0.0004, slippage: float = 10):
        self.commission_rate = commission_rate
        self.slippage = slippage
        self.trades = []

    def simulate(self, df: pd.DataFrame, signals: pd.Series) -> pd.DataFrame:
        in_position = False
        direction = 0
        entry_price = 0
        entry_time = None

        prev_row = None
        prev_contract = None
        seen_contracts = set()

        for i in range(1, len(df)):
            row = df.iloc[i]
            signal = signals.iloc[i - 1]
            current_contract = row.get("contract_code")
            if prev_row is not None:
                prev_contract = prev_row.get("contract_code")

            # --- Ролловер: закрываем по предыдущей свече, открываем по текущей ---
            if current_contract and current_contract not in seen_contracts:
                seen_contracts.add(current_contract)

                if in_position and prev_contract and prev_contract != current_contract:
                    self._close_trade(
                        exit_time=prev_row["datetime"],
                        exit_price=prev_row["close"],
                        direction=direction,
                        entry_time=entry_time,
                        entry_price=entry_price,
                        contract_code=prev_contract,
                        exit_reason="rollover"
                    )
                    in_position = False
                    direction = 0

                    if signal != 0:
                        in_position, direction, entry_price, entry_time = self._open_trade(signal, row)

            # --- Открытие позиции ---
            if not in_position and signal != 0:
                in_position, direction, entry_price, entry_time = self._open_trade(signal, row)

            # --- Перезаход в другую сторону ---
            elif in_position and signal != 0 and signal != direction:
                self._close_trade(
                    exit_time=row["datetime"],
                    exit_price=row["open"],
                    direction=direction,
                    entry_time=entry_time,
                    entry_price=entry_price,
                    contract_code=current_contract,
                    exit_reason="signal_change"
                )
                in_position, direction, entry_price, entry_time = self._open_trade(signal, row)

            prev_row = row

        return pd.DataFrame(self.trades)

    def _open_trade(self, signal, row):
        return True, signal, row["open"], row["datetime"]

    def _close_trade(self, exit_time, exit_price, direction, entry_time, entry_price, contract_code, exit_reason):
        gross_pnl = (exit_price - entry_price) * direction
        commission = (abs(entry_price) + abs(exit_price)) * self.commission_rate
        slippage_cost = self.slippage * 2
        net_pnl = gross_pnl - commission - slippage_cost

        self.trades.append({
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
        })
