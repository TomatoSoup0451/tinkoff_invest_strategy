import pandas as pd
from simulators.base import TradeSimulatorBase

class BasicTradeSimulator(TradeSimulatorBase):
    def __init__(self, commission_rate: float = 0.0004, slippage: float = 10):
        self.commission_rate = commission_rate
        self.slippage = slippage

    def simulate(self, df: pd.DataFrame, signals: pd.Series) -> pd.DataFrame:
        in_position = False
        direction = 0
        entry_price = 0
        entry_time = None
        trades = []

        for i in range(1, len(df)):
            signal = signals.iloc[i - 1]
            row = df.iloc[i]
            open_price = row["open"]
            time = row["datetime"]

            if not in_position and signal != 0:
                in_position = True
                direction = signal
                entry_price = open_price
                entry_time = time

            elif in_position and signal == 0:
                gross_pnl = (open_price - entry_price) * direction
                commission = (abs(entry_price) + abs(open_price)) * self.commission_rate
                slippage_cost = self.slippage * 2  # не умножай на direction!
                net_pnl = gross_pnl - commission - slippage_cost

                trades.append({
                    "entry_time": entry_time,
                    "exit_time": time,
                    "side": "long" if direction == 1 else "short",
                    "entry_price": entry_price,
                    "exit_price": open_price,
                    "pnl_raw": gross_pnl,
                    "commission": commission,
                    "slippage": slippage_cost,
                    "pnl_net": net_pnl
                })
                in_position = False
                direction = 0

        return pd.DataFrame(trades)
