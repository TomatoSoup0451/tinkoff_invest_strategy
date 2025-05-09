import pandas as pd
from simulators.base import TradeSimulatorBase

class BasicTradeSimulator(TradeSimulatorBase):
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
                pnl = (open_price - entry_price) * direction
                trades.append({
                    "entry_time": entry_time,
                    "exit_time": time,
                    "side": "long" if direction == 1 else "short",
                    "entry_price": entry_price,
                    "exit_price": open_price,
                    "pnl_raw": pnl,
                    "pnl_net": pnl
                })
                in_position = False
                direction = 0

        return pd.DataFrame(trades)
