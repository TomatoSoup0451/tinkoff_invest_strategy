import pandas as pd

from bot.analyzers.base import SignalAnalyzerBase
from indicators import calculate_indicators, add_signals


class BasicStrategy:
    def __init__(self, analyzer: SignalAnalyzerBase):
        self.analyzer = analyzer
        self.trades = []
        self.strategy_id = self.generate_strategy_id()

    def generate_strategy_id(self) -> str:
        params_str = "_".join(f"{k}{v}" for k, v in self.analyzer.params.items())
        return f"{self.analyzer.__class__.__name__}_{params_str}"

    def run(self, df: pd.DataFrame) -> dict:
        df = df.copy()
        df = self.analyzer.calculate(df)
        df["signal"] = df.apply(self.analyzer.get_signal, axis=1)
        df = self.simulate_trades(df)
        return self.evaluate(df)

    def simulate_trades(self, df: pd.DataFrame) -> pd.DataFrame:
        in_position = False
        direction = 0
        entry_price = 0
        entry_time = None
        trades = []

        for i in range(1, len(df)):
            signal = df.iloc[i - 1]["signal"]
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

        self.trades = pd.DataFrame(trades)
        return df

    def evaluate(self, df: pd.DataFrame) -> dict:
        trades_df = self.trades
        if trades_df.empty:
            return {"pnl": 0, "trades": 0, "winrate": 0, "drawdown": 0}

        total_trades = len(trades_df)
        wins = trades_df[trades_df["pnl_net"] > 0]
        losses = trades_df[trades_df["pnl_net"] <= 0]
        winrate = float(len(wins) / total_trades * 100)
        avg_win = float(wins["pnl_net"].mean() or 0)
        avg_loss = float(losses["pnl_net"].mean() or 0)
        max_drawdown = float((trades_df["pnl_net"].cumsum().cummax() - trades_df["pnl_net"].cumsum()).max())
        total_profit = float(trades_df["pnl_net"].sum())

        return {
            "pnl": round(total_profit, 2),
            "trades": total_trades,
            "winrate": round(winrate, 2),
            "avg_win": round(avg_win, 2),
            "avg_loss": round(avg_loss, 2),
            "drawdown": round(max_drawdown, 2)
        }

