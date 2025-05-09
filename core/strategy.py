import pandas as pd

from analyzers.base import SignalAnalyzerBase


class BasicStrategy:
    def __init__(self, analyzer, simulator):
        self.analyzer = analyzer
        self.simulator = simulator
        self.trades = []
        self.strategy_id = self.generate_strategy_id()

    def generate_strategy_id(self) -> str:
        params_str = "_".join(f"{k}{v}" for k, v in self.analyzer.params.items())
        return f"{self.analyzer.__class__.__name__}_{params_str}"

    def run(self, df: pd.DataFrame) -> dict:
        df = df.copy()
        df = self.analyzer.calculate(df)
        df["signal"] = df.apply(self.analyzer.get_signal, axis=1)
        self.trades = self.simulator.simulate(df, df["signal"])
        return self.evaluate()

    def evaluate(self) -> dict:
        if self.trades.empty:
            return {"pnl": 0, "trades": 0, "winrate": 0, "drawdown": 0}

        pnl_series = self.trades["pnl_net"].cumsum()
        max_drawdown = (pnl_series.cummax() - pnl_series).max()
        wins = self.trades[self.trades["pnl_net"] > 0]
        losses = self.trades[self.trades["pnl_net"] <= 0]

        return {
            "pnl": round(self.trades["pnl_net"].sum(), 2),
            "trades": len(self.trades),
            "winrate": round(len(wins) / len(self.trades) * 100, 2),
            "avg_win": round(wins["pnl_net"].mean() or 0, 2),
            "avg_loss": round(losses["pnl_net"].mean() or 0, 2),
            "drawdown": round(max_drawdown, 2),
        }

