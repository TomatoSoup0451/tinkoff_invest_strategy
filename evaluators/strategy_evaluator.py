from evaluators import metrics
import pandas as pd

class StrategyEvaluator:
    def __init__(self):
        pass

    def evaluate(self, trades_df: pd.DataFrame) -> dict:
        if trades_df.empty:
            return {
                "pnl_raw": 0.0,
                "pnl_net": 0.0,
                "trades": 0,
                "winrate": 0.0,
                "drawdown": 0.0,
                "sharpe": 0.0,
                "profit_factor": 0.0,
                "expectancy": 0.0
            }

        pnl_raw_series = trades_df["pnl_raw"]
        pnl_net_series = trades_df["pnl_net"]

        return {
            "pnl_raw": round(pnl_raw_series.sum(), 2),
            "pnl_net": round(pnl_net_series.sum(), 2),
            "trades": len(trades_df),
            "winrate": round(metrics.winrate(trades_df), 2),
            "drawdown": round(metrics.max_drawdown(pnl_net_series), 2),
            "sharpe": round(metrics.sharpe_ratio(pnl_net_series), 2),
            "profit_factor": round(metrics.profit_factor(trades_df), 2),
            "expectancy": round(metrics.expectancy(trades_df), 2)
        }
