from typing import Optional
import pandas as pd

from analyzers.base import SignalAnalyzerBase
from evaluators.strategy_evaluator import StrategyEvaluator
from core.logger import get_logger

log = get_logger(__name__)


class BasicStrategy:
    def __init__(self, analyzer: SignalAnalyzerBase, simulator):
        self.analyzer = analyzer
        self.simulator = simulator
        self.trades = []
        self.evaluator = StrategyEvaluator()
        self.strategy_id = self.generate_strategy_id()

    def run(self, hourly_df: pd.DataFrame, minute_df: Optional[pd.DataFrame] = None) -> dict:
        hourly_df = hourly_df.copy()
        hourly_df = self.analyzer.calculate(hourly_df)
        hourly_df["signal"] = hourly_df.apply(self.analyzer.get_signal, axis=1)

        self.trades = self.simulator.simulate(hourly_df, hourly_df["signal"], minute_df=minute_df)

        result = self.evaluator.evaluate(self.trades)
        result["trades_df"] = self.trades.copy()
        return result

    def generate_strategy_id(self) -> str:
        params_str = "_".join(f"{k}{v}" for k, v in self.analyzer.params.items())
        return f"{self.analyzer.__class__.__name__}_{params_str}"
