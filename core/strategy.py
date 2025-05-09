import pandas as pd

from analyzers.base import SignalAnalyzerBase
from evaluators import metrics
from evaluators.strategy_evaluator import StrategyEvaluator
from core.logger import get_logger

log = get_logger(__name__)

class BasicStrategy:
    def __init__(self, analyzer, simulator):
        self.analyzer = analyzer
        self.simulator = simulator
        self.trades = []
        self.evaluator = StrategyEvaluator()
        self.strategy_id = self.generate_strategy_id()

    def run(self, df: pd.DataFrame) -> dict:
        df = df.copy()
        df = self.analyzer.calculate(df)
        df["signal"] = df.apply(self.analyzer.get_signal, axis=1)

        self.trades = self.simulator.simulate(df, df["signal"])

        result = self.evaluator.evaluate(self.trades)
        result["trades_df"] = self.trades.copy()  # добавляем сделки в результат
        return result

    def generate_strategy_id(self) -> str:
        params_str = "_".join(f"{k}{v}" for k, v in self.analyzer.params.items())
        return f"{self.analyzer.__class__.__name__}_{params_str}"
