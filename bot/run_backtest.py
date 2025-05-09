from analyzers.sma_rsi import SMARSIAnalyzer
from core.strategy import BasicStrategy
from core.historical_backtester import HistoricalBacktester
from pathlib import Path
from writers.md_writer import save_summary_table
import os
import re

from simulators.basic import BasicTradeSimulator


def sanitize_filename(name: str) -> str:
    return re.sub(r'[^a-zA-Z0-9_\-.]', '_', str(name))



analyzer = SMARSIAnalyzer(sma=50, rsi=14)
simulator = BasicTradeSimulator()
strategy_class = lambda: BasicStrategy(analyzer, simulator)

bt = HistoricalBacktester(
    strategy_class=strategy_class,
    data_dir=Path("../data/candles_filtered"),
    window_days=30,
    stride_days=10
)
bt.run()

# Печать результатов
# summary по всем окнам
save_summary_table(bt.results)

