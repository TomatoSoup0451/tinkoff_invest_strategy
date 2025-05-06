from bot.analyzers.sma_rsi import SMARSIAnalyzer
from strategy import BasicStrategy
from historical_backtester import HistoricalBacktester
from pathlib import Path
from report import save_summary_table, save_markdown_table
import os
import re

def sanitize_filename(name: str) -> str:
    return re.sub(r'[^a-zA-Z0-9_\-.]', '_', str(name))


report_dir = "report"
os.makedirs(report_dir, exist_ok=True)

analyzer = SMARSIAnalyzer(sma=50, rsi=14, rsi_buy=55, rsi_sell=45)
strategy = lambda: BasicStrategy(analyzer)

bt = HistoricalBacktester(
    strategy_class=strategy,
    data_dir=Path("data/candles_filtered"),
    window_days=30,
    stride_days=10
)
bt.run()
bt.run()

# Печать результатов
# summary по всем окнам
save_summary_table(bt.results, report_dir)

