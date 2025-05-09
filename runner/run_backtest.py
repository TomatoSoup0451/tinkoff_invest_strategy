from pathlib import Path
import itertools
from tqdm import tqdm
from concurrent.futures import ProcessPoolExecutor, as_completed

from core.backtester import BacktestRunner
from providers.parquet import ParquetDataProvider
from core.strategy import BasicStrategy
from simulators.basic import BasicTradeSimulator
from analyzers.sma_rsi import SMARSIAnalyzer
from writers.md_writer import save_summary_table, save_strategy_summary
from evaluators.aggregator import aggregate_by_strategy


# Глобальные параметры
ROOT_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT_DIR / "data" / "candles_filtered"
fixed_atr = 14

# Параметры стратегии
sma_values = [20, 40, 60, 80, 100]
rsi_values = [7, 14, 21]
rsi_buy_thresholds = [50, 55, 60, 65, 70]
rsi_sell_thresholds = [30, 35, 40, 45, 50]

param_grid = [
    (sma, rsi, rsi_buy, rsi_sell)
    for sma, rsi, rsi_buy, rsi_sell in itertools.product(
        sma_values, rsi_values, rsi_buy_thresholds, rsi_sell_thresholds
    )
    if rsi_buy > rsi_sell
]


def run_one_strategy(args):
    sma, rsi, rsi_buy, rsi_sell = args
    strategy_id = f"SMARSI_sma{sma}_rsi{rsi}_atr{fixed_atr}_buy{rsi_buy}_sell{rsi_sell}"

    provider = ParquetDataProvider(DATA_DIR)

    def strategy_class():
        analyzer = SMARSIAnalyzer(
            sma=sma,
            rsi=rsi,
            atr=fixed_atr,
            rsi_buy=rsi_buy,
            rsi_sell=rsi_sell,
        )
        simulator = BasicTradeSimulator(commission_rate=0.0004, slippage=10)
        return BasicStrategy(analyzer=analyzer, simulator=simulator)

    bt = BacktestRunner(
        strategy_class=strategy_class,
        data_provider=provider
    )

    bt.run()

    for row in bt.results:
        row["strategy_id"] = strategy_id

    save_summary_table(bt.results, strategy_id)

    return bt.results


def main():
    all_results = []

    with ProcessPoolExecutor(max_workers=16) as executor:
        futures = {executor.submit(run_one_strategy, args): args for args in param_grid}

        for future in tqdm(as_completed(futures), total=len(futures), desc="🧪 Testing strategies"):
            try:
                results = future.result()
                if results:
                    all_results.extend(results)
            except Exception as e:
                pass  # Можно логировать ошибку, если захочешь

    strategy_df = aggregate_by_strategy(all_results)
    save_strategy_summary(strategy_df)


if __name__ == "__main__":
    from multiprocessing import freeze_support
    freeze_support()  # для Windows
    main()
