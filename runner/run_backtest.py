from multiprocessing.util import debug
from pathlib import Path
import itertools
from tqdm import tqdm
from concurrent.futures import ProcessPoolExecutor, as_completed

from core.backtester import BacktestRunner
from providers.parquet import ParquetDataProvider
from core.strategy import BasicStrategy
from providers.rollover_hourly import RolloverHourlyProvider
from providers.rollover_minutes import RolloverMinuteProvider
from simulators.basic import BasicTradeSimulator
from analyzers.sma_rsi import SMARSIAnalyzer
from writers.md_writer import save_summary_table, save_strategy_summary, save_markdown_table
from evaluators.aggregator import aggregate_by_strategy
from config import StrategyConfig


# Глобальные параметры
ROOT_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT_DIR / "data" / "candles_filtered"
fixed_atr = 14
config = StrategyConfig(mode="debug")



param_grid = [
    (sma, rsi, rsi_buy, rsi_sell)
    for sma, rsi, rsi_buy, rsi_sell in itertools.product(
        config.sma_values, config.rsi_values,
        config.rsi_buy_thresholds, config.rsi_sell_thresholds
    )
    if rsi_buy > rsi_sell
]

def run_one_strategy(args):

    sma, rsi, rsi_buy, rsi_sell = args
    strategy_id = f"SMARSI_sma{sma}_rsi{rsi}_atr{fixed_atr}_buy{rsi_buy}_sell{rsi_sell}"

    provider = RolloverMinuteProvider(DATA_DIR, debug_output=config.debug_data_provider)

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
        data_provider=provider,
        exclude_days_end=0,
        exclude_days_start=0,


    )

    bt.run()

    if config.save_individual_reports:
        for row in bt.results:
            row["strategy_id"] = strategy_id
            trades = row.get("trades_df")
            if trades is not None and not trades.empty:
                save_markdown_table(trades, name=row["strategy_id"], max_rows=1000)

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
                print(e)
                pass  # Можно логировать ошибку, если захочешь

    strategy_df = aggregate_by_strategy(all_results)
    save_strategy_summary(strategy_df)

def hourly_minutes_comparison():
    hourly_provider = RolloverHourlyProvider(DATA_DIR)
    minutes_provider = RolloverMinuteProvider(DATA_DIR)
    print(hourly_provider.get_dataframes())
    print(minutes_provider.get_dataframes())
    print(hourly_provider.get_cached_df())
    print(minutes_provider.get_cached_df())
    diff = hourly_provider.get_cached_df().compare(minutes_provider.get_cached_df())
    print(diff)

if __name__ == "__main__":
    from multiprocessing import freeze_support
    freeze_support()  # для Windows
    main()
