import pandas as pd
from typing import List
from evaluators.metrics import sharpe_ratio, profit_factor, winrate, expectancy, max_drawdown

def aggregate_by_strategy(results: List[dict]) -> pd.DataFrame:
    df = pd.DataFrame(results)
    if "strategy_id" not in df.columns:
        raise ValueError("Отсутствует поле strategy_id в результатах")

    strategy_groups = df.groupby("strategy_id")
    aggregated = []

    for strategy_id, group in strategy_groups:
        # Объединяем все сделки по всем окнам
        all_trades = pd.concat(group["trades_df"].tolist(), ignore_index=True)

        pnl_raw_total = group["pnl_raw"].sum()
        pnl_net_total = group["pnl_net"].sum()
        drawdown_max = max_drawdown(all_trades["pnl_net"])
        trades_total = len(all_trades)
        winrate_val = winrate(all_trades)
        sharpe_val = sharpe_ratio(all_trades["pnl_net"])
        profit_factor_val = profit_factor(all_trades)
        expectancy_val = expectancy(all_trades)

        aggregated.append({
            "strategy_id": strategy_id,
            "pnl_raw": round(pnl_raw_total, 2),
            "pnl_net": round(pnl_net_total, 2),
            "drawdown": round(drawdown_max, 2),
            "sharpe": round(sharpe_val, 2) if pd.notna(sharpe_val) else None,
            "winrate": round(winrate_val, 2) if pd.notna(winrate_val) else None,
            "trades": trades_total,
            "profit_factor": round(profit_factor_val, 2) if pd.notna(profit_factor_val) else None,
            "expectancy": round(expectancy_val, 2) if pd.notna(expectancy_val) else None
        })

    return pd.DataFrame(aggregated)
