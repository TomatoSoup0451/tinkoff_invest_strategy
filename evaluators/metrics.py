import pandas as pd
import numpy as np

def max_drawdown(pnl_series: pd.Series) -> float:
    """Максимальная просадка equity"""
    cumulative = pnl_series.cumsum()
    drawdown = cumulative.cummax() - cumulative
    return float(drawdown.max())

def sharpe_ratio(pnl_series: pd.Series, risk_free_rate: float = 0.0) -> float:
    """Sharpe ratio: assumes pnl in points per trade"""
    returns = pnl_series
    excess_returns = returns - risk_free_rate
    std = np.std(excess_returns)
    if len(returns) == 0 or std == 0 or np.isnan(std):
        return np.nan
    return float(np.mean(excess_returns) / std) * np.sqrt(len(returns))

def winrate(trades_df: pd.DataFrame) -> float:
    """Процент прибыльных сделок"""
    total = len(trades_df)
    if total == 0:
        return np.nan
    wins = trades_df[trades_df["pnl_net"] > 0]
    return float(len(wins)) / total * 100

def profit_factor(trades_df: pd.DataFrame) -> float:
    """Отношение прибыли к убыткам"""
    gross_profit = trades_df[trades_df["pnl_net"] > 0]["pnl_net"].sum()
    gross_loss = abs(trades_df[trades_df["pnl_net"] < 0]["pnl_net"].sum())
    if gross_loss == 0:
        return np.nan
    return float(gross_profit / gross_loss)

def expectancy(trades_df: pd.DataFrame) -> float:
    """Ожидаемая прибыль на сделку"""
    total = len(trades_df)
    if total == 0:
        return np.nan
    wins = trades_df[trades_df["pnl_net"] > 0]
    losses = trades_df[trades_df["pnl_net"] <= 0]
    avg_win = wins["pnl_net"].mean() if not wins.empty else 0.0
    avg_loss = losses["pnl_net"].mean() if not losses.empty else 0.0
    win_ratio = len(wins) / total
    return win_ratio * avg_win + (1 - win_ratio) * avg_loss
