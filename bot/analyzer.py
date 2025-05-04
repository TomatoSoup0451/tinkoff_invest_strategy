def analyze_trades(trades_df, name: str):
    if trades_df.empty:
        print("Нет сделок для анализа.")
        return

    total_trades = len(trades_df)
    wins = trades_df[trades_df["pnl_net"] > 0]
    losses = trades_df[trades_df["pnl_net"] <= 0]
    winrate = float(len(wins) / total_trades * 100)
    avg_win = float(wins["pnl_net"].mean() or 0)
    avg_loss = float(losses["pnl_net"].mean() or 0)
    max_drawdown = float((trades_df["pnl_net"].cumsum().cummax() - trades_df["pnl_net"].cumsum()).max())
    total_profit = float(trades_df["pnl_net"].sum())

    stats = {
        "Актив": name,
        "Всего сделок": total_trades,
        "Winrate": round(winrate, 2),
        "Средняя прибыль": round(avg_win, 2),
        "Средний убыток": round(avg_loss, 2),
        "Макс. просадка": round(max_drawdown, 2),
        "Итоговая прибыль": round(total_profit, 2),
    }

    print(f"\n📊 Статистика по {name}:")
    for k, v in stats.items():
        print(f"{k}: {v}")

    return stats