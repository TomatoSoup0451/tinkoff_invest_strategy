import asyncio
import os
from datetime import timedelta
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

from tinkoff.invest import AsyncClient, CandleInterval
from tinkoff.invest.sandbox.async_client import AsyncSandboxClient
from tinkoff.invest.utils import now

from dotenv import load_dotenv

from tabulate import tabulate

load_dotenv()
TOKEN = os.getenv("TOKEN_SANDBOX")


async def get_futures_spec(ticker: str):
    async with AsyncClient(TOKEN) as client:
        response = await client.instruments.futures()
        for fut in response.instruments:
            if fut.ticker == ticker:
                print(f"Найден: {fut.ticker} | FIGI: {fut.figi} | Название: {fut.name} "
                      f"| Шаг цены: {fut.min_price_increment} | Лотность: {fut.lot} | Стоимость пункта цены: {fut.min_price_increment_amount}")

                return {
                    "figi": fut.figi,
                    "step_price": (
                            (fut.min_price_increment_amount.units + fut.min_price_increment_amount.nano / 1e9)
                            /
                            (fut.min_price_increment.units + fut.min_price_increment.nano / 1e9)
                    ),
                    "lot": fut.lot
                }
        print(f"Не найдено совпадений для {ticker}")
        return None


def calculate_indicators(df):
    df["sma_50"] = df["close"].rolling(window=50).mean()
    delta = df["close"].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
    rs = gain / loss
    df["rsi_14"] = 100 - (100 / (1 + rs))
    tr = pd.concat([
        (df["high"] - df["low"]),
        (df["high"] - df["close"].shift()).abs(),
        (df["low"] - df["close"].shift()).abs(),
    ], axis=1).max(axis=1)
    df["atr_14"] = tr.rolling(window=14).mean()
    return df


def add_signals(df):
    df["signal"] = 0
    long_condition = (
        (df["close"] > df["sma_50"]) &
        (df["rsi_14"] > 55) &
        (df["atr_14"] > df["atr_14"].rolling(window=20).mean())
    )
    short_condition = (
        (df["close"] < df["sma_50"]) &
        (df["rsi_14"] < 45) &
        (df["atr_14"] > df["atr_14"].rolling(window=20).mean())
    )
    df.loc[long_condition, "signal"] = 1
    df.loc[short_condition, "signal"] = -1
    return df


def apply_position(df):
    df["position"] = 0
    for i in range(1, len(df)):
        if df["signal"].iloc[i] != 0:
            df.iloc[i, df.columns.get_loc("position")] = df["signal"].iloc[i]
        else:
            df.iloc[i, df.columns.get_loc("position")] = df["position"].iloc[i - 1]
    return df


def calculate_pnl(df, step_price: float, lot: float, commission_rate=0.0004):
    trades = []
    entry_price = entry_time = None
    for i in range(1, len(df)):
        prev_pos = df["position"].iloc[i - 1]
        curr_pos = df["position"].iloc[i]
        if prev_pos == 0 and curr_pos != 0:
            entry_price = df["open"].iloc[i]
            entry_time = df.index[i]
        elif prev_pos != 0 and curr_pos != prev_pos:
            exit_price = df["open"].iloc[i]
            exit_time = df.index[i]
            direction = prev_pos
            price_diff = (exit_price - entry_price) * direction
            pnl_raw = price_diff * step_price * lot
            commission = (entry_price + exit_price)* step_price * commission_rate * lot
            pnl_net = pnl_raw - commission
            trades.append({
                "entry_time": entry_time,
                "exit_time": exit_time,
                "side": "long" if direction == 1 else "short",
                "entry_price": entry_price,
                "exit_price": exit_price,
                "pnl_raw": pnl_raw,
                "pnl_net": pnl_net,
            })
            if curr_pos != 0:
                entry_price, entry_time = exit_price, exit_time
            else:
                entry_price = entry_time = None
    return pd.DataFrame(trades)


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


from tabulate import tabulate

def save_markdown_table(trades_df, name: str, path_md="latest_trades.md", path_img="equity_curve.png", max_rows=50):
    if trades_df.empty:
        print("Нет сделок для сохранения.")
        return

    # График equity
    trades_df["equity"] = trades_df["pnl_net"].cumsum()
    trades_df.set_index("exit_time")["equity"].plot(figsize=(10, 4), title="Equity Curve", grid=True)
    plt.xlabel("Дата выхода из сделки")
    plt.ylabel("Накопленная прибыль (₽)")
    plt.tight_layout()
    plt.savefig(path_img)
    plt.close()

    # Копируем и форматируем
    df = trades_df.copy()
    df = df[["entry_time", "exit_time", "side", "entry_price", "exit_price", "pnl_raw", "pnl_net"]]
    df = df.tail(max_rows)
    df.columns = ["Entry Time", "Exit Time", "Side", "Entry Price", "Exit Price", "PnL Raw", "PnL Net"]

    # Форматируем даты
    df["Entry Time"] = df["Entry Time"].dt.strftime("%d.%m.%Y %H:%M")
    df["Exit Time"] = df["Exit Time"].dt.strftime("%d.%m.%Y %H:%M")

    # Markdown-таблица
    markdown_table = tabulate(df.values.tolist(), headers=df.columns, tablefmt="github")

    # Сохраняем
    lines = [
        f"# 📋 Последние сделки по {name}",
        f"![Equity Curve]({path_img})\n",
        markdown_table
    ]

    with open(path_md, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

    print(f"Markdown-таблица сохранена: {path_md}")
    print(f"График equity сохранён: {path_img}")



from tabulate import tabulate  # добавь в начало файла

def save_summary_table(stats_list, filename="summary.md"):
    df = pd.DataFrame(stats_list)
    df.set_index("Актив", inplace=True)

    def format_number(x):
        if isinstance(x, (int, float)):
            return f"{x:>,.2f}".replace(",", " ").replace(".", ",")
        return str(x)

    df_formatted = df.applymap(format_number)

    # Формируем строки markdown вручную
    headers = ["Актив"] + df_formatted.columns.tolist()
    header_line = "| " + " | ".join(headers) + " |"
    separator_line = "| " + " | ".join(["---:" for _ in headers]) + " |"  # выравнивание по правому краю

    lines = [f"# 📊 Сводная таблица по активам\n", header_line, separator_line]

    for idx, row in df_formatted.iterrows():
        line = f"| {idx} | " + " | ".join(row.values) + " |"
        lines.append(line)

    with open(filename, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

    print(f"Сводная таблица сохранена в {filename}")






async def main():
    tickers = {
        "RIM5": "RTS",
        "MMM5": "MOEX",
        "BRM5": "BRENT",
        "GZM5": "GAZ"
    }
    summary_stats = []

    for ticker, name in tickers.items():
        spec = await get_futures_spec(ticker)
        if not spec:
            continue
        async with AsyncSandboxClient(TOKEN) as client:
            candles = []
            async for candle in client.get_all_candles(
                figi=spec["figi"],
                from_=now() - timedelta(days=360),
                interval=CandleInterval.CANDLE_INTERVAL_HOUR,
            ):
                candles.append({
                    "time": candle.time,
                    "open": candle.open.units + candle.open.nano / 1e9,
                    "high": candle.high.units + candle.high.nano / 1e9,
                    "low": candle.low.units + candle.low.nano / 1e9,
                    "close": candle.close.units + candle.close.nano / 1e9,
                    "volume": candle.volume,
                })

            df = pd.DataFrame(candles).set_index("time")
            df = calculate_indicators(df)
            df = add_signals(df)
            df = apply_position(df)

            trades_df = calculate_pnl(df, step_price=spec["step_price"], lot=spec["lot"])
            stats = analyze_trades(trades_df, name)
            save_markdown_table(trades_df, name, f"{name}_trades.md", f"{name}_equity.png")

            if stats:
                summary_stats.append(stats)

    save_summary_table(summary_stats)


if __name__ == "__main__":
    asyncio.run(main())
