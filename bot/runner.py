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

load_dotenv()
TOKEN = os.getenv("TOKEN_SANDBOX")  # или боевой

# === Индикаторы ===

async def get_futures_spec(ticker: str):
    async with AsyncClient(TOKEN) as client:
        response = await client.instruments.futures()
        for fut in response.instruments:
            if fut.ticker == ticker:
                print(f"Найден: {fut.ticker} | FIGI: {fut.figi} | Название: {fut.name}")
                return {
                    "figi": fut.figi,
                    "step_price": fut.min_price_increment_amount.units + fut.min_price_increment_amount.nano / 1e9,
                    "lot": fut.basic_asset_size.units + fut.basic_asset_size.nano / 1e9 or fut.lot
                }
        print(f"Не найдено совпадений для {ticker}")
        return None


async def run_backtest_for_figi(figi: str, name: str):
    async with AsyncSandboxClient(TOKEN) as client:
        candles = []
        async for candle in client.get_all_candles(
            figi=figi,
            from_=now() - timedelta(days=90),
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

        df = pd.DataFrame(candles)
        df.set_index("time", inplace=True)
        df = calculate_indicators(df)
        df = add_signals(df)
        df = apply_position(df)
        trades_df = calculate_pnl(df)
        analyze_trades(trades_df)
        save_markdown_table(trades_df, name, path_md=f"{name}_trades.md", path_img=f"{name}_equity.png")

        print(f"🧾 {name} — готово.\n")

def save_summary_table(stats_list, filename="summary.md"):
    import pandas as pd

    df = pd.DataFrame(stats_list)
    df.set_index("Актив", inplace=True)

    with open(filename, "w", encoding="utf-8") as f:
        f.write("# 📊 Сводная таблица по активам\n\n")
        f.write(df.to_markdown(tablefmt="github"))

    print(f"Сводная таблица сохранена в {filename}")



def save_markdown_table(trades_df, name: str, path_md="latest_trades.md", path_img="equity_curve.png", max_rows=20):
    if trades_df.empty:
        print("Нет сделок для сохранения.")
        return

    # Сохраняем график
    trades_df["equity"] = trades_df["pnl_net"].cumsum()
    trades_df.set_index("exit_time")["equity"].plot(figsize=(10, 4), title="Equity Curve", grid=True)
    plt.xlabel("Дата выхода из сделки")
    plt.ylabel("Накопленная прибыль (₽)")
    plt.tight_layout()
    plt.savefig(path_img)
    plt.close()

    # Markdown-таблица
    df = trades_df.copy()
    df = df[["entry_time", "exit_time", "side", "entry_price", "exit_price", "pnl_raw", "pnl_net"]]
    df = df.tail(max_rows)

    lines = []
    lines.append(f"# 📋 Последние сделки по {name}\n")
    lines.append(f"![Equity Curve]({path_img})\n")
    lines.append("| Entry Time       | Exit Time        | Side  | Entry Price | Exit Price | PnL Raw | PnL Net |")
    lines.append("|------------------|------------------|-------|-------------|------------|---------|---------|")

    for _, row in df.iterrows():
        lines.append(
            f"| {row['entry_time']:%Y-%m-%d %H:%M} "
            f"| {row['exit_time']:%Y-%m-%d %H:%M} "
            f"| {row['side']:<5} "
            f"| {row['entry_price']:>11.1f} "
            f"| {row['exit_price']:>10.1f} "
            f"| {row['pnl_raw']:>7.1f} "
            f"| {row['pnl_net']:>7.1f} |"
        )

    with open(path_md, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

    print(f"Markdown-таблица сохранена: {path_md}")
    print(f"График equity сохранён: {path_img}")


def analyze_trades(trades_df, name: str):
    if trades_df.empty:
        print("Нет сделок для анализа.")
        return

    total_trades = len(trades_df)
    wins = trades_df[trades_df["pnl_net"] > 0]
    losses = trades_df[trades_df["pnl_net"] <= 0]
    winrate = len(wins) / total_trades * 100
    avg_win = wins["pnl_net"].mean()
    avg_loss = losses["pnl_net"].mean()
    max_drawdown = trades_df["pnl_net"].cumsum().cummax() - trades_df["pnl_net"].cumsum()

    print(f"\n📊 Статистика по {name}:")
    print(f"Всего сделок: {total_trades}")
    print(f"Winrate: {winrate:.2f}%")
    print(f"Средняя прибыль: {avg_win:.2f} ₽")
    print(f"Средний убыток: {avg_loss:.2f} ₽")
    print(f"Максимальная просадка: {max_drawdown.max():.2f} ₽")
    print(f"Итоговая чистая прибыль: {trades_df['pnl_net'].sum():.2f} ₽")

    return {
        "Актив": name,
        "Всего сделок": total_trades,
        "Winrate": round(winrate, 2),
        "Средняя прибыль": round(avg_win, 2),
        "Средний убыток": round(avg_loss, 2),
        "Макс. просадка": round(max_drawdown.max(), 2),
        "Итоговая прибыль": round(trades_df["pnl_net"].sum(), 2),
    }




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

# === Получение FIGI ===

def add_signals(df):
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

    df["signal"] = 0
    df.loc[long_condition, "signal"] = 1
    df.loc[short_condition, "signal"] = -1

    return df

async def find_figi(ticker: str):
    async with AsyncClient(TOKEN) as client:
        response = await client.instruments.futures()
        for future in response.instruments:
            if future.ticker == ticker:
                print(f"Найден: {future.ticker} | FIGI: {future.figi} | Название: {future.name}")
                return future.figi
        print("Не найдено совпадений.")
        return None

# === Расчёт позиции по сигналам ===

def apply_position(df):
    df["position"] = 0
    for i in range(1, len(df)):
        if df["signal"].iloc[i] != 0:
            df.iloc[i, df.columns.get_loc("position")] = df["signal"].iloc[i]
        else:
            df.iloc[i, df.columns.get_loc("position")] = df["position"].iloc[i - 1]
    return df

def calculate_pnl(df, commission_per_trade=0.0004):
    trades = []
    entry_price = None
    entry_time = None

    for i in range(1, len(df)):
        prev_pos = df["position"].iloc[i - 1]
        curr_pos = df["position"].iloc[i]

        # Новый вход
        if prev_pos == 0 and curr_pos != 0:
            entry_price = df["open"].iloc[i]
            entry_time = df.index[i]

        # Выход или смена направления
        elif prev_pos != 0 and curr_pos != prev_pos:
            exit_price = df["open"].iloc[i]
            exit_time = df.index[i]

            pnl_raw = (exit_price - entry_price) * prev_pos
            commission = (entry_price + exit_price) * commission_per_trade
            pnl_net = pnl_raw - commission

            trades.append({
                "entry_time": entry_time,
                "exit_time": exit_time,
                "side": "long" if prev_pos == 1 else "short",
                "entry_price": entry_price,
                "exit_price": exit_price,
                "pnl_raw": pnl_raw,
                "pnl_net": pnl_net,
            })

            # Сразу новая позиция?
            if curr_pos != 0:
                entry_price = exit_price
                entry_time = exit_time
            else:
                entry_price = None
                entry_time = None

    return pd.DataFrame(trades)

# === Основной код ===

async def main():
    tickers = {
        "RIM5": "RTS",
        "MMM5": "MOEX",
        "BRM5": "BRENT",
        "GZM5": "GAZ"
    }

    summary_stats = []

    for ticker, name in tickers.items():
        figi = await find_figi(ticker)
        if figi:
            async with AsyncSandboxClient(TOKEN) as client:
                candles = []
                async for candle in client.get_all_candles(
                    figi=figi,
                    from_=now() - timedelta(days=90),
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

                df = pd.DataFrame(candles)
                df.set_index("time", inplace=True)
                df = calculate_indicators(df)
                df = add_signals(df)
                df = apply_position(df)
                trades_df = calculate_pnl(df)
                stats = analyze_trades(trades_df, name=name)
                save_markdown_table(trades_df, path_md=f"{name}_trades.md", path_img=f"{name}_equity.png", name=name)

                if stats:
                    summary_stats.append(stats)
        else:
            print(f"FIGI не найдено для {ticker}")

    save_summary_table(summary_stats, filename="summary.md")



if __name__ == "__main__":
    asyncio.run(main())
