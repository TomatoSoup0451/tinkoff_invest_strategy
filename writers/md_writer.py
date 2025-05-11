import os
import pandas as pd
import matplotlib.pyplot as plt
from tabulate import tabulate
from datetime import datetime
from pathlib import Path
from core.logger import get_logger

log = get_logger(__name__)

ROOT_DIR = Path(__file__).resolve().parents[1]
_REPORT_DIR = None


def get_report_dir() -> Path:
    global _REPORT_DIR
    if _REPORT_DIR is None:
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        _REPORT_DIR = ROOT_DIR / "reports" / timestamp
        _REPORT_DIR.mkdir(parents=True, exist_ok=True)
        log.info(f"Создана папка для отчётов: {_REPORT_DIR}")
    return _REPORT_DIR


def format_number(x):
    if isinstance(x, (int, float)):
        return f"{x:,.2f}".replace(",", " ").replace(".", ",")
    return str(x)


def save_markdown_table(
    trades_df,
    name: str,
    max_rows=50,
    start: datetime = None,
    end: datetime = None,
    ticker: str = None
):
    if trades_df.empty:
        return

    report_dir = get_report_dir()

    suffix_parts = []

    if ticker:
        suffix_parts.append(ticker)

    if start and end:
        suffix_parts.append(f"{start.date()}_{end.date()}")

    suffix = "_" + "_".join(suffix_parts) if suffix_parts else ""

    path_md = report_dir / f"{name}{suffix}_trades.md"
    path_img = report_dir / f"{name}{suffix}_equity.png"

    trades_df = trades_df.copy()
    trades_df["equity"] = trades_df["pnl_net"].cumsum()
    trades_df.set_index("exit_time")["equity"].plot(figsize=(10, 4), title="Equity Curve", grid=True)
    plt.xlabel("Дата выхода из сделки")
    plt.ylabel("Накопленная прибыль (₽)")
    plt.tight_layout()
    plt.savefig(path_img)
    plt.close()

    base_cols = ["entry_time", "exit_time", "side", "entry_price", "exit_price", "pnl_raw"]
    optional_cols = ["commission", "slippage", "contract_code", "exit_reason"]
    final_cols = base_cols + [col for col in optional_cols if col in trades_df.columns] + ["pnl_net"]

    df = trades_df[final_cols].tail(max_rows)
    df.columns = [col.replace("_", " ").title() for col in df.columns]

    for col in ["Entry Time", "Exit Time"]:
        df[col] = pd.to_datetime(df[col]).dt.strftime("%d.%m.%Y %H:%M")

    markdown_table = tabulate(df.values.tolist(), headers=df.columns, tablefmt="github")

    lines = [
        f"# 📋 Последние сделки по {name}",
        f"![Equity Curve]({path_img.name})\n",
        markdown_table
    ]

    with open(path_md, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

    log.info(f"💾 Отчёт сохранён: {path_md.name}, график: {path_img.name}")



def save_summary_table(results: list[dict], strategy_id: str):
    report_dir = get_report_dir()
    filename = report_dir / f"summary_{strategy_id}.md"

    df = pd.DataFrame(results)
    if df.empty:
        log.warning(f"❗Пустой результат — не создаю summary для {strategy_id}")
        return

    def calc_trade_stat(row, key: str) -> float:
        trades = getattr(row.get("strategy", None), "trades", pd.DataFrame())
        if not trades.empty and key in trades.columns:
            return trades[key].sum()
        return 0.0

    df["commission_total"] = df.apply(lambda row: calc_trade_stat(row, "commission"), axis=1)
    df["slippage_total"] = df.apply(lambda row: calc_trade_stat(row, "slippage"), axis=1)

    # Удаляем ненужные колонки
    for col in ["source", "strategy", "trades_df"]:
        if col in df.columns:
            df.drop(columns=[col], inplace=True)

    # Приводим даты
    if "start" in df.columns:
        df["start"] = pd.to_datetime(df["start"]).dt.date
    if "end" in df.columns:
        df["end"] = pd.to_datetime(df["end"]).dt.date

    df.set_index("contract", inplace=True)

    df_formatted = df.apply(lambda col: col.map(format_number))

    headers = df_formatted.columns.tolist()
    header_line = "| contract | " + " | ".join(headers) + " |"
    separator_line = "|---" + "|---:" * len(headers) + "|"

    lines = [f"# 📊 Результаты по стратегии `{strategy_id}`", header_line, separator_line]
    for idx, row in df_formatted.iterrows():
        line = f"| {idx} | " + " | ".join(row.values) + " |"
        lines.append(line)

    with open(filename, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

    log.info(f"📄 Summary сохранён: {filename.name}")


def save_strategy_summary(strategy_df: pd.DataFrame):
    report_dir = get_report_dir()
    path = report_dir / "strategy_summary.md"

    df = strategy_df.copy()
    df = df.sort_values("sharpe", ascending=False)

    base_cols = ["pnl_raw", "pnl_net", "drawdown", "sharpe", "winrate", "trades", "profit_factor", "expectancy"]
    headers = ["strategy_id"] + base_cols
    header_line = "| " + " | ".join(headers) + " |"
    separator_line = "|---" + "|---:" * (len(headers) - 1) + "|"

    lines = ["# 🧠 Сравнение стратегий", header_line, separator_line]
    for _, row in df.iterrows():
        line = f"| {row['strategy_id']} | " + " | ".join(format_number(row[col]) for col in base_cols) + " |"
        lines.append(line)

    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

    log.info(f"📊 Сводка по стратегиям сохранена: {path.name}")
