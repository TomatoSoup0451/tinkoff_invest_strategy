import os
import pandas as pd
import matplotlib.pyplot as plt
from tabulate import tabulate
from datetime import datetime
from pathlib import Path
from core.logger import get_logger
# глобальное хранилище для времени запуска
_REPORT_DIR = None

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


def save_markdown_table(trades_df, name: str, max_rows=50):
    if trades_df.empty:
        return

    report_dir = get_report_dir()
    path_md = report_dir / f"{name}_trades.md"
    path_img = report_dir / f"{name}_equity.png"

    trades_df = trades_df.copy()
    trades_df["equity"] = trades_df["pnl_net"].cumsum()
    trades_df.set_index("exit_time")["equity"].plot(figsize=(10, 4), title="Equity Curve", grid=True)
    plt.xlabel("Дата выхода из сделки")
    plt.ylabel("Накопленная прибыль (₽)")
    plt.tight_layout()
    plt.savefig(path_img)
    plt.close()

    df = trades_df[["entry_time", "exit_time", "side", "entry_price", "exit_price", "pnl_raw", "pnl_net"]].tail(max_rows)
    df.columns = ["Entry Time", "Exit Time", "Side", "Entry Price", "Exit Price", "PnL Raw", "PnL Net"]
    df["Entry Time"] = df["Entry Time"].dt.strftime("%d.%m.%Y %H:%M")
    df["Exit Time"] = df["Exit Time"].dt.strftime("%d.%m.%Y %H:%M")
    markdown_table = tabulate(df.values.tolist(), headers=df.columns, tablefmt="github")

    lines = [
        f"# 📋 Последние сделки по {name}",
        f"![Equity Curve]({path_img.name})\n",
        markdown_table
    ]

    with open(path_md, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

    log.info(f"💾 Отчёт сохранён: {path_md.name}, график: {path_img.name}")


def save_summary_table(stats_list):
    report_dir = get_report_dir()
    filename = report_dir / "summary.md"
    df = pd.DataFrame(stats_list)
    for col in ["source", "strategy"]:
        if col in df.columns:
            df.drop(columns=[col], inplace=True)
    if "start" in df.columns:
        df["start"] = pd.to_datetime(df["start"]).dt.date
    if "end" in df.columns:
        df["end"] = pd.to_datetime(df["end"]).dt.date
    df.set_index("Актив", inplace=True)

    def format_number(x):
        if isinstance(x, (int, float)):
            return f"{x:>,.2f}".replace(",", " ").replace(".", ",")
        return str(x)

    df_formatted = df.apply(lambda col: col.map(format_number))

    headers = ["Актив"] + df_formatted.columns.tolist()
    header_line = "| " + " | ".join(headers) + " |"
    separator_line = "| " + " | ".join(["---:" for _ in headers]) + " |"

    lines = [f"# 📊 Сводная таблица по активам\n", header_line, separator_line]
    for idx, row in df_formatted.iterrows():
        line = f"| {idx} | " + " | ".join(row.values) + " |"
        lines.append(line)

    with open(filename, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

    log.info(f"📊 Сводная таблица сохранена: {filename.name}")
