import os

import pandas as pd
import matplotlib.pyplot as plt
from tabulate import tabulate

def save_markdown_table(trades_df, name: str, report_dir: str, max_rows=50):
    if trades_df.empty:
        print("Нет сделок для сохранения.")
        return

    path_md = os.path.join(report_dir, f"{name}_trades.md")
    path_img = os.path.join(report_dir, f"{name}_equity.png")

    trades_df["equity"] = trades_df["pnl_net"].cumsum()
    trades_df.set_index("exit_time")["equity"].plot(figsize=(10, 4), title="Equity Curve", grid=True)
    plt.xlabel("Дата выхода из сделки")
    plt.ylabel("Накопленная прибыль (₽)")
    plt.tight_layout()
    plt.savefig(path_img)
    plt.close()

    df = trades_df.copy()
    df = df[["entry_time", "exit_time", "side", "entry_price", "exit_price", "pnl_raw", "pnl_net"]]
    df = df.tail(max_rows)
    df.columns = ["Entry Time", "Exit Time", "Side", "Entry Price", "Exit Price", "PnL Raw", "PnL Net"]
    df["Entry Time"] = df["Entry Time"].dt.strftime("%d.%m.%Y %H:%M")
    df["Exit Time"] = df["Exit Time"].dt.strftime("%d.%m.%Y %H:%M")
    markdown_table = tabulate(df.values.tolist(), headers=df.columns, tablefmt="github")

    lines = [
        f"# 📋 Последние сделки по {name}",
        f"![Equity Curve]({os.path.basename(path_img)})\n",
        markdown_table
    ]

    with open(path_md, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

def save_summary_table(stats_list, report_dir: str):
    filename = os.path.join(report_dir, "summary.md")
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

    # ✅ Современно, без applymap
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
