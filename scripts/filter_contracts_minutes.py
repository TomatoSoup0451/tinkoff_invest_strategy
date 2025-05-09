from pathlib import Path
import pandas as pd
import re

# Абсолютные пути
BASE_DIR = Path(__file__).resolve().parents[1]
INPUT_DIR = BASE_DIR / "data" / "candles"
OUTPUT_DIR = BASE_DIR / "data" / "candles_filtered"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Глубина выборки
DAYS = 100

# Regex для парсинга имён файлов
pattern = re.compile(r"(?P<contract>FUTRTS\d{6})_(?P<year>\d{4})")

def main():
    files_by_contract = {}
    for path in INPUT_DIR.glob("FUTRTS*.parquet"):
        match = pattern.match(path.stem)
        if not match:
            continue
        contract = match.group("contract")
        year = int(match.group("year"))
        files_by_contract.setdefault(contract, []).append((year, path))

    for contract, entries in files_by_contract.items():
        # Загрузка всех файлов по контракту
        df_list = []
        for _, path in sorted(entries):
            df = pd.read_parquet(path)
            df_list.append(df)

        full_df = pd.concat(df_list)
        full_df["datetime"] = pd.to_datetime(full_df["datetime"], utc=True)
        full_df = full_df.sort_values("datetime")

        # Обрезка по дате
        end_date = full_df["datetime"].max()
        start_date = end_date - pd.Timedelta(days=DAYS)
        trimmed_df = full_df[(full_df["datetime"] >= start_date)].reset_index(drop=True)

        output_filename = f"{contract}_{DAYS}d_candle_interval_minute.parquet"
        trimmed_df.to_parquet(OUTPUT_DIR / output_filename, index=False)
        print(f"✅ Saved {output_filename} with {len(trimmed_df)} rows from {start_date.date()} to {end_date.date()}")

if __name__ == "__main__":
    main()
