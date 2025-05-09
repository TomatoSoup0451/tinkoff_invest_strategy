from pathlib import Path
import pandas as pd
import re

INPUT_DIR = Path("../data/candles_filtered")
OUTPUT_PATH = Path("data/candles_joined/RTS_merged_latest.parquet")
OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)

pattern = re.compile(r"(?P<contract>FUTRTS\d{6})_90d_candle_interval_hour.parquet")

# Сортировка контрактов по дате экспирации
def contract_sort_key(contract_code):
    month = int(contract_code[6:8])
    year = int("20" + contract_code[8:])
    return year * 12 + month

def main():
    # List and sort all relevant files by контракт
    files = []
    for path in INPUT_DIR.glob("FUTRTS*_90d_candle_interval_hour.parquet"):
        match = pattern.match(path.name)
        if not match:
            continue
        contract = match.group("contract")
        files.append((contract, path))

    files.sort(key=lambda x: contract_sort_key(x[0]))

    # Собираем и объединяем
    dfs = []
    for contract, path in files:
        df = pd.read_parquet(path)
        df = df.copy()
        df["ticker"] = contract
        dfs.append(df)

    full_df = pd.concat(dfs).sort_values("datetime").reset_index(drop=True)
    full_df.to_parquet(OUTPUT_PATH, index=False)
    print(f"Saved merged dataset with {len(full_df)} rows to {OUTPUT_PATH}")

if __name__ == "__main__":
    main()