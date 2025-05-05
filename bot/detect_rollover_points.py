from pathlib import Path
import pandas as pd
import re

INPUT_DIR = Path("data/candles_filtered")
ROLLING_DAYS = 1

pattern = re.compile(r"(FUTRTS\d{6})_(\d{4})")  # контракт + год

expiration_months = {
    "03": 3,
    "06": 6,
    "09": 9,
    "12": 12,
}

def load_volume_by_day(path: Path) -> pd.DataFrame:
    df = pd.read_parquet(path)
    df["date"] = pd.to_datetime(df["datetime"], utc=True).dt.date
    grouped = df.groupby("date")["volume"].sum().reset_index()
    return grouped

def extract_expiry_from_code(contract_code: str, year: int) -> pd.Timestamp:
    month = expiration_months.get(contract_code[6:8])
    return pd.Timestamp(year=year, month=month, day=1) + pd.offsets.MonthEnd(0)

def find_rollover_day(curr_df, next_df, expiry_date):
    merged = pd.merge(curr_df, next_df, on="date", how="inner", suffixes=("_curr", "_next"))
    merged["next_gt_curr"] = merged["volume_next"] > merged["volume_curr"]
    merged["next_gt_curr_rolling"] = merged["next_gt_curr"].rolling(ROLLING_DAYS).sum()

    for i in range(ROLLING_DAYS - 1, len(merged)):
        if merged.iloc[i]["next_gt_curr_rolling"] == ROLLING_DAYS:
            switch_date = merged.iloc[i]["date"]
            delta_days = (expiry_date.date() - switch_date).days
            return switch_date, delta_days

    return None, None

def main():
    files = sorted(INPUT_DIR.glob("FUTRTS*_180d_candle_interval_hour.parquet"))
    contracts = []
    for f in files:
        match = pattern.match(f.stem)
        if match:
            contract_code = match.group(1)
            year = int(match.group(2))
            contracts.append((contract_code, f, year))

    # Сортировка по (год, месяц экспирации)
    contracts = sorted(
        contracts,
        key=lambda x: (x[2], expiration_months.get(x[0][6:8]))
    )

    rollover_rows = []
    for (curr_code, curr_path, curr_year), (next_code, next_path, next_year) in zip(contracts, contracts[1:]):
        expiry = extract_expiry_from_code(curr_code, curr_year)
        curr_vol = load_volume_by_day(curr_path)
        next_vol = load_volume_by_day(next_path)
        switch_date, delta = find_rollover_day(curr_vol, next_vol, expiry)
        rollover_rows.append({
            "current": curr_code,
            "next": next_code,
            "expiry": expiry.date(),
            "switch_date": switch_date,
            "days_before_expiry": delta
        })

    df_result = pd.DataFrame(rollover_rows)

    if not df_result.empty and "days_before_expiry" in df_result.columns:
        df_result["days_before_expiry"] = pd.to_numeric(df_result["days_before_expiry"], errors="coerce")
        print(df_result.to_string(index=False))
        print("\n📊 Среднее количество дней до экспирации при переключении:",
              df_result["days_before_expiry"].dropna().mean())
    else:
        print("❌ Нет данных о переходах между фьючерсами. Возможно, условия слишком строгие.")

if __name__ == "__main__":
    main()
