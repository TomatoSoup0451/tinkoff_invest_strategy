from pathlib import Path
import pandas as pd
import re

# Path to folder with raw parquet files
INPUT_DIR = Path("data/candles")
OUTPUT_DIR = Path("data/candles_filtered")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
DAYS = 90

# Regex to parse filenames like FUTRTS032200_2020.parquet
pattern = re.compile(r"(?P<contract>FUTRTS\d{6})_(?P<year>\d{4})")

# Mapping from contract codes to expiration month
expiration_months = {
    "03": 3,
    "06": 6,
    "09": 9,
    "12": 12,
}

def main():
    # Read all files and group them by contract (e.g., FUTRTS032200)
    files_by_contract = {}
    for path in INPUT_DIR.glob("FUTRTS*.parquet"):
        match = pattern.match(path.stem)
        if not match:
            continue
        contract = match.group("contract")
        year = int(match.group("year"))
        files_by_contract.setdefault(contract, []).append((year, path))

    for contract, entries in files_by_contract.items():
        # Determine expiration date
        expiry_year = max(year for year, _ in entries)
        month_code = contract[6:8]  # e.g., 03 for March
        month = expiration_months.get(month_code)
        if not month:
            continue

        expiry_date = (pd.Timestamp(expiry_year, month, 1) +
                       pd.offsets.MonthEnd(0)).tz_localize("UTC") - pd.Timedelta(days=3)
        start_date = expiry_date - pd.Timedelta(days=DAYS)

        # Load and concatenate all data for this contract
        df_list = []
        for _, path in sorted(entries):
            df = pd.read_parquet(path)
            df_list.append(df)
        full_df = pd.concat(df_list)
        full_df["datetime"] = pd.to_datetime(full_df["datetime"], utc=True)
        full_df = full_df.sort_values("datetime")

        # Filter the 180-day window
        mask = (full_df["datetime"] >= start_date) & (full_df["datetime"] <= expiry_date)
        trimmed_df = full_df.loc[mask].reset_index(drop=True)

        # Save with new naming convention
        output_filename = f"{contract}_{DAYS}d_candle_interval_hour.parquet"
        trimmed_df.to_parquet(OUTPUT_DIR / output_filename, index=False)
        print(f"Saved {output_filename} with {len(trimmed_df)} rows")

if __name__ == "__main__":
    main()