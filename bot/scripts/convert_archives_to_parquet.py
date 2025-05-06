import os
import zipfile
from pathlib import Path
import pandas as pd
from io import TextIOWrapper

# Constants
RAW_DATA_DIR = Path("../raw_data/historic_candles")
OUTPUT_DIR = Path("../data/candles")

# Columns: id, datetime, open, high, low, close, volume, empty
COLUMNS = ["id", "datetime", "open", "high", "low", "close", "volume", "_"]
DTYPE = {
    "open": "float",
    "high": "float",
    "low": "float",
    "close": "float",
    "volume": "int"
}

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

for zip_path in RAW_DATA_DIR.glob("*.zip"):
    all_minutes = []
    with zipfile.ZipFile(zip_path, 'r') as z:
        for file_name in z.namelist():
            if not file_name.endswith(".csv"):
                continue
            with z.open(file_name) as f:
                df = pd.read_csv(
                    TextIOWrapper(f, encoding="utf-8", newline=""),
                    sep=";",
                    header=None,
                    names=COLUMNS,
                    usecols=[1, 2, 3, 4, 5, 6],
                    dtype=DTYPE,
                    parse_dates=["datetime"]
                )
                all_minutes.append(df)

    if not all_minutes:
        continue

    # Combine all minutes into one DataFrame
    minutes_df = pd.concat(all_minutes).sort_values("datetime")
    minutes_df.set_index("datetime", inplace=True)

    # Resample to hourly candles
    hours_df = minutes_df.resample("1H", label="left", closed="left").agg({
        "open": "first",
        "high": "max",
        "low": "min",
        "close": "last",
        "volume": "sum"
    }).dropna()

    # Reset index and save
    hours_df.reset_index(inplace=True)

    # Generate output filename from zip file name
    base_name = zip_path.stem  # e.g., FUTRTS032200_2020
    output_file = OUTPUT_DIR / f"{base_name}.parquet"
    hours_df.to_parquet(output_file, index=False)
    print(f"Saved {output_file}")
