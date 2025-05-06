from pathlib import Path
import pandas as pd

# Папки с исходными parquet-файлами
INPUT_DIRS = [
    Path("../data/candles"),
    Path("../data/candles_filtered"),
    Path("../data/candles_joined"),
]

# Папка для сохранения CSV
OUTPUT_DIR = Path("../data/candles_human_csv")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

def convert_datetime_column(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure 'datetime' column is in readable UTC format"""
    df = df.copy()
    if "datetime" in df.columns:
        df["datetime"] = pd.to_datetime(df["datetime"], utc=True)
    return df

def main():
    for input_dir in INPUT_DIRS:
        for file in input_dir.glob("*.parquet"):
            df = pd.read_parquet(file)
            df = convert_datetime_column(df)

            # Сохраняем с тем же именем, но как .csv
            relative_path = file.relative_to(input_dir).with_suffix(".csv")
            output_path = OUTPUT_DIR / relative_path
            output_path.parent.mkdir(parents=True, exist_ok=True)

            df.to_csv(output_path, sep=";", index=False, encoding="utf-8")
            print(f"Saved {output_path} with {len(df)} rows")

if __name__ == "__main__":
    main()
