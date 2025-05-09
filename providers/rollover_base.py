from pathlib import Path
from typing import List, Tuple
import pandas as pd
import re

from providers.parquet import ParquetDataProvider


class RolloverProviderBase(ParquetDataProvider):
    def __init__(self, data_dir: Path, source_name: str = "ROLL_COMBINED", debug_output: bool = False):
        super().__init__(data_dir)
        self.source_name = source_name
        self.debug_output = debug_output
        self._cached_df = None

    def get_dataframes(self) -> List[Tuple[str, pd.DataFrame]]:
        if self._cached_df is not None:
            return [(self.source_name, self._cached_df)]

        raw = self.load_raw_dataframes()
        sorted_contracts = self.tag_and_sort_contracts(raw)
        combined_df = self.combine_contracts(sorted_contracts)

        if self.debug_output:
            debug_path = Path("../data/debug") / f"{self.source_name}.csv"
            debug_path.parent.mkdir(parents=True, exist_ok=True)
            combined_df.to_csv(debug_path, index=False)
            print(f"[DEBUG] Объединённый DataFrame сохранён в {debug_path}")

        self._cached_df = combined_df
        return [(self.source_name, combined_df)]

    def load_raw_dataframes(self) -> List[Tuple[str, pd.DataFrame]]:
        raise NotImplementedError("Subclasses must implement load_raw_dataframes")

    def tag_and_sort_contracts(self, raw: List[Tuple[str, pd.DataFrame]]) -> List[Tuple[str, pd.DataFrame]]:
        contract_dfs = []
        for source, df in raw:
            contract_code = self._extract_contract_code(source)
            df = df.copy()
            df["contract_code"] = contract_code
            df["datetime"] = pd.to_datetime(df["datetime"], utc=True)
            contract_dfs.append((contract_code, df))

        return sorted(contract_dfs, key=lambda x: x[1]["datetime"].min())

    def combine_contracts(self, sorted_contracts: List[Tuple[str, pd.DataFrame]]) -> pd.DataFrame:
        combined = []
        for i, (code, df) in enumerate(sorted_contracts):
            df = df.copy()
            if i == 0:
                combined.append(df)
                continue

            prev_max_dt = combined[-1]["datetime"].max()
            filtered = df[df["datetime"] > (prev_max_dt - pd.Timedelta(minutes=1))]
            combined.append(filtered)

        return pd.concat(combined).sort_values("datetime").reset_index(drop=True)

    def _extract_contract_code(self, source: str) -> str:
        match = re.match(r"(FUTRTS\d{6})", source)
        return match.group(1) if match else "UNKNOWN"

    def get_cached_df(self):
        return self._cached_df
