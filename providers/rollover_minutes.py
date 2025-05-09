from pathlib import Path
from typing import List, Tuple, Optional
import pandas as pd
from providers.rollover_hourly import RolloverHourlyProvider
from core.logger import get_logger

log = get_logger(__name__)


class RolloverMinuteProvider(RolloverHourlyProvider):
    def __init__(self, data_dir: Path, source_name: str = "ROLL_MINUTE_COMBINED", debug_output: bool = False):
        super().__init__(data_dir, source_name, debug_output)
        self._cached_minute_df: Optional[pd.DataFrame] = None

    def load_minute_data(self):
        """Lazy load minute-level data with contract codes."""
        if self._cached_minute_df is not None:
            return self._cached_minute_df

        raw_files = list(self.data_dir.glob("*_candle_interval_minute.parquet"))
        contract_dfs = []

        for path in raw_files:
            df = pd.read_parquet(path)
            df = df.copy()
            df["datetime"] = pd.to_datetime(df["datetime"], utc=True)
            df["contract_code"] = self._extract_contract_code(path.stem)
            contract_dfs.append(df)

        if not contract_dfs:
            self._cached_minute_df = pd.DataFrame()
        else:
            self._cached_minute_df = pd.concat(contract_dfs).sort_values("datetime").reset_index(drop=True)

        if self.debug_output:
            debug_path = Path("data/debug") / f"{self.source_name}_raw_minute.csv"
            debug_path.parent.mkdir(parents=True, exist_ok=True)
            self._cached_minute_df.to_csv(debug_path, index=False)
            log.info(f"[DEBUG] Сырые минутные данные сохранены в {debug_path}")

        return self._cached_minute_df

    def get_minute_slice(self, contract_code: str, start: pd.Timestamp, end: pd.Timestamp) -> pd.DataFrame:
        """Возвращает минутные свечи по указанному контракту и временному диапазону."""
        df = self.load_minute_data()
        mask = (df["contract_code"] == contract_code) & (df["datetime"] >= start) & (df["datetime"] <= end)
        return df.loc[mask].copy()
