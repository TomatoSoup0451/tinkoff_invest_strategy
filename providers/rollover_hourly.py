from pathlib import Path
from typing import List, Tuple
import pandas as pd
import re

from providers.rollover_base import RolloverProviderBase  # ← твой новый базовый класс

class RolloverHourlyProvider(RolloverProviderBase):
    def __init__(self, data_dir: Path, source_name: str = "ROLL_HOURLY_COMBINED", debug_output: bool = False):
        super().__init__(data_dir, source_name, debug_output)

    def load_raw_dataframes(self) -> List[Tuple[str, pd.DataFrame]]:
        """Загружает только часовые parquet-файлы."""
        hour_files = list(self.data_dir.glob("*_candle_interval_hour.parquet"))
        return [(p.stem, pd.read_parquet(p)) for p in hour_files]
