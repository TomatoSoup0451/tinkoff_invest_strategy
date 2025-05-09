from providers.base import DataProvider
from pathlib import Path
import pandas as pd
from typing import List, Tuple
from core.logger import get_logger

log = get_logger(__name__)

class ParquetDataProvider(DataProvider):
    def __init__(self, data_dir: Path):
        self.data_dir = data_dir
        self._cached_data: List[Tuple[str, pd.DataFrame]] = None  # кеш для уже считанных данных

    def get_dataframes(self) -> List[Tuple[str, pd.DataFrame]]:
        # Если данные уже были загружены — возвращаем кеш
        if self._cached_data is not None:
            return self._cached_data

        data = []
        parquet_files = sorted(self.data_dir.glob("*.parquet"))
        for file in parquet_files:
            try:
                df = pd.read_parquet(file)
                df["datetime"] = pd.to_datetime(df["datetime"], utc=True)
                log.info(f"📦 Загружен файл: {file.name}, строк: {len(df)}")
                if not df.empty:
                    data.append((file.name, df))
            except Exception as e:
                log.error(f"❌ Ошибка при чтении {file.name}: {e}")

        self._cached_data = data  # сохраняем кеш
        return data
