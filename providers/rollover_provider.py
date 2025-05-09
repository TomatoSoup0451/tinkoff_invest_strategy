from providers.parquet import ParquetDataProvider
from typing import List, Tuple
from pathlib import Path
import pandas as pd
import re


class RolloverDataProvider(ParquetDataProvider):
    def __init__(self, data_dir: Path, source_name: str = "ROLL_COMBINED", debug_output: bool = False):
        super().__init__(data_dir)
        self.source_name = source_name
        self.debug_output = debug_output
        self._cached_df = None

    def get_dataframes(self) -> List[Tuple[str, pd.DataFrame]]:
        if self._cached_df is not None:
            return [(self.source_name, self._cached_df)]

        raw = super().get_dataframes()

        # Привязываем код контракта и приводим datetime
        contract_dfs = []
        for source, df in raw:
            match = re.match(r"(FUTRTS\d{6})", source)
            contract_code = match.group(1) if match else "UNKNOWN"
            df = df.copy()
            df["contract_code"] = contract_code
            df["datetime"] = pd.to_datetime(df["datetime"], utc=True)
            contract_dfs.append((contract_code, df))

        if not contract_dfs:
            return []

        # Сортировка по времени старта контракта
        sorted_contracts = sorted(contract_dfs, key=lambda x: x[1]["datetime"].min())

        combined = []
        for i, (code, df) in enumerate(sorted_contracts):
            df = df.copy()
            if i == 0:
                combined.append(df)
                continue

            # Получаем максимум времени у предыдущего контракта
            prev_max_dt = combined[-1]["datetime"].max()
            # Добавляем только свечи строго после (или на 1 минуту позже)
            filtered = df[df["datetime"] > (prev_max_dt - pd.Timedelta(minutes=1))]
            combined.append(filtered)

        all_df = pd.concat(combined).sort_values("datetime").reset_index(drop=True)

        if self.debug_output:
            debug_path = Path("data/debug") / f"{self.source_name}.csv"
            debug_path.parent.mkdir(parents=True, exist_ok=True)
            all_df.to_csv(debug_path, index=False)
            print(f"[DEBUG] Объединённый DataFrame сохранён в {debug_path}")

        self._cached_df = all_df
        return [(self.source_name, all_df)]
