from abc import ABC, abstractmethod

import pandas as pd


class DataProvider(ABC):
    @abstractmethod
    def get_dataframes(self) -> list[tuple[str, pd.DataFrame]]:
        """Возвращает список пар: название (source), DataFrame"""
