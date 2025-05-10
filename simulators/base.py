from abc import ABC, abstractmethod
import pandas as pd

class TradeSimulatorBase(ABC):
    rollover_aware = False
    @abstractmethod
    def simulate(self, df: pd.DataFrame, signals: pd.Series) -> pd.DataFrame:
        """Возвращает DataFrame сделок"""
        pass
