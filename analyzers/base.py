from abc import ABC, abstractmethod
import pandas as pd

class SignalAnalyzerBase(ABC):
    def __init__(self, **params):
        self.params = params

    @abstractmethod
    def calculate(self, df: pd.DataFrame) -> pd.DataFrame:
        """Добавляет индикаторы в датафрейм"""
        pass

    @abstractmethod
    def get_signal(self, row: pd.Series) -> int:
        """На основе строки возвращает сигнал: -1, 0 или 1"""
        pass
