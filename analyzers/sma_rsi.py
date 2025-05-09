import pandas as pd

from analyzers.base import SignalAnalyzerBase
from core.logger import get_logger

log = get_logger(__name__)


class SMARSIAnalyzer(SignalAnalyzerBase):
    def __init__(self, sma=50, rsi=14, atr=14, rsi_buy=55, rsi_sell=45):
        super().__init__(sma=sma, rsi=rsi, atr=atr, rsi_buy=rsi_buy, rsi_sell=rsi_sell)

    def calculate(self, df: pd.DataFrame) -> pd.DataFrame:
        df["sma"] = df["close"].rolling(self.params["sma"]).mean()
        delta = df["close"].diff()
        gain = delta.where(delta > 0, 0).rolling(self.params["rsi"]).mean()
        loss = -delta.where(delta < 0, 0).rolling(self.params["rsi"]).mean()
        rs = gain / loss
        df["rsi"] = 100 - (100 / (1 + rs))
        tr = pd.concat([
            df["high"] - df["low"],
            (df["high"] - df["close"].shift()).abs(),
            (df["low"] - df["close"].shift()).abs()
        ], axis=1).max(axis=1)
        df["atr"] = tr.rolling(self.params["atr"]).mean()
        return df

    def get_signal(self, row: pd.Series) -> int:
        signal = 0
        if row["close"] > row["sma"] and row["rsi"] > self.params["rsi_buy"]:
            signal = 1
        elif row["close"] < row["sma"] and row["rsi"] < self.params["rsi_sell"]:
            signal = -1

        # Логгируем всё, не только сигналы
        return signal
