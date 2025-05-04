import pandas as pd

def calculate_indicators(df):
    df["sma_50"] = df["close"].rolling(window=50).mean()
    delta = df["close"].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
    rs = gain / loss
    df["rsi_14"] = 100 - (100 / (1 + rs))
    tr = pd.concat([
        (df["high"] - df["low"]),
        (df["high"] - df["close"].shift()).abs(),
        (df["low"] - df["close"].shift()).abs(),
    ], axis=1).max(axis=1)
    df["atr_14"] = tr.rolling(window=14).mean()
    return df

def add_signals(df):
    df["signal"] = 0
    long_condition = (
        (df["close"] > df["sma_50"]) &
        (df["rsi_14"] > 55) &
        (df["atr_14"] > df["atr_14"].rolling(window=20).mean())
    )
    short_condition = (
        (df["close"] < df["sma_50"]) &
        (df["rsi_14"] < 45) &
        (df["atr_14"] > df["atr_14"].rolling(window=20).mean())
    )
    df.loc[long_condition, "signal"] = 1
    df.loc[short_condition, "signal"] = -1
    return df