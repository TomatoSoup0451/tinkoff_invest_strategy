import os
from typing import Optional

import pandas as pd
from datetime import timedelta
from tinkoff.invest import AsyncClient, CandleInterval
from tinkoff.invest.sandbox.async_client import AsyncSandboxClient
from tinkoff.invest.utils import now
from dotenv import load_dotenv

from core.catalogs.futures_catalog import ARCHIVE_FUTURES

load_dotenv()
TOKEN = os.getenv("TOKEN_SANDBOX")

CANDLE_DIR = "../data/candles"
os.makedirs(CANDLE_DIR, exist_ok=True)


async def get_futures_spec(ticker: str):
    async with AsyncClient(TOKEN) as client:
        response = await client.instruments.futures()
        for fut in response.instruments:
            if fut.ticker == ticker:
                print(f"Найден: {fut.ticker} | FIGI: {fut.figi} | Название: {fut.name}")
                return {
                    "figi": fut.figi,
                    "step_price": (
                        (fut.min_price_increment_amount.units + fut.min_price_increment_amount.nano / 1e9) /
                        (fut.min_price_increment.units + fut.min_price_increment.nano / 1e9)
                    ),
                    "lot": fut.lot
                }
        print(f"Не найдено совпадений для {ticker}")
        return None


async def _download_candles(figi: str, days: int, interval: CandleInterval) -> pd.DataFrame:
    candles = []
    async with AsyncSandboxClient(TOKEN) as client:
        async for candle in client.get_all_candles(
            figi=figi,
            from_=now() - timedelta(days=days),
            interval=interval,
        ):
            candles.append({
                "time": candle.time,
                "open": candle.open.units + candle.open.nano / 1e9,
                "high": candle.high.units + candle.high.nano / 1e9,
                "low": candle.low.units + candle.low.nano / 1e9,
                "close": candle.close.units + candle.close.nano / 1e9,
                "volume": candle.volume,
            })

    if not candles:
        print(f"⚠️ Нет доступных свечей для FIGI {figi} за {days} дней")
        return pd.DataFrame()  # безопасно вернёт пустой DataFrame

    df = pd.DataFrame(candles).set_index("time")
    return df


async def load_candles(
    figi: str,
    days: int = 365,
    interval: CandleInterval = CandleInterval.CANDLE_INTERVAL_HOUR,
    force_refresh: bool = False
) -> pd.DataFrame:
    filename = os.path.join(CANDLE_DIR, f"{figi}_{days}d_{interval.name.lower()}.parquet")

    # Чтение из кэша, если файл существует и не форсирован перезапрос
    if os.path.exists(filename) and not force_refresh:
        print(f"📂 Загрузка свечей из кэша: {filename}")
        return pd.read_parquet(filename)

    print(f"🌐 Загрузка свечей из API для FIGI {figi} на {days} дней")
    df = await _download_candles(figi, days, interval)

    if df.empty:
        print(f"⚠️ Не сохраняем кэш: пустой датафрейм для FIGI {figi}")
        return df

    df.to_parquet(filename)
    print(f"💾 Свечи сохранены в кэш: {filename}")
    return df

async def get_active_futures_spec(ticker: str):
    async with AsyncClient(TOKEN) as client:
        response = await client.instruments.futures()
        for fut in response.instruments:
            if fut.ticker == ticker:
                print(f"🔍 Активный: {fut.ticker} | FIGI: {fut.figi}")
                step_price = ((fut.min_price_increment_amount.units + fut.min_price_increment_amount.nano / 1e9)
                              / (fut.min_price_increment.units + fut.min_price_increment.nano / 1e9))
                print(f"step_price: {step_price}")
                return {
                    "figi": fut.figi,
                    "step_price": (
                        (fut.min_price_increment_amount.units + fut.min_price_increment_amount.nano / 1e9)
                        / (fut.min_price_increment.units + fut.min_price_increment.nano / 1e9)
                    ),
                    "lot": fut.lot,
                    "name": fut.name,
                }
    return None


def get_spec_from_archive(ticker: str) -> Optional[dict]:
    spec = ARCHIVE_FUTURES.get(ticker)
    if spec:
        print(f"📦 Используем архивную спецификацию для {ticker}")
        return {
            "figi": spec["figi"],
            "step_price": spec["step_price"],
            "lot": spec["lot"],
            "name": spec["name"],
        }
    return None