import asyncio
from bot.backtester import BacktestRunner  # Убедись, что путь правильный

async def main():
    tickers = {
        "RIM5": "RTS",
        "MMM5": "MOEX",
        "BRM5": "BRENT",
        "GZM5": "GAZ"
    }

    runner = BacktestRunner(
        tickers=tickers,
        days=60,              # Глубина выгрузки
        window_days=None,      # или укажи например 60
        stride_days=None,      # или укажи например 30
        save_reports=True
    )

    await runner.run_all()

if __name__ == "__main__":
    asyncio.run(main())
