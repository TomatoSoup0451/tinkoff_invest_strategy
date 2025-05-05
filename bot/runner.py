import asyncio
from bot.backtester import BacktestRunner  # Убедись, что путь правильный

async def main():
    tickers = {
        "RIM5": "RTS-6.25",
        "RIH5": "RTS-3.25",
        "RIZ4": "RTS-12.24",
    }

    runner = BacktestRunner(
        tickers=tickers,
        days=365,              # Глубина выгрузки
        window_days=None,      # или укажи например 60
        stride_days=None,      # или укажи например 30
        save_reports=True
    )

    await runner.run_all()

if __name__ == "__main__":
    asyncio.run(main())
