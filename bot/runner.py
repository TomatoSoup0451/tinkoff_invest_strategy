import asyncio
from bot.backtester import BacktestRunner  # Убедись, что путь правильный

async def main():
    tickers = {
        "RIH2": "RTS-3.22",
        "RIM2": "RTS-6.22",
        "RIU2": "RTS-9.22",
        "RIZ2": "RTS-12.22",
        "RIH3": "RTS-3.23",
        "RIM3": "RTS-6.23",
        "RIU3": "RTS-9.23",
        "RIZ3": "RTS-12.23",
        "RIH4": "RTS-3.24",
        "RIM4": "RTS-6.24",
        "RIU4": "RTS-9.24",
        "RIZ4": "RTS-12.24",
        "RIH5": "RTS-3.25",
        "RIM5": "RTS-6.25",
        "RIU5": "RTS-9.25",
        "RIZ5": "RTS-12.25",
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
