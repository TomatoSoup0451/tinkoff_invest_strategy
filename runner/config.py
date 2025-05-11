from dataclasses import dataclass

@dataclass
class StrategyConfig:
    mode: str = "debug"  # "debug" | "full"

    # Параметры стратегий (будут переопределены в зависимости от режима)
    sma_values: tuple = ()
    rsi_values: tuple = ()
    rsi_buy_thresholds: tuple = ()
    rsi_sell_thresholds: tuple = ()

    atr_period: int = 14
    save_individual_reports: bool = False
    debug_data_provider: bool = False
    max_workers: int = 8

    def __post_init__(self):
        if self.mode == "debug":
            self.sma_values = (20,)
            self.rsi_values = (7,)
            self.rsi_buy_thresholds = (60,)
            self.rsi_sell_thresholds = (45,)
            self.save_individual_reports = True
            self.debug_data_provider = True
            self.max_workers = 16

        elif self.mode == "full":
            self.sma_values = (20, 40, 60, 100)
            self.rsi_values = (7, 14, 21)
            self.rsi_buy_thresholds = (60, 65, 70, 75)
            self.rsi_sell_thresholds = (30, 35, 40, 45)
            self.save_individual_reports = False
            self.debug_data_provider = False
            self.max_workers = 16
