import pandas as pd
from simulators.base import TradeSimulatorBase
from core.logger import get_logger
from datetime import timedelta

log = get_logger(__name__)


class RolloverTradeSimulator(TradeSimulatorBase):
    rollover_aware = True

    def __init__(self, commission_rate: float = 0.0004, slippage: float = 10):
        self.commission_rate = commission_rate
        self.slippage = slippage
        self.trades = []

    def simulate(self, hourly_df: pd.DataFrame, signals: pd.Series, minute_df: pd.DataFrame = None) -> pd.DataFrame:
        self.trades = []

        df = hourly_df.copy()
        df["signal"] = signals
        df = df.sort_values("datetime").reset_index(drop=True)

        # Вычисляем момент, когда можно роллироваться с каждого контракта
        rollover_ready = {
            code: group["datetime"].max() - timedelta(hours=24)
            for code, group in df.groupby("contract_code")
        }

        in_position = False
        direction = 0
        entry_price = 0
        entry_time = None

        active_contract = None
        rollover_allowed = False
        seen_contracts = set()
        last_active_row = None

        for i in range(len(df)):
            row = df.iloc[i]
            current_contract = row["contract_code"]
            time = row["datetime"]
            signal = signals.iloc[i - 1] if i > 0 else 0


            # Инициализация контракта
            if active_contract is None:
                active_contract = current_contract
                seen_contracts.add(active_contract)

            # Обновляем "последнюю строку активного контракта"
            if current_contract == active_contract:
                last_active_row = row

            # Проверка на возможность ролловера
            if time >= rollover_ready[active_contract]:
                rollover_allowed = True

            # Если встретили новый контракт
            if current_contract != active_contract:
                if current_contract in seen_contracts:
                    continue  # уже обрабатывали

                if rollover_allowed:
                    # Закрываем позицию по последней свече старого контракта
                    if in_position and last_active_row is not None:
                        self._close_trade(
                            exit_time=last_active_row["datetime"],
                            exit_price=last_active_row["close"],
                            direction=direction,
                            entry_time=entry_time,
                            entry_price=entry_price,
                            contract_code=active_contract,
                            exit_reason="rollover"
                        )
                        in_position = False
                        direction = 0

                    # Переключаемся на новый контракт
                    active_contract = current_contract
                    seen_contracts.add(active_contract)
                    rollover_allowed = False
                    last_active_row = row  # сбрасываем

                    # Переносим позицию, если она была
                    if direction != 0:
                        in_position, direction, entry_price, entry_time = self._open_trade(direction, row)

                    continue
                else:
                    continue  # рано переходить

            # --- Открытие новой позиции ---
            if not in_position and signal != 0:
                in_position, direction, entry_price, entry_time = self._open_trade(signal, row)

            # --- Перезаход в другую сторону ---
            elif in_position and signal != 0 and signal != direction:
                self._close_trade(
                    exit_time=row["datetime"],
                    exit_price=row["open"],
                    direction=direction,
                    entry_time=entry_time,
                    entry_price=entry_price,
                    contract_code=current_contract,
                    exit_reason="signal_change"
                )
                in_position, direction, entry_price, entry_time = self._open_trade(signal, row)

        return pd.DataFrame(self.trades)

    def _open_trade(self, signal, row):
        return True, signal, row["open"], row["datetime"]

    def _close_trade(self, exit_time, exit_price, direction, entry_time, entry_price, contract_code, exit_reason):
        gross_pnl = (exit_price - entry_price) * direction
        commission = (abs(entry_price) + abs(exit_price)) * self.commission_rate
        slippage_cost = self.slippage * 2
        net_pnl = gross_pnl - commission - slippage_cost

        self.trades.append({
            "entry_time": entry_time,
            "exit_time": exit_time,
            "side": "long" if direction == 1 else "short",
            "entry_price": entry_price,
            "exit_price": exit_price,
            "pnl_raw": gross_pnl,
            "commission": commission,
            "slippage": slippage_cost,
            "pnl_net": net_pnl,
            "contract_code": contract_code,
            "exit_reason": exit_reason
        })
