import pandas as pd
from simulators.base import TradeSimulatorBase
from core.logger import get_logger

log = get_logger(__name__)

class BasicTradeSimulator(TradeSimulatorBase):
    def __init__(self, commission_rate: float = 0.0004, slippage: float = 10):
        self.commission_rate = commission_rate
        self.slippage = slippage

    def simulate(self, df: pd.DataFrame, signals: pd.Series) -> pd.DataFrame:
        in_position = False
        direction = 0
        entry_price = 0
        entry_time = None
        trades = []

        prev_row = None
        prev_contract = None
        seen_contracts = set()

        for i in range(1, len(df)):
            row = df.iloc[i]
            signal = signals.iloc[i - 1]

            current_contract = row.get("contract_code")
            if prev_row is not None:
                prev_contract = prev_row.get("contract_code")

            # --- Обнаружен новый контракт, ранее не встречавшийся ---
            if current_contract and current_contract not in seen_contracts:
                seen_contracts.add(current_contract)

                # Если в позиции — закрываем по close предыдущего бара
                if in_position and prev_contract and prev_contract != current_contract:
                    exit_price = prev_row["close"]
                    exit_time = prev_row["datetime"]

                    gross_pnl = (exit_price - entry_price) * direction
                    commission = (abs(entry_price) + abs(exit_price)) * self.commission_rate
                    slippage_cost = self.slippage * 2
                    net_pnl = gross_pnl - commission - slippage_cost

                    trades.append({
                        "entry_time": entry_time,
                        "exit_time": exit_time,
                        "side": "long" if direction == 1 else "short",
                        "entry_price": entry_price,
                        "exit_price": exit_price,
                        "pnl_raw": gross_pnl,
                        "commission": commission,
                        "slippage": slippage_cost,
                        "pnl_net": net_pnl,
                        "contract_code": prev_contract
                    })

                    in_position = False
                    direction = 0

                    # Открываем новую позицию, если сигнал активен
                    if signal != 0:
                        in_position = True
                        direction = signal
                        entry_price = row["open"]
                        entry_time = row["datetime"]

            # --- Если не в позиции, но появился сигнал — открываем ---
            if not in_position and signal != 0:
                in_position = True
                direction = signal
                entry_price = row["open"]
                entry_time = row["datetime"]

            # --- Если в позиции и сигнал сменился — перезаходим ---
            elif in_position and signal != 0 and signal != direction:
                exit_price = row["open"]
                exit_time = row["datetime"]

                gross_pnl = (exit_price - entry_price) * direction
                commission = (abs(entry_price) + abs(exit_price)) * self.commission_rate
                slippage_cost = self.slippage * 2
                net_pnl = gross_pnl - commission - slippage_cost

                trades.append({
                    "entry_time": entry_time,
                    "exit_time": exit_time,
                    "side": "long" if direction == 1 else "short",
                    "entry_price": entry_price,
                    "exit_price": exit_price,
                    "pnl_raw": gross_pnl,
                    "commission": commission,
                    "slippage": slippage_cost,
                    "pnl_net": net_pnl,
                    "contract_code": current_contract
                })

                # новая позиция
                in_position = True
                direction = signal
                entry_price = row["open"]
                entry_time = row["datetime"]

            prev_row = row

        return pd.DataFrame(trades)
