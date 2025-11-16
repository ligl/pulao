"""
tick_backtest.py
基于 vn.py 4.x 的本地 Tick CSV 回测示例（把 Tick CSV 导入 BacktestingEngine）
注意：在你的环境中已安装 vnpy 且 vnpy 版本为 4.x
"""

from datetime import datetime
import pandas as pd
import os

# vn.py imports

from vnpy.trader.constant import Interval, Exchange
from vnpy.trader.object import TickData
from vnpy_ctastrategy.backtesting import BacktestingEngine
from vnpy_ctastrategy.base import BacktestingMode

# 请把下面路径换成你自己的策略类路径
from pulao.strategy import PulaoStrategy
from pulao.utils import enable_console_log

def load_tick_csv_to_ticks(csv_path: str, vt_symbol: str):
    """
    将本地 tick CSV 转换为 vn.py 的 TickData 列表
    CSV 列名建议包含：datetime,last_price,volume,open_interest,bid_price_1,bid_volume_1,ask_price_1,ask_volume_1
    datetime 格式示例： "2025-11-14 09:00:00.123" 或 "2025-11-14 09:00:00"
    vt_symbol 示例："rb99.SHFE" 或 "IF88.CFFEX"（symbol.exchange）
    """
    df = pd.read_csv(csv_path)
    # 尝试解析 datetime 字段
    if "datetime" not in df.columns:
        raise ValueError("CSV 必须包含 datetime 列，格式如 '2025-11-14 09:00:00.123'")
    df["datetime"] = df["datetime"].astype(str).apply(
        lambda x: x if "." in x else x + ".000")  # 统一数据格式
    df["datetime"] = pd.to_datetime(df["datetime"], format="%Y%m%d%H%M%S.%f")

    # parse vt_symbol
    if "." not in vt_symbol:
        raise ValueError("vt_symbol 需形如 'rb99.SHFE'，包含点分隔 exchange")
    symbol, exch = vt_symbol.split(".")
    exchange = Exchange[exch] if exch in Exchange.__members__ else Exchange(Exchange._member_map_.get(exch, exch))

    # df = df.head(1000) # TODO: 取部分数据用于开发测试流程
    ticks = []
    for _, row in df.iterrows():
        t = TickData(
            gateway_name=exch,
            symbol=symbol,
            exchange=exchange,
            datetime=row["datetime"].to_pydatetime(),
            name=symbol,
            volume=int(row.get("volume", 0)),
            low_price=float(row.get("low",0)),
            high_price=float(row.get("high",0)),
            last_price=float(row.get("current", 0)),
            open_interest=int(row.get("position", 0))
        )
        ticks.append(t)
    return ticks


def run_tick_backtest(csv_path: str, vt_symbol: str):
    # 1. 创建回测引擎
    engine = BacktestingEngine()

    # 2. 设置回测参数（注意 mode=BacktestingMode.TICK）
    engine.set_parameters(
        vt_symbol=vt_symbol,
        interval=Interval.TICK,            # 即使是 tick 模式，一些版本仍需传 interval（用 Interval.TICK）
        start=datetime(2025, 1, 1),
        end=datetime(2025, 12, 31),
        rate=0.0001,                       # 手续费率（依据品种调整）
        slippage=1,                        # 滑点（价格跳动单位）
        size=1,                            # 合约乘数
        pricetick=1,                       # 最小变动价位
        capital=100000,                    # 回测初始资金
        mode=BacktestingMode.TICK,         # 关键：使用 Tick 回测模式
    )

    # 3. 添加策略（这里示例使用内置示例策略，替换为你的 Pulao Strategy）
    engine.add_strategy(PulaoStrategy, {})

    # 4. 载入本地 tick 数据
    ticks = load_tick_csv_to_ticks(csv_path, vt_symbol)
    if not ticks:
        raise RuntimeError("没有读取到 Tick 数据，请检查 CSV 格式与路径。")
    print(f"Loaded {len(ticks)} ticks from {csv_path}")

    engine.history_data = ticks

    # 5. 初始化并运行回测
    #engine.load_data()        # 初始化策略（加载参数、初始化状态）
    engine.run_backtesting()        # 运行回测（内部会把 history_data 推给策略的 on_tick / on_bar 回调）

    # 6. 输出回测结果
    df = engine.calculate_result()
    engine.calculate_statistics()
    engine.show_chart()


if __name__ == "__main__":
    enable_console_log()
    # 示例：把路径改成你的 CSV 文件和合约
    csv_path = os.path.abspath("../dataset/I2601.XDCE_tick.csv")
    vt_symbol = "I2601.DCE"   # 注意大小写与 Exchange 对应
    run_tick_backtest(csv_path, vt_symbol)
