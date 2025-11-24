import logging
from time import sleep
from typing import Tuple

import pandas as pd

from IPython.display import display

from pulao.constant import SwingPointLevel, SwingPointType, FractalType
from pulao.trend import TrendManager
from pulao.swing import SwingManager
from pulao.bar import SBar, SBarManager, CBarManager, CBar
from vnpy.trader.constant import Exchange, Interval
from vnpy.trader.object import BarData

import polars as pl
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from pulao.logging import init_logging

listener = init_logging(level=logging.DEBUG)

df = pl.read_csv("../dataset/I8888.XDCE_60m.csv", try_parse_dates=True)
df = df.head(100)  # test

sbar_list = []
columns = df.columns

for idx, row in enumerate(df.iter_rows()):
    row_dict = dict(zip(columns, row))
    # datetime,open,close,high,low,volume,money,open_interest,signal
    datetime = row_dict["datetime"]
    open = row_dict["open"]
    close = row_dict["close"]
    high = row_dict["high"]
    low = row_dict["low"]
    volume = row_dict["volume"]
    money = row_dict["money"]
    open_interest = row_dict["open_interest"]

    bar = BarData(
        gateway_name="ctp-test",
        symbol="i8888",
        exchange=Exchange.SHFE,
        interval=Interval.MINUTE,
        datetime=datetime,
        open_price=open,
        close_price=close,
        high_price=high,
        low_price=low,
        volume=volume,
        open_interest=open_interest,
        turnover=money,
    )
    sbar = SBar(index= len(sbar_list), symbol=bar.symbol, exchange=bar.exchange.value, interval=bar.interval.value,datetime=datetime,turnover=money,open_price=open,close_price=close,high_price=high,low_price=low,volume=volume)

    sbar_list.append(sbar)
# 模拟行情数据接收
sbar_manager = SBarManager()
cbar_manager = CBarManager(sbar_manager=sbar_manager)
swing_manager = SwingManager(cbar_manager=cbar_manager)
trend_manager = TrendManager(swing_manager=swing_manager)

# 流入模拟数据
for sbar in sbar_list:
    sbar_manager.append(sbar)
