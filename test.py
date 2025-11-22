from IPython.core.display import HTML
from typing import List, Tuple, Union
from datetime import timedelta, time

from chinese_calendar import holidays

from pulao.constant import SwingPointType, SwingPointLevel, SwingDirection, BaseEnum
from vnpy.trader.constant import Exchange, Direction, Interval
from pulao.bar import SBar,SBarManager
from vnpy.trader.object import BarData
import polars as pl
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from pulao.swing import SwingManager, Swing

from IPython.display import display

from pulao.trend import TrendManager
import pandas as pd
import chinese_calendar as cc
from datetime import datetime, timedelta

def generate_cn_futures_rangebreaks(df, datetime_col="datetime", holidays=None):
    """
    生成国内期货标准 xaxis.rangebreaks
    - df: Polars 或 Pandas DataFrame，必须包含 datetime 列
    - datetime_col: datetime 列名
    - holidays: 可选，列表格式 ['2025-01-01', '2025-01-02'] 等节假日
    返回: Plotly 可用的 rangebreaks 列表
    """
    holidays = holidays or []

    # 转 Pandas datetime
    if isinstance(df, pl.DataFrame):
        df_pd = df.to_pandas()
    else:
        df_pd = df.copy()
    df_pd[datetime_col] = pd.to_datetime(df_pd[datetime_col])

    rangebreaks = []

    # 数据日期范围
    start_date = df_pd[datetime_col].min().date()
    end_date = df_pd[datetime_col].max().date()
    total_days = pd.date_range(start=start_date, end=end_date, freq='D')

    for day in total_days:
        day_str = day.strftime("%Y-%m-%d")

        # 1️⃣ 周末或节假日折叠
        if day.weekday() >= 5 or day_str in holidays:
            rangebreaks.append({"bounds": [day, day + timedelta(days=1)]})
            continue

        # 2️⃣ 白天非交易时间折叠
        # 上午交易 09:00-10:15、10:30-11:30 → 非交易时间 10:15-10:30
        morning_break_start = datetime.combine(day, datetime.strptime("10:15", "%H:%M").time())
        morning_break_end = datetime.combine(day, datetime.strptime("10:30", "%H:%M").time())
        rangebreaks.append({"bounds": [morning_break_start, morning_break_end]})

        # 午休折叠 11:30-13:30
        lunch_start = datetime.combine(day, datetime.strptime("11:30", "%H:%M").time())
        lunch_end = datetime.combine(day, datetime.strptime("13:30", "%H:%M").time())
        rangebreaks.append({"bounds": [lunch_start, lunch_end]})

        # 非交易时间 15:01-21:00
        post_day_start = datetime.combine(day, datetime.strptime("15:01", "%H:%M").time())
        night_pre_start = datetime.combine(day, datetime.strptime("21:00", "%H:%M").time())
        rangebreaks.append({"bounds": [post_day_start, night_pre_start]})

        # 非交易时间 23:00-次日 09:00
        night_end = datetime.combine(day, datetime.strptime("23:00", "%H:%M").time())
        next_day_morning = datetime.combine(day + timedelta(days=1), datetime.strptime("09:00", "%H:%M").time())
        rangebreaks.append({"bounds": [night_end, next_day_morning]})

    return rangebreaks

df = pl.read_csv("../dataset/I8888.XDCE_60m.csv", try_parse_dates=True)
df = df.head(1000)  # test

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
    sbar = SBar(bar)

    #display(bar.to_dict())
    sbar_list.append(sbar)
# 模拟行情数据接收
sbar_manager = SBarManager()
swing_manager = SwingManager(cbar_manager=sbar_manager)
trend_manager = TrendManager(swing_manager)

for sbar in sbar_list:
    sbar_manager.append(sbar)
#
df_pandas = sbar_manager.df_sbar.to_pandas()

df_pandas = df_pandas.head(100)
display(df_pandas)

# 用 Plotly 画 K 线 + 成交量 + 持仓量
# 一个非常小的时间偏移，用来防止边界被包含（1微秒）
EPS = pd.Timedelta(microseconds=1)
# 国内期货交易时段
TRADING_SESSIONS = [
    (time(9, 0),  time(10, 15)),
    (time(10, 30), time(11, 30)),
    (time(13, 30), time(15, 0)),
    (time(21, 0),  time(23, 0)),
]


def _merge_intervals(intervals: List[Tuple[datetime, datetime]]) -> List[Tuple[datetime, datetime]]:
    """合并重叠或相邻区间（闭开区间处理已经通过EPS调整）"""
    if not intervals:
        return []
    intervals = sorted(intervals, key=lambda x: x[0])
    merged = [list(intervals[0])]
    for s, e in intervals[1:]:
        last_s, last_e = merged[-1]
        # 若当前开始 <= 上一区间结束（允许微小连接），合并
        if s <= last_e + pd.Timedelta(microseconds=1):
            merged[-1][1] = max(last_e, e)
        else:
            merged.append([s, e])
    return [(s, e) for s, e in merged]
def generate_cn_futures_rangebreaks_fixed(
    df: Union[pd.DataFrame, pl.DataFrame],
    datetime_col: str = "datetime",
    holidays: List[str] = None
) -> List[dict]:
    """
    生成 Plotly 用的 rangebreaks（已修正边界），规则为国内期货：
      - 日盘：09:00-10:15,10:30-11:30,13:30-15:00
      - 夜盘：21:00-23:00
      - 折叠：10:15-10:30,11:30-13:30,15:00-21:00,23:00-next 09:00
      - 周末/holidays 整天折叠

    返回值：[{ "bounds": ["YYYY-MM-DD HH:MM:SS.ffffff", "YYYY-MM-DD HH:MM:SS.ffffff"] }, ...]
    （可以直接传给 fig.update_xaxes(rangebreaks=...））
    """
    holidays = set(holidays or [])

    # 转 pandas
    if isinstance(df, pl.DataFrame):
        df_pd = df.to_pandas()
    else:
        df_pd = df.copy()

    if datetime_col not in df_pd.columns:
        raise ValueError(f"datetime column '{datetime_col}' not found")

    df_pd[datetime_col] = pd.to_datetime(df_pd[datetime_col])
    if df_pd[datetime_col].isna().all():
        return []

    start_date = df_pd[datetime_col].dt.date.min()
    end_date = df_pd[datetime_col].dt.date.max()

    days = pd.date_range(start=start_date, end=end_date, freq="D").to_pydatetime()

    folded = []  # 临时区间（datetime, datetime）

    for day in days:
        day_date = day.date()
        day_str = day_date.strftime("%Y-%m-%d")
        weekday = day.weekday()

        # 整天折叠：周末或节假日
        if weekday >= 5 or day_str in holidays:
            s = datetime.combine(day_date, time(0, 0))
            e = datetime.combine(day_date + timedelta(days=1), time(0, 0))
            folded.append((s, e))
            continue

        # 生成当天所有交易区间（datetime）
        sessions = [(datetime.combine(day_date, s), datetime.combine(day_date, e)) for s, e in TRADING_SESSIONS]
        sessions.sort()

        # 非交易区间填充：从零点开始，依序填充每个交易段之间的空隙，最后到次日零点。
        # 关键：把非交易区间端点用 EPS 微调，避免把交易段边界点误判为折叠
        cursor = datetime.combine(day_date, time(0, 0))
        for s, e in sessions:
            if s > cursor:
                # 将折叠区间结束微调为 s - EPS（保留恰好在 s 时刻的数据点）
                folded_start = cursor
                folded_end = s - EPS
                if folded_end > folded_start:
                    folded.append((folded_start, folded_end))
            # 将 cursor 移到交易段结束之后（+EPS以避免下一个 folded 包含端点）
            cursor = e + EPS

        # 最后一天尾部（从 cursor 到次日零点）
        day_end = datetime.combine(day_date + timedelta(days=1), time(0, 0))
        if cursor < day_end:
            folded.append((cursor, day_end))

    # 合并区间
    merged = _merge_intervals(folded)

    # 截到数据范围附近以减少 rangebreaks 长度（可选）
    data_start = datetime.combine(start_date, time(0, 0))
    data_end = datetime.combine(end_date + timedelta(days=1), time(0, 0))

    result = []
    for s, e in merged:
        if e <= data_start or s >= data_end:
            continue
        s_clip = max(s, data_start)
        e_clip = min(e, data_end)
        # 使用 ISO 字符串（带空格分隔日期时间，Plotly 可接受）
        result.append({"bounds": [s_clip.isoformat(sep=' '), e_clip.isoformat(sep=' ')]})

    return result

fig = make_subplots(
    rows=2, cols=1,
    shared_xaxes=True,
    row_heights=[0.7, 0.3],
    vertical_spacing=0.03,
    specs=[[{"secondary_y": False}], [{"secondary_y": True}]]
)
# K 线
fig.add_trace(go.Candlestick(
    x=df_pandas['datetime'],
    open=df_pandas['open_price'],
    high=df_pandas['high_price'],
    low=df_pandas['low_price'],
    close=df_pandas['close_price'],
    name='OHLC'
), row=1, col=1)


fig.add_trace(go.Bar(
    x=df_pandas['datetime'],
    y=df_pandas['volume'],
    name='Volume',
), row=2, col=1, secondary_y=False)

# 副图：持仓量折线（右Y轴）
fig.add_trace(go.Scatter(
    x=df_pandas['datetime'],
    y=df_pandas['open_interest'],
    mode='lines',
    name='Open Interest',
    line=dict(color='orange', width=1)
), row=2, col=1, secondary_y=True)

holidays = [d for d in cc.get_holidays(df_pandas["datetime"].min(),df_pandas["datetime"].max())]
# display(holidays)
rangebreaks = generate_cn_futures_rangebreaks_fixed(
    df_pandas,
    datetime_col="datetime",
    holidays=holidays # 可选
)


fig.update_layout(
    title='Pulao Chart',
    height=900,
    hovermode='x unified',    # X轴悬停联动虚线
    xaxis_rangeslider_visible=False,   # 滑块可以放到底部子图
    hoversubplots='axis'
)

fig.update_xaxes(
    rangebreaks=rangebreaks,
    showgrid=False,
    showspikes=True,              # 启用每行 spike
    spikemode="across",           # 横跨子图宽度
    spikesnap="cursor",           # 跟随鼠标
)

fig.update_yaxes(
    showgrid=False,
    showspikes=True,              # 启用每行 spike
    spikemode="across",           # 横跨子图宽度
    spikesnap="cursor",           # 跟随鼠标
)

# 如果你想把 rangeslider 放在成交量下面（推荐）
fig.update_xaxes(rangeslider_visible=True, row=2, col=1)
fig.update_traces(xaxis='x')
fig.show()
# 趋势测试
#trend = trend_manager.get_trend()
#display(trend)
#trend_manager.get_swing_list(trend)

#df = swing_manager.df_cbar.with_row_index("_idx_")
#index = df.filter((pl.col("swing_point_type") != "") & (pl.col("swing_point_level") == 2)).tail(1).select(pl.col("index")).item()
#df.slice(index)
