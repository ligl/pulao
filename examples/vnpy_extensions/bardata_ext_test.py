from datetime import \
    datetime

from vnpy.trader.constant import \
    Exchange
from vnpy.trader.object import BarData

from pulao.bar import SBar

# 导入bardata扩展

bar = BarData(gateway_name="ctp", symbol="rb2601", exchange=Exchange.SHFE, datetime=datetime.now())
bar.open_price = 100
bar.close_price = 105
bar.high_price = 108
bar.low_price = 98

pulaoBar = SBar(bar) # noqa: SPELLING

# 使用扩展属性
print("BarData 动态扩展属性：")
print(pulaoBar.direction)       # 1（阳线）
print(pulaoBar.body_ratio)      # 0.416...
print(pulaoBar.upper_shadow)    # 3
print(pulaoBar.lower_shadow)    # 2
print(pulaoBar.shadow_ratio)
