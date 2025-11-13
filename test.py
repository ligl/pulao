from datetime import \
    datetime

from vnpy.trader.constant import \
    Exchange
from vnpy.trader.object import BarData

bar = BarData(gateway_name="ctp", symbol="rb2601", exchange=Exchange.SHFE, datetime=datetime.now())
bar.open_price = 100
bar.close_price = 105
bar.high_price = 108
bar.low_price = 98

print(bar.direction)       # 1（阳线）
print(bar.body_ratio)      # 0.416...
print(bar.upper_shadow)    # 3
print(bar.lower_shadow)    # 2
print(bar.shadow_ratio)