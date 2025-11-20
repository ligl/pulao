from typing import Any
from pulao.events import Observable
from .trend import Trend
from ..constant import EventType, TrendDirection
from ..swing import SwingManager


class TrendManager(Observable):
    swing_manager: SwingManager

    def __init__(self, swing_manager: SwingManager):
        super().__init__()
        self.swing_manager = swing_manager
        self.swing_manager.subscribe(self._on_swing_changed)

    def _on_swing_changed(self, event: EventType, payload: Any):
        self.detect()

    def detect(self):
        """
        趋势检测识别
        """
        # region 检测算法
        #
        # 1. 趋势定义
        # 1.1 趋势高低点关系定义趋势，至少由4个相临高低点定义
        # 2. 趋势分类
        # 2.1 上涨趋势
        # 2.1.1 高点抬高，低点抬高
        # 2.2 下跌趋势
        # 2.2.1 高点降低，低点降低
        # 2.3 横盘区间
        # 2.3.1 趋势点没有明显高低关系，呈横向震荡区间，视觉上呈收敛三角形或扩散三角形
        #
        # endregion

    def get_trend(self, index:int=None) -> Trend | None:
        """
        获取指定趋势
        :param index: 指定趋势，如果未指定，获取最新的趋势
        :return: Trend or None
        """
        #
        # 获取连续的3个已完成的波段用于确定趋势
        # 从右往左排序（1.2.3...）
        # swing_current：当前波段
        # swing_prev_1：前一波段
        # swing_prev_2: 再往前一波段
        # swing_next_1：后一波段
        # swing_next_2：再往后一波段
        #


        swing_current = self.swing_manager.get_swing(index)
        while True: # 查最近一个已完成的波段，作为趋势的判断边界
            if swing_current is None: # 查完所有波段后，一个完成的都没有，不满足趋势的判定条件
                return None
            if swing_current.is_completed:
                break
            swing_current = self.swing_manager.prev_swing(swing_current.index) # 查前一个波段

        swing_prev_1 = self.swing_manager.prev_swing(swing_current.index)
        if swing_prev_1 is None:
            return None
        swing_prev_2 = self.swing_manager.prev_swing(swing_prev_1.index)
        if swing_prev_2 is None:
            return None

        trend = Trend()
        # 趋势判定
        if swing_prev_2.high_price < swing_prev_1.high_price < swing_current.high_price and swing_prev_2.low_price < swing_prev_1.low_price < swing_current.low_price: # 上涨趋势
            trend.direction = TrendDirection.UP
        elif swing_prev_2.high_price > swing_prev_1.high_price > swing_current.high_price and swing_prev_2.low_price > swing_prev_1.low_price > swing_current.low_price: # 下降趋势:
            trend.direction = TrendDirection.DOWN
        else: # 横向区间
            trend.direction = TrendDirection.RANGE

        trend.start_index = swing_prev_2.index
        trend.end_index = swing_current.index
        trend.start_index_bar = swing_prev_2.start_index_bar
        trend.end_index_bar = swing_current.end_index_bar

        trend.is_completed = True

        trend.high_price = max(swing_current.high_price, swing_prev_1.high_price, swing_prev_2.high_price)
        trend.low_price = min(swing_current.low_price, swing_prev_1.low_price, swing_prev_2.low_price)

        # 查找此趋势的开始位置
        tmp_swing_next_1 = swing_prev_2
        tmp_swing_next_2 = swing_prev_1
        while True:
            tmp_swing_current = self.swing_manager.prev_swing(tmp_swing_next_1.index)
            if tmp_swing_current is None:
                break
            if trend.direction == TrendDirection.UP:
                if tmp_swing_current.high_price < tmp_swing_next_2.high_price and tmp_swing_current.low_price < tmp_swing_next_2.low_price: # 向前延伸
                    trend.start_index = tmp_swing_current.index
                    trend.start_index_bar = tmp_swing_current.start_index_bar
                    trend.low_price = min(trend.low_price, tmp_swing_current.low_price)
                else: # 终结-起始位置已确定
                    trend.start_index = tmp_swing_next_1.index
                    trend.start_index_bar = tmp_swing_next_1.start_index_bar
                    trend.low_price = min(trend.low_price, tmp_swing_next_1.low_price)
                    break
            elif trend.direction == TrendDirection.DOWN:
                if tmp_swing_current.high_price > tmp_swing_next_2.high_price and tmp_swing_current.low_price > tmp_swing_next_2.low_price: # 向前延伸
                    trend.start_index = tmp_swing_current.index
                    trend.start_index_bar = tmp_swing_current.start_index_bar
                    trend.high_price = max(trend.high_price, tmp_swing_current.high_price)
                else: # 终结-起始位置已确定
                    trend.start_index = tmp_swing_next_1.index
                    trend.start_index_bar = tmp_swing_next_1.start_index_bar
                    trend.high_price = max(trend.high_price, tmp_swing_next_1.high_price)
                    break
            else:
                # 当前趋势为横向区间，要判断其开始位置需要往前回溯
                # 往前继续找一个趋势类型，
                # 1. 如果是上涨或下跌，此区间起点确定
                # 2. 如果是区间，一直找，直至不是区间
                # 3. 调整区间边界高低点
                # 当前波段的范围是否有原有区间重叠，有重叠视为区间延续
                overlap = max(tmp_swing_current.low_price, trend.low_price) <= min(tmp_swing_current.high_price, trend.high_price)
                if overlap:
                    trend.start_index = tmp_swing_current.start_index # 调整开始位置
                    trend.start_index_bar = tmp_swing_current.start_index_bar
                    # TODO 区间价格如何调整？
                else:
                    break

            tmp_swing_next_2 = tmp_swing_next_1
            tmp_swing_next_1 = tmp_swing_current

        return trend

    def prev_opposite_trend(self, index:int=None) -> Trend | None:
        """
        前一个与当前趋势相反方向的趋势
        :param index: 指定趋势，如果未指定，获取最新的趋势
        :return: Trend or None
        """


    def prev_same_trend(self, index:int=None):
        """
        前一个与当前趋势相同方向的趋势
        :param index: 指定趋势，如果未指定，获取最新的趋势
        :return: Trend or None
        """

    def get_swing_list(self, trend:Trend):
        return self.swing_manager.get_swing_list(trend.start_index, trend.end_index)

    def add(self, trend: Trend):
        self.notify(EventType.TREND_CHANGED, trend)
