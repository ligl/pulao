from typing import List

from pulao.constant import KeyZoneOrigin, Direction, KeyZoneOrientation
from pulao.keyzone.builder.base_builder import KeyZoneBuilder
from pulao.keyzone.keyzone import KeyZone


class TrendKeyZoneBuilder(KeyZoneBuilder):
    origin_type = KeyZoneOrigin.TREND

    def build(self) -> List[KeyZone] | None:
        trends = self.mtc.get_trend_window(5, self.timeframe)

        keyzone_list = []
        for trend in trends or []:
            if trend.is_completed:
                # 波段高低点各建立一个KeyZone

                # 波段高点
                upper, lower, _ = self.get_upper_lower(
                    trend.sbar_end_id if trend.direction == Direction.UP else trend.sbar_start_id,
                    3, Direction.UP)
                high_keyzone = KeyZone(
                    origin_type=self.origin_type,
                    timeframe=self.timeframe,
                    orientation=KeyZoneOrientation.HORIZONTAL,
                    sbar_start_id=trend.sbar_end_id if trend.direction == Direction.UP else trend.sbar_start_id,
                    sbar_end_id=trend.sbar_end_id if trend.direction == Direction.UP else trend.sbar_start_id,
                    upper=upper,
                    lower=lower,
                )
                keyzone_list.append(high_keyzone)

                # 波段低点
                upper, lower, _ = self.get_upper_lower(
                    trend.sbar_start_id if trend.direction == Direction.UP else trend.sbar_end_id,
                    3, Direction.DOWN)
                low_keyzone = KeyZone(
                    origin_type=self.origin_type,
                    timeframe=self.timeframe,
                    orientation=KeyZoneOrientation.HORIZONTAL,
                    sbar_start_id=trend.sbar_end_id if trend.direction == Direction.UP else trend.sbar_start_id,
                    sbar_end_id=trend.sbar_end_id if trend.direction == Direction.UP else trend.sbar_start_id,
                    upper=upper,
                    lower=lower,
                )
                keyzone_list.append(low_keyzone)
            else:
                # 只建立波段起点的KeyZone
                keyzone = KeyZone(
                    origin_type=self.origin_type,
                    timeframe=self.timeframe,
                    orientation=KeyZoneOrientation.HORIZONTAL,
                    sbar_start_id=trend.sbar_start_id,
                    sbar_end_id=trend.sbar_start_id,
                )
                if trend.direction == Direction.DOWN:
                    # 向下波段，在高点建立KeyZone
                    upper, lower, _ = self.get_upper_lower(
                        trend.sbar_start_id,
                        3, Direction.UP)
                else:
                    # 向上波段，在低点建立KeyZone
                    # 波段低点
                    upper, lower, _ = self.get_upper_lower(
                        trend.sbar_start_id,
                        3, Direction.DOWN)
                keyzone.upper = upper
                keyzone.lower = lower
                keyzone_list.append(keyzone)

        return keyzone_list
