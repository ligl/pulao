from typing import List

from pulao.constant import KeyZoneOrigin, KeyZoneOrientation, Direction
from .base_builder import KeyZoneBuilder
from ..keyzone import KeyZone


class SwingKeyZoneBuilder(KeyZoneBuilder):
    origin_type = KeyZoneOrigin.SWING

    def build(self) -> KeyZone | List[KeyZone] | None:
        swings = self.mtc.get_swing_window(5, self.timeframe)

        keyzone_list = []
        for swing in swings or []:
            if swing.is_completed:
                # 波段高低点各建立一个KeyZone

                # 波段高点
                upper, lower,_ = self.get_upper_lower(
                    swing.sbar_end_id if swing.direction == Direction.UP else swing.sbar_start_id,
                    3, Direction.UP)
                high_keyzone = KeyZone(
                    origin_type=self.origin_type,
                    timeframe=self.timeframe,
                    orientation=KeyZoneOrientation.HORIZONTAL,
                    sbar_start_id=swing.sbar_end_id if swing.direction == Direction.UP else swing.sbar_start_id,
                    sbar_end_id=swing.sbar_end_id if swing.direction == Direction.UP else swing.sbar_start_id,
                    upper=upper,
                    lower=lower,
                )
                keyzone_list.append(high_keyzone)

                # 波段低点
                upper, lower,_ = self.get_upper_lower(
                    swing.sbar_start_id if swing.direction == Direction.UP else swing.sbar_end_id,
                    3, Direction.DOWN)
                low_keyzone = KeyZone(
                    origin_type=self.origin_type,
                    timeframe=self.timeframe,
                    orientation=KeyZoneOrientation.HORIZONTAL,
                    sbar_start_id=swing.sbar_start_id if swing.direction == Direction.UP else swing.sbar_end_id,
                    sbar_end_id=swing.sbar_start_id if swing.direction == Direction.UP else swing.sbar_end_id,
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
                    sbar_start_id=swing.sbar_start_id,
                    sbar_end_id=swing.sbar_start_id,
                )
                if swing.direction == Direction.DOWN:
                    # 向下波段，在高点建立KeyZone
                    upper, lower, _ = self.get_upper_lower(
                         swing.sbar_start_id,
                        3, Direction.UP)
                else:
                    # 向上波段，在低点建立KeyZone
                    # 波段低点
                    upper, lower, _ = self.get_upper_lower(
                        swing.sbar_start_id,
                        3, Direction.DOWN)
                keyzone.upper = upper
                keyzone.lower = lower
                keyzone_list.append(keyzone)

        return keyzone_list
