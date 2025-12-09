from typing import List

from pulao.constant import KeyZoneOrigin, KeyZoneOrientation, Direction
from .base_builder import KeyZoneBuilder
from ..keyzone import KeyZone


class ChannelKeyZoneBuilder(KeyZoneBuilder):
    origin_type = KeyZoneOrigin.CHANNEL

    def build(self) -> KeyZone | List[KeyZone] | None:
        raise NotImplementedError()
