from .key_zone import KeyZone, KeyZoneType
from pulao.trend.trend_manager import Trend


class SupportZone(KeyZone):
    @property
    def zone_type(self) -> KeyZoneType:
        return KeyZoneType.SUPPORT

    def update_zone(self, new_trend: "Trend"):
        pass
