from typing import List
from pulao.events import Observable

class KeyZone:
    zone_type: str  # 'support' / 'resistance'
    start_index: int
    end_index: int

    def __init__(self, zone_type: str, start_index: int, end_index: int):
        self.zone_type = zone_type
        self.start_index = start_index
        self.end_index = end_index

class KeyZoneManager(Observable):
    key_zones: List[KeyZone]

    def __init__(self):
        super().__init__()
        self.key_zones = []

    def add(self, key_zone: KeyZone):
        self.key_zones.append(key_zone)
        self.notify("key_zone.created",key_zone)


#------------------------------------------ 具体的KeyZone类 --------------------------------------------------#
class SupportZone(KeyZone):
    zone_type = 'support'

class ResistanceZone(KeyZone):
    zone_type = 'resistance'

class OscillationZone(KeyZone):
    zone_type = 'oscillation'
