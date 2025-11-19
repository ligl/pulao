from pulao.key_zone import KeyZone
from pulao.object import BaseDecorator


@BaseDecorator()
class SupplyDemand:
    key_zone: KeyZone
    strength: float

    def __init__(self, key_zone: KeyZone, strength: float):
        self.key_zone = key_zone
        self.strength = strength
