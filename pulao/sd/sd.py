from pulao.keyzone.keyzone import KeyZone
from pulao.object import BaseDecorator


@BaseDecorator()
class SupplyDemand:

    def __init__(self, keyzone: KeyZone, strength: float):
        self.key_zone = keyzone
        self.strength = strength
