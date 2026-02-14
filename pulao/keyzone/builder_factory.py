from pulao.constant import Timeframe, KeyZoneOrigin
from .builder import KeyZoneBuilder
from ..mtc.mtc import MultiTimeframeContext


class KeyZoneFactory:
    _builders = {}

    @classmethod
    def register(cls, builder: type[KeyZoneBuilder]):
        cls._builders[builder.origin_type] = builder

    @classmethod
    def create(cls, mtc: MultiTimeframeContext, origin_type: KeyZoneOrigin, timeframe: Timeframe) -> KeyZoneBuilder:
        if origin_type not in cls._builders:
            raise ValueError(f"Unknown origin_type: {origin_type}")
        return cls._builders[origin_type](mtc, timeframe)
