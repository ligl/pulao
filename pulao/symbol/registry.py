from .base import Symbol

class SymbolRegistry:
    _symbols = {}

    @classmethod
    def register(cls, sym: Symbol):
        cls._symbols[sym.code] = sym

    @classmethod
    def get(cls, code: str) -> Symbol:
        return cls._symbols[code]

    @classmethod
    def exists(cls, code: str) -> bool:
        return code in cls._symbols

    @classmethod
    def all(cls):
        return list(cls._symbols.values())

    @classmethod
    def clear(cls):
        cls._symbols.clear()
