import json
import yaml
from pathlib import Path

from .registry import SymbolRegistry
from .future import FutureSymbol
from .crypto_contract import CryptoContractSymbol

class SymbolLoader:

    CLASS_MAP = {
        "future": FutureSymbol,
        "crypto_contract": CryptoContractSymbol
    }

    @classmethod
    def load(cls, file_path: str):
        path = Path(file_path)
        if not path.exists():
            raise FileNotFoundError(f"Symbol file not found: {file_path}")

        # JSON 或 YAML 自动解析
        if path.suffix == ".json":
            data = json.loads(path.read_text(encoding="utf-8"))
        elif path.suffix in [".yaml", ".yml"]:
            data = yaml.safe_load(path.read_text(encoding="utf-8"))
        else:
            raise ValueError("Unsupported file type. Use .json or .yaml")

        for name, config in data.items():
            print(name, config)
            asset_type = config["asset_type"]

            if asset_type not in cls.CLASS_MAP:
                raise ValueError(f"Unsupported asset_type: {asset_type}")

            cls_type = cls.CLASS_MAP[asset_type]
            del config["asset_type"]
            sym = cls_type(**config)
            SymbolRegistry.register(sym)

        return SymbolRegistry.all()
