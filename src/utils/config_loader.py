import os
from functools import lru_cache
from pathlib import Path

import yaml

_CONFIG_DIR = Path(__file__).resolve().parents[2] / "configs"


def _load_yaml(filename: str) -> dict:
    path = _CONFIG_DIR / filename
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")
    with open(path) as f:
        return yaml.safe_load(f)


@lru_cache(maxsize=None)
def load_config() -> dict:
    env = os.getenv("ENV", "prod")
    return {**_load_yaml("base.yaml"), **_load_yaml(f"{env}.yaml")}


@lru_cache(maxsize=None)
def load_table_config(layer: str) -> dict:
    return _load_yaml("tables.yaml")["s3"][layer]
