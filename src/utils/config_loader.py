import yaml
from pathlib import Path
import os

CONFIG_DIR = Path(__file__).resolve().parents[2] / "configs"

def load_yaml(filename: str):
    config_path = CONFIG_DIR / filename

    if not config_path.exists():
        raise FileNotFoundError(f"Configuration file {config_path} not found.")
    
    with open(config_path, "r") as file:
        return yaml.safe_load(file)

def load_config():
    env = os.getenv("ENV", "prod")

    base_config = load_yaml("base.yaml")
    env_config = load_yaml(f"{env}.yaml")

    return {**base_config, **env_config}

def load_table_config(layer: str) -> dict:
    return load_yaml("tables.yaml")["s3"][layer]