import os

import yaml

CONFIG_ROOT = os.environ.get("CONFIG_ROOT", "/opt/project/config")

REQUIRED = {
    "bronze": ["name", "source", "target"],
    "silver": ["name", "source", "columns", "primary_key", "cdc", "writer", "target"],
}


def load_config(layer: str, name: str) -> dict:
    path = os.path.join(CONFIG_ROOT, layer, f"{name}.yaml")
    with open(path, encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    missing = [k for k in REQUIRED[layer] if k not in cfg]
    if missing:
        raise ValueError(f"Config {path} thiếu các khóa: {missing}")
    return cfg