import yaml, pathlib

CFG_PATH = pathlib.Path(__file__).resolve().parent.parent / "config.yaml"

def load_cfg() -> dict:
    with open(CFG_PATH, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)
