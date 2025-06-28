import yaml, pathlib

CFG_PATH = pathlib.Path(__file__).resolve().parent.parent / "config.yaml"

def load_cfg() -> dict:
    with open(CFG_PATH, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    numeric_keys = {
        "cash": float,
        "core_ratio": float,
        "alpha_ratio": float,
        "num_alpha": int,
        "stop_loss": float,
        "max_drawdown": float,
        "min_amount": float,
        "lot": int,
    }
    for k, typ in numeric_keys.items():
        if k in cfg:
            cfg[k] = typ(cfg[k])
    return cfg
