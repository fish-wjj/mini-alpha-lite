from src.factor_model import score
import pandas as pd

def test_score_rank():
    df = pd.DataFrame({
        "ts_code":["a","b","c"],
        "pe_ttm":[5,10,15],
        "pb":[1,2,3],
        "pct_chg_20d":[0.1,0.05,0.02]
    })
    ranked = score(df)
    assert ranked.iloc[0]["ts_code"] == "a"
