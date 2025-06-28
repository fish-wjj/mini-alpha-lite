import pandas as pd
def industry_momentum(df: pd.DataFrame) -> pd.Series:
    """示例：把 ts_code 后三位映射申万一级行业指数涨幅"""
    # 假数据：随机行业涨幅
    return pd.Series(0.02, index=df.index)

def size_factor(df: pd.DataFrame) -> pd.Series:
    """自由流通市值——越小越好"""
    return -df["total_mv"].replace(0, df["total_mv"].median()).apply(np.log)
