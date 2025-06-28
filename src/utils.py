def _fetch_roa(trade_date: str) -> pd.DataFrame:
    """优先 period 批量；失败返回空 DataFrame"""
    try:
        return safe_query(
            pro.fina_indicator,
            period=_last_quarter(trade_date),
            fields="ts_code,roa"
        )
    except Exception as e:
        logger.warning(f"ROA 拉取失败({e})，全部填 0")
        return pd.DataFrame(columns=["ts_code","roa"])
