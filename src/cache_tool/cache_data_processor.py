import pandas as pd


def merge_with_deduplication(
    cached_data: pd.DataFrame, chunk: pd.DataFrame
) -> pd.DataFrame:
    """
    将新数据与现有数据合并，并处理首尾重叠的数据点。

    Args:
        cached_data (pd.DataFrame): 已有的缓存数据。
        chunk (pd.DataFrame): 新加载的数据块。

    Returns:
        pd.DataFrame: 合并并去重后的 DataFrame。
    """
    if cached_data.empty:
        return chunk

    if chunk.empty:
        return cached_data

    # 检查首尾是否相等
    if chunk.iloc[0, 0] == cached_data.iloc[-1, 0]:
        # 用新数据的第一行替换旧数据的最后一行
        cached_data.iloc[-1] = chunk.iloc[0]
        # 移除新数据的第一行，准备合并
        chunk = chunk.iloc[1:]

    # 如果新数据块处理后不为空，则进行合并
    if not chunk.empty:
        return pd.concat([cached_data, chunk], ignore_index=True)
    else:
        return cached_data
