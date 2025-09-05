import pandas as pd


def merge_with_deduplication(
    cached_data: pd.DataFrame, chunk: pd.DataFrame
) -> pd.DataFrame:
    """
    将新数据与现有数据合并，并删除重叠的重复数据点。

    Args:
        cached_data (pd.DataFrame): 已有的缓存数据。
        chunk (pd.DataFrame): 新加载的数据块。

    Returns:
        pd.DataFrame: 合并并去重后的 DataFrame。
    """
    # 如果任一 DataFrame 为空，则直接返回另一个
    if cached_data.empty:
        return chunk
    if chunk.empty:
        return cached_data

    # 使用 pd.concat 合并两个 DataFrame
    # ignore_index=True 确保索引连续且不会有重复
    merged_df = pd.concat([cached_data, chunk], ignore_index=True)

    # 对合并后的 DataFrame 进行去重
    # 'subset' 参数指定根据哪些列来判断重复，通常是时间戳列
    # 'keep' 参数指定保留哪个重复项：'first' (第一个) 或 'last' (最后一个)
    # 对于你的场景，由于新数据更重要，'last' 是一个合适的选择，它会保留新数据中的那一行。
    # 这里假设你的时间戳列名为 'time'，请根据实际情况修改
    deduplicated_df = merged_df.drop_duplicates(subset=["time"], keep="last")
    deduplicated_df = deduplicated_df.reset_index(drop=True)

    return deduplicated_df
