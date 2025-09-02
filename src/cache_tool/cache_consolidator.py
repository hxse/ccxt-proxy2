import pandas as pd
from pathlib import Path

from .cache_utils import get_sorted_cache_files, get_file_info
from .cache_file_io import write_to_cache, read_cache_file
from .cache_data_processor import merge_with_deduplication


def consolidate_cache(
    cache_dir: Path,
    cache_size: int,
    symbol: str,
    period: str,
    file_type: str = "parquet",
) -> None:
    """
    整理缓存目录中的文件。

    查找连续的、大小小于 cache_size 的缓存文件，将它们分组，然后合并后重新写入。
    """
    if not cache_dir.exists():
        print("❌ 缓存目录不存在，无需整理。")
        return

    sorted_cache_files = get_sorted_cache_files(cache_dir, symbol, period, file_type)
    if not sorted_cache_files:
        print("✅ 缓存目录中没有需要整理的有效文件。")
        return

    print(f"\n--- 开始尝试整理 {symbol} {period} 的缓存文件 ---")

    # 使用二维列表来收集连续的文件块
    files_to_merge = []

    current_group = []
    for i in range(len(sorted_cache_files)):
        current_file = sorted_cache_files[i]
        current_info = get_file_info(current_file.name)
        if not current_info:
            continue

        # 只有当文件大小小于 cache_size 时才考虑将其加入待合并队列
        if current_info["count"] < cache_size:
            # 检查是否与前一个文件连续
            is_continuous = False
            if current_group:
                last_file_info = get_file_info(current_group[-1].name)
                # 检查当前文件的开始时间是否与前一个文件的结束时间相等
                if (
                    last_file_info
                    and current_info["start_time"] == last_file_info["end_time"]
                ):
                    is_continuous = True

            if not current_group or is_continuous:
                current_group.append(current_file)
                print(f"✅ 将文件 {current_file.name} 添加到当前连续组。")
            else:
                # 遇到不连续的文件，保存当前组并开始新的组
                files_to_merge.append(current_group)
                current_group = [current_file]
                print(f"⚠️ 遇到不连续，新开一组，添加文件 {current_file.name}。")
        else:
            # 遇到大于或等于 cache_size 的文件，结束当前组
            if current_group:
                files_to_merge.append(current_group)
                current_group = []

    # 循环结束时，保存最后一个组
    if current_group:
        files_to_merge.append(current_group)

    print("\n--- 开始合并和重新写入缓存文件 ---")
    merged_count = 0

    for group in files_to_merge:
        # 只有当一个组包含多于一个文件时才进行合并
        if len(group) > 1:
            merged_count += 1
            print(f"--- 正在处理第 {merged_count} 个待合并文件块 ---")

            merged_data = pd.DataFrame()

            # 1. 加载并合并所有文件
            for f in group:
                try:
                    data_to_merge = read_cache_file(f, file_type)
                    merged_data = merge_with_deduplication(merged_data, data_to_merge)
                    print(f"📦 已加载并合并文件: {f.name}")
                except Exception as e:
                    print(f"❌ 无法读取文件 {f.name}: {e}")

            # 2. 删除旧文件
            for f in group:
                if f.exists():
                    print(f"🗑️ 删除旧缓存文件: {f.name}")
                    f.unlink()

            # 3. 写入新合并的数据
            write_to_cache(
                symbol, period, merged_data, cache_dir, cache_size, file_type
            )

    if merged_count == 0:
        print("✅ 没有需要合并的文件块，缓存已是最优状态。")

    print("--- 缓存整理完成 ---")
