import polars as pl
from pathlib import Path

from .cache_utils import (
    get_sorted_cache_files,
    get_file_info,
    group_continuous_files,
)
from .cache_file_io import write_to_cache, read_cache_file
from .cache_data_processor import merge_with_deduplication
from .cache_utils import (
    find_max_diff_sequence,
    find_cache_size_sequences,
)


def _process_stream(
    files_to_process: list[Path],
    cache_dir: Path,
    cache_size: int,
    symbol: str,
    period: str,
    file_type: str = "parquet",
    reverse: bool = False,
    start_time: int | None = None,  # 调试用
) -> None:
    """
    采用“流式处理”的逻辑来合并小文件。

    核心思想是：
    1. 使用一个单一的内存缓冲区（current_df）来累积数据。
    2. 如果 `reverse` 为 `True`，则逆序读取文件，以实现向前合并。
    3. 每读取一个文件，就将它的数据与缓冲区 `current_df` 合并并去重。
    4. 当缓冲区的数据量达到或超过 `cache_size` 时，立即将缓冲区的数据写入新文件。
    5. `write_to_cache` 函数内部处理分块写入，并返回一个包含新文件的列表。
    6. 函数会读取新文件列表中的最后一个文件（代表剩余数据），将其加载到内存作为新的 `current_df`，并从磁盘中删除该文件。
    7. 每处理完一个文件，就立即将其从磁盘中删除，避免等待。
    """
    current_df = pl.DataFrame()

    # 根据 reverse 参数决定文件遍历顺序
    if reverse:
        files_to_process = files_to_process[::-1]

    for file_path in files_to_process:
        df_to_add = read_cache_file(file_path, file_type)

        if df_to_add.is_empty():
            file_path.unlink()  # 文件为空，直接删除
            continue

        # 根据处理方向调整合并顺序，确保时间序列的正确性
        if reverse:
            # 反向处理时，将新数据放在前面
            current_df = merge_with_deduplication(df_to_add, current_df)
        else:
            # 正常处理时，将新数据放在后面
            current_df = merge_with_deduplication(current_df, df_to_add)

        # 处理完当前文件后，立即删除
        file_path.unlink()

        # 当累积数据达到或超过 cache_size 时，进行处理和写入
        if len(current_df) >= cache_size:
            written_files = write_to_cache(
                symbol,
                period,
                current_df,
                cache_dir,
                cache_size,
                file_type,
                reverse=reverse,
            )

            current_df = pl.DataFrame()
            if written_files:
                last_file = written_files[0] if reverse else written_files[-1]
                last_file_info = get_file_info(last_file.name)
                if last_file_info and last_file_info.get("count", 0) < cache_size:
                    current_df = read_cache_file(last_file, file_type)
                    last_file.unlink()

    # 循环结束后，处理缓冲区中剩余的数据（如果有）
    if not current_df.is_empty():
        write_to_cache(
            symbol,
            period,
            current_df,
            cache_dir,
            cache_size,
            file_type,
            reverse=reverse,
        )


def _get_files_to_process(
    cache_dir: Path,
    cache_size: int,
    symbol: str,
    period: str,
    file_type: str = "parquet",
):
    """
    一个生成器函数，用于按组生成需要处理的文件列表。
    它会找到连续的文件序列，并将其划分为向前合并和向后合并两部分。

    核心思想：
    1. 获取所有缓存文件并按时间分组。
    2. 在每个文件组中，找到由“满文件”（文件行数 == cache_size）组成的最长连续序列。
    3. 以这个最长序列为“核心”，将序列之前的文件分配给“向前合并”处理，将序列之后的文件分配给“向后合并”处理。
    4. 如果没有满文件序列，则所有文件都分配给“向后合并”处理。

    Yields:
        tuple[list[Path], list[Path]]: 一个元组，包含两个列表：
        第一个是需要向前合并的文件列表，第二个是需要向后合并的文件列表。
    """
    # 1. 获取并分组所有缓存文件
    sorted_cache_files = get_sorted_cache_files(cache_dir, symbol, period, file_type)
    if not sorted_cache_files:
        # 如果没有文件，直接返回
        return

    sorted_cache_files_2d = group_continuous_files(sorted_cache_files)

    # 2. 遍历每个文件组并处理
    for all_files in sorted_cache_files_2d:
        cache_size_sequences = find_cache_size_sequences(all_files, cache_size)

        # 3. 根据是否有满文件序列来划分文件
        if not cache_size_sequences:
            # 如果没有找到任何满文件序列，所有文件都应该向后合并
            files_to_process_forward = []
            files_to_process_backward = all_files
        else:
            # 找到由满文件组成的最长连续序列的起始和结束索引
            max_seq_info = find_max_diff_sequence(cache_size_sequences)

            if max_seq_info is None:
                files_to_process_forward = []
                files_to_process_backward = all_files
            else:
                _, max_seq_start, max_seq_end = max_seq_info

                # 将最长序列之前的文件作为向前合并的数组
                files_to_process_forward = all_files[:max_seq_start]
                # 将最长序列之后的文件作为向后合并的数组
                files_to_process_backward = all_files[max_seq_end:]

        # 4. 生成文件列表，供后续处理
        yield files_to_process_forward, files_to_process_backward


def consolidate_cache(
    cache_dir: Path,
    cache_size: int,
    symbol: str,
    period: str,
    file_type: str = "parquet",
    start_time: int | None = None,  # 调试用
) -> None:
    """
    整理缓存目录中的文件。

    该函数使用生成器来迭代需要处理的文件组，从而实现内存高效的处理。
    """
    for files_to_process_forward, files_to_process_backward in _get_files_to_process(
        cache_dir, cache_size, symbol, period, file_type
    ):
        # 对向前部分进行流式处理，并设置 reverse=True 来实现向前合并
        _process_stream(
            files_to_process_forward,
            cache_dir,
            cache_size,
            symbol,
            period,
            file_type,
            reverse=True,
            start_time=start_time,  # 调试用
        )

        # 对向后部分进行流式处理
        _process_stream(
            files_to_process_backward,
            cache_dir,
            cache_size,
            symbol,
            period,
            file_type,
            reverse=False,
            start_time=start_time,  # 调试用
        )


def check_for_overlaps(
    cache_dir: Path,
    cache_size: int,
    symbol: str,
    period: str,
    file_type: str = "parquet",
) -> None:
    """
    检查缓存目录中是否存在文件时间重叠，并处理重叠部分。

    如果发现重叠，将保留最新文件中的数据，并删除旧文件中的重叠部分。
    """
    print(f"\n--- 开始检查 {symbol} {period} 的缓存文件重叠情况 ---")

    sorted_files = get_sorted_cache_files(cache_dir, symbol, period, file_type)
    if len(sorted_files) < 2:
        print("✅ 文件数量不足，无需检查重叠。")
        return

    # 遍历所有文件，将第一个文件的结束时间与后续文件的开始时间进行比较
    for i in range(len(sorted_files) - 1):
        file_a = sorted_files[i]
        file_b = sorted_files[i + 1]

        info_a = get_file_info(file_a.name)
        info_b = get_file_info(file_b.name)

        if not info_a or not info_b:
            continue

        # 检查文件B是否完全包含文件A
        if (
            info_b["start_time"] <= info_a["start_time"]
            and info_b["end_time"] >= info_a["end_time"]
        ):
            print(f"🔄 文件B {file_b.name} 完全包含旧文件A {file_a.name}。删除文件A。")
            file_a.unlink()
            continue

        # 检查文件A是否完全包含文件B
        if (
            info_a["start_time"] <= info_b["start_time"]
            and info_a["end_time"] >= info_b["end_time"]
        ):
            print(f"🔄 文件A {file_a.name} 完全包含新文件B {file_b.name}。删除文件B。")
            file_b.unlink()
            continue

        # 检查是否存在重叠
        # 如果 A 的结束时间 > B 的开始时间，说明有重叠
        if info_a["end_time"] > info_b["start_time"]:
            print(f"⚠️ 发现重叠！文件 {file_a.name} 和 {file_b.name} 存在时间重叠。")
            print(f"   > 文件A时间范围: {info_a['start_time']} - {info_a['end_time']}")
            print(f"   > 文件B时间范围: {info_b['start_time']} - {info_b['end_time']}")

            # 加载文件A的数据
            df_a = read_cache_file(file_a, file_type)
            if df_a.is_empty():
                print(f"❌ 无法读取文件 {file_a.name}，跳过处理。")
                continue

            # 确定重叠时间段的开始点 (取文件B的开始时间，这是新数据的起点)
            overlap_start_time = info_b["start_time"]

            # 从文件A中删除与文件B重叠的部分
            original_len_a = len(df_a)
            # 保留A中时间戳 <= 文件B开始时间的数据
            df_a_new = df_a.filter(pl.col("time") <= overlap_start_time)

            if len(df_a_new) < original_len_a:
                print(
                    f"🔄 正在移除文件 {file_a.name} 中的 {original_len_a - len(df_a_new)} 条重叠数据。"
                )

                # 如果A中所有数据都重叠，则删除文件A
                if df_a_new.is_empty():
                    print(f"🗑️ 文件 {file_a.name} 已被完全覆盖，删除旧文件。")
                    file_a.unlink()
                else:
                    # 删除旧文件A
                    file_a.unlink()
                    print(f"🗑️ 删除旧文件: {file_a.name}")

                    # 使用写缓存函数，它可以处理文件命名和路径
                    write_to_cache(
                        symbol, period, df_a_new, cache_dir, cache_size, file_type
                    )

    print("\n--- 重叠检查和清理完成 ---")
