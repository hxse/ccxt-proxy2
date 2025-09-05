from typing import List, Tuple
import math


def get_chunk_slices(
    total_rows: int, cache_size: int, forward: bool
) -> List[Tuple[int, int]]:
    """
    计算正向或反向写入时每个文件块的切片范围。

    Args:
        total_rows (int): 总数据行数。
        cache_size (int): 每个缓存文件存储的数据行数。
        forward (bool): 写入方向。True 为正向，False 为反向。

    Returns:
        List[Tuple[int, int]]: 包含每个文件块切片范围 (start_index, end_index) 的列表。
    """
    if total_rows <= 0 or cache_size <= 0:
        return []

    if cache_size == 1:
        # 特殊处理 cache_size=1 的情况
        slices = [(i, i + 1) for i in range(total_rows)]
        # 对于正向和反向，切片相同，无需去除任何切片
        return slices

    if forward:
        step = cache_size - 1
        slices = [
            (start, min(start + cache_size, total_rows))
            for start in range(0, total_rows, step)
        ]
        # 去除最后一个长度为1的切片（当切片数大于1时）
        if len(slices) > 1 and slices[-1][1] - slices[-1][0] == 1:
            slices = slices[:-1]
        return slices
    else:
        step = cache_size - 1
        first_chunk_size = (total_rows - 1) % step + 1

        # 确定剩余切片的数量
        remaining_rows = total_rows - first_chunk_size
        num_remaining_chunks = remaining_rows // step
        if remaining_rows % step > 0:
            num_remaining_chunks += 1

        # 构建第一个切片
        first_slice = [(0, first_chunk_size)]

        # 构建剩余切片
        remaining_slices = [
            (
                first_chunk_size - 1 + i * step,
                min(first_chunk_size - 1 + i * step + cache_size, total_rows),
            )
            for i in range(num_remaining_chunks)
        ]

        # 合并切片
        slices = first_slice + remaining_slices

        # 去除第一个长度为1的切片（当切片数大于1时）
        if len(slices) > 1 and slices[0][1] - slices[0][0] == 1:
            slices = slices[1:]

        return slices


# --- 在 __main__ 下进行验证 ---
if __name__ == "__main__":
    test_params = [
        (3, 1),
        (3, 5),
        (3, 3),
        (5, 3),
        (5, 4),
        (5, 2),
        (10, 4),
    ]

    for total_rows, cache_size in test_params:
        print(f"\n==========================================")
        print(f"测试参数: total_rows = {total_rows}, cache_size = {cache_size}")
        print(f"==========================================")
        natural_numbers = list(range(1, total_rows + 1))

        # --- 正向验证 ---
        print("\n--- 正向写入验证 ---")
        forward_slices = get_chunk_slices(total_rows, cache_size, forward=True)

        for start, end in forward_slices:
            chunk = natural_numbers[start:end]
            print(f"数量: {end - start} 切片: {start}-{end} -> {chunk}")

        # --- 反向验证 ---
        print("\n--- 反向写入验证 ---")
        reverse_slices = get_chunk_slices(total_rows, cache_size, forward=False)

        for start, end in reverse_slices:
            chunk = natural_numbers[start:end]
            print(f"数量: {end - start} 切片: {start}-{end} -> {chunk}")
