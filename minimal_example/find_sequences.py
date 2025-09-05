from typing import List, Tuple, Any


def find_consecutive_sequences(data: List[Any]) -> List[Tuple[Any, int, int]]:
    """
    找出列表中所有连续重复的元素及其索引。

    Args:
        data: 任何类型的列表。

    Returns:
        一个元组列表，每个元组包含 (元素值, 起始索引, 结束索引)。
        结束索引是序列中最后一个元素的下一个位置，类似切片或 range 的惯例。
    """
    if not data:
        return []

    results = []
    start_index = 0

    for i in range(1, len(data)):
        if data[i] != data[i - 1]:
            # 连续序列中断，记录上一个序列。结束索引为 i。
            if i > start_index:
                results.append((data[start_index], start_index, i))
            start_index = i

    # 记录最后一个序列。结束索引为 len(data)。
    if len(data) > start_index:
        results.append((data[start_index], start_index, len(data)))

    return results


# --- 示例用法 ---
if __name__ == "__main__":
    my_list = [1, 1, 2, 3, 4, 5, 5, 6, 5, 7, 8, 5, 5, 2, 3, 5, 5, 5]

    sequences = find_consecutive_sequences(my_list)
    print("--- 所有连续序列 (切片索引) ---")
    print(sequences)

    print("\n--- 值为 5 的连续序列 ---")
    fives_sequences = [
        [value, start, end] for value, start, end in sequences if value == 5
    ]
    print(fives_sequences)

    print("\n--- 使用新索引进行切片验证 ---")
    # 验证第一个连续的 5 序列
    value, start, end = sequences[4]
    print(f"原始列表中的序列: {my_list[start:end]}")
    assert my_list[start:end] == [5, 5]

    # 验证最后一个连续的 5 序列
    value, start, end = sequences[-1]
    print(f"原始列表中的序列: {my_list[start:end]}")
    assert my_list[start:end] == [5, 5, 5]
