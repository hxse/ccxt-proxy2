from typing import List, Tuple, Any


def find_max_diff_sequence(
    sequences: List[Tuple[Any, int, int]],
) -> Tuple[Any, int, int] | None:
    """
    找出列表中 end - start 差值最大的元组。

    Args:
        sequences: 一个元组列表，每个元组的格式为 (值, 起始索引, 结束索引)。

    Returns:
        差值最大的元组。如果列表为空，则返回 None。
    """
    if not sequences:
        return None

    # 使用 max 函数和生成器表达式，以 end - start 作为比较依据
    return max(sequences, key=lambda seq: seq[2] - seq[1])


# --- 示例用法 ---
if __name__ == "__main__":
    my_sequences = [(5, 0, 2), (5, 3, 6), (5, 4, 9)]

    result = find_max_diff_sequence(my_sequences)

    print(f"原始列表: {my_sequences}")
    print(f"end - start 差值最大的元组是: {result}")

    # 验证另一个示例
    my_sequences_2 = [("a", 10, 15), ("b", 20, 33), ("c", 50, 60)]
    result_2 = find_max_diff_sequence(my_sequences_2)

    print(f"\n原始列表: {my_sequences_2}")
    print(f"end - start 差值最大的元组是: {result_2}")

    # 验证空列表
    result_3 = find_max_diff_sequence([])
    print(f"\n空列表结果: {result_3}")
