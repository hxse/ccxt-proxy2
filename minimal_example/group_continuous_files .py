from pathlib import Path
from typing import List, Dict, Tuple
from datetime import datetime, timezone
import shutil
import pytest

# ================
# 文件: src/cache_tool/cache_utils.py (已合并到此文件)
# ================


def parse_timestamp_string(ts_str: str) -> int:
    dt = datetime.strptime(ts_str, "%Y%m%dT%H%M%SZ")
    return int(dt.replace(tzinfo=timezone.utc).timestamp() * 1000)


def get_file_info(filename: str) -> dict | None:
    try:
        parts = Path(filename).stem.split(" ")
        symbol, period, start_str, end_str, count = (
            parts[0],
            parts[1],
            parts[2],
            parts[3],
            int(parts[4]),
        )
        return {
            "symbol": symbol,
            "period": period,
            "count": count,
            "start_time": parse_timestamp_string(start_str),
            "end_time": parse_timestamp_string(end_str),
        }
    except (ValueError, IndexError):
        return None


def sanitize_symbol(symbol: str) -> str:
    translator = str.maketrans("/:", "__")
    return symbol.translate(translator)


def get_sorted_cache_files(
    cache_dir: Path, symbol: str, period: str, file_type: str = "csv"
) -> list[Path]:
    if not cache_dir.exists():
        return []

    all_files = [f for f in cache_dir.iterdir() if f.suffix == f".{file_type}"]
    sanitized_symbol = sanitize_symbol(symbol)

    files_with_info = []
    for f in all_files:
        info = get_file_info(f.name)
        if info and info["symbol"] == sanitized_symbol and info["period"] == period:
            files_with_info.append((f, info))

    sorted_files = sorted(files_with_info, key=lambda x: x[1]["start_time"])
    return [f for f, info in sorted_files]


# ================
# 文件: minimal_example/calc.py
# ================


def group_continuous_files(sorted_files: List[Path]) -> List[List[Path]]:
    if not sorted_files:
        return []

    file_groups = []

    for file in sorted_files:
        info = get_file_info(file.name)
        if not info:
            continue

        is_new_group = False
        if not file_groups:
            is_new_group = True
        else:
            last_group = file_groups[-1]
            last_file_in_group = last_group[-1]
            last_info = get_file_info(last_file_in_group.name)

            if not last_info or last_info["end_time"] != info["start_time"]:
                is_new_group = True

        if is_new_group:
            file_groups.append([file])
        else:
            file_groups[-1].append(file)

    return file_groups


# --- 最小化示例验证 ---
if __name__ == "__main__":
    mock_dir = Path("mock_cache_dir")

    if mock_dir.exists():
        shutil.rmtree(mock_dir)
    mock_dir.mkdir(exist_ok=True)

    # 使用你提供的正确文件名格式
    file_names = [
        "BTC_USDT 15m 20230101T000000Z 20230101T001500Z 100.csv",
        "BTC_USDT 15m 20230101T001500Z 20230101T003000Z 100.csv",
        "BTC_USDT 15m 20230101T003000Z 20230101T004500Z 100.csv",
        # 间断
        "BTC_USDT 15m 20230101T010000Z 20230101T011500Z 100.csv",
        "BTC_USDT 15m 20230101T011500Z 20230101T013000Z 100.csv",
        # 间断
        "BTC_USDT 15m 20230101T020000Z 20230101T021500Z 100.csv",
        "BTC_USDT 15m 20230101T021500Z 20230101T023000Z 100.csv",
        "BTC_USDT 15m 20230101T023000Z 20230101T024500Z 100.csv",
    ]

    for name in file_names:
        (mock_dir / name).touch()

    all_files = get_sorted_cache_files(mock_dir, "BTC/USDT", "15m", file_type="csv")

    print("排序后的文件列表:")
    for f in all_files:
        print(f.name)

    grouped_files = group_continuous_files(all_files)

    print("\n分组结果:")
    for group in grouped_files:
        print([f.name for f in group])

    assert len(grouped_files) == 3
    assert len(grouped_files[0]) == 3
    assert len(grouped_files[1]) == 2
    assert len(grouped_files[2]) == 3

    shutil.rmtree(mock_dir)

    print("\n✅ 验证通过！")
