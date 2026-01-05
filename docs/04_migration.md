# 迁移指南

## 概述

本文档描述如何将现有的分块缓存系统（`cache_tool/`）迁移到新的按时间分块 + 日志分离架构。

---

## 迁移步骤

### 第一阶段：备份现有代码

```bash
# 1. 创建备份分支
git checkout -b backup/old-cache-system
git push origin backup/old-cache-system

# 2. 回到主分支
git checkout main
```

---

### 第二阶段：移除旧代码

#### 删除的文件

```bash
# 删除旧的 cache_tool 模块
rm src/cache_tool/cache_consolidator.py
rm src/cache_tool/cache_data_processor.py
rm src/cache_tool/cache_entry.py
rm src/cache_tool/cache_fetcher.py
rm src/cache_tool/cache_file_io.py
rm src/cache_tool/cache_read_chunk.py
rm src/cache_tool/cache_utils.py
rm src/cache_tool/cache_write_overlap_handler.py

# 删除旧测试
rm Test/test_ohlcv_cache_consolidation_feature.py
rm Test/test_ohlcv_cache_edge_cases.py
rm Test/test_ohlcv_cache_multiple_calls.py
rm Test/utils.py
```

#### 保留的文件

```
src/cache_tool/__init__.py  # 保留，将更新导出
Test/conftest.py            # 保留，将更新 fixtures
```

---

### 第三阶段：创建新代码结构

```bash
# 创建新文件
touch src/cache_tool/config.py
touch src/cache_tool/storage.py
touch src/cache_tool/log_manager.py
touch src/cache_tool/continuity.py
touch src/cache_tool/entry.py

# 创建新测试
touch Test/test_storage.py
touch Test/test_log_manager.py
touch Test/test_continuity.py
touch Test/test_integration.py
touch Test/utils.py
```

从 `docs/02_core_code.md` 复制核心代码到对应文件。
从 `docs/03_test_code.md` 复制测试代码到对应文件。

---

### 第四阶段：更新 Router 层

#### 修改 `src/tools/ccxt_utils.py`

将旧的 `get_ohlcv_with_cache_lock` 调用替换为新的 `get_ohlcv_with_cache`：

```python
# 旧代码
from src.cache_tool.cache_entry import get_ohlcv_with_cache_lock

# 新代码
from src.cache_tool.entry import get_ohlcv_with_cache
```

#### 修改 `src/router/trader_router.py`

接口保持不变，内部调用改为新模块：

```python
# fetch_ohlcv_ccxt 函数内部改用新的 entry
```

---

### 第五阶段：数据迁移（可选）

如果需要保留现有缓存数据：

```python
import polars as pl
from pathlib import Path
from src.cache_tool.storage import save_ohlcv
from src.cache_tool.log_manager import rebuild_log_from_data
from src.cache_tool.config import get_data_dir

def migrate_old_cache(old_cache_dir: Path, new_base_dir: Path, period_ms: int):
    """将旧的分块缓存迁移到新格式"""
    
    # 1. 读取所有旧文件
    old_files = sorted(old_cache_dir.glob("*.parquet"))
    if not old_files:
        print(f"无文件需要迁移: {old_cache_dir}")
        return
    
    # 2. 合并所有数据
    dfs = [pl.read_parquet(f) for f in old_files]
    merged = pl.concat(dfs).unique(subset=["time"]).sort("time")
    
    # 3. 解析 symbol 和 period（从文件名）
    sample_name = old_files[0].stem
    parts = sample_name.split(" ")
    symbol = parts[0].replace("_", "/")
    period = parts[1]
    
    print(f"迁移: {symbol} {period}, 共 {len(merged)} 条数据")
    
    # 4. 写入新格式
    save_ohlcv(new_base_dir, symbol, period, merged)
    
    # 5. 重建日志
    data_dir = get_data_dir(new_base_dir, symbol, period)
    rebuild_log_from_data(data_dir, period_ms)
    
    print(f"迁移完成: {data_dir}")

# 使用示例
migrate_old_cache(
    old_cache_dir=Path("./data/ohlcv"),
    new_base_dir=Path("./data/ohlcv_new"),
    period_ms=15 * 60 * 1000,
)
```

---

### 第六阶段：验证

```bash
# 运行所有测试
uv run pytest Test/ -v

# 启动服务测试
uv run uvicorn src.main:app --host 127.0.0.1 --port 5123
```

---

## 新旧 API 对照

| 旧 API | 新 API |
|--------|--------|
| `get_ohlcv_with_cache_lock()` | `get_ohlcv_with_cache()` |
| `handle_cache_write()` | `save_ohlcv()` + `append_log()` |
| `get_next_continuous_cache_chunk()` | `read_ohlcv()` |
| `consolidate_cache()` | `compact_log()`（可选） |
| `check_for_overlaps()` | 不需要（写入时自动去重） |

---

## 新旧参数对照

| 旧参数 | 新参数 | 说明 |
|--------|--------|------|
| `cache_size` | 不需要 | 按时间分块无需此参数 |
| `page_size` | 不需要 | 单文件读取无需分页 |
| `file_type` | 固定 parquet | 日志用 jsonl |
| `enable_consolidate` | 不需要 | 无碎片整理 |

---

## 目录结构对比

### 旧结构
```
data/ohlcv/
  BTC_USDT 15m 20230101T060000Z 20230101T093000Z 0015.parquet
  BTC_USDT 15m 20230101T093000Z 20230101T120000Z 0015.parquet
  ...
```

### 新结构
```
data/ohlcv/
  BTC_USDT_15m/
    2023.parquet
    2024.parquet
    fetch_log.jsonl
  ETH_USDT_15m/
    2023.parquet
    fetch_log.jsonl
```

---

## 回滚计划

如果迁移后发现问题，可以回滚：

```bash
# 1. 切换到备份分支
git checkout backup/old-cache-system

# 2. 恢复旧数据（如果已删除）
# 从备份恢复 data/ohlcv/ 目录
```

---

## 检查清单

- [ ] 备份旧代码（git 分支）
- [ ] 备份旧数据（如果需要）
- [ ] 删除旧的 cache_tool 文件
- [ ] 删除旧的测试文件
- [ ] 创建新的 cache_tool 文件
- [ ] 创建新的测试文件
- [ ] 更新 router 层调用
- [ ] 迁移旧数据（可选）
- [ ] 运行测试验证
- [ ] 手动测试 API
- [ ] 提交代码
