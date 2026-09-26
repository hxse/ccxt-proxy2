"""应用级缓存所有权；消费者只借用资源，不关闭它。"""

from threading import Lock

from src.cache_tool import DuckDbOhlcvCache
from src.tools.config_types import OhlcvCacheConfig


class CacheResource:
    def __init__(self, config: OhlcvCacheConfig):
        self._config = config.model_copy(deep=True)
        self._lock = Lock()
        self._cache: DuckDbOhlcvCache | None = None
        self._closed = False

    def get(self) -> DuckDbOhlcvCache:
        with self._lock:
            if self._closed:
                raise RuntimeError("application cache resource is closed")
            if self._cache is None:
                self._cache = DuckDbOhlcvCache(
                    self._config.database_path,
                    self._config.max_rows_per_series,
                    self._config.max_rows_total,
                )
            return self._cache

    def close(self) -> None:
        with self._lock:
            self._closed = True
            if self._cache is not None:
                self._cache.close()
                self._cache = None
