"""在 SDK 协程里登记订阅，避免进入同步 getter 的长等待分支。"""

import time
from threading import Event
from typing import Any

from fastapi import HTTPException


def require_budget(deadline: float, stop: Event | None = None) -> float:
    if stop is not None and stop.is_set():
        raise HTTPException(503, detail="TQ_NOT_READY")
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise HTTPException(504, detail="TQ_DATA_TIMEOUT")
    return remaining


def get_serial(api: Any, request, deadline: float, stop: Event | None):
    require_budget(deadline, stop)

    async def subscribe():
        return api.get_kline_serial(
            request.symbol,
            request.duration_seconds,
            min(request.data_length, 10000),
            adj_type=request.adj_type,
        )

    task = api.create_task(subscribe())
    try:
        while not task.done():
            remaining = require_budget(deadline, stop)
            api.wait_update(deadline=time.time() + min(0.2, remaining))
        frame = task.result()
        while not api.is_serial_ready(frame):
            remaining = require_budget(deadline, stop)
            api.wait_update(deadline=time.time() + min(0.2, remaining))
        require_budget(deadline, stop)
        return frame.copy(deep=True)
    finally:
        # 只取消外层登记任务，SDK 自己管理的共享 serial 订阅继续保留。
        if not task.done():
            task.cancel()
