from pydantic import BaseModel, Field, model_validator
from datetime import datetime
from typing import Literal

# 支持的周期类型
from src.base_types import VALID_PERIODS


class OHLCVRow(BaseModel):
    """单条 OHLCV 数据"""

    time: int = Field(..., description="毫秒级时间戳", ge=0)
    open: float = Field(..., ge=0)
    high: float = Field(..., ge=0)
    low: float = Field(..., ge=0)
    close: float = Field(..., ge=0)
    volume: float = Field(..., ge=0)

    @model_validator(mode="after")
    def validate_hloc(self):
        """验证 high >= low, high >= open/close, low <= open/close"""
        if self.high < self.low:
            raise ValueError(f"high ({self.high}) must >= low ({self.low})")
        if self.high < max(self.open, self.close):
            raise ValueError("high must >= max(open, close)")
        if self.low > min(self.open, self.close):
            raise ValueError("low must <= min(open, close)")
        return self


class LogEntry(BaseModel):
    """获取日志条目"""

    fetch_time: datetime = Field(default_factory=lambda: datetime.now())
    data_start: int = Field(..., description="数据起始时间戳（毫秒）", ge=0)
    data_end: int = Field(..., description="数据结束时间戳（毫秒）", ge=0)
    count: int | None = Field(
        default=None, ge=0, description="数据条数（合并后为 None）"
    )
    source: str = Field(default="api", description="数据来源")

    @model_validator(mode="after")
    def validate_range(self):
        """验证 data_start <= data_end"""
        if self.data_start > self.data_end:
            raise ValueError(
                f"data_start ({self.data_start}) must <= data_end ({self.data_end})"
            )
        return self


class DataRange(BaseModel):
    """数据时间范围"""

    start: int = Field(..., ge=0)
    end: int = Field(..., ge=0)

    @model_validator(mode="after")
    def validate_range(self):
        """验证 start <= end"""
        if self.start > self.end:
            raise ValueError(f"start ({self.start}) must <= end ({self.end})")
        return self


class Gap(BaseModel):
    """数据断裂信息"""

    gap_after: int = Field(..., description="断裂点之前的时间戳", ge=0)
    gap_before: int = Field(..., description="断裂点之后的时间戳", ge=0)

    @model_validator(mode="after")
    def validate_gap(self):
        """验证 gap_after < gap_before"""
        if self.gap_after >= self.gap_before:
            raise ValueError(
                f"gap_after ({self.gap_after}) must < gap_before ({self.gap_before})"
            )
        return self


class PartitionWindow(BaseModel):
    """分块窗口类型"""

    window: Literal["month", "year", "decade"]


class DataLocation(BaseModel):
    """数据位置参数"""

    exchange: str = Field(..., min_length=1, description="交易所，如 binance")
    mode: Literal["live", "demo"] = Field(..., description="实盘或模拟")
    market: Literal["future", "spot"] = Field(..., description="合约或现货")
    symbol: str = Field(..., min_length=1, description="交易对，如 BTC/USDT")
    period: VALID_PERIODS = Field(..., description="周期，如 15m")
