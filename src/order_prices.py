"""委托价格的十进制运算与公开调整信息；不读取交易所或行情。"""

import math
from dataclasses import dataclass
from decimal import (
    ROUND_CEILING,
    ROUND_FLOOR,
    ROUND_HALF_EVEN,
    Decimal,
    InvalidOperation,
    localcontext,
)

from pydantic import BaseModel, Field

from src.domain_errors import DomainError

_FLOAT_NOISE_TICK_FRACTION = Decimal("0.000001")


class PriceAdjustment(BaseModel):
    requested_price: str = Field(description="调用方原报价，十进制字符串。")
    submitted_price: str = Field(description="后端对齐后实际提交的限价。")
    tick_size: str = Field(description="此次使用的价格步长。")
    adjusted: bool = Field(description="报价是否因步长发生调整。")


class OrderPriceError(DomainError):
    status_code = 422

    def __init__(self, code: str, message: str, **context):
        self.code = code
        super().__init__(message)
        if context:
            self.detail["price_context"] = {
                key: decimal_text(value) if isinstance(value, Decimal) else value
                for key, value in context.items()
            }


class PriceRulesUnavailable(OrderPriceError):
    status_code = 503

    def __init__(self, message: str):
        super().__init__("PRICE_RULES_UNAVAILABLE", message)


def decimal_text(value: Decimal) -> str:
    return (
        format(value, "f").rstrip("0").rstrip(".")
        if "." in format(value, "f")
        else format(value, "f")
    )


def rule_decimal(value, field: str, *, allow_zero: bool = False) -> Decimal:
    try:
        result = Decimal(str(value))
    except (InvalidOperation, ValueError):
        raise PriceRulesUnavailable(f"上游价格资料缺少有效的 {field}。") from None
    if (
        isinstance(value, bool)
        or not result.is_finite()
        or result < 0
        or (result == 0 and not allow_zero)
    ):
        raise PriceRulesUnavailable(f"上游价格资料中的 {field} 无效。")
    return result


@dataclass(frozen=True)
class PriceRules:
    tick: Decimal
    lower: Decimal | None = None
    upper: Decimal | None = None
    origin: Decimal = Decimal(0)

    def __post_init__(self):
        rule_decimal(self.tick, "tick_size")
        rule_decimal(self.origin, "price_origin", allow_zero=True)
        for name in ("lower", "upper"):
            value = getattr(self, name)
            if value is not None:
                rule_decimal(value, name, allow_zero=True)
        if (
            self.lower is not None
            and self.upper is not None
            and self.lower > self.upper
        ):
            raise PriceRulesUnavailable("上游价格下界大于上界。")


def check_price_bounds(
    value: Decimal, lower: Decimal | None, upper: Decimal | None, **context
) -> None:
    if (lower is not None and value < lower) or (upper is not None and value > upper):
        raise OrderPriceError(
            "PRICE_OUT_OF_RANGE",
            "委托价格超出交易所允许范围。",
            submitted_price=value,
            lower_bound=lower,
            upper_bound=upper,
            **context,
        )


def prepare_price(
    value, side: str, rules: PriceRules, *, field: str = "price", strict: bool = False
) -> PriceAdjustment:
    try:
        requested = Decimal(str(value))
    except (InvalidOperation, ValueError):
        raise OrderPriceError(
            "INVALID_ORDER_PRICE", "价格必须是有限正数。", field=field
        ) from None
    if isinstance(value, bool) or not requested.is_finite() or requested <= 0:
        raise OrderPriceError(
            "INVALID_ORDER_PRICE", "价格必须是有限正数。", field=field
        )
    if side not in {"buy", "sell"}:
        raise OrderPriceError(
            "INVALID_ORDER_PRICE", "报价方向必须是 buy 或 sell。", field=field
        )
    # 提高局部精度，避免默认 28 位上下文使除法在网格边界提前四舍五入。
    with localcontext() as context:
        context.prec = max(
            50,
            len(requested.as_tuple().digits)
            + abs(requested.adjusted() - rules.tick.adjusted())
            + 10,
        )
        rounding = ROUND_FLOOR if side == "buy" else ROUND_CEILING
        units = (requested - rules.origin) / rules.tick
        submitted = (
            rules.origin + units.to_integral_value(rounding=rounding) * rules.tick
        )
        if not strict and isinstance(value, float):
            # 只还原普通浮点报价的极小尾差，不吞掉真实价差或移动触发阈值。
            nearest = (
                rules.origin
                + units.to_integral_value(rounding=ROUND_HALF_EVEN) * rules.tick
            )
            tolerance = min(
                2 * Decimal.from_float(math.ulp(value)),
                rules.tick * _FLOAT_NOISE_TICK_FRACTION,
            )
            if nearest > 0 and abs(requested - nearest) <= tolerance:
                submitted = nearest
    details = {"field": field, "requested_price": requested, "tick_size": rules.tick}
    if strict and submitted != requested:
        raise OrderPriceError(
            "INVALID_PRICE_PRECISION",
            "触发价格不符合步长；后端不会自动移动触发阈值。",
            **details,
        )
    if submitted <= 0:
        raise OrderPriceError(
            "INVALID_ORDER_PRICE",
            "按步长对齐后的价格必须大于零。",
            submitted_price=submitted,
            **details,
        )
    check_price_bounds(submitted, rules.lower, rules.upper, **details)
    return PriceAdjustment(
        requested_price=decimal_text(requested),
        submitted_price=decimal_text(submitted),
        tick_size=decimal_text(rules.tick),
        adjusted=requested != submitted,
    )
