from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class RetentionPolicy:
    providers: tuple[str, ...]
    live: int
    sandbox: int
    keep_years: int | None = None

    def __post_init__(self):
        if (
            not self.providers
            or len(set(self.providers)) != len(self.providers)
            or not set(self.providers) <= {"tq", "ccxt"}
        ):
            raise ValueError("invalid retention providers")
        if any(
            type(value) is not int or value < 0 for value in (self.live, self.sandbox)
        ):
            raise ValueError("retention counts must be nonnegative integers")
        if self.keep_years is not None and (
            "tq" not in self.providers
            or type(self.keep_years) is not int
            or self.keep_years <= 0
        ):
            raise ValueError("invalid auxiliary retention")

    def rules(self):
        return {
            "providers": list(self.providers),
            "modes": {"live": self.live, "sandbox": self.sandbox},
            "auxiliary": None
            if self.keep_years is None
            else {"keep_years": self.keep_years},
        }


class MaintenanceFailure(RuntimeError):
    def __init__(self, report):
        self.report = report
        super().__init__("cache maintenance failed")
