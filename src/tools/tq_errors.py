"""不导入 SDK 的 TQ 边界异常。"""


class TqLegacyMetadataCallForbidden(RuntimeError):
    def __init__(self, method: str):
        super().__init__(f"{method} is disabled; use project metadata range query")
