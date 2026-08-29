from collections.abc import Collection

from fastapi import HTTPException, Request


def reject_unknown_query_params(request: Request, allowed: Collection[str]) -> None:
    unknown = sorted(set(request.query_params) - set(allowed))
    if not unknown:
        return
    raise HTTPException(
        status_code=422,
        detail=[
            {
                "type": "extra_forbidden",
                "loc": ["query", name],
                "msg": "Extra inputs are not permitted",
                "input": request.query_params.get(name),
            }
            for name in unknown
        ],
    )


def reject_query_params_on_non_get(request: Request) -> None:
    if request.method not in {"GET", "HEAD"}:
        reject_unknown_query_params(request, set())
