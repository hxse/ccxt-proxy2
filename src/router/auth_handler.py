from datetime import timedelta
from typing import Literal

from fastapi import APIRouter, Depends, Response
from fastapi.security import OAuth2PasswordRequestForm
from fastapi_login import LoginManager
from fastapi_login.exceptions import InvalidCredentialsException
from pydantic import BaseModel, Field

from src.router.query_validation import reject_query_params_on_non_get
from src.tools.config_types import UserConfig
from src.tools.shared import config

# 创建鉴权路由
auth_router = APIRouter(
    prefix="/auth",
    dependencies=[Depends(reject_query_params_on_non_get)],
    tags=["Auth"],
)


# FastAPI-Login 相关
SECRET = config.SECRET
ACCESS_TOKEN_EXPIRES_IN_SECONDS = 60 * 60


class TokenResponse(BaseModel):
    access_token: str = Field(description="用于 Authorization: Bearer 的 JWT")
    token_type: Literal["bearer"] = Field(description="固定为 bearer")
    expires_in: int = Field(description="Access token 有效期，单位秒", examples=[3600])


# 初始化 LoginManager
manager = LoginManager(SECRET, token_url="/auth/token")


# 定义一个本地函数来获取用户，用于登录路由
@manager.user_loader()
def get_user(username: str) -> UserConfig | None:
    return config.users.get(username)


@auth_router.post(
    "/token",
    response_model=TokenResponse,
    summary="获取 Bearer access token",
    description=(
        "使用 OAuth2 Password Grant 校验项目配置中的用户名和密码。成功后返回 "
        "60 分钟 JWT，并同步写入登录 Cookie；项目不提供 refresh token。"
    ),
    response_description="包含 access token、bearer 类型和过期秒数。",
    responses={401: {"description": "用户名不存在或密码错误。"}},
)
def login(response: Response, data: OAuth2PasswordRequestForm = Depends()):
    """
    用户登录，成功后将 Token 写入 Cookie。
    """
    # 使用本地配置读取用户，避免 user_loader 装饰后类型变为未知可等待对象
    user = config.users.get(data.username)

    if not user:
        raise InvalidCredentialsException

    if user.password != data.password:
        raise InvalidCredentialsException

    access_token = manager.create_access_token(
        data={"sub": data.username},
        expires=timedelta(seconds=ACCESS_TOKEN_EXPIRES_IN_SECONDS),
    )
    manager.set_cookie(response, access_token)

    return {
        "access_token": access_token,
        "token_type": "bearer",
        "expires_in": ACCESS_TOKEN_EXPIRES_IN_SECONDS,
    }
