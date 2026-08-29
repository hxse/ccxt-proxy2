from datetime import timedelta

from fastapi import APIRouter, Depends, Response
from fastapi.security import OAuth2PasswordRequestForm
from fastapi_login import LoginManager
from fastapi_login.exceptions import InvalidCredentialsException

from src.tools.config_types import UserConfig
from src.tools.shared import config

# 创建鉴权路由
auth_router = APIRouter(prefix="/auth", tags=["Auth"])


# FastAPI-Login 相关
SECRET = config.SECRET


# 初始化 LoginManager
manager = LoginManager(SECRET, token_url="/auth/token")


# 定义一个本地函数来获取用户，用于登录路由
@manager.user_loader()
def get_user(username: str) -> UserConfig | None:
    return config.users.get(username)


@auth_router.post("/token")
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
        data={"sub": data.username}, expires=timedelta(minutes=60)
    )
    manager.set_cookie(response, access_token)

    return {"access_token": access_token, "token_type": "bearer"}
