"""本地薄转发：实例动作与 SSH 使用相同 Shell 实现。"""

from pathlib import Path

from scripts.container_common import command

MANAGER = Path(__file__).with_name("container_manage.sh")


def execute(root, action):
    command(
        ["sh", MANAGER, root, action, "", "", "false", "", "local"],
        capture=False,
        timeout=None if action == "logs" else 120,
    )


def activate(root, image, source, *, guard):
    """调用方已经持有本地项目操作锁。"""
    guard.check()
    command(
        [
            "sh",
            MANAGER,
            root,
            "activate",
            source,
            str(guard.expected),
            "false",
            image,
            "local",
        ],
        capture=False,
        timeout=240,
    )
