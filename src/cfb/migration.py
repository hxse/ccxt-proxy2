"""旧单实例目录一次性拆分；保存完整旧目录，不改变 Journal 身份或记录。"""

import argparse
import fcntl
import shutil
import stat
from pathlib import Path


class MigrationError(RuntimeError):
    pass


def _ignore_special(directory, names):
    return [name for name in names if not (
        stat.S_ISDIR((Path(directory) / name).lstat().st_mode)
        or stat.S_ISREG((Path(directory) / name).lstat().st_mode)
        or stat.S_ISLNK((Path(directory) / name).lstat().st_mode))]


def _copy(source: Path, target: Path) -> None:
    if source.is_symlink():
        raise MigrationError("数据根目录不能是符号链接")
    shutil.copytree(source, target, symlinks=True, ignore=_ignore_special)


def migrate(root: Path) -> bool:
    root.parent.mkdir(parents=True, exist_ok=True)
    staging = root.with_name(".cfb-dual-staging")
    backup = root.with_name("cfb-legacy")
    with root.with_name(".cfb-migration.lock").open("a") as migration_lock:
        fcntl.flock(migration_lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        if any(path.is_symlink() for path in (root, staging, backup)):
            raise MigrationError("数据根目录不能是符号链接")
        # 目录切换的两个 rename 之间若掉电，只接受已经准备完整的暂存目录。
        if not root.exists() and backup.exists():
            marker = staging / ".ready"
            if (not marker.is_file() or marker.read_text() != "cfb-mode-layout-v1\n"
                    or not all((staging / mode).is_dir() for mode in ("sandbox", "live"))):
                raise MigrationError("旧数据备份存在，迁移未完成；拒绝以空库启动")
            staging.rename(root)
            return True
        legacy = [root / name for name in ("state", "sessions", "artifacts", "logs")]
        if not any(path.exists() for path in legacy):
            return False
        if root.is_symlink() or backup.exists() or staging.exists():
            raise MigrationError("存在迁移备份或暂存目录，请核查后继续；不会覆盖")
        if any((root / mode).exists() for mode in ("sandbox", "live")):
            raise MigrationError("新旧布局混合，拒绝覆盖已有模式数据")
        with (root / ".session.lock").open("a") as owner:
            fcntl.flock(owner, fcntl.LOCK_EX | fcntl.LOCK_NB)
            staging.mkdir(mode=0o700)
            moved = False
            try:
                for mode, prefix in (("sandbox", "simnow-"), ("live", "live-")):
                    destination = staging / mode
                    destination.mkdir(mode=0o700)
                    # 整份历史记录及保护证据保留；原 namespace 隔离继续生效。
                    for name in ("state", "artifacts"):
                        if (root / name).exists():
                            _copy(root / name, destination / name)
                    sessions = destination / "sessions"
                    sessions.mkdir()
                    source_sessions = root / "sessions"
                    if source_sessions.is_symlink():
                        raise MigrationError("会话根目录不能是符号链接")
                    for session in source_sessions.glob("*"):
                        if not session.name.startswith(("simnow-", "live-")):
                            raise MigrationError("未知会话目录，无法确定模式")
                        if session.name.startswith(prefix):
                            _copy(session, sessions / session.name)
                (staging / ".ready").write_text("cfb-mode-layout-v1\n")
                root.rename(backup)
                moved = True
                staging.rename(root)
            except BaseException:
                if moved and not root.exists():
                    backup.rename(root)
                if staging.exists():
                    shutil.rmtree(staging)
                raise
    return True


def main() -> int:
    parser = argparse.ArgumentParser(description="在旧实例停止后拆分 CFB 数据，保留完整旧备份")
    parser.add_argument("--root", type=Path, required=True)
    args = parser.parse_args()
    try:
        changed = migrate(args.root)
        print("CFB 数据已按模式拆分，旧目录保留为 cfb-legacy。" if changed else "CFB 数据布局无需迁移。")
        return 0
    except (OSError, MigrationError):
        print("CFB 数据迁移失败：请检查目录布局、剩余空间和旧实例锁；禁止使用空库替代。")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
