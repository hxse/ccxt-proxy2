"""显式构建 CFB 原生验证镜像；运行阶段断网且不挂载业务配置和数据。"""

from pathlib import Path

from scripts.container_common import command, project_lock, require_runtime

ROOT = Path(__file__).resolve().parents[1]


def main() -> None:
    require_runtime()
    with project_lock(ROOT):
        command(["podman", "build", "--layers", "--force-rm", "--target=runtime",
                 "--file", ROOT / "containers/cfb/Containerfile",
                 "--tag", "localhost/ccxt-proxy2-cfb:verification-runtime", ROOT],
                capture=False, timeout=None)
        command(["podman", "build", "--layers", "--force-rm",
                 "--ignorefile", ROOT / "containers/cfb/verification.ignore",
                 "--file", ROOT / "containers/cfb/Verification.Containerfile",
                 "--tag", "localhost/ccxt-proxy2-cfb:verification", ROOT],
                capture=False, timeout=None)
        command(["podman", "run", "--rm", "--pull=never", "--network=none",
                 "--tmpfs", "/data:rw", "--shm-size=256m",
                 "localhost/ccxt-proxy2-cfb:verification"], capture=False, timeout=None)


if __name__ == "__main__":
    main()
