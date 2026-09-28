"""Podman JSON 协议替身，记录进程生命周期和资源引用。"""

import copy
import json
from pathlib import Path

from scripts.container_common import NAME, PREFIX, DeploymentError

OLD_IMAGE = "sha256:" + "a" * 64
NEW_IMAGE = "sha256:" + "b" * 64
CACHE_IMAGE = "sha256:" + "c" * 64
DEPENDENCY_IMAGE = "sha256:" + "d" * 64


class ContainerEngine:
    def __init__(self):
        self.events = []
        self.containers = {}
        self.images = {
            identity: self.image(identity) for identity in (OLD_IMAGE, NEW_IMAGE)
        }
        self.invalid_config = False
        self.unready_image = None
        self.fail_image = None
        self.busy_port = False
        self.rootless = True
        self.native_platform = "linux amd64"
        self.hold_ready = False
        self.fail_cleanup = False
        self.fail_build = False
        self.fail_smoke = False

    @staticmethod
    def image(identity):
        return {
            "Id": identity,
            "Os": "linux",
            "Architecture": "amd64",
            "RepoTags": [],
            "Parent": "",
            "Labels": {
                PREFIX + "project": NAME,
                PREFIX + "kind": "runtime",
                PREFIX + "release": "true",
            },
        }

    def container(self, name, image, root, identity, *, running=True):
        result = {
            "Name": name,
            "Image": image,
            "State": {"Running": running},
            "Config": {
                "Labels": {
                    PREFIX + "project": NAME,
                    PREFIX + "directory": str(root),
                    PREFIX + "configuration": identity,
                }
            },
            "Mounts": [
                {"Source": str(root / ".container/configs" / identity / "config.toml")}
            ],
        }
        self.containers[name] = result
        return result

    def resolve_image(self, reference):
        for identity, info in self.images.items():
            if (
                identity.removeprefix("sha256:") == reference.removeprefix("sha256:")
                or reference in info["RepoTags"]
            ):
                return identity
        raise DeploymentError("missing image")

    def tag(self, identity, tag):
        for info in self.images.values():
            if tag in info["RepoTags"]:
                info["RepoTags"].remove(tag)
        self.images[identity]["RepoTags"].append(tag)

    def __call__(self, *raw, **kwargs):
        args = tuple(str(item) for item in raw)
        self.events.append(args)
        if args[:2] == ("image", "inspect"):
            info = self.images[self.resolve_image(args[2])]
            if "--format" in args:
                template = args[args.index("--format") + 1]
                if template == "{{.Id}}":
                    return info["Id"]
                if template == "{{.Parent}}":
                    return info["Parent"]
                if template.startswith('{{index .Labels "'):
                    return info["Labels"].get(template.split('"')[1], "")
                if template == "{{range .RepoTags}}{{println .}}{{end}}":
                    return "\n".join(info["RepoTags"])
                if template.startswith("{{.Os}} {{.Architecture}}"):
                    return " ".join(
                        [
                            info["Os"],
                            info["Architecture"],
                            *[
                                info["Labels"].get(PREFIX + name, "")
                                for name in ("project", "kind", "release")
                            ],
                        ]
                    )
                raise AssertionError(template)
            return json.dumps([info])
        if args[:2] == ("image", "exists"):
            self.resolve_image(args[2])
            return ""
        if args[0] == "build":
            if self.fail_build:
                raise DeploymentError("build failed")
            tag = args[args.index("--tag") + 1]
            if "--target=dependencies" in args:
                identity = DEPENDENCY_IMAGE
                info = self.images.setdefault(identity, self.image(identity))
                info["Labels"][PREFIX + "kind"] = "build-cache"
                info["Labels"].pop(PREFIX + "release", None)
            else:
                identity = NEW_IMAGE
                layer = self.images.setdefault(CACHE_IMAGE, self.image(CACHE_IMAGE))
                layer["Labels"][PREFIX + "kind"] = "build-cache"
                layer["Labels"].pop(PREFIX + "release", None)
                info = self.images.setdefault(identity, self.image(identity))
                info["Parent"] = CACHE_IMAGE
            self.tag(identity, tag)
            return identity
        if args[0] == "info":
            if "{{.Host.OS}} {{.Host.Arch}}" in args:
                return self.native_platform
            return str(self.rootless).lower()
        if args[0] == "images":
            if self.fail_cleanup:
                raise DeploymentError("cleanup failed")
            filters = [
                args[index + 1].removeprefix("label=").split("=", 1)
                for index, arg in enumerate(args)
                if arg == "--filter"
            ]
            return "\n".join(
                identity
                for identity, info in self.images.items()
                if all(info["Labels"].get(key) == value for key, value in filters)
            )
        if args[0] == "ps":
            names = list(self.containers)
            if "--filter" in args:
                value = args[args.index("--filter") + 1]
                if value.startswith("name=^"):
                    names = [name for name in names if name == value[6:-1]]
                elif value.startswith("ancestor="):
                    names = [
                        name
                        for name in names
                        if self.containers[name]["Image"] == value[9:]
                    ]
            return "\n".join(names)
        if args[:2] == ("container", "inspect"):
            if "--format" in args:
                item = self.containers[args[2]]
                template = args[args.index("--format") + 1]
                if template == "{{.Image}}":
                    return item["Image"]
                if template == "{{.State.Running}}":
                    return str(item["State"]["Running"]).lower()
                if template == "{{range .Mounts}}{{println .Source}}{{end}}":
                    return "\n".join(mount["Source"] for mount in item["Mounts"])
                if template.startswith('{{index .Config.Labels "'):
                    return item["Config"]["Labels"].get(template.split('"')[1], "")
                raise AssertionError(template)
            return json.dumps(
                [copy.deepcopy(self.containers[name]) for name in args[2:]]
            )
        if args[0] == "run":
            if "--tmpfs" in args and self.fail_smoke:
                raise DeploymentError("smoke failed")
            if "--tmpfs" not in args and self.invalid_config:
                raise DeploymentError("配置校验失败")
            return ""
        if args[0] == "exec":
            if self.hold_ready:
                raise DeploymentError("not ready yet")
            if self.containers[args[1]]["Image"] == self.unready_image:
                self.containers[args[1]]["State"]["Running"] = False
                raise DeploymentError("not ready")
            return ""
        if args[0] in {"start", "stop"}:
            if args[0] == "start" and (
                self.containers[args[-1]]["Image"] == self.fail_image or self.busy_port
            ):
                raise DeploymentError("start failed or port already bound")
            self.containers[args[-1]]["State"]["Running"] = args[0] == "start"
            return ""
        if args[0] == "rename":
            self.containers[args[2]] = self.containers.pop(args[1])
            self.containers[args[2]]["Name"] = args[2]
            return ""
        if args[0] == "create":
            if NAME in self.containers:
                raise DeploymentError("container already exists")
            metadata = dict(
                args[index + 1].split("=", 1)
                for index, arg in enumerate(args)
                if arg == "--label"
            )
            obj = self.container(
                NAME,
                args[-1],
                Path(metadata[PREFIX + "directory"]),
                metadata[PREFIX + "configuration"],
                running=False,
            )
            obj["Mounts"] = [
                {"Source": args[index + 1].split(":", 1)[0]}
                for index, arg in enumerate(args)
                if arg == "--volume"
            ]
            obj["Config"]["Labels"] = metadata
            obj["Config"]["Env"] = [
                args[index + 1] for index, arg in enumerate(args) if arg == "--env"
            ]
            return NAME
        if args[0] == "rm":
            assert not self.containers[args[1]]["State"]["Running"]
            del self.containers[args[1]]
            return ""
        if args[:2] == ("image", "rm"):
            assert "--no-prune" in args
            identity = self.resolve_image(args[-1])
            info = self.images[identity]
            if args[-1] in info["RepoTags"] and len(info["RepoTags"]) > 1:
                info["RepoTags"].remove(args[-1])
                return ""
            if any(item["Image"] == identity for item in self.containers.values()):
                raise DeploymentError("image is in use")
            if any(item["Parent"] == identity for item in self.images.values()):
                raise DeploymentError("image has dependent children")
            del self.images[identity]
            return ""
        if args[0] == "load":
            return ""
        if args[0] == "tag":
            self.tag(self.resolve_image(args[1]), args[2])
            return ""
        if args[0] == "untag":
            self.images[self.resolve_image(args[1])]["RepoTags"].remove(args[2])
            return ""
        if args[0] == "logs":
            return "startup failure details"
        raise AssertionError(f"unexpected Podman call: {args}")


if __name__ == "__main__":
    import fcntl
    import os
    import sys

    state_file = Path(os.environ["FAKE_PODMAN_STATE"])
    with state_file.with_suffix(".lock").open("w") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        engine = ContainerEngine()
        engine.__dict__.update(json.loads(state_file.read_text()))
        try:
            print(engine(*sys.argv[1:]))
        except DeploymentError as exc:
            if sys.argv[1:3] != ["image", "exists"]:
                print(str(exc), file=sys.stderr)
            raise SystemExit(1) from None
        finally:
            state_file.write_text(json.dumps(engine.__dict__))
