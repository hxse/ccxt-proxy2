# SSH 引导脚本，内容通过 sh -c 传入；不依赖宿主 Python 或容器管理 socket。
set -eu
umask 077
relative=$1
action=$2
expected=$3
keep_config=$4
case "$relative" in ''|/*|~*|*:*|..|../*|*/../*|*/..|.) echo '远端目录无效' >&2; exit 1 ;; esac
case "$action" in generation|inventory|status|logs|stop|start|upload|build|build-start|upload-build|upload-build-start) ;; *) exit 1 ;; esac
case "$keep_config" in true|false) ;; *) exit 1 ;; esac
if [ "$keep_config" = true ]; then
    case "$action" in upload|upload-build|upload-build-start) ;; *) exit 1 ;; esac
fi
root=$(realpath -m -- "$HOME/$relative")
incoming=$(mktemp -d)
trap 'rm -rf -- "$incoming"' EXIT
trap 'exit 130' HUP INT TERM
cat > "$incoming/bundle.tar.gz"
tar -tf "$incoming/bundle.tar.gz" > "$incoming/names"
tar -tvf "$incoming/bundle.tar.gz" > "$incoming/types"
if grep -qv '^-' "$incoming/types"; then echo '归档必须全部为普通文件' >&2; exit 1; fi
cat > "$incoming/required" <<'FILES'
scripts/container_env.sh
scripts/container_instance.sh
scripts/container_manage.sh
scripts/container_validate.py
scripts/container_source.sh
scripts/container_manifest.sh
scripts/container_build.sh
scripts/container_cleanup.sh
scripts/container_smoke.py
FILES
case "$action" in upload|upload-build|upload-build-start)
    printf '%s\n' source.delta.tar.gz source.manifest source.base >> "$incoming/required"
    if [ "$keep_config" = false ]; then
        printf '%s\n' config.toml market_data.toml >> "$incoming/required"
    fi ;;
esac
LC_ALL=C sort "$incoming/names" > "$incoming/actual"
LC_ALL=C sort "$incoming/required" > "$incoming/allowed"
if ! cmp -s "$incoming/actual" "$incoming/allowed"; then
    echo '归档包含非法、重复文件，或缺少必需文件' >&2; exit 1
fi
mkdir "$incoming/files"
tar -xf "$incoming/bundle.tar.gz" --no-same-owner --no-same-permissions -C "$incoming/files"
chmod -R u=rwX,go= "$incoming/files"
sh "$incoming/files/scripts/container_manage.sh" "$root" "$action" "$incoming/files" "$expected" "$keep_config" '' remote
