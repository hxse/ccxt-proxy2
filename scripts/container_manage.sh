#!/bin/sh
# 同一入口供本地 Python CLI 和远端 SSH 接收器调用；宿主只执行 Shell/Podman。
set -eu
umask 077
root=$1
action=$2
source=$3
expected=$4
keep_config=$5
image=$6
app_profile=${7:-}
case "$app_profile" in local|remote) ;; *) echo '必须明确部署配置场景' >&2; exit 1 ;; esac
script_dir=$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd)
. "$script_dir/container_env.sh"
. "$script_dir/container_instance.sh"
. "$script_dir/container_source.sh"
. "$script_dir/container_manifest.sh"
. "$script_dir/container_build.sh"
pending=false
created=false
renamed=false
was_running=false
started_existing=false
trap finish EXIT
trap 'exit 130' HUP INT TERM

case "$action" in generation|inventory|status|logs|stop|start|upload|build|build-start|upload-build|upload-build-start|activate|build-local) ;; *) fail '动作无效' ;; esac
case "$keep_config" in true|false) ;; *) fail '配置上传选项无效' ;; esac
case "$action" in start|build-start|upload-build-start|activate)
    case "$expected" in ''|*[!0-9]*) fail '启动请求必须指定停止代次' ;; esac ;;
esac
if [ "$action" = generation ]; then printf '{"generation":%s}\n' "$(generation)"; exit; fi
pm info >/dev/null
if [ "$action" = status ]; then status; exit; fi
if [ "$action" = logs ]; then
    exists "$name" || fail '受管容器不存在，无法查看日志'
    owned "$name"
    podman logs --follow --tail=100 "$name"
    exit
fi
mkdir -p -- "$metadata"
exec 8> "$metadata/control.lock"
if [ "$action" = stop ]; then stop; exit; fi
# 本地 activate 的外层 CLI 已持有同一操作锁；远端写动作在此加锁。
if [ "$action" != activate ] && [ "$action" != build-local ]; then exec 9> "$metadata/operation.lock"; operation_lock; fi
if [ "$action" = inventory ]; then source_inventory; exit; fi

case "$action" in upload|upload-build|upload-build-start) upload_source ;; esac
case "$action" in build|build-start|upload-build|upload-build-start) build_uploaded ;; esac
if [ "$action" = build-local ]; then
    build_candidate "$source"
    publish_image
    clean_resources
    printf '%s\n' "本地镜像构建完成：$image_tag"
fi
case "$action" in start|build-start|upload-build-start)
    prepared_source
    image=$prepared_image
    activate
    for config_name in config.toml market_data.toml; do
        atomic_copy "$source/$config_name" "$root/$config_name"
    done ;;
    activate) activate ;;
esac
