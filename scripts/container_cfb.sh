# 受管 CFB 独立镜像/实例；与主服务共用宿主 Podman，不向容器开放管理 socket。
cfb_name=ccxt-proxy2-cfb
cfb_tag=localhost/ccxt-proxy2-cfb:latest
cfb_backup=ccxt-proxy2-cfb-replacing

cfb_read_layout() {
    cfb_config=$1
    if [ -n "${cfb_layout_override:-}" ]; then
        cfb_layout_text=$cfb_layout_override
    else
        cfb_layout_text=$(pm run --rm --pull=never --network=none --entrypoint "$python" \
            --env "CCXT_PROXY_PROFILE=$app_profile" \
            --volume "$cfb_config:/app/config.toml:ro" "$image" -m src.cfb.layout) || return 1
    fi
    set -- $cfb_layout_text
    [ "$#" -eq 5 ] || return 1
    cfb_enabled=$1 cfb_identity=$2 cfb_vnc=$3 cfb_port=$4 cfb_web=$5
    case "$cfb_enabled:$cfb_vnc" in true:true|true:false|false:false) ;; *) return 1 ;; esac
    if [ "$cfb_enabled" = true ]; then valid_hash "$cfb_identity" || return 1; fi
    case "$cfb_port:$cfb_web" in *[!0-9:]*|:*) return 1 ;; esac
}

cfb_source_identity() (
    cd -- "$1"
    { find src/cfb containers/cfb -type f ! -path '*/__pycache__/*' ! -path '*/.venv/*';
      printf '%s\n' src/base_types.py src/tools/config_types.py src/tools/config_loader.py \
          src/tools/config_profiles.py src/tools/market_data_types.py src/tools/deployment_types.py;
    } | LC_ALL=C sort | xargs sha256sum | sha256sum | cut -d ' ' -f 1
)

cfb_prepare_image() {
    cfb_context=$1
    cfb_read_layout "$2" || return 1
    if [ "$cfb_enabled" != true ]; then cfb_record_image none; return 0; fi
    cfb_hash=$(cfb_source_identity "$cfb_context") || return 1
    if pm image exists "$cfb_tag" && \
       [ "$(pm image inspect "$cfb_tag" --format '{{index .Labels "io.ccxt-proxy2.source"}}')" = "$cfb_hash" ]; then
        cfb_record_image "$(pm image inspect "$cfb_tag" --format '{{.Id}}')"
        return 0
    fi
    [ "$(pm info --format '{{.Host.OS}} {{.Host.Arch}}')" = 'linux amd64' ] || {
        printf '%s\n' '当前 CFB 固定终端仅支持 linux/amd64' >&2; return 1;
    }
    cfb_candidate="localhost/ccxt-proxy2-cfb:build-$$"
    printf '%s\n' '构建独立 CFB 执行镜像……'
    if ! podman build --layers --force-rm --file "$cfb_context/containers/cfb/Containerfile" \
        --target runtime --label "io.ccxt-proxy2.source=$cfb_hash" --tag "$cfb_candidate" "$cfb_context" 7>&- 8>&- 9>&-; then
        pm image rm --no-prune "$cfb_candidate" >/dev/null 2>&1 || true
        return 1
    fi
    if ! pm run --rm --pull=never --network=none --entrypoint "$python" "$cfb_candidate" -c '
from pathlib import Path
import subprocess
from src.cfb.ipc import Request
from src.cfb.network import PROXY_LIBRARIES
assert Request(kind="health", request_id="cfb-smoke").version == 1
for path, cls in zip(PROXY_LIBRARIES, (1, 2)):
    assert path.read_bytes()[:5] == b"\x7fELF" + bytes([cls])
subprocess.run(["wine", "--version"], check=True, timeout=10)
assert Path("/opt/bridge/native/cfb-hook.dll").is_file()
'; then
        pm image rm --no-prune "$cfb_candidate" >/dev/null 2>&1 || true
        return 1
    fi
    pm tag "$cfb_candidate" "$cfb_tag" || return 1
    pm untag "$cfb_candidate" "$cfb_candidate" >/dev/null || return 1
    cfb_record_image "$(pm image inspect "$cfb_tag" --format '{{.Id}}')"
}

cfb_record_image() {
    if [ -n "${image:-}" ]; then
        mkdir -p -- "$metadata/cfb-images"
        printf '%s\n' "$1" > "$metadata/cfb-images/${image#sha256:}"
    fi
}

cfb_owned() {
    [ "$(field "$1" '{{index .Config.Labels "io.ccxt-proxy2.component"}}')" = cfb ] && \
    [ "$(field "$1" '{{index .Config.Labels "io.ccxt-proxy2.directory"}}')" = "$root" ]
}

cfb_wait() {
    cfb_end=$(($(date +%s) + 30))
    while [ "$(date +%s)" -lt "$cfb_end" ]; do
        running "$cfb_name" || return 1
        if timeout --kill-after=2 7 podman exec "$cfb_name" "$python" -m src.cfb.ctl health 7>&- 8>&- 9>&- >/dev/null 2>&1; then return 0; fi
        sleep 1
    done
    return 1
}

cfb_rollback() {
    cfb_result=$?
    trap - EXIT HUP INT TERM
    if [ "$cfb_pending" = true ]; then
        if [ "$cfb_created" = true ] && exists "$cfb_name"; then
            pm logs --tail=200 "$cfb_name" > "$metadata/cfb-failure.tmp" 2>&1 || true
            tail -c 65536 "$metadata/cfb-failure.tmp" > "$metadata/cfb-startup.log"
            rm -f -- "$metadata/cfb-failure.tmp"
            pm stop --time=30 "$cfb_name" >/dev/null || cfb_result=1
            pm rm "$cfb_name" >/dev/null || cfb_result=1
        fi
        if [ "$cfb_renamed" = true ]; then
            pm rename "$cfb_backup" "$cfb_name" || cfb_result=1
        fi
        if [ "$cfb_was_running" = true ] && [ "$(generation)" = "$expected" ]; then
            pm start "$cfb_name" >/dev/null || cfb_result=1
        fi
    fi
    exit "$cfb_result"
}

cfb_ensure() (
    cfb_pending=false cfb_created=false cfb_renamed=false cfb_was_running=false
    trap cfb_rollback EXIT
    trap 'exit 130' HUP INT TERM
    cfb_read_layout "$1" || return 1
    [ "$cfb_enabled" = true ] || return 0
    if [ -n "${cfb_layout_override:-}" ]; then
        cfb_image=$(pm image inspect "$cfb_tag" --format '{{.Id}}') || return 1
    else
        [ -f "$metadata/cfb-images/${image#sha256:}" ] || return 1
        cfb_image=$(cat "$metadata/cfb-images/${image#sha256:}")
        valid_hash "${cfb_image#sha256:}" || return 1
    fi
    [ "$(pm image inspect "$cfb_image" --format '{{index .Labels "io.ccxt-proxy2.kind"}}')" = cfb-runtime ] || return 1
    cfb_old=false cfb_was_running=false
    if exists "$cfb_backup"; then printf '%s\n' 'CFB 上次替换未收尾，请先检查受管备份实例。' >&2; return 1; fi
    if exists "$cfb_name"; then
        cfb_owned "$cfb_name" || return 1
        if [ "$(field "$cfb_name" '{{.Image}}')" = "$cfb_image" ] && \
           [ "$(field "$cfb_name" '{{index .Config.Labels "io.ccxt-proxy2.configuration"}}')" = "$cfb_identity" ]; then
            running "$cfb_name" || pm start "$cfb_name" >/dev/null || return 1
            cfb_wait
            return $?
        fi
        cfb_old=true
        if running "$cfb_name"; then cfb_was_running=true; fi
        cfb_pending=true
        pm stop --time=30 "$cfb_name" >/dev/null || return 1
        pm rename "$cfb_name" "$cfb_backup" || return 1
        cfb_renamed=true
    fi
    mkdir -p -- "$root/data/cfb"
    set -- --detach --name "$cfb_name" --pull=never --restart=unless-stopped \
        --label "${prefix}project=$name" --label "${prefix}component=cfb" \
        --label "${prefix}directory=$root" --label "${prefix}configuration=$cfb_identity" \
        --label "${prefix}profile=$app_profile" --env "CCXT_PROXY_PROFILE=$app_profile" \
        --stop-timeout=30 --shm-size=256m --security-opt=no-new-privileges \
        --log-driver=k8s-file --log-opt=max-size=20mb \
        --volume "$cfb_config:/app/config.toml:ro" --volume "$root/data/cfb:/data:rw"
    if [ "$cfb_vnc" = true ]; then
        set -- "$@" --publish "127.0.0.1:$cfb_port:$cfb_port" --publish "127.0.0.1:$cfb_web:$cfb_web"
    fi
    cfb_pending=true cfb_created=true
    if pm run "$@" "$cfb_image" >/dev/null && cfb_wait; then
        if [ "$cfb_old" = true ]; then pm rm "$cfb_backup" >/dev/null || return 1; fi
        cfb_pending=false
        printf '%s\n' 'CFB 执行进程已启动；终端交易就绪由 /cfb/readyz 查询。'
        return 0
    fi
    return 1
)

cfb_stop() {
    for cfb_item in "$cfb_name" "$cfb_backup"; do
        if exists "$cfb_item"; then
            cfb_owned "$cfb_item" || return 1
            if running "$cfb_item"; then pm stop --time=30 "$cfb_item" >/dev/null || return 1; fi
        fi
    done
}
