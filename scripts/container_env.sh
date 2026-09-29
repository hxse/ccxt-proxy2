# 本地与远端唯一的 Podman/文件操作实现；由 container_manage.sh 加载。
name=ccxt-proxy2
backup=ccxt-proxy2-replacing
prefix=io.ccxt-proxy2.
image_tag=localhost/ccxt-proxy2:latest
dependency_tag=localhost/ccxt-proxy2:dependencies
container_network=trading-net
python=/app/.venv/bin/python
metadata="$root/.container"

fail() { printf '%s\n' "$*" >&2; exit 1; }
pm() { timeout --kill-after=10 60 podman "$@" 7>&- 8>&- 9>&-; }
hash_file() { sha256sum -- "$1" | cut -d ' ' -f 1; }
valid_hash() { [ "${#1}" = 64 ] && ! printf '%s' "$1" | LC_ALL=C grep -q '[^0-9a-f]'; }

generation() {
    counter=0
    if [ -f "$metadata/stop-generation" ]; then counter=$(cat "$metadata/stop-generation"); fi
    case "$counter" in ''|*[!0-9]*) fail '停止代次无效' ;; esac
    printf '%s\n' "$counter"
}
check_start() {
    if [ -n "$expected" ] && [ "$(generation)" != "$expected" ]; then
        printf '%s\n' '停止请求已取消本次后续启动；上传结果可以保留' >&2
        exit 130
    fi
}
gate() { flock 8; }
ungate() { flock -u 8; }
operation_lock() {
    if ! flock -n 9; then
        printf '%s\n' '另一个容器任务正在执行，等待项目锁；可按 Ctrl+C 取消。' >&2
        while ! flock -n 9; do
            if [ "$action" = start ]; then check_start; fi
            sleep 0.2
        done
    fi
}
atomic_copy() {
    temporary=$(mktemp "$2.XXXXXX")
    cat -- "$1" > "$temporary"
    chmod 600 "$temporary"
    mv -f -- "$temporary" "$2"
}
configuration_id() {
    config_hash=$(hash_file "$1/config.toml")
    plan_hash=$(hash_file "$1/market_data.toml")
    printf '2-loopback-5123-shell\n%s\n%s\n' "$config_hash" "$plan_hash" | sha256sum | cut -d ' ' -f 1
}
read_prepared() {
    [ -f "$metadata/prepared" ] || fail '远端没有准备版本，请先 --upload 上传配置'
    [ "$(wc -l < "$metadata/prepared")" -eq 2 ] || fail '准备版本元数据无效'
    prepared_image=$(sed -n '1p' "$metadata/prepared")
    prepared_config=$(sed -n '2p' "$metadata/prepared")
    valid_hash "${prepared_image#sha256:}" && valid_hash "$prepared_config" || fail '准备版本元数据无效'
}
prepared_source() {
    read_prepared
    source="$metadata/configs/$prepared_config"
    [ "$(configuration_id "$source")" = "$prepared_config" ] || fail '准备配置快照已改变，请重新上传'
}
snapshot_config() {
    identity=$(configuration_id "$source")
    snapshot="$metadata/configs/$identity"
    mkdir -p -- "$snapshot"
    for config_name in config.toml market_data.toml; do
        if [ ! -e "$snapshot/$config_name" ]; then
            atomic_copy "$source/$config_name" "$snapshot/$config_name"
        fi
    done
    [ "$(configuration_id "$snapshot")" = "$identity" ] || fail '配置快照不完整'
}
validate_config() {
    if [ "$app_profile" = local ] && [ "$action" = activate ]; then
        config_mount=$(realpath -- "$source")
        plan_mount=$(realpath -- "$root/market_data.toml")
    else
        config_mount="$source/config.toml"
        plan_mount="$source/market_data.toml"
    fi
    pm run --rm --pull=never --network=none --entrypoint "$python" \
        --env "CCXT_PROXY_PROFILE=$app_profile" \
        --volume "$config_mount:/app/config.toml:ro" \
        --volume "$plan_mount:/app/market_data.toml:ro" \
        "$image" -c "$(cat "$script_dir/container_validate.py")" >/dev/null 2>&1 ||
        fail '容器配置校验失败：请检查 TOML、后台用户/5123 地址、data 路径及挂载权限；值已隐藏'
}
inspect_image() {
    details=$(pm image inspect "$1" --format '{{.Os}} {{.Architecture}} {{index .Labels "io.ccxt-proxy2.project"}} {{index .Labels "io.ccxt-proxy2.kind"}} {{index .Labels "io.ccxt-proxy2.release"}}')
    native=$(pm info --format '{{.Host.OS}} {{.Host.Arch}}')
    [ "$details" = "$native ccxt-proxy2 runtime true" ] || fail '镜像必须是匹配 Podman 原生平台的本项目运行镜像'
    image=$(pm image inspect "$1" --format '{{.Id}}')
    valid_hash "${image#sha256:}" || fail '镜像身份无效'
}
exists() {
    instance=$(pm ps --all --quiet --filter "name=^$1$") || fail '无法检查容器状态'
    [ -n "$instance" ]
}
field() { pm container inspect "$1" --format "$2"; }
ensure_network() {
    pm network create --ignore "$container_network" >/dev/null ||
        fail "无法创建或复用容器网络：$container_network"
}
uses_container_network() {
    container_networks=$(field "$1" '{{range $name, $_ := .NetworkSettings.Networks}}{{println $name}}{{end}}') ||
        fail '无法检查容器所属网络'
    printf '%s\n' "$container_networks" | grep -Fxq "$container_network"
}
owned() {
    [ "$(field "$1" '{{index .Config.Labels "io.ccxt-proxy2.project"}}')" = "$name" ] &&
        [ "$(field "$1" '{{index .Config.Labels "io.ccxt-proxy2.directory"}}')" = "$root" ] ||
        fail '同名容器不属于本项目目录，拒绝操作'
}
running() {
    instance_running=$(field "$1" '{{.State.Running}}') || fail '无法检查容器运行状态'
    [ "$instance_running" = true ]
}

clean_resources() {
    sh "$script_dir/container_cleanup.sh" "$root" "$image"
}
