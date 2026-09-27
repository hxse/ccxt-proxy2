# 实例生命周期；调用方持操作锁，停止只使用单独的生命周期门禁。
wait_ready() {
    ready_end=$(($(date +%s) + 60))
    while [ "$(date +%s)" -lt "$ready_end" ]; do
        check_start
        running "$name" || fail '容器已退出，服务未就绪'
        if timeout --kill-after=2 5 podman exec "$name" "$python" -c '
import json, urllib.request
with urllib.request.urlopen("http://127.0.0.1:5123/readyz", timeout=2) as response:
    assert response.status == 200 and json.load(response)["status"] == "ready"
' 8>&- 9>&- >/dev/null 2>&1; then return; fi
        sleep 1
    done
    fail '服务在 60 秒内未就绪'
}

finish() {
    result=$?
    trap - EXIT HUP INT TERM
    if [ "$pending" = true ]; then
        set +e
        if [ "$created" = true ]; then
            pm logs --tail=250 "$name" > "$metadata/failure.tmp" 2>&1
            tail -c 65536 "$metadata/failure.tmp" > "$metadata/last-startup.log"
            chmod 600 "$metadata/last-startup.log"
            rm -f -- "$metadata/failure.tmp"
            printf '%s\n' '末尾日志已保存到 .container/last-startup.log（权限 0600）。' >&2
        fi
        gate
        recovered=true
        restore_running=false
        if [ "$created" = true ]; then
            pm stop --time=30 "$name" >/dev/null || recovered=false
            pm rm "$name" >/dev/null || recovered=false
        fi
        if [ "$renamed" = true ]; then pm rename "$backup" "$name" || recovered=false; fi
        if [ "$was_running" = true ] && [ "$(generation)" = "$expected" ]; then
            if pm start "$name" >/dev/null; then restore_running=true; else recovered=false; fi
        elif [ "$started_existing" = true ]; then
            pm stop --time=30 "$name" >/dev/null || recovered=false
        fi
        ungate
        if [ "$recovered" != true ]; then printf '%s\n' '恢复原实例失败，请检查保留的容器和日志。' >&2; fi
        if [ "$restore_running" = true ] && [ "$recovered" = true ]; then
            wait_ready
            printf '%s\n' '本次启动失败，已恢复原实例。' >&2
        fi
    fi
    if [ -n "${candidate:-}" ]; then
        pm image rm --no-prune "$candidate" >/dev/null 2>&1 || true
    fi
    if [ -n "${build_work:-}" ]; then rm -rf -- "$build_work"; fi
    if [ -n "${build_context:-}" ]; then rm -rf -- "$build_context"; fi
    exit "$result"
}

activate() {
    check_start
    inspect_image "$image"
    old=false
    if exists "$name"; then owned "$name"; old=true; fi
    if exists "$backup"; then fail '存在上次中断留下的替换容器，请检查 ccxt-proxy2-replacing'; fi
    validate_config
    snapshot_config
    if [ "$old" = true ] &&
        [ "$(field "$name" '{{.Image}}' | sed 's/^sha256://')" = "${image#sha256:}" ] &&
        [ "$(field "$name" '{{index .Config.Labels "io.ccxt-proxy2.configuration"}}')" = "$identity" ]; then
        gate
        check_start
        if ! running "$name"; then
            pending=true
            started_existing=true
            pm start "$name" >/dev/null
        fi
        ungate
        wait_ready
        check_start
        pending=false
        printf '%s\n' '容器已就绪，复用现有实例。'
    else
        gate
        check_start
        pending=true
        if [ "$old" = true ]; then
            if running "$name"; then was_running=true; pm stop --time=30 "$name" >/dev/null; fi
            pm rename "$name" "$backup"
            renamed=true
        fi
        mkdir -p -- "$root/data"
        pm create --name "$name" --pull=never --restart=unless-stopped \
            --publish 127.0.0.1:5123:5123 --label "${prefix}project=$name" \
            --label "${prefix}directory=$root" --label "${prefix}configuration=$identity" \
            --log-driver=k8s-file --log-opt=max-size=10mb \
            --volume "$snapshot/config.toml:/app/config.toml:ro" \
            --volume "$snapshot/market_data.toml:/app/market_data.toml:ro" \
            --volume "$root/data:/app/data:rw" "$image" >/dev/null
        created=true
        pm start "$name" >/dev/null
        ungate
        wait_ready
        gate
        check_start
        if [ "$renamed" = true ]; then pm rm "$backup" >/dev/null; fi
        pending=false
        ungate
        printf '%s\n' '容器已启动，就绪地址：http://127.0.0.1:5123'
    fi
    clean_resources
}

status() {
    state=absent
    active_image=null
    ready_image=null
    if exists "$backup"; then owned "$backup"; state=updating; fi
    if exists "$name"; then
        owned "$name"
        state=stopped
        if running "$name"; then state=running; fi
        active_image="\"$(field "$name" '{{.Image}}')\""
    fi
    if [ -f "$metadata/prepared" ]; then read_prepared; ready_image="\"$prepared_image\""; fi
    printf '{"container":"%s","state":"%s","image_id":%s,"prepared_image_id":%s}\n' "$name" "$state" "$active_image" "$ready_image"
}

stop() {
    gate
    present=false
    for item in "$name" "$backup"; do
        if exists "$item"; then owned "$item"; present=true; fi
    done
    next=$(($(generation) + 1))
    printf '%s\n' "$next" > "$metadata/stop-generation.tmp"
    mv -f -- "$metadata/stop-generation.tmp" "$metadata/stop-generation"
    for item in "$name" "$backup"; do
        if exists "$item" && running "$item"; then pm stop --time=30 "$item" >/dev/null; fi
    done
    ungate
    state=absent
    if [ "$present" = true ]; then state=stopped; fi
    printf '{"container":"%s","state":"%s"}\n' "$name" "$state"
}
