# 上传源码版本；配置与构建上下文分别保存。
read_uploaded() {
    [ -f "$metadata/uploaded" ] || fail '远端没有上传源码，请先 --upload'
    [ "$(wc -l < "$metadata/uploaded")" -eq 2 ] || fail '上传版本元数据无效'
    uploaded_source=$(sed -n '1p' "$metadata/uploaded")
    uploaded_config=$(sed -n '2p' "$metadata/uploaded")
    valid_hash "$uploaded_source" && valid_hash "$uploaded_config" || fail '上传版本元数据无效'
}
uploaded_config_source() {
    read_uploaded
    source="$metadata/configs/$uploaded_config"
    [ "$(configuration_id "$source")" = "$uploaded_config" ] || fail '上传配置快照已改变，请重新上传'
}
source_inventory() (
    if [ ! -f "$metadata/uploaded" ]; then printf 'none\n'; exit; fi
    read_uploaded
    printf '%s\n' "$uploaded_source"
    version="$metadata/sources/$uploaded_source"
    # 旧整包上传没有文件清单，首次新上传自然发送完整白名单。
    [ -f "$version/manifest" ] || exit 0
    scratch=$(mktemp -d "$metadata/inventory.XXXXXX")
    trap 'rm -rf -- "$scratch"' EXIT
    validate_manifest "$version/manifest" "$scratch"
    check_tree_types "$version/files"
    while IFS= read -r member; do
        # 返回实际内容摘要，远端文件损坏/缺失时下一次上传能够修复。
        if [ -f "$version/files/$member" ]; then
            printf '%s  %s\n' "$(hash_file "$version/files/$member")" "$member"
        fi
    done < "$scratch/names"
)
upload_source() (
    incoming=$source
    current=none
    if [ -f "$metadata/uploaded" ]; then read_uploaded; current=$uploaded_source; fi
    base=$(cat "$incoming/source.base")
    [ "$base" = none ] || valid_hash "$base" || fail '源码基线无效'
    [ "$base" = "$current" ] || fail '远端源码版本已变化，请重新上传'
    work=$(mktemp -d "$metadata/source-upload.XXXXXX")
    destination=''
    cleanup_source_upload() {
        if [ -d "$work/replaced" ] && [ ! -e "$destination" ]; then
            mv -- "$work/replaced" "$destination"
        fi
        rm -rf -- "$work"
    }
    trap cleanup_source_upload EXIT
    trap 'exit 130' HUP INT TERM
    mkdir -p "$work/version/files" "$work/check" "$work/old"
    validate_manifest "$incoming/source.manifest" "$work/check"
    validate_source_delta "$incoming/source.delta.tar.gz" "$work/check"
    old="$metadata/sources/$base"
    if [ -f "$old/manifest" ]; then
        validate_manifest "$old/manifest" "$work/old"
        check_tree_types "$old/files"
        cp -a -- "$old/files/." "$work/version/files/"
        while IFS= read -r member; do
            if ! grep -Fxq "$member" "$work/check/names"; then rm -f -- "$work/version/files/$member"; fi
        done < "$work/old/names"
    fi
    tar -xf "$incoming/source.delta.tar.gz" --no-same-owner --no-same-permissions -C "$work/version/files"
    cp -- "$incoming/source.manifest" "$work/version/manifest"
    verify_source_tree "$work/version/files" "$work/version/manifest"
    source_hash=$(hash_file "$work/version/manifest")
    if [ "$keep_config" = true ]; then
        if [ -f "$metadata/uploaded" ]; then uploaded_config_source; else prepared_source; fi
    fi
    snapshot_config
    mkdir -p -- "$metadata/sources"
    destination="$metadata/sources/$source_hash"
    [ ! -L "$destination" ] || fail '源码目标不得为符号链接'
    if [ -e "$destination" ]; then mv -- "$destination" "$work/replaced"; fi
    mv -- "$work/version" "$destination"
    printf '%s\n%s\n' "$source_hash" "$identity" > "$metadata/uploaded.tmp"
    mv -f -- "$metadata/uploaded.tmp" "$metadata/uploaded"
    if ! clean_resources; then fail '新源码及配置已保存，但旧资源清理失败；本次不继续构建'; fi
    printf '%s\n' '源码及原配置已增量上传，尚未构建或改变运行实例。'
)
