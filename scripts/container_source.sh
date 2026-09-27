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
validate_source_archive() (
    check_dir=$(mktemp -d "$metadata/source-check.XXXXXX")
    trap 'rm -rf -- "$check_dir"' EXIT
    tar -tf "$1" > "$check_dir/names" || exit 1
    tar -tvf "$1" > "$check_dir/types" || exit 1
    if grep -qv '^-' "$check_dir/types"; then fail '源码归档必须全部为普通文件'; fi
    if [ -n "$(sort "$check_dir/names" | uniq -d)" ]; then fail '源码归档包含重复文件'; fi
    if LC_ALL=C grep -q '[^a-zA-Z0-9_./+-]' "$check_dir/names"; then fail '源码归档路径无效'; fi
    while IFS= read -r member; do
        case "$member" in /*|../*|*/../*|*/..|*/./*|*/__pycache__/*) fail '源码归档路径无效' ;; esac
        case "$member" in
            Dockerfile|.dockerignore|pyproject.toml|uv.lock|src/*.py|src/openapi/*.json|\
            vendor/vnpy_ctp/*.tar.gz|vendor/vnpy_ctp/*.patch|vendor/vnpy_ctp/README.md|\
            vendor/vnpy_ctp/upstream.json|vendor/vnpy_ctp/SHA256SUMS|\
            scripts/collect_market_data.py|scripts/prune_market_data.py|scripts/market_data_pipeline.py) ;;
            *) fail '源码归档包含非构建文件' ;;
        esac
    done < "$check_dir/names"
    for required in Dockerfile .dockerignore pyproject.toml uv.lock src/main.py \
        scripts/collect_market_data.py scripts/prune_market_data.py scripts/market_data_pipeline.py; do
        grep -Fxq "$required" "$check_dir/names" || fail '源码归档缺少构建文件'
    done
    grep -Eq '^vendor/vnpy_ctp/[^/]+\.tar\.gz$' "$check_dir/names" || fail '源码归档缺少 CTP 源码包'
)
upload_source() {
    incoming=$source
    source_hash=$(cat "$incoming/source.sha256")
    valid_hash "$source_hash" || fail '源码摘要无效'
    [ "$(hash_file "$incoming/source.tar.gz")" = "$source_hash" ] || fail '源码归档校验失败'
    validate_source_archive "$incoming/source.tar.gz"
    if [ "$keep_config" = true ]; then
        if [ -f "$metadata/uploaded" ]; then uploaded_config_source; else prepared_source; fi
    fi
    snapshot_config
    mkdir -p -- "$metadata/sources"
    atomic_copy "$incoming/source.tar.gz" "$metadata/sources/$source_hash.tar.gz"
    printf '%s\n%s\n' "$source_hash" "$identity" > "$metadata/uploaded.tmp"
    mv -f -- "$metadata/uploaded.tmp" "$metadata/uploaded"
    if ! clean_resources; then fail '新源码及配置已保存，但旧资源清理失败；本次不继续构建'; fi
    printf '%s\n' '源码及配置已上传，尚未构建或改变运行实例。'
}
