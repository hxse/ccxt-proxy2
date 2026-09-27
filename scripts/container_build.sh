# 同一构建流程供本地和远端调用；主入口的退出处理负责清理候选与临时目录。
build_candidate() {
    context=$1
    build_work=$(mktemp -d "$metadata/build.XXXXXX")
    candidate="$image_tag-build-$(basename "$build_work")"
    printf '%s\n' '构建依赖阶段（命中缓存时不重复编译）……'
    podman build --platform=linux/amd64 --layers --force-rm --target=dependencies \
        --tag "$dependency_tag" "$context" 8>&- 9>&-
    printf '%s\n' '构建运行镜像……'
    podman build --platform=linux/amd64 --layers --force-rm --tag "$candidate" "$context" 8>&- 9>&-
    inspect_image "$candidate"
    mkdir "$build_work/smoke"
    printf 'SECRET = "%s"\n' 'isolated-smoke-key-is-not-a-real-secret' > "$build_work/smoke/config.toml"
    printf '[tq_collection]\nenabled = false\n[retention]\nenabled = false\n' > "$build_work/smoke/market_data.toml"
    pm run --rm --pull=never --network=none --tmpfs /app/data:rw --entrypoint "$python" \
        --volume "$build_work/smoke/config.toml:/app/config.toml:ro" \
        --volume "$build_work/smoke/market_data.toml:/app/market_data.toml:ro" \
        "$image" -c "$(cat "$script_dir/container_smoke.py")"
}
publish_image() {
    pm tag "$image" "$image_tag"
    pm untag "$image" "$candidate"
    candidate=''
}
build_uploaded() {
    uploaded_config_source
    archive="$metadata/sources/$uploaded_source.tar.gz"
    [ "$(hash_file "$archive")" = "$uploaded_source" ] || fail '已上传源码摘要不一致，请重新上传'
    validate_source_archive "$archive"
    build_context=$(mktemp -d "$metadata/context.XXXXXX")
    tar -xf "$archive" --no-same-owner --no-same-permissions -C "$build_context"
    build_candidate "$build_context"
    validate_config
    publish_image
    printf '%s\n%s\n' "$image" "$uploaded_config" > "$metadata/prepared.tmp"
    mv -f -- "$metadata/prepared.tmp" "$metadata/prepared"
    if ! clean_resources; then fail '新准备版本已保存，但旧资源清理失败；本次不继续启动'; fi
    printf '%s\n' '服务器镜像构建与配置校验完成，已保存准备版本。'
}
