# 只处理受管源码；清单、差异包和完整树分别校验后才允许构建。
validate_source_names() {
    if LC_ALL=C grep -q '[^a-zA-Z0-9_./+-]' "$1"; then fail '源码清单路径无效'; fi
    if [ -n "$(sort "$1" | uniq -d)" ]; then fail '源码清单包含重复路径'; fi
    while IFS= read -r member; do
        case "$member" in ''|/*|./*|../*|*/../*|*/..|*/./*|*/.|*//*|*/__pycache__/*) fail '源码清单路径无效' ;; esac
        case "$member" in
            Dockerfile|.dockerignore|pyproject.toml|uv.lock|src/*.py|src/openapi/*.json|\
            vendor/vnpy_ctp/*.tar.gz|vendor/vnpy_ctp/*.patch|vendor/vnpy_ctp/README.md|\
            vendor/vnpy_ctp/upstream.json|vendor/vnpy_ctp/SHA256SUMS|\
            scripts/collect_market_data.py|scripts/prune_market_data.py|scripts/market_data_pipeline.py) ;;
            *) fail '源码清单包含非构建文件' ;;
        esac
    done < "$1"
}
validate_manifest() {
    [ -s "$1" ] || fail '源码清单为空'
    if LC_ALL=C grep -Eqv '^[0-9a-f]{64}  [a-zA-Z0-9_./+-]+$' "$1"; then fail '源码清单格式无效'; fi
    cut -c67- "$1" > "$2/names"
    validate_source_names "$2/names"
    LC_ALL=C sort "$2/names" > "$2/sorted"
    cmp -s "$2/names" "$2/sorted" || fail '源码清单必须按路径排序'
    for required in Dockerfile .dockerignore pyproject.toml uv.lock src/main.py \
        scripts/collect_market_data.py scripts/prune_market_data.py scripts/market_data_pipeline.py; do
        grep -Fxq "$required" "$2/names" || fail '源码清单缺少构建文件'
    done
    grep -Eq '^vendor/vnpy_ctp/[^/]+\.tar\.gz$' "$2/names" || fail '源码清单缺少 CTP 源码包'
}
check_tree_types() {
    [ -d "$1" ] && [ ! -L "$1" ] || fail '源码树不存在或不是目录'
    [ -z "$(find "$1" ! -type f ! -type d -print -quit)" ] || fail '源码树只能包含普通文件与目录'
}
verify_source_tree() (
    tree=$1
    manifest=$(realpath -- "$2")
    scratch=$(mktemp -d "$metadata/verify.XXXXXX")
    trap 'rm -rf -- "$scratch"' EXIT
    validate_manifest "$manifest" "$scratch"
    check_tree_types "$tree"
    find "$tree" -type f -printf '%P\n' | LC_ALL=C sort > "$scratch/actual"
    cmp -s "$scratch/actual" "$scratch/names" || fail '源码树文件集合与清单不一致'
    (cd -- "$tree" && sha256sum --check --status "$manifest") || fail '源码内容校验失败'
)
validate_source_delta() {
    tar -tf "$1" > "$2/delta-names" || fail '源码差异归档无效'
    tar -tvf "$1" > "$2/delta-types" || fail '源码差异归档无效'
    if grep -qv '^-' "$2/delta-types"; then fail '源码差异归档必须全部为普通文件'; fi
    validate_source_names "$2/delta-names"
    while IFS= read -r member; do
        grep -Fxq "$member" "$2/names" || fail '源码差异归档包含清单外文件'
    done < "$2/delta-names"
}
