#!/bin/sh
# 只清理本项目；当前版本、依赖缓存与外部引用的祖先均受保护。
set -eu
umask 077
root=$1
image=$2
script_dir=$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd)
. "$script_dir/container_env.sh"
. "$script_dir/container_source.sh"
work=$(mktemp -d "$metadata/cleanup.XXXXXX")
trap 'rm -rf -- "$work"' EXIT
touch "$work/keep" "$work/mounts" "$work/graph" "$work/candidates"
keep_ancestry() {
    ancestor=${1#sha256:}
    while valid_hash "$ancestor"; do
        if grep -Fxq "$ancestor" "$work/keep"; then break; fi
        printf '%s\n' "$ancestor" >> "$work/keep"
        parent=$(pm image inspect "$ancestor" --format '{{.Parent}}')
        ancestor=${parent#sha256:}
    done
}
if [ -n "$image" ]; then keep_ancestry "$image"; fi
prepared_config=''
uploaded_config=''
uploaded_source=''
if [ -f "$metadata/prepared" ]; then read_prepared; keep_ancestry "$prepared_image"; fi
if [ -f "$metadata/uploaded" ]; then read_uploaded; fi
for tag in "$image_tag" "$dependency_tag"; do
    if pm image exists "$tag"; then
        keep_ancestry "$(pm image inspect "$tag" --format '{{.Id}}')"
    fi
done
all_containers=$(pm ps --all --quiet)
for container_id in $all_containers; do
    keep_ancestry "$(field "$container_id" '{{.Image}}')"
    field "$container_id" '{{range .Mounts}}{{println .Source}}{{end}}' >> "$work/mounts"
done
for folder in "$metadata/configs/"*; do
    [ -d "$folder" ] && [ ! -L "$folder" ] || continue
    valid_hash "${folder##*/}" || continue
    [ "${folder##*/}" != "$prepared_config" ] && [ "${folder##*/}" != "$uploaded_config" ] || continue
    if ! grep -Fxq "$folder/config.toml" "$work/mounts" &&
        ! grep -Fxq "$folder/market_data.toml" "$work/mounts"; then rm -r -- "$folder"; fi
done
for archive in "$metadata/sources/"*.tar.gz; do
    [ -f "$archive" ] && [ ! -L "$archive" ] || continue
    archive_name=${archive##*/}
    archive_hash=${archive_name%.tar.gz}
    valid_hash "$archive_hash" || continue
    [ "$archive_hash" = "$uploaded_source" ] || rm -- "$archive"
done
for version in "$metadata/sources/"*; do
    [ -d "$version" ] && [ ! -L "$version" ] || continue
    valid_hash "${version##*/}" || continue
    [ "${version##*/}" = "$uploaded_source" ] || rm -r -- "$version"
done
candidates=$(pm images --all --quiet --no-trunc)
for item in $candidates; do
    item=${item#sha256:}
    owner=$(pm image inspect "$item" --format '{{index .Labels "io.ccxt-proxy2.project"}}')
    if [ "$owner" != "$name" ]; then keep_ancestry "$item"; continue; fi
    kind=$(pm image inspect "$item" --format '{{index .Labels "io.ccxt-proxy2.kind"}}')
    case "$kind" in
        runtime)
            if [ "$(pm image inspect "$item" --format '{{index .Labels "io.ccxt-proxy2.release"}}')" != true ]; then
                keep_ancestry "$item"; continue
            fi ;;
        build-cache) ;;
        *) keep_ancestry "$item"; continue ;;
    esac
    tags=$(pm image inspect "$item" --format '{{range .RepoTags}}{{println .}}{{end}}')
    if printf '%s\n' "$tags" | grep -v '^$' | grep -qv '^localhost/ccxt-proxy2:'; then keep_ancestry "$item"; fi
    parent=$(pm image inspect "$item" --format '{{.Parent}}')
    parent=${parent#sha256:}
    printf '%s\n' "$item" >> "$work/candidates"
    if valid_hash "$parent"; then printf '%s %s\n' "$item" "$parent"; else printf '%s %s\n' "$item" "$item"; fi >> "$work/graph"
done
tsort "$work/graph" > "$work/ordered"
while IFS= read -r item; do
    grep -Fxq "$item" "$work/candidates" || continue
    if grep -Fxq "$item" "$work/keep"; then continue; fi
    # 不强删：扫描之后若新增引用，Podman 仍可拒绝，避免越过保护。
    pm image rm --no-prune "$item" >/dev/null
done < "$work/ordered"
