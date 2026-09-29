#!/bin/sh
# 宿主源码服务复用正式 CFB 构建与生命周期，不另造一套容器命令。
set -eu
umask 077
root=$1
cfb_host_config=$2
app_profile=$3
cfb_layout_override=$4
expected=$5
case "$app_profile" in dev|local|remote) ;; *) exit 2 ;; esac
script_dir=$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd)
. "$script_dir/container_env.sh"
. "$script_dir/container_cfb.sh"
mkdir -p -- "$metadata"
exec 8> "$metadata/control.lock"
cfb_prepare_image "$root" "$cfb_host_config"
check_start
ensure_network
gate
check_start
cfb_ensure "$cfb_host_config"
ungate
