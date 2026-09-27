#!/usr/bin/env bash
set -euo pipefail
# pip/uv 无法提供这些系统库；NixOS 仅为本次运行加入缓存中的库和工具。
if [[ -e /etc/NIXOS ]]; then
    ctp_runtime_paths="$(nix build --no-link --no-write-lock-file --print-out-paths \
        nixpkgs#libglvnd nixpkgs#libxkbcommon nixpkgs#fontconfig.lib nixpkgs#freetype \
        nixpkgs#glib.out nixpkgs#dbus.lib nixpkgs#zlib nixpkgs#wayland \
        nixpkgs#libx11 nixpkgs#libxcb nixpkgs#libxcb-cursor \
        nixpkgs#libxcb-image nixpkgs#libxcb-keysyms \
        nixpkgs#libxcb-render-util nixpkgs#libxcb-wm \
        nixpkgs#glibcLocales nixpkgs#dmidecode)"
    while IFS= read -r ctp_runtime_path; do
        export LD_LIBRARY_PATH="$ctp_runtime_path/lib${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
        export PATH="$ctp_runtime_path/bin:$PATH"
        if [[ -f "$ctp_runtime_path/lib/locale/locale-archive" ]]; then
            export LOCALE_ARCHIVE="$ctp_runtime_path/lib/locale/locale-archive"
        fi
    done <<< "$ctp_runtime_paths"
fi
exec uv run --no-project --no-config --isolated --python 3.13 \
    --with vnpy==4.4.0 --with vnpy_ctp==6.7.11.4 \
    --with vnpy_riskmanager==2.0.0 --with 'pydantic>=2,<3' \
    python script/ctp_assessment.py "$@"
