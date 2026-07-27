#!/usr/bin/env bash
# Body of the CI armhf lane. Runs INSIDE an arm32v7/debian:trixie container
# on the arm64 runner (see the `armhf` job in ci.yml for the host-side
# canary and the fleet caveat). Everything here executes as native AArch32
# EL0 code under the runner's 64-bit kernel — real 32-bit, not emulation.
set -euo pipefail

# Canary: prove this is really a 32-bit armhf userspace before spending any
# build time. The container would fail to start at all on a CPU without
# AArch32 EL0, but an accidental arm64 image pull would land here instead.
[ "$(getconf LONG_BIT)" = "32" ]
[ "$(dpkg --print-architecture)" = "armhf" ]

export DEBIAN_FRONTEND=noninteractive
apt-get update
# clang provides the libclang that ffmpeg-sys-next's bindgen loads at build
# time. zlib1g-dev backs the FFmpeg .pc files' Requires.private, which
# pkg-config resolves even for a shared-library query. The libav*-dev set
# covers every default ffmpeg-next component (codec/device/filter/format/
# resample/scale).
apt-get install -y --no-install-recommends \
  build-essential pkg-config clang curl ca-certificates zlib1g-dev \
  libavcodec-dev libavdevice-dev libavfilter-dev libavformat-dev \
  libavutil-dev libswresample-dev libswscale-dev

# Trixie is expected to carry FFmpeg 7.1.x (libavcodec major 61), the same
# major.minor as the crate's FFmpeg 7.1 matrix lane; ffmpeg-sys-next probes
# the headers and enables cfg flags up to ffmpeg_7_1, exactly like that lane.
# Fail loudly if a point release ever moves the major.
avcodec_version="$(pkg-config --modversion libavcodec)"
case "$avcodec_version" in
  61.*) ;;
  *) echo "Expected libavcodec 61.x (FFmpeg 7.1), found $avcodec_version" >&2; exit 1 ;;
esac

# Trixie's packaged rustc is frozen at branch time; every other lane tracks
# stable via rustup, so this one does too. `uname -m` inside the container
# reports the 64-bit kernel's machine string, which would make rustup
# self-detect the wrong host triple — pin it explicitly.
curl -sSf -o /tmp/rustup-init \
  https://static.rust-lang.org/rustup/dist/armv7-unknown-linux-gnueabihf/rustup-init
chmod +x /tmp/rustup-init
/tmp/rustup-init -y --profile minimal --default-toolchain stable \
  --default-host armv7-unknown-linux-gnueabihf
. "$HOME/.cargo/env"
rustc -vV

# GPU features are omitted: the runner is headless either way and 32-bit GPU
# userspace is not a supported target of this lane. `static` is a
# provisioning choice, not a feature to type-check here. The cli semantic
# goldens self-skip (no ffmpeg binary is installed, only libraries); the
# in-process cli gates run for real.
FEATURES=async,rtmp,flv,subtitle,cli
cargo build --features "$FEATURES" --verbose
cargo test --lib --features "$FEATURES"

# End-to-end 32-bit proof for the RTMP server: real-TCP rml_rtmp watchers
# against the embedded server, exercising the poller-token generation
# round-trip on a 32-bit word — the path whose decode bug once left the
# server inert on 32-bit targets. The publish leg re-encodes test.mp4 to
# H.264 in-process, which works on this lane because Debian's libavcodec is
# a GPL build carrying libx264 (the from-source and vcpkg lanes have no
# H.264 encoder). Same feature set, so the library artifacts are reused.
cargo test --features "$FEATURES" --test rtmp_loopback
