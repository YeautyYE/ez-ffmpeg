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

# Make this a no-Vulkan libavutil, which is the only 32-bit configuration the
# current ffmpeg-sys-next can bind.
#
# ffmpeg-sys-next ships a minimal <vulkan/vulkan.h> shim for bindgen (it does
# not want the real Vulkan SDK as a build dependency) and that shim asserts
# `sizeof(VkPhysicalDeviceFeatures2) == 240`. 240 is the 64-bit size: the
# struct is a 4-byte enum, a pointer, and 55 u32s, so a 32-bit ABI lays it out
# at 228 — which is the correct size there, per real Vulkan. Only the
# assertion is wrong, and it aborts bindgen on every 32-bit target.
#
# The shim is reached only through hwcontext_wrapper.h's
# `#if __has_include(<libavutil/hwcontext_vulkan.h>)`, and FFmpeg installs
# that header only when configured with Vulkan. Debian enables Vulkan, so
# removing the header here reproduces a libavutil built without it: the whole
# Vulkan block is skipped and nothing else about the install changes.
#
# Done this way rather than by building FFmpeg from source without Vulkan
# because Debian's libavcodec is a GPL build carrying libx264, which the
# rtmp_loopback proof at the end of this script needs; a minimal from-source
# build has no H.264 encoder and would silently drop that coverage.
#
# Ask dpkg for paths because Debian uses a multiarch triplet include directory.
# The compiler guard is the real check: an empty lookup must not silently pass
# while the header remains reachable. Use __has_include, not #include, since
# the header's <vulkan/vulkan.h> dependency is absent in this container.
# Inverting the probe also fails the lane for a missing compiler or bad flags.
#
# Revert this once ffmpeg-sys-next makes the assertion pointer-width aware:
# https://github.com/zmwangx/rust-ffmpeg-sys/pull/143 (reported as issue 137).
while IFS= read -r vulkan_hwcontext; do
  mv "$vulkan_hwcontext" "$vulkan_hwcontext.disabled-for-32bit"
  echo "removed $vulkan_hwcontext (32-bit: ffmpeg-sys-next's Vulkan bindgen shim asserts a 64-bit struct size)"
done < <(dpkg -L libavutil-dev | grep '/libavutil/hwcontext_vulkan\.h$' || true)

# pkg-config output is intentionally unquoted to pass separate -I arguments.
# shellcheck disable=SC2046
if ! printf '#if __has_include(<libavutil/hwcontext_vulkan.h>)\n#error still reachable\n#endif\n' | \
  cc $(pkg-config --cflags libavutil) -E -x c - >/dev/null; then
  echo "libavutil/hwcontext_vulkan.h is still reachable (or the probe could not run); ffmpeg-sys-next would bind its 64-bit-only Vulkan shim and abort." >&2
  exit 1
fi

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
