#!/usr/bin/env bash
#
# ws-ckpt sidecar entrypoint.
#
# Runs the preflight checks, then `exec`s the ws-ckpt daemon so the daemon
# replaces this shell as PID 1. That is what makes the kubelet's SIGTERM land
# directly on the daemon. The daemon deliberately does NOT umount or
# `losetup -d` on SIGTERM; the pod's preStop hook owns loop-device cleanup
# (see k8s-sidecar-example.yaml).
#
# There is deliberately no `sleep infinity`, no retry loop, and no trap that
# keeps the container alive after the daemon exits: a dead daemon must
# surface as a container restart, not as a healthy container serving nothing.
#
# Configuration (environment):
#   WS_CKPT_MOUNT_PATH     btrfs mount point; must match the pod's
#                          mount-anchor volumeMount path exactly
#   WS_CKPT_SOCKET         Unix socket path for CLI <-> daemon IPC
#   WS_CKPT_LOG_LEVEL      debug/info/warn/error
#   WS_CKPT_SKIP_PREFLIGHT set to 1 to skip preflight (debugging only)
#   RUST_LOG               daemon log filter, defaults to info
#
# Any argument passed to the container image is treated as a full command and
# executed instead of the daemon, which keeps `kubectl debug`-style overrides
# possible without a second image.

set -uo pipefail

log() {
	printf '[ws-ckpt-entrypoint] %s\n' "$*" >&2
}

# Escape hatch: `command: ["/usr/local/bin/ws-ckpt-entrypoint"]` plus
# `args: ["sleep", "3600"]` runs that command instead of the daemon.
if (($# > 0)); then
	log "explicit command override, exec: $*"
	exec "$@"
fi

WS_CKPT_BIN="${WS_CKPT_BIN:-/usr/local/bin/ws-ckpt}"
MOUNT_PATH="${WS_CKPT_MOUNT_PATH:-/mnt/btrfs-workspace}"
SOCKET="${WS_CKPT_SOCKET:-/run/ws-ckpt/ws-ckpt.sock}"
LOG_LEVEL="${WS_CKPT_LOG_LEVEL:-info}"
export RUST_LOG="${RUST_LOG:-info}"

if [[ ! -x "$WS_CKPT_BIN" ]]; then
	log "FAIL: ws-ckpt binary '$WS_CKPT_BIN' is missing or not executable"
	exit 127
fi

preflight() {
	# User-space tools (btrfs, losetup, rsync, ...) are installed by the
	# Dockerfile itself, so they are not re-checked here. Preflight only
	# validates what the image cannot guarantee: the host kernel and the
	# deployment's device exposure.

	# Best-effort module load; only works privileged, harmless otherwise.
	modprobe btrfs 2>/dev/null || true
	modprobe loop 2>/dev/null || true

	# Both backends need kernel btrfs support. This is the single most common
	# misconfiguration, so reject it here with an actionable message instead
	# of at first init.
	if ! grep -qw btrfs /proc/filesystems; then
		log "FAIL: kernel btrfs support unavailable (/proc/filesystems)"
		log "      run 'modprobe btrfs' on the node, or use a node image with btrfs built in"
		return 1
	fi

	# Loop devices are only needed for the BtrfsLoop backend (ext4/xfs hosts).
	# On native-btrfs hosts BtrfsBase works without them, so warn only.
	if [[ ! -e /dev/loop-control ]]; then
		log "WARNING: /dev/loop-control missing; BtrfsLoop backend unavailable"
		log "         (fine if the daemon's state directory is on native btrfs)"
	fi

	return 0
}

log "ws-ckpt version: $("$WS_CKPT_BIN" --version 2>&1 | head -1)"
log "uid=$(id -u) gid=$(id -g) mount-path=${MOUNT_PATH} socket=${SOCKET}"

if [[ "${WS_CKPT_SKIP_PREFLIGHT:-0}" == "1" ]]; then
	log "WARNING: WS_CKPT_SKIP_PREFLIGHT=1, skipping prerequisite validation"
else
	if ! preflight; then
		log "FAIL: preflight failed; not starting the daemon"
		exit 1
	fi
fi

# The socket directory and state directory may be fresh emptyDir volumes.
mkdir -p "$(dirname "$SOCKET")" /var/lib/ws-ckpt

declare -a cmd=(
	"$WS_CKPT_BIN" daemon
	--mount-path "$MOUNT_PATH"
	--socket "$SOCKET"
	--log-level "$LOG_LEVEL"
)

log "exec: ${cmd[*]}"
exec "${cmd[@]}"
