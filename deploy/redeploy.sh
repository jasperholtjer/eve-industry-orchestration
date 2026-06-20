#!/usr/bin/env bash
# Redeploy the orchestrator after a code or config change on the Dagster LXC.
#
# Run as root inside the container (`pct enter 211`). The work is split by
# identity on purpose: the repo and the venv are owned by `corpus` (group 988,
# the only identity that can write /mnt/eve), while systemd restarts need root.
# A root `git pull` would rewrite the corpus-owned tree as root, so this script
# drops to `corpus` for the pull and sync and stays root only for the rest.
#
# This handles the recurring update only. First-time host setup (LXC, UID/GID
# map, NFS mount, corpus binary, uv) lives in homelab_docs:
#   docs/howto/deploy-dagster-lxc.md
#
# Usage: bash deploy/redeploy.sh
set -euo pipefail

SERVICE_USER="${SERVICE_USER:-corpus}"
REPO_DIR="${REPO_DIR:-/opt/eve-industry-orchestration}"
DAGSTER_HOME="${DAGSTER_HOME:-/var/lib/dagster}"
SERVICES=(dagster-daemon dagster-webserver)

if [[ "$(id -u)" -ne 0 ]]; then
  echo "error: run as root (systemctl restart needs it); it drops to ${SERVICE_USER} for the repo work" >&2
  exit 1
fi

# uv installs user-local to ~/.local/bin; a non-interactive `su -` may not pick
# it up from the profile, so put it on PATH explicitly.
run_as_user() {
  su - "${SERVICE_USER}" -c "export PATH=\"\$HOME/.local/bin:\$PATH\"; $1"
}

echo "==> Pulling latest code as ${SERVICE_USER}"
run_as_user "git -C '${REPO_DIR}' pull --ff-only"

echo "==> Syncing dependencies"
run_as_user "cd '${REPO_DIR}' && uv sync --frozen"

echo "==> Publishing instance config to ${DAGSTER_HOME}"
install -d -o "${SERVICE_USER}" -g 988 "${DAGSTER_HOME}"
install -o "${SERVICE_USER}" -g 988 -m 0644 \
  "${REPO_DIR}/deploy/dagster.yaml" "${DAGSTER_HOME}/dagster.yaml"

echo "==> Restarting services"
systemctl restart "${SERVICES[@]}"

echo "==> Status"
systemctl is-active "${SERVICES[@]}"
echo "Done. UI: http://192.168.2.211:3000"
