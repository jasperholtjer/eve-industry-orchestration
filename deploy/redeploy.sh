#!/usr/bin/env bash
# Redeploy the orchestrator after a code or config change on the Dagster LXC.
#
# Run as root inside the container (`pct enter 211`, which drops you in /mnt/eve).
# Works from any directory — every path here is absolute. The script pulls the
# latest code itself, so a redeploy is a single command:
#
#   bash /opt/eve-industry-orchestration/deploy/redeploy.sh
#
# For a shorter invocation, symlink it onto PATH once (as root):
#   ln -s /opt/eve-industry-orchestration/deploy/redeploy.sh /usr/local/bin/redeploy
# then just run `redeploy` from anywhere.
#
# The work is split by identity on purpose: the repo and venv are owned by
# `corpus` (group 988, the only identity that can write /mnt/eve), while systemd
# restarts and the binary install (/usr/local, root-owned) need root. A root
# `git pull` would rewrite the corpus-owned tree as root, so this drops to
# `corpus` for the pull and sync and stays root for the rest.
#
# The corpus binary and its version-matched dataset configs are pinned by
# CORPUS_VERSION and pulled from the private corpus repo's GitHub Release. To
# bump corpus: edit the pin below (or pass CORPUS_VERSION=v0.1.6) and re-run.
# Downloading from a private repo needs gh authenticated as root — `gh auth
# login` once, or export GH_TOKEN. First-time host setup (LXC, UID/GID map, NFS
# mount, gh auth, uv) lives in homelab_docs: docs/howto/deploy-dagster-lxc.md
#
# Container sizing tracks the gold_heavy pool, not max_concurrent_runs: a Gold
# build streams its rolling window (corpus >= v0.1.6) and peaks ~3-4 GiB, so peak
# Gold RAM ~= gold_heavy limit x ~4 GiB. At the default pool limit 2 budget ~8 GiB
# for Gold alone; size the LXC >= 12 GiB (or drop the pool to 1 at 8 GiB). Set on
# the Proxmox host (not in this container), e.g.:
#   pct set 211 --cores 4 --memory 12288 --swap 2048
# Raising the pool without matching RAM thrashes swap and risks an OOM-killed daemon.
set -euo pipefail

SERVICE_USER="${SERVICE_USER:-corpus}"
REPO_DIR="${REPO_DIR:-/opt/eve-industry-orchestration}"
DAGSTER_HOME="${DAGSTER_HOME:-/var/lib/dagster}"
SERVICES=(dagster-daemon dagster-webserver)

# Corpus binary pin. The asset names mirror the corpus release workflow; the
# install paths reuse the same env vars the systemd units pass to Dagster, so
# the running binary and the deployed one can never drift.
CORPUS_VERSION="${CORPUS_VERSION:-v0.1.6}"
CORPUS_REPO="${CORPUS_REPO:-jasperholtjer/eve-industry-corpus}"
CORPUS_TARGET="${CORPUS_TARGET:-x86_64-unknown-linux-musl}"
CORPUS_BIN="${CORPUS_BINARY_PATH:-/usr/local/bin/corpus}"
DATASETS_DIR="${CORPUS_DATASETS_DIR:-/usr/local/share/corpus/datasets}"

# uv installs user-local to ~/.local/bin; a non-interactive `su -` may not pick
# it up from the profile, so put it on PATH explicitly.
run_as_user() {
  su - "${SERVICE_USER}" -c "export PATH=\"\$HOME/.local/bin:\$PATH\"; $1"
}

# Pull the pinned corpus binary + datasets from the private release and install
# them into root-owned /usr/local. Idempotent: skips the download when the
# installed binary already reports the pinned version and the datasets are
# present. Verifies the release SHA256SUMS before installing, and asserts
# `corpus --version` after, so a bad or truncated download fails the redeploy
# instead of silently shipping a stale or corrupt binary.
pull_corpus() {
  local tmp staging got bin_asset ds_asset

  if [[ -x "${CORPUS_BIN}" ]] \
    && "${CORPUS_BIN}" --version 2>/dev/null | grep -qx "corpus ${CORPUS_VERSION}" \
    && [[ -d "${DATASETS_DIR}" ]]; then
    echo "    corpus already pinned at ${CORPUS_VERSION}, skipping pull"
    return 0
  fi

  if ! command -v gh >/dev/null 2>&1; then
    echo "error: gh CLI is required to download from the private ${CORPUS_REPO}" >&2
    echo "       run 'gh auth login' as root once, or export GH_TOKEN" >&2
    exit 1
  fi

  bin_asset="corpus-${CORPUS_VERSION}-${CORPUS_TARGET}"
  ds_asset="corpus-datasets-${CORPUS_VERSION}.tar.gz"

  tmp="$(mktemp -d)"

  echo "    downloading ${CORPUS_VERSION} from ${CORPUS_REPO}"
  gh release download "${CORPUS_VERSION}" \
    --repo "${CORPUS_REPO}" \
    --pattern "${bin_asset}" \
    --pattern "${ds_asset}" \
    --pattern "SHA256SUMS" \
    --dir "${tmp}" --clobber

  echo "    verifying checksums"
  (cd "${tmp}" && sha256sum --check SHA256SUMS)

  echo "    installing binary -> ${CORPUS_BIN}"
  install -m 0755 "${tmp}/${bin_asset}" "${CORPUS_BIN}"

  # Extract to a staging dir and swap, so a failed extraction never leaves a
  # half-written datasets tree in place. The tarball's top-level `datasets/`
  # maps onto DATASETS_DIR regardless of its basename.
  echo "    installing datasets -> ${DATASETS_DIR}"
  staging="$(mktemp -d)"
  tar -xzf "${tmp}/${ds_asset}" -C "${staging}"
  install -d "$(dirname "${DATASETS_DIR}")"
  rm -rf "${DATASETS_DIR}"
  mv "${staging}/datasets" "${DATASETS_DIR}"
  chmod -R a+rX "${DATASETS_DIR}"

  got="$("${CORPUS_BIN}" --version)"
  if [[ "${got}" != "corpus ${CORPUS_VERSION}" ]]; then
    echo "error: ${CORPUS_BIN} reports '${got}', expected 'corpus ${CORPUS_VERSION}'" >&2
    exit 1
  fi

  # Clean up on success only; a mid-function failure leaves the temp dirs for
  # inspection (they sit under /tmp and clear on reboot).
  rm -rf "${tmp}" "${staging}"
  echo "    corpus ${CORPUS_VERSION} installed and verified"
}

# Wrapped in a function so bash parses the whole body before executing: the
# git pull below may update this very file, and a half-read script would break.
main() {
  if [[ "$(id -u)" -ne 0 ]]; then
    echo "error: run as root (systemctl restart needs it); it drops to ${SERVICE_USER} for the repo work" >&2
    exit 1
  fi

  echo "==> Pulling latest code as ${SERVICE_USER}"
  run_as_user "git -C '${REPO_DIR}' pull --ff-only"

  echo "==> Syncing dependencies"
  run_as_user "cd '${REPO_DIR}' && uv sync --frozen"

  echo "==> Ensuring corpus ${CORPUS_VERSION} is installed"
  pull_corpus

  echo "==> Publishing instance config to ${DAGSTER_HOME}"
  install -d -o "${SERVICE_USER}" -g 988 "${DAGSTER_HOME}"
  install -o "${SERVICE_USER}" -g 988 -m 0644 \
    "${REPO_DIR}/deploy/dagster.yaml" "${DAGSTER_HOME}/dagster.yaml"

  echo "==> Restarting services"
  systemctl restart "${SERVICES[@]}"

  echo "==> Status"
  systemctl is-active "${SERVICES[@]}"
  echo "corpus: $("${CORPUS_BIN}" --version 2>/dev/null || echo 'not found')"
  echo "Done. UI: http://192.168.2.211:3000"
}

main "$@"
