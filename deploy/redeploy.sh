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
# The corpus binary and its version-matched dataset configs come from the
# private corpus repo's GitHub Release. By default redeploy installs the LATEST
# release; set CORPUS_VERSION=vX.Y.Z (env, or the default below) to pin an exact
# version instead. Either way the host re-runs redeploy to move corpus.
# Downloading from a private repo needs gh authenticated as root — `gh auth
# login` once, or export GH_TOKEN. First-time host setup (LXC, UID/GID map, NFS
# mount, gh auth, uv) lives in homelab_docs: docs/howto/deploy-dagster-lxc.md
#
# Container sizing. Two independent axes:
#   - RAM tracks the heavy pools, not max_concurrent_runs. A Gold build streams
#     its rolling window (corpus >= v0.1.6) and peaks ~3-4 GiB; worst case is the
#     `heavy` pool (2 Gold) plus the `market_orders` pool (1 Silver) ≈ 3 x ~4 GiB
#     ≈ 12 GiB. Size the LXC >= 12 GiB (observed peak is far lower, ~4 GiB). This
#     is unchanged by the Silver/Gold pool split — same number of heavy slots.
#   - CORES gate the market-orders backfill: its Silver parses with rayon and is
#     CPU-bound (observed loadavg ~= cores, I/O-wait ~0), so the backfill scales
#     ~linearly with cores. 8 cores roughly halves market-orders backfill time vs
#     4, well within RAM headroom.
# Set both on the Proxmox host (not in this container), e.g.:
#   pct set 211 --cores 8 --memory 12288 --swap 2048
# Raising RAM does NOT speed up the CPU-bound backfill — add cores for that.
# Raising the heavy pool without matching RAM thrashes swap and risks an OOM-killed daemon.
set -euo pipefail

SERVICE_USER="${SERVICE_USER:-corpus}"
REPO_DIR="${REPO_DIR:-/opt/eve-industry-orchestration}"
DAGSTER_HOME="${DAGSTER_HOME:-/var/lib/dagster}"
SERVICES=(dagster-daemon dagster-webserver)

# Corpus version. Empty = install the latest release (resolved via gh below); set
# CORPUS_VERSION=vX.Y.Z to pin an exact version. The asset names mirror the corpus
# release workflow; the install paths reuse the same env vars the systemd units
# pass to Dagster, so the running binary and the deployed one can never drift.
CORPUS_VERSION="${CORPUS_VERSION:-}"
CORPUS_REPO="${CORPUS_REPO:-jasperholtjer/eve-industry-corpus}"
CORPUS_TARGET="${CORPUS_TARGET:-x86_64-unknown-linux-musl}"
CORPUS_BIN="${CORPUS_BINARY_PATH:-/usr/local/bin/corpus}"
DATASETS_DIR="${CORPUS_DATASETS_DIR:-/usr/local/share/corpus/datasets}"

# Context-dataset secrets (corpus ADR-0047). The systemd units load these from an
# optional root-only EnvironmentFile (see dagster-{daemon,webserver}.service). The
# path must match the unit's `EnvironmentFile=` line.
SECRETS_ENV="${SECRETS_ENV:-/etc/eve-industry-orchestration/secrets.env}"
# `news` needs no secret (its backfill discovers the Contentful token from the
# public site bundle, ADR-0049); only the transcript paths need keys.
CONTEXT_SECRET_KEYS=(SUPADATA_API_KEY YOUTUBE_API_KEY)

# uv installs user-local to ~/.local/bin; a non-interactive `su -` may not pick
# it up from the profile, so put it on PATH explicitly.
run_as_user() {
  su - "${SERVICE_USER}" -c "export PATH=\"\$HOME/.local/bin:\$PATH\"; $1"
}

# Resolve the version to install. An explicit CORPUS_VERSION is a pin; an empty
# one means "latest", looked up from the corpus repo's releases via gh. Resolved
# once, up front, so the skip check, asset names, and `--version` assert all see a
# concrete tag. A pinned run never needs the network here; only "latest" does.
resolve_corpus_version() {
  if [[ -n "${CORPUS_VERSION}" ]]; then
    return 0
  fi
  if ! command -v gh >/dev/null 2>&1; then
    echo "error: gh CLI is required to resolve the latest ${CORPUS_REPO} release" >&2
    echo "       run 'gh auth login' as root once, or export GH_TOKEN" >&2
    exit 1
  fi
  CORPUS_VERSION="$(gh release view --repo "${CORPUS_REPO}" --json tagName --jq '.tagName')"
  if [[ -z "${CORPUS_VERSION}" ]]; then
    echo "error: could not resolve the latest ${CORPUS_REPO} release tag" >&2
    exit 1
  fi
  echo "    no pin set; resolved latest release ${CORPUS_VERSION}"
}

# Pull the corpus binary + datasets from the private release and install them
# into root-owned /usr/local. Idempotent: skips the download when the
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

# Validate the repo's dagster.yaml against the installed Dagster BEFORE it is
# published or the services restart, so an invalid instance config fails the
# deploy with the running Dagster untouched instead of crash-looping it. This
# loads the instance the same way the daemon/webserver do, catching cross-field
# rules the YAML schema alone misses (e.g. `max_concurrent_runs` in
# `run_coordinator` vs the `concurrency` block). Runs in a throwaway DAGSTER_HOME.
validate_instance_config() {
  local tmp rc
  echo "==> Validating instance config"
  tmp="$(mktemp -d)"
  cp "${REPO_DIR}/deploy/dagster.yaml" "${tmp}/dagster.yaml"
  set +e
  DAGSTER_HOME="${tmp}" "${REPO_DIR}/.venv/bin/python" - <<'PY'
import os, sys
from dagster import DagsterInstance
try:
    DagsterInstance.from_config(os.environ["DAGSTER_HOME"]).dispose()
except Exception as exc:  # surface any config error verbatim
    print(f"    {type(exc).__name__}: {str(exc).splitlines()[0]}", file=sys.stderr)
    sys.exit(1)
PY
  rc=$?
  set -e
  rm -rf "${tmp}"
  if [[ "${rc}" -ne 0 ]]; then
    echo "error: deploy/dagster.yaml is invalid; live config and services left untouched" >&2
    exit 1
  fi
  echo "    dagster.yaml OK"
}

# Advisory check of the context-dataset secrets file (corpus ADR-0047). NEVER
# aborts the deploy: the `news` dataset needs no secret, so a box that only runs it
# is fine without the file. But `transcripts` fetch/backfill fail at runtime
# without their key, so this surfaces a missing file or key at deploy time instead
# of inside a run. Greps for a defined, non-empty
# `KEY=value` line (the EnvironmentFile format; tolerates leading whitespace and a
# stray `export`), never sourcing the file — its values are opaque secrets.
check_context_secrets() {
  echo "==> Checking context-dataset secrets (${SECRETS_ENV})"
  if [[ ! -f "${SECRETS_ENV}" ]]; then
    echo "    note: ${SECRETS_ENV} absent — the 'news' daily fetch still works, but" >&2
    echo "          'transcripts' fetch/backfill and the 'news' backfill fail until it" >&2
    echo "          defines: ${CONTEXT_SECRET_KEYS[*]}" >&2
    return 0
  fi
  local missing=() key
  for key in "${CONTEXT_SECRET_KEYS[@]}"; do
    if ! grep -Eq "^[[:space:]]*(export[[:space:]]+)?${key}=.+" "${SECRETS_ENV}"; then
      missing+=("${key}")
    fi
  done
  if [[ "${#missing[@]}" -gt 0 ]]; then
    echo "    warning: ${SECRETS_ENV} is missing keys: ${missing[*]}" >&2
    echo "             the datasets needing them will fail at runtime" >&2
    return 0
  fi
  echo "    all context-dataset secrets present"
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

  resolve_corpus_version
  echo "==> Ensuring corpus ${CORPUS_VERSION} is installed"
  pull_corpus

  # Validate before touching the live config: a bad dagster.yaml aborts here,
  # leaving the running Dagster on its current (working) config.
  validate_instance_config

  echo "==> Publishing instance config to ${DAGSTER_HOME}"
  install -d -o "${SERVICE_USER}" -g 988 "${DAGSTER_HOME}"
  install -o "${SERVICE_USER}" -g 988 -m 0644 \
    "${REPO_DIR}/deploy/dagster.yaml" "${DAGSTER_HOME}/dagster.yaml"

  # Per-pool limits below `default_limit` cannot live in dagster.yaml (it only
  # carries `default_limit`); they persist in the instance DB. Set the
  # `market_orders` CPU pool to 1 here so the override is reproducible at deploy
  # time rather than a one-off manual CLI call. Runs as `corpus` so it writes the
  # corpus-owned instance DB under DAGSTER_HOME. Idempotent: re-setting the same
  # limit is a no-op. See deploy/dagster.yaml for why this pool is limit 1.
  echo "==> Setting per-pool concurrency limits"
  run_as_user "cd '${REPO_DIR}' && DAGSTER_HOME='${DAGSTER_HOME}' \
    uv run dagster instance concurrency set market_orders 1"

  # Install the systemd units from the repo (root-owned /etc/systemd/system) so
  # unit changes — env like RAYON_NUM_THREADS, ExecStart — ship with a redeploy
  # instead of being a manual one-off. daemon-reload picks up edits before the
  # restart below. install is a no-op when the unit is byte-identical.
  echo "==> Installing systemd units"
  install -m 0644 "${REPO_DIR}/deploy/dagster-daemon.service" \
    /etc/systemd/system/dagster-daemon.service
  install -m 0644 "${REPO_DIR}/deploy/dagster-webserver.service" \
    /etc/systemd/system/dagster-webserver.service
  systemctl daemon-reload

  # Advisory: report on the secrets file the units just referenced, before the
  # restart makes it live. Never blocks the deploy.
  check_context_secrets

  echo "==> Restarting services"
  # Clear any prior failed state so an earlier crash-loop's start-limit does not
  # block this (good-config) restart.
  systemctl reset-failed "${SERVICES[@]}" 2>/dev/null || true
  systemctl restart "${SERVICES[@]}"

  echo "==> Status"
  systemctl is-active "${SERVICES[@]}"
  echo "corpus: $("${CORPUS_BIN}" --version 2>/dev/null || echo 'not found')"
  echo "Done. UI: http://192.168.2.211:3000"
}

main "$@"
