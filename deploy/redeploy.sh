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
#   - RAM tracks the sum of the memory-bearing pools, not max_concurrent_runs.
#     THE MEMORY BUDGET block in deploy/dagster.yaml is the one copy of that
#     arithmetic — which pools carry memory, each holder's peak, and the worst
#     case against the box. Read it there before sizing the LXC.
#   - CORES gate the market-orders backfill: its Silver parses with rayon and is
#     CPU-bound (observed loadavg ~= cores, I/O-wait ~0), so the backfill scales
#     ~linearly with cores. 8 cores roughly halves market-orders backfill time vs
#     4, well within RAM headroom.
# Set both on the Proxmox host (not in this container), e.g.:
#   pct set 211 --cores 8 --memory 12288 --swap 2048
# Raising RAM does NOT speed up the CPU-bound backfill — add cores for that.
# Raising a memory-bearing pool's limit without matching RAM thrashes swap and
# risks an OOM-killed daemon. `max_concurrent_runs` is the NAS spindle's I/O cap,
# not a memory backstop, so raising IT is not the same lever.
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
# gnu, not musl: only the default-features build carries `embed-engine`, and it
# links a prebuilt onnxruntime that exists for glibc only. A binary built on the
# release runner's glibc 2.39 runs on this LXC's Debian 13.
CORPUS_TARGET="${CORPUS_TARGET:-x86_64-unknown-linux-gnu}"
CORPUS_BIN="${CORPUS_BINARY_PATH:-/usr/local/bin/corpus}"
DATASETS_DIR="${CORPUS_DATASETS_DIR:-/usr/local/share/corpus/datasets}"

# ONNX model dir for `corpus enrich embed` (corpus ADR-0053). Same shape as the
# corpus release below: absent — fetch it, present — skip. The ~540 MB lands once
# and every later deploy costs two stat calls. The path must match the systemd
# units' CORPUS_EMBEDDING_MODEL_DIR. The revision IS the pin: a different export
# is a different embedding generation, and vectors from two generations do not
# compare.
MODEL_DIR="${CORPUS_EMBEDDING_MODEL_DIR:-/usr/local/share/corpus/models/bge-m3}"
MODEL_REPO="${CORPUS_EMBEDDING_MODEL_REPO:-onnx-community/bge-m3-ONNX}"
MODEL_REVISION="${CORPUS_EMBEDDING_MODEL_REVISION:-25b9af8e87a38eb120cfe87125383677b9cd309e}"
MODEL_FILES=(onnx/model_quantized.onnx tokenizer.json)

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

  # --ignore-missing: SHA256SUMS covers every asset of the release and only two of
  # them are downloaded here, so a plain --check fails on the ones not fetched.
  echo "    verifying checksums"
  (cd "${tmp}" && sha256sum --ignore-missing --check SHA256SUMS)

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

# Ensure the embedding model dir (corpus ADR-0053), fetching only what is missing.
# Straight from the pinned revision, so the box needs no HuggingFace CLI and no
# auth — the repo is public. The fetch runs on the repo venv's Python, the one
# `validate_instance_config` already depends on: the LXC ships with neither curl
# nor wget, and a deploy script is the wrong place to apt-install one. Downloads
# into a temp dir and moves each file into place only after the whole set arrived,
# because the presence check IS the idempotence key: a truncated file left behind
# would be "present" forever after.
#
# Advisory, and that is why it is the LAST thing before the restart: it NEVER
# aborts the deploy. Only `corpus enrich embed` reads this dir, so a box that
# cannot reach HuggingFace still deploys and runs every other dataset — it loses
# the news/transcripts embeddings until the next redeploy retries.
provision_embedding_model() {
  echo "==> Checking embedding model (${MODEL_DIR})"
  local missing=() f tmp rc=0
  for f in "${MODEL_FILES[@]}"; do
    [[ -f "${MODEL_DIR}/${f}" ]] || missing+=("${f}")
  done
  if [[ "${#missing[@]}" -eq 0 ]]; then
    echo "    present"
    return 0
  fi

  echo "    fetching ${missing[*]} from ${MODEL_REPO}@${MODEL_REVISION:0:7} (once; the full set is ~540 MB)"
  tmp="$(mktemp -d)"
  set +e
  "${REPO_DIR}/.venv/bin/python" - \
    "${tmp}" "${MODEL_REPO}" "${MODEL_REVISION}" "${missing[@]}" <<'PY'
import pathlib, sys, urllib.request

tmp, repo, revision, *names = sys.argv[1:]
for name in names:
    dest = pathlib.Path(tmp, name)
    dest.parent.mkdir(parents=True, exist_ok=True)
    url = f"https://huggingface.co/{repo}/resolve/{revision}/{name}"
    try:
        with urllib.request.urlopen(url) as src, dest.open("wb") as out:
            total = int(src.headers.get("Content-Length", 0))
            done = 0
            while chunk := src.read(1 << 20):
                out.write(chunk)
                done += len(chunk)
                pct = f"{done * 100 // total}%" if total else f"{done >> 20} MiB"
                print(f"\r    {name}: {pct}", end="", flush=True)
        # A short body is a truncated transfer, not a valid file: fail the set.
        if total and done != total:
            raise OSError(f"got {done} of {total} bytes")
    except Exception as exc:
        print(f"\r    {name}: {type(exc).__name__}: {exc}", file=sys.stderr)
        sys.exit(1)
    print(f"\r    {name}: done ({done >> 20} MiB)")
PY
  rc=$?
  set -e
  if [[ "${rc}" -ne 0 ]]; then
    rm -rf "${tmp}"
    echo "    note: download failed — news/transcripts embeddings fail until a" >&2
    echo "          later redeploy gets it; no other dataset is affected" >&2
    return 0
  fi

  for f in "${missing[@]}"; do
    install -D -m 0644 "${tmp}/${f}" "${MODEL_DIR}/${f}"
  done
  rm -rf "${tmp}"
  echo "    installed -> ${MODEL_DIR}"
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

  # Three pools are declared; `heavy` and `market_orders` take `default_limit: 1`
  # from dagster.yaml, and `everef_download` is the only one above it. A per-pool
  # limit cannot live in dagster.yaml at all (it only carries `default_limit`); it
  # persists in the instance DB, so it is set here to keep the override
  # reproducible at deploy time rather than a one-off manual CLI call. The default
  # is deliberately the MEMORY-SAFE limit, and the single override is the one pool
  # whose limit costs no memory: if this call is ever lost, the box falls back to
  # slower EVE Ref downloads, never to two concurrent 4.4 GiB embeds. Runs as
  # `corpus` so it writes the corpus-owned instance DB under DAGSTER_HOME.
  # Idempotent: re-setting the same limit is a no-op. A pool that gains or loses a
  # non-default limit needs a matching change here; the memory budget and the
  # reason each pool has the limit it has are in deploy/dagster.yaml, which owns
  # that arithmetic.
  echo "==> Setting per-pool concurrency limits"
  run_as_user "cd '${REPO_DIR}' && DAGSTER_HOME='${DAGSTER_HOME}' \
    uv run dagster instance concurrency set everef_download 2"

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
  provision_embedding_model

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
